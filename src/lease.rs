//! Lease lifecycle for `dev.mcpg.cluster.consul`.
//!
//! Consul has Sessions + KV CAS for distributed locks. The plugin
//! binds those primitives to the `ActiveLease` trait surface:
//!
//! - `acquire_*(key, ttl)` runs `session_create(ttl)` then
//!   `kv_acquire(<key>, session_id)`. If the KV slot is held by
//!   another session the call destroys the new session + returns
//!   `BackendUnavailable`. After acquire we read the slot back to
//!   harvest the `LockIndex` — Consul's per-key monotonic counter,
//!   used as the fencing token.
//! - **Renewal** is `session_renew(session_id)`. Consul returns
//!   404 / empty array when the session has already expired —
//!   both surfaces translate to `LeaseExpired`.
//! - **Release** is `kv_release(key, session_id) +
//!   session_destroy(session_id)`. Idempotent via an
//!   `AtomicBool`; the background renewal task aborts on drop.
//!
//! State lifecycle mirrors the etcd plugin: `Arc<LeaseState>`
//! shared between async-trait holders and the FFI leaked pointer;
//! sync renew/release borrow via `Arc::increment_strong_count`,
//! the final `lease_drop` reclaims via `Arc::from_raw`.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::SecondsFormat;
use mcpg_cluster_api::{ActiveLease, ClusterError};
use mcpg_plugin_protocol::async_trait;
use mcpg_plugin_sdk::ffi::WatchHandleBox;
use tokio::runtime::Handle as RuntimeHandle;
use tokio::task::AbortHandle;
use tokio::time::sleep;

use crate::client::{ConsulClient, SessionBehavior};

pub(crate) struct LeaseState {
    pub(crate) client: ConsulClient,
    pub(crate) session_id: String,
    pub(crate) lock_key: String,
    pub(crate) lock_index: u64,
    /// The (clamped) session TTL. Drives the local `expires_at` estimate
    /// on renewal — Consul's renew response carries no fresh expiry.
    pub(crate) ttl: Duration,
    pub(crate) expires_at: StdMutex<String>,
    pub(crate) released: AtomicBool,
    pub(crate) renewal_abort: StdMutex<Option<AbortHandle>>,
}

impl Drop for LeaseState {
    fn drop(&mut self) {
        if let Some(h) = self.renewal_abort.lock().unwrap().take() {
            h.abort();
        }
    }
}

pub(crate) struct ConsulLeaseHandle(pub(crate) Arc<LeaseState>);

#[async_trait]
impl ActiveLease for ConsulLeaseHandle {
    fn fencing_token(&self) -> u64 {
        self.0.lock_index
    }

    fn expires_at(&self) -> String {
        self.0.expires_at.lock().unwrap().clone()
    }

    async fn renew(&self) -> Result<(), ClusterError> {
        renew_state(&self.0).await
    }

    async fn release(&self) -> Result<(), ClusterError> {
        release_state(&self.0).await
    }
}

// ---------------------------------------------------------------------------
// Acquire
// ---------------------------------------------------------------------------

/// Single attempt at acquiring the lease. Returns:
///   `Ok(Some(state))` — acquired, renewal task spawned.
///   `Ok(None)`        — backend reported the lock is held by
///                       another session (Consul `kv_acquire`
///                       returned `false`). Caller chooses how
///                       to handle (retry / decline).
///   `Err(...)`        — backend unreachable or refused the call.
pub(crate) async fn try_acquire_async(
    client: ConsulClient,
    name: String,
    key: String,
    node_id: String,
    ttl: Duration,
    renew_before_expiry_percent: u32,
) -> Result<Option<Arc<LeaseState>>, ClusterError> {
    if name.trim().is_empty() {
        return Err(ClusterError::InvalidReference {
            message: "lease name must not be empty".into(),
        });
    }
    // Clamp to Consul's 10s session minimum and carry the TTL as a real
    // `Duration` from here on. This value is SECONDS, not milliseconds.
    let ttl = Duration::from_secs(ttl.as_secs().max(10));
    let session_id = client
        .session_create(&name, ttl.as_secs(), SessionBehavior::Release)
        .await?;
    let acquired = client
        .kv_acquire(&key, &session_id, node_id.as_bytes())
        .await?;
    if !acquired {
        let _ = client.session_destroy(&session_id).await;
        return Ok(None);
    }
    // The LockIndex IS the fencing token and must be strictly monotonic
    // and non-zero. A Consul-acquired key always has LockIndex >= 1; a
    // read failure / missing key / 0 here means we can't mint a valid
    // token. Returning 0 silently defeats split-brain protection
    // (0 <= every real token), so fail the acquire instead: release the
    // session (Release behavior frees the KV claim) and surface
    // BackendUnavailable.
    let lock_index = match client.kv_read(&key).await {
        Ok(Some(entry)) if entry.lock_index > 0 => entry.lock_index,
        _ => {
            let _ = client.session_destroy(&session_id).await;
            return Err(ClusterError::BackendUnavailable {
                reason: format!(
                    "consul lease '{name}': acquired but could not read back a valid \
                     LockIndex for the fencing token; released the session to avoid \
                     handing out an unsafe token-0 lease"
                ),
            });
        }
    };

    let expires_at = StdMutex::new(rfc3339_after(ttl));
    let state = Arc::new(LeaseState {
        client: client.clone(),
        session_id: session_id.clone(),
        lock_key: key,
        lock_index,
        ttl,
        expires_at,
        released: AtomicBool::new(false),
        renewal_abort: StdMutex::new(None),
    });

    // Background renewal task. Sleeps a fraction of the TTL then fires
    // session_renew. Stops on AbortHandle drop or LeaseExpired.
    let sleep_for = renewal_sleep(ttl, renew_before_expiry_percent);
    let renewal_state = Arc::clone(&state);
    let join = RuntimeHandle::current().spawn(async move {
        loop {
            sleep(sleep_for).await;
            if renewal_state.released.load(Ordering::SeqCst) {
                break;
            }
            if renew_state(&renewal_state).await.is_err() {
                // LeaseExpired or backend unreachable. Stop
                // renewing — caller will see LeaseExpired on
                // their next renew/release attempt too.
                break;
            }
        }
    });
    *state.renewal_abort.lock().unwrap() = Some(join.abort_handle());
    Ok(Some(state))
}

/// Blocking acquire — polls [`try_acquire_async`] with a small
/// jittered backoff until the backend hands us the lease. The
/// outer caller (`ClusterBackend::acquire_*`) gets the spec-
/// compliant "wait until the holder releases" semantic. Use
/// [`try_acquire_async`] from a tight loop where blocking would
/// defeat the purpose.
///
/// Backoff: 200 ms → 400 ms → 800 ms (clamped). Tail is
/// long-poll-friendly without hammering Consul.
pub(crate) async fn acquire_async(
    client: ConsulClient,
    name: String,
    key: String,
    node_id: String,
    ttl: Duration,
    renew_before_expiry_percent: u32,
) -> Result<Arc<LeaseState>, ClusterError> {
    let mut delay = Duration::from_millis(200);
    let cap = Duration::from_millis(800);
    loop {
        match try_acquire_async(
            client.clone(),
            name.clone(),
            key.clone(),
            node_id.clone(),
            ttl,
            renew_before_expiry_percent,
        )
        .await?
        {
            Some(state) => return Ok(state),
            None => {
                sleep(delay).await;
                delay = std::cmp::min(delay * 2, cap);
            }
        }
    }
}

pub(crate) fn acquire_sync(
    runtime: &RuntimeHandle,
    client: ConsulClient,
    name: String,
    key: String,
    node_id: String,
    ttl_ms: u64,
    renew_before_expiry_percent: u32,
) -> Result<(WatchHandleBox, u64, String), ClusterError> {
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let state = runtime.block_on(async move {
        acquire_async(client, name, key, node_id, ttl, renew_before_expiry_percent).await
    })?;
    wrap_state(state)
}

/// Non-blocking acquire. `Ok(None)` when the backend reports the lock
/// is held by another session.
pub(crate) fn try_acquire_sync(
    runtime: &RuntimeHandle,
    client: ConsulClient,
    name: String,
    key: String,
    node_id: String,
    ttl_ms: u64,
    renew_before_expiry_percent: u32,
) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
    let ttl = Duration::from_millis(ttl_ms.max(1));
    let state_opt = runtime.block_on(async move {
        try_acquire_async(client, name, key, node_id, ttl, renew_before_expiry_percent).await
    })?;
    match state_opt {
        Some(state) => wrap_state(state).map(Some),
        None => Ok(None),
    }
}

fn wrap_state(state: Arc<LeaseState>) -> Result<(WatchHandleBox, u64, String), ClusterError> {
    let token = state.lock_index;
    let expires = state.expires_at.lock().unwrap().clone();
    let raw = Arc::into_raw(state);
    Ok((WatchHandleBox(raw as *mut ()), token, expires))
}

// ---------------------------------------------------------------------------
// Renew + release
// ---------------------------------------------------------------------------

pub(crate) async fn renew_state(state: &LeaseState) -> Result<(), ClusterError> {
    if state.released.load(Ordering::SeqCst) {
        return Err(ClusterError::LeaseExpired);
    }
    state.client.session_renew(&state.session_id).await?;
    // We don't get a fresh expires_at from the renew call — Consul's
    // response shape doesn't include it. Bump locally by the lease's
    // actual TTL (best estimate; Consul's real expiry is server-side).
    *state.expires_at.lock().unwrap() = rfc3339_after(state.ttl);
    Ok(())
}

pub(crate) async fn release_state(state: &LeaseState) -> Result<(), ClusterError> {
    if state.released.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    if let Some(h) = state.renewal_abort.lock().unwrap().take() {
        h.abort();
    }
    let _ = state
        .client
        .kv_release(&state.lock_key, &state.session_id)
        .await;
    let _ = state.client.session_destroy(&state.session_id).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync FFI helpers
// ---------------------------------------------------------------------------

/// SAFETY: caller MUST pass a `WatchHandleBox` produced by
/// `acquire_sync`. Pointer is valid for the duration of the
/// borrow per the host vtable contract.
pub(crate) unsafe fn borrow_state(handle: &WatchHandleBox) -> Option<Arc<LeaseState>> {
    let ptr = handle.0 as *const LeaseState;
    if ptr.is_null() {
        return None;
    }
    unsafe {
        Arc::increment_strong_count(ptr);
        Some(Arc::from_raw(ptr))
    }
}

/// SAFETY: exactly one `lease_drop` per `acquire_sync`.
pub(crate) unsafe fn drop_state(handle: WatchHandleBox) {
    let ptr = handle.0 as *const LeaseState;
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Arc::from_raw(ptr);
    }
}

pub(crate) fn renew_sync(
    runtime: &RuntimeHandle,
    handle: WatchHandleBox,
) -> Result<String, ClusterError> {
    let state = unsafe { borrow_state(&handle) }.ok_or(ClusterError::LeaseExpired)?;
    runtime.block_on(async move {
        renew_state(&state).await?;
        Ok(state.expires_at.lock().unwrap().clone())
    })
}

pub(crate) fn release_sync(
    runtime: &RuntimeHandle,
    handle: WatchHandleBox,
) -> Result<(), ClusterError> {
    let state = unsafe { borrow_state(&handle) };
    let state = match state {
        Some(s) => s,
        None => return Ok(()),
    };
    runtime.block_on(async move { release_state(&state).await })
}

// ---------------------------------------------------------------------------

fn rfc3339_after(ttl: Duration) -> String {
    let dt = chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default();
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Renewal-task sleep before each `session_renew`: a `(100 - pct)%`
/// fraction of the lease TTL (so we renew with `pct%` of the TTL still
/// to spare), floored at 1s. `pct` is `renew_before_expiry_percent`,
/// clamped to 1..=99.
fn renewal_sleep(ttl: Duration, renew_before_expiry_percent: u32) -> Duration {
    let pct = renew_before_expiry_percent.clamp(1, 99);
    let sleep_for = ttl.saturating_mul(100u32.saturating_sub(pct)) / 100;
    if sleep_for.is_zero() {
        Duration::from_secs(1)
    } else {
        sleep_for
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_sleep_is_seconds_scale_not_millis() {
        // A 30s TTL with the default-ish 80% policy must yield a
        // SECONDS-scale sleep (30s × 20% = 6s).
        assert_eq!(
            renewal_sleep(Duration::from_secs(30), 80),
            Duration::from_secs(6)
        );
        assert_eq!(
            renewal_sleep(Duration::from_secs(60), 80),
            Duration::from_secs(12)
        );
        // Whatever the percentage, a 30s TTL never renews in the ms range.
        assert!(renewal_sleep(Duration::from_secs(30), 99) >= Duration::from_millis(200));
    }

    #[test]
    fn renewal_sleep_floors_at_one_second() {
        // Defensive: a degenerate TTL whose fraction truncates to 0ns
        // still renews at 1s rather than spin-looping.
        assert_eq!(
            renewal_sleep(Duration::from_nanos(50), 99),
            Duration::from_secs(1)
        );
    }
}
