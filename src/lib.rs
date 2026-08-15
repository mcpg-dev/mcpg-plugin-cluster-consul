//! `dev.mcpg.cluster.consul` — Consul `cluster` plugin.
//!
//! This crate is the implementation; operator-
//! facing summary lives in `README.md`.
//!
//! # v0.1 scope (current)
//!
//! - `node_info()` — reports this gateway instance's identity from
//!   operator config + agent metadata.
//! - `list_peers()` — `GET /v1/catalog/service/<name>` returns
//!   every healthy gateway instance registered with the Consul
//!   catalog.
//! - `publish(topic, routing_key, payload)` —
//!   `PUT /v1/event/fire/<topic>`. Consul Events have no metadata
//!   channel for routing keys, so we wrap the payload in a small
//!   versioned envelope (see `envelope.rs`). Best-effort delivery
//!   via Consul's gossip layer.
//! - `subscribe(topic, _, routing_key)` — long-poll
//!   `GET /v1/event/list` with `?index=` blocking semantics.
//!   Decodes the envelope, drops malformed events, and applies an
//!   exact-match routing-key filter when the subscriber supplies
//!   one. Returns a stream of `PublishedMessage`s.
//! - **Lease ops** (`acquire_leadership`, `acquire_lock`, lease
//!   renew/release/drop) via Consul Sessions API + KV CAS. See
//!   `lease.rs` for the per-lease state machine. Background
//!   renewal task fires every `ttl × (1 - renew_pct/100)`.
//!   Fencing token is the KV `LockIndex` (Consul's per-key
//!   monotonic counter).
//! - **`watch_peers()`** — long-poll on the catalog index. Diffs
//!   each successive snapshot against the previous one and emits
//!   `Joined`/`Left` events. Stream lives until cancelled.
//!
//! # Deferred
//!
//! - **Auto-registration** of the gateway instance with the
//!   Consul agent (`PUT /v1/agent/service/register`). Operators
//!   handle registration via Consul agent config / sidecar.
//! - **Cross-DC coordination** (Consul WAN gossip primitives).
//! - **Consul Connect mTLS** to the agent.

mod client;
mod config;
mod envelope;
mod kv;
mod lease;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use mcpg_cluster_api::{
    BoxActiveLease, BoxPeerEventStream, BoxPublishedMessageStream, ClusterBackend, ClusterError,
    ClusterNodeInfo, ClusterPeer, KeyValueStore, PeerHealth, PublishedMessage,
};
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::{SyncClusterBackend, WatchHandleBox};
use tokio::runtime::Runtime;

pub use config::{ConfigError, ConsulConfig};
pub use kv::ConsulKv;

const PLUGIN_ID: &str = "dev.mcpg.cluster.consul";

pub struct ConsulBackend {
    inner: Arc<Inner>,
}

struct Inner {
    manifest: PluginManifest,
    config: ConsulConfig,
    node_id: String,
    started_at: String,
    client: client::ConsulClient,
    /// Consul-backed KV primitive. Built eagerly: `ConsulClient::new`
    /// performs no network I/O (it only constructs a reqwest client), so
    /// there is no connection to defer — the boot reachability probe
    /// (CC-2) is what validates the broker is live. The accessor returns
    /// `Some` once the plugin is constructed.
    kv: Arc<ConsulKv>,
    runtime: Runtime,
}

impl ConsulBackend {
    pub fn from_config_json(config_json: &str) -> Self {
        // Load-time manifest derivation builds + drops an instance only to
        // read its plugin-wide manifest. It has no real connection config, so
        // the host passes the manifest-probe sentinel (`{}`). Substitute a
        // placeholder config (no eager network I/O — the reqwest client is
        // built but never connects) so construction succeeds for that probe;
        // a REAL config still flows through parse + validate below, so a
        // genuinely misconfigured coordinator still refuses to load.
        if mcpg_plugin_protocol::is_manifest_probe_config(config_json) {
            let cfg = ConsulConfig::parse(
                "{\"address\":\"http://127.0.0.1:8500\",\"service_name\":\"manifest-probe\"}",
            )
            .expect("manifest-probe placeholder consul config is valid");
            return Self::from_validated_config(cfg);
        }
        let cfg = ConsulConfig::parse(config_json).unwrap_or_else(|err| {
            tracing::error!(
                plugin_id = PLUGIN_ID,
                error = %err,
                "consul cluster: config parse failed; refusing to register"
            );
            panic!(
                "consul cluster config parse failed: {err}. A misconfigured \
                 cluster_backend is a security hole; refusing to load."
            )
        });
        Self::from_validated_config(cfg)
    }

    fn from_validated_config(cfg: ConsulConfig) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("consul cluster: failed to build tokio runtime");
        let client = client::ConsulClient::new(
            cfg.address.clone(),
            cfg.token.clone(),
            cfg.datacenter.clone(),
        )
        .unwrap_or_else(|err| panic!("consul cluster: failed to build HTTP client: {err}"));
        let node_id = cfg.resolved_node_id();
        let started_at = now_rfc3339();
        let kv = Arc::new(ConsulKv::new(client.clone(), cfg.kv_prefix.clone()));
        tracing::info!(
            plugin_id = PLUGIN_ID,
            address = %cfg.address,
            service_name = %cfg.service_name,
            node_id = %node_id,
            "consul cluster: configured"
        );
        Self {
            inner: Arc::new(Inner {
                manifest: PluginManifest {
                    id: PLUGIN_ID.into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                    name: "Consul Cluster Coordinator".into(),
                    plugin_class: PluginClass::Cluster,
                    protocol_version: "1.0".into(),
                    license: None,
                    required_capabilities: Vec::new(),
                    tags: Vec::new(),
                    // Slot roles (cache/kv/bus), not primitive accessors.
                    // Consul backs the `bus` slot via its Event API
                    // (coordinator-level publish/subscribe) AND the `kv` slot
                    // via the Consul KV HTTP API (ModifyIndex CAS + a LOGICAL,
                    // emulated TTL — Consul KV has no native per-key TTL; see
                    // `kv.rs`). It has no native cache-eviction role.
                    provides: vec!["bus".into(), "kv".into()],
                    provides_schemes: Vec::new(),
                    module_path_prefix: ::std::module_path!()
                        .split("::")
                        .next()
                        .unwrap_or("")
                        .to_owned(),
                    backend_profile: None,
                },
                config: cfg,
                node_id,
                started_at,
                client,
                kv,
                runtime,
            }),
        }
    }
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Whole-millisecond TTL → `Duration` (None == no TTL).
fn ttl_from_ms(ttl_ms: Option<u64>) -> Option<Duration> {
    ttl_ms.map(Duration::from_millis)
}

/// Apply ±20% randomized jitter to a fixed retry sleep so N
/// replicas that lost Consul at the same instant don't long-poll-retry
/// in lock-step. No `rand` dep — sub-second nanos as decorrelation
/// entropy (jitter needs decorrelation, not cryptographic randomness).
fn jittered(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    let span = base_ms * 2 / 5; // 40% window → ±20%
    if span == 0 {
        return base;
    }
    let entropy = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    Duration::from_millis(base_ms - base_ms / 5 + entropy % (span + 1))
}

fn decode_event_payload(b64: Option<&str>) -> Bytes {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as B64;
    match b64 {
        Some(s) => B64
            .decode(s.as_bytes())
            .map(Bytes::from)
            .unwrap_or_default(),
        None => Bytes::new(),
    }
}

// ---------------------------------------------------------------------------
// Async ClusterBackend impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ClusterBackend for ConsulBackend {
    // `cluster_provides()` uses the default impl: it derives the role
    // set from `manifest().provides` (= bus, kv).

    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn key_value_store(&self) -> Option<Arc<dyn KeyValueStore>> {
        Some(Arc::clone(&self.inner.kv) as Arc<dyn KeyValueStore>)
    }

    async fn node_info(&self) -> ClusterNodeInfo {
        ClusterNodeInfo {
            node_id: self.inner.node_id.clone(),
            address: self.inner.config.address.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            started_at: self.inner.started_at.clone(),
            roles: vec![],
        }
    }

    async fn list_peers(&self) -> Vec<ClusterPeer> {
        match self
            .inner
            .client
            .catalog_service(&self.inner.config.service_name)
            .await
        {
            Ok(entries) => entries
                .into_iter()
                .map(|e| ClusterPeer {
                    node_id: e.service_id,
                    address: format!("{}:{}", e.address, e.service_port),
                    last_seen: now_rfc3339(),
                    health: PeerHealth::Healthy,
                    roles: vec![],
                })
                .collect(),
            Err(err) => {
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    error = ?err,
                    "consul cluster: list_peers failed; returning empty"
                );
                vec![]
            }
        }
    }

    async fn watch_peers(&self) -> BoxPeerEventStream {
        let client = self.inner.client.clone();
        let service_name = self.inner.config.service_name.clone();
        let wait_seconds = self.inner.config.subscribe_wait_sec;
        let (tx, rx) = tokio::sync::mpsc::channel::<mcpg_cluster_api::PeerEvent>(64);
        tokio::spawn(async move {
            let mut index: u64 = 0;
            let mut last_set: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            loop {
                if tx.is_closed() {
                    break;
                }
                match client
                    .catalog_service_blocking(&service_name, index, wait_seconds)
                    .await
                {
                    Ok((entries, new_index)) => {
                        index = new_index;
                        let cur_set: std::collections::BTreeSet<String> = entries
                            .iter()
                            .map(|e| e.service_id.clone())
                            .filter(|s| !s.is_empty())
                            .collect();
                        // Joined: in cur_set, not in last_set
                        for entry in &entries {
                            if entry.service_id.is_empty() || last_set.contains(&entry.service_id) {
                                continue;
                            }
                            let evt = mcpg_cluster_api::PeerEvent::Joined {
                                peer: ClusterPeer {
                                    node_id: entry.service_id.clone(),
                                    address: format!("{}:{}", entry.address, entry.service_port),
                                    last_seen: now_rfc3339(),
                                    health: PeerHealth::Healthy,
                                    roles: vec![],
                                },
                            };
                            if tx.send(evt).await.is_err() {
                                return;
                            }
                        }
                        // Left: in last_set, not in cur_set
                        for gone in last_set.difference(&cur_set) {
                            let evt = mcpg_cluster_api::PeerEvent::Left {
                                node_id: gone.clone(),
                            };
                            if tx.send(evt).await.is_err() {
                                return;
                            }
                        }
                        last_set = cur_set;
                    }
                    Err(err) => {
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            error = ?err,
                            "consul cluster: watch_peers poll failed; backoff"
                        );
                        tokio::time::sleep(jittered(Duration::from_secs(5))).await;
                    }
                }
            }
        });
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    async fn acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.kv_prefix);
        let state = lease::acquire_async(
            self.inner.client.clone(),
            format!("mcpg-leadership-{role}"),
            key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(Box::new(lease::ConsulLeaseHandle(state)))
    }

    async fn acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<BoxActiveLease, ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.kv_prefix);
        let state = lease::acquire_async(
            self.inner.client.clone(),
            format!("mcpg-lock-{key}"),
            full_key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(Box::new(lease::ConsulLeaseHandle(state)))
    }

    async fn try_acquire_leadership(
        &self,
        role: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.kv_prefix);
        let state_opt = lease::try_acquire_async(
            self.inner.client.clone(),
            format!("mcpg-leadership-{role}"),
            key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(state_opt.map(|state| Box::new(lease::ConsulLeaseHandle(state)) as BoxActiveLease))
    }

    async fn try_acquire_lock(
        &self,
        key: &str,
        lease_ttl: Duration,
    ) -> Result<Option<BoxActiveLease>, ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.kv_prefix);
        let state_opt = lease::try_acquire_async(
            self.inner.client.clone(),
            format!("mcpg-lock-{key}"),
            full_key,
            self.inner.node_id.clone(),
            lease_ttl,
            self.inner.config.lease_renew_before_expiry_percent,
        )
        .await?;
        Ok(state_opt.map(|state| Box::new(lease::ConsulLeaseHandle(state)) as BoxActiveLease))
    }

    async fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Bytes,
    ) -> Result<(), ClusterError> {
        // Consul's Event API has no metadata channel for routing
        // keys, so we wrap the caller payload in a tiny envelope
        // (see `envelope.rs`). Subscribers decode + filter.
        let wire = envelope::encode(routing_key, &payload).map_err(|e| ClusterError::Internal {
            reason: format!("publish envelope: {e}"),
        })?;
        self.inner.client.fire_event(topic, wire).await
    }

    async fn subscribe(
        &self,
        topic: &str,
        _group: Option<&str>,
        routing_key: Option<&str>,
    ) -> Result<BoxPublishedMessageStream, ClusterError> {
        let topic = topic.to_owned();
        let filter_rk = routing_key.map(str::to_owned);
        let client = self.inner.client.clone();
        let node_id = self.inner.node_id.clone();
        let wait_seconds = self.inner.config.subscribe_wait_sec;

        // Long-poll loop. Yields PublishedMessage values via a
        // tokio mpsc channel; the receiver is the returned
        // Stream. Caller drops the stream when done; sender side
        // detects close and exits.
        let (tx, rx) = tokio::sync::mpsc::channel::<PublishedMessage>(64);
        tokio::spawn(async move {
            let mut index: u64 = 0;
            let mut seen_ids: std::collections::VecDeque<String> =
                std::collections::VecDeque::with_capacity(256);
            loop {
                if tx.is_closed() {
                    break;
                }
                let result = client.list_events(&topic, index, wait_seconds).await;
                match result {
                    Ok((events, new_index)) => {
                        index = new_index;
                        for ev in events {
                            // Dedupe by event ID — Consul replays
                            // events on each long-poll response.
                            if seen_ids.iter().any(|id| id == &ev.id) {
                                continue;
                            }
                            if seen_ids.len() >= 256 {
                                seen_ids.pop_front();
                            }
                            seen_ids.push_back(ev.id.clone());
                            let raw = decode_event_payload(ev.payload_b64.as_deref());
                            let (msg_rk, payload) = match envelope::decode(&raw) {
                                Ok(pair) => pair,
                                Err(err) => {
                                    // Drop malformed events — most
                                    // likely a non-mcpg publisher
                                    // firing on the same topic.
                                    tracing::warn!(
                                        plugin_id = PLUGIN_ID,
                                        topic = %topic,
                                        event_id = %ev.id,
                                        error = %err,
                                        "consul cluster: dropping event with bad envelope"
                                    );
                                    continue;
                                }
                            };
                            // Subscriber-side routing-key filter:
                            // exact-match against `Some(rk)` if the
                            // subscriber asked for one; pass-through
                            // when subscriber filter is None.
                            if let Some(want) = filter_rk.as_deref()
                                && msg_rk.as_deref() != Some(want)
                            {
                                continue;
                            }
                            let msg = PublishedMessage {
                                topic: topic.clone(),
                                routing_key: msg_rk,
                                payload,
                                from_node: node_id.clone(),
                            };
                            if tx.send(msg).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(err) => {
                        // Backoff briefly then retry. Consul
                        // unreachable shouldn't tear down the
                        // subscriber — operators may temporarily
                        // restart Consul.
                        tracing::warn!(
                            plugin_id = PLUGIN_ID,
                            topic = %topic,
                            error = ?err,
                            "consul cluster: subscribe poll failed; backoff"
                        );
                        tokio::time::sleep(jittered(Duration::from_secs(5))).await;
                    }
                }
            }
        });
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

// ---------------------------------------------------------------------------
// Sync FFI — required by declare_plugin!'s cluster_backend arm
// ---------------------------------------------------------------------------

impl SyncClusterBackend for ConsulBackend {
    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn node_info(&self) -> ClusterNodeInfo {
        self.inner
            .runtime
            .block_on(async { ClusterBackend::node_info(self).await })
    }

    fn list_peers(&self) -> Vec<ClusterPeer> {
        self.inner
            .runtime
            .block_on(async { ClusterBackend::list_peers(self).await })
    }

    fn publish(
        &self,
        topic: &str,
        routing_key: Option<&str>,
        payload: Vec<u8>,
    ) -> Result<(), ClusterError> {
        self.inner.runtime.block_on(async {
            ClusterBackend::publish(self, topic, routing_key, Bytes::from(payload)).await
        })
    }

    // The FFI pub/sub + peer-watch slots reuse the same async impls via
    // the shared `cluster_forward` helper, which spawns a forwarder
    // bridging each stream item to `emit_event` and returns a cancel-safe
    // handle.
    fn subscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        routing_key: Option<&str>,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, ClusterError> {
        let stream = self
            .inner
            .runtime
            .block_on(async { ClusterBackend::subscribe(self, topic, group, routing_key).await })?;
        Ok(
            mcpg_plugin_sdk::ffi::cluster_forward::forward_cluster_stream(
                self.inner.runtime.handle(),
                stream,
                emit_event,
            ),
        )
    }

    fn watch_peers(
        &self,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, ClusterError> {
        let stream = self
            .inner
            .runtime
            .block_on(async { ClusterBackend::watch_peers(self).await });
        Ok(
            mcpg_plugin_sdk::ffi::cluster_forward::forward_cluster_stream(
                self.inner.runtime.handle(),
                stream,
                emit_event,
            ),
        )
    }

    fn cancel_stream(&self, stream_handle: WatchHandleBox) {
        // SAFETY: `stream_handle` was produced by our `subscribe` /
        // `watch_peers` above via `forward_cluster_stream` and has not been
        // cancelled yet — the host vtable contract.
        unsafe { mcpg_plugin_sdk::ffi::cluster_forward::cancel_cluster_stream(stream_handle) }
    }

    fn acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<(WatchHandleBox, u64, String), ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.kv_prefix);
        lease::acquire_sync(
            self.inner.runtime.handle(),
            self.inner.client.clone(),
            format!("mcpg-leadership-{role}"),
            key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<(WatchHandleBox, u64, String), ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.kv_prefix);
        lease::acquire_sync(
            self.inner.runtime.handle(),
            self.inner.client.clone(),
            format!("mcpg-lock-{key}"),
            full_key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn try_acquire_leadership(
        &self,
        role: &str,
        ttl_ms: u64,
    ) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
        let key = format!("{}leadership/{role}", self.inner.config.kv_prefix);
        lease::try_acquire_sync(
            self.inner.runtime.handle(),
            self.inner.client.clone(),
            format!("mcpg-leadership-{role}"),
            key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn try_acquire_lock(
        &self,
        key: &str,
        ttl_ms: u64,
    ) -> Result<Option<(WatchHandleBox, u64, String)>, ClusterError> {
        let full_key = format!("{}locks/{key}", self.inner.config.kv_prefix);
        lease::try_acquire_sync(
            self.inner.runtime.handle(),
            self.inner.client.clone(),
            format!("mcpg-lock-{key}"),
            full_key,
            self.inner.node_id.clone(),
            ttl_ms,
            self.inner.config.lease_renew_before_expiry_percent,
        )
    }

    fn lease_renew(&self, lease_handle: WatchHandleBox) -> Result<String, ClusterError> {
        lease::renew_sync(self.inner.runtime.handle(), lease_handle)
    }

    fn lease_release(&self, lease_handle: WatchHandleBox) -> Result<(), ClusterError> {
        lease::release_sync(self.inner.runtime.handle(), lease_handle)
    }

    fn lease_drop(&self, lease_handle: WatchHandleBox) {
        // SAFETY: host vtable contract — exactly one `lease_drop`
        // per acquire, and the pointer is still valid.
        unsafe { lease::drop_state(lease_handle) }
    }

    // KV primitive over FFI — block on the plugin's own runtime, routing
    // each method through the same `KeyValueStore` impl `key_value_store()`
    // exposes.
    fn kv_get(&self, key: &str) -> Result<Option<mcpg_cluster_api::Entry>, ClusterError> {
        let kv = Arc::clone(&self.inner.kv);
        self.inner.runtime.block_on(async { kv.get(key).await })
    }

    fn kv_put(&self, key: &str, value: Vec<u8>, ttl_ms: Option<u64>) -> Result<(), ClusterError> {
        let kv = Arc::clone(&self.inner.kv);
        self.inner
            .runtime
            .block_on(async { kv.put(key, Bytes::from(value), ttl_from_ms(ttl_ms)).await })
    }

    fn kv_put_if_absent(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<bool, ClusterError> {
        let kv = Arc::clone(&self.inner.kv);
        self.inner.runtime.block_on(async {
            kv.put_if_absent(key, Bytes::from(value), ttl_from_ms(ttl_ms))
                .await
        })
    }

    fn kv_delete(&self, key: &str) -> Result<bool, ClusterError> {
        let kv = Arc::clone(&self.inner.kv);
        self.inner.runtime.block_on(async { kv.delete(key).await })
    }

    fn kv_list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, mcpg_cluster_api::Entry)>, ClusterError> {
        let kv = Arc::clone(&self.inner.kv);
        self.inner
            .runtime
            .block_on(async { kv.list_prefix(prefix, limit).await })
    }

    fn kv_expire(&self, key: &str, ttl_ms: Option<u64>) -> Result<bool, ClusterError> {
        let kv = Arc::clone(&self.inner.kv);
        self.inner
            .runtime
            .block_on(async { kv.expire(key, ttl_from_ms(ttl_ms)).await })
    }

    /// Consul holds no backend-level background task. Its only
    /// spawned tasks are per-stream (`subscribe` / `watch_peers`, torn
    /// down by the host via `cancel_stream`) and per-lease renewal (owned
    /// by each `ActiveLease`, torn down via `lease_release` / `lease_drop`)
    /// — all drained through their own vtable slots within the host's
    /// window. So `shutdown` has nothing of its own to abort; it just
    /// records the drain.
    fn shutdown(&self) {
        tracing::info!(
            plugin_id = PLUGIN_ID,
            "consul cluster: shutdown — no backend-level background tasks \
             (streams/leases drain via their own handles)"
        );
    }
}

declare_plugin! {
    plugin_id: "dev.mcpg.cluster.consul",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        cluster_backend as cluster {
            inner_name: "",
            plugin_type: ConsulBackend,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| -> ConsulBackend {
                ConsulBackend::from_config_json(cfg)
            },
        }
    ],
}

// Avoid unused warning for streaming import
#[allow(dead_code)]
fn _streaming_marker(_: WatchHandleBox) {}
#[allow(dead_code)]
fn _stream_marker(_: Pin<Box<dyn Stream<Item = ()> + Send>>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn build_config() -> ConsulConfig {
        ConsulConfig::parse(
            &json!({
                "address": "http://consul.test:8500",
                "service_name": "mcpg",
                "node_id": "node-test"
            })
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn config_validation_works() {
        let cfg = build_config();
        assert_eq!(cfg.service_name, "mcpg");
        assert_eq!(cfg.resolved_node_id(), "node-test");
    }

    // Tests use the plugin's bundled runtime via the sync FFI
    // surface to avoid the "drop runtime within async context"
    // panic that #[tokio::test] would trigger.

    #[test]
    fn node_info_reports_configured_identity() {
        let plugin = ConsulBackend::from_validated_config(build_config());
        let info = SyncClusterBackend::node_info(&plugin);
        assert_eq!(info.node_id, "node-test");
        assert_eq!(info.address, "http://consul.test:8500");
    }

    #[test]
    fn acquire_leadership_surfaces_unreachable_as_backend_unavailable() {
        // Without a running Consul, session_create fails immediately —
        // verify the failure mode is `BackendUnavailable` (the
        // contract that lets operators distinguish "consul down"
        // from "your config is wrong").
        let cfg = ConsulConfig::parse(
            &json!({
                "address": "http://127.0.0.1:1",
                "service_name": "mcpg"
            })
            .to_string(),
        )
        .unwrap();
        let plugin = ConsulBackend::from_validated_config(cfg);
        let err = plugin
            .inner
            .runtime
            .block_on(async {
                ClusterBackend::acquire_leadership(&plugin, "test-role", Duration::from_secs(60))
                    .await
            })
            .err()
            .expect("expected error");
        assert!(
            matches!(err, ClusterError::BackendUnavailable { .. }),
            "expected BackendUnavailable, got {err:?}"
        );
    }

    #[test]
    fn acquire_lock_surfaces_unreachable_as_backend_unavailable() {
        let cfg = ConsulConfig::parse(
            &json!({
                "address": "http://127.0.0.1:1",
                "service_name": "mcpg"
            })
            .to_string(),
        )
        .unwrap();
        let plugin = ConsulBackend::from_validated_config(cfg);
        let err = plugin
            .inner
            .runtime
            .block_on(async {
                ClusterBackend::acquire_lock(&plugin, "test-key", Duration::from_secs(60)).await
            })
            .err()
            .expect("expected error");
        assert!(
            matches!(err, ClusterError::BackendUnavailable { .. }),
            "expected BackendUnavailable, got {err:?}"
        );
    }

    // Live `watch_peers` test (verifying Joined/Left events on
    // catalog changes) needs a running Consul; covered at the
    // testcontainers-pending integration layer. The unit-test
    // surface here is the unreachable-Consul path covered by
    // `list_peers_handles_unreachable_consul_gracefully` below.

    #[test]
    fn list_peers_handles_unreachable_consul_gracefully() {
        let cfg = ConsulConfig::parse(
            &json!({
                "address": "http://127.0.0.1:1",
                "service_name": "mcpg"
            })
            .to_string(),
        )
        .unwrap();
        let plugin = ConsulBackend::from_validated_config(cfg);
        let peers = SyncClusterBackend::list_peers(&plugin);
        assert!(peers.is_empty());
    }
}
