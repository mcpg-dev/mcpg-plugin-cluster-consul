//! Consul-backed `KeyValueStore` for `dev.mcpg.cluster.consul`.
//!
//! Maps the host's durable KV primitive onto the Consul KV HTTP API:
//!
//! | Trait method     | Consul KV primitive |
//! |---|---|
//! | `get`            | `GET /v1/kv/<key>` (decode value + drop if logically expired) |
//! | `put`            | `PUT /v1/kv/<key>` (last-writer-wins) |
//! | `put_if_absent`  | `PUT /v1/kv/<key>?cas=0` (create-only single-winner) |
//! | `delete`         | `GET` (existence) + `DELETE /v1/kv/<key>` |
//! | `list_prefix`    | `GET /v1/kv/<prefix>?recurse` |
//! | `expire`         | `GET` (read value + ModifyIndex) + `PUT ?cas=<index>` |
//!
//! # TTL — LOGICAL (emulated), not native
//!
//! Consul KV has **no native per-key TTL** (the only TTL Consul exposes
//! is the Sessions API, used here for leases — not for arbitrary KV).
//! This impl therefore emulates a **logical TTL**: every value is wrapped
//! in a small envelope carrying an absolute `expires_at` (Unix ms), and a
//! key whose `expires_at` is in the past is treated as **absent** on
//! `get` / `list_prefix` (lazy expiry). It is NOT proactively reaped — an
//! expired key lingers in Consul until it is next read, overwritten, or
//! explicitly deleted; `put_if_absent` also treats a logically-expired
//! key as absent and reclaims it via a CAS overwrite. This matches the
//! observable contract ("expired == absent") even though Consul itself
//! keeps the bytes around. Operators relying on Consul storage limits
//! should note that cold expired keys are not garbage-collected.
//!
//! # CAS / single-winner
//!
//! Consul's `ModifyIndex` is the per-key monotonic version. `?cas=0`
//! writes only when the key does not exist (the create-once primitive
//! behind `put_if_absent`); `?cas=<index>` writes only when the key is
//! still at `index` (the read-modify-write guard `expire` uses). The
//! trait's `Entry` exposes no version field, so `ModifyIndex` stays
//! internal to this backend.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use mcpg_cluster_api::{ClusterError, Entry, KeyValueStore};

use crate::client::ConsulClient;

/// Consul-backed KV state. Shares the coordinator's HTTP client; keys
/// are namespaced under `<kv_prefix>kv/` to stay clear of the
/// coordinator's `leadership/` and `locks/` keyspaces.
#[derive(Clone)]
pub struct ConsulKv {
    client: ConsulClient,
    key_prefix: String,
}

impl std::fmt::Debug for ConsulKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsulKv")
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

impl ConsulKv {
    /// Construct a `ConsulKv` over the coordinator's HTTP client. The
    /// `kv_prefix` is the operator's deployment prefix (e.g. `mcpg/`);
    /// this impl appends a `kv/` segment so KV keys don't collide with
    /// the lease keyspace.
    pub(crate) fn new(client: ConsulClient, kv_prefix: String) -> Self {
        Self {
            client,
            key_prefix: format!("{kv_prefix}kv/"),
        }
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}{key}", self.key_prefix)
    }

    fn logical_key(&self, full: &str) -> String {
        full.strip_prefix(&self.key_prefix)
            .map(str::to_owned)
            .unwrap_or_else(|| full.to_owned())
    }
}

/// Logical-TTL value envelope. The first byte is a version tag; byte 1
/// is a flag (0 == no expiry, 1 == an 8-byte big-endian `expires_at`
/// Unix-ms stamp follows); the remainder is the caller's opaque value.
///
/// A fixed-width header keeps the encoding prefix-independent and cheap
/// to parse. The envelope is internal to this backend — callers never
/// see it (every read decodes back to the raw value bytes).
mod envelope {
    const VERSION: u8 = 0x01;
    const FLAG_NONE: u8 = 0x00;
    const FLAG_EXPIRES: u8 = 0x01;

    /// Wrap `value` with an optional absolute expiry (Unix ms).
    pub(super) fn encode(value: &[u8], expires_at_unix_ms: Option<u64>) -> Vec<u8> {
        let mut out = Vec::with_capacity(value.len() + 10);
        out.push(VERSION);
        match expires_at_unix_ms {
            None => out.push(FLAG_NONE),
            Some(ms) => {
                out.push(FLAG_EXPIRES);
                out.extend_from_slice(&ms.to_be_bytes());
            }
        }
        out.extend_from_slice(value);
        out
    }

    /// Decode an envelope into `(value, expires_at_unix_ms)`. Returns
    /// `None` for a malformed / unknown-version envelope (a foreign
    /// writer touched the key) so the caller can treat it as absent.
    pub(super) fn decode(bytes: &[u8]) -> Option<(Vec<u8>, Option<u64>)> {
        if bytes.len() < 2 || bytes[0] != VERSION {
            return None;
        }
        match bytes[1] {
            FLAG_NONE => Some((bytes[2..].to_vec(), None)),
            FLAG_EXPIRES => {
                if bytes.len() < 10 {
                    return None;
                }
                let ms = u64::from_be_bytes(bytes[2..10].try_into().ok()?);
                Some((bytes[10..].to_vec(), Some(ms)))
            }
            _ => None,
        }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Convert a `ttl` into an absolute `expires_at` Unix-ms stamp.
fn expires_at_from_ttl(ttl: Duration) -> u64 {
    now_unix_ms().saturating_add(ttl.as_millis().min(u128::from(u64::MAX)) as u64)
}

/// Reconstruct an [`Entry`] from a decoded envelope, returning `None`
/// when the entry has logically expired (lazy expiry == absent).
fn entry_from_decoded(value: Vec<u8>, expires_at_unix_ms: Option<u64>) -> Option<Entry> {
    match expires_at_unix_ms {
        Some(ms) if ms <= now_unix_ms() => None,
        Some(ms) => Some(Entry {
            bytes: Bytes::from(value),
            expires_at: Some(UNIX_EPOCH + Duration::from_millis(ms)),
        }),
        None => Some(Entry {
            bytes: Bytes::from(value),
            expires_at: None,
        }),
    }
}

#[async_trait]
impl KeyValueStore for ConsulKv {
    async fn get(&self, key: &str) -> Result<Option<Entry>, ClusterError> {
        let full = self.full_key(key);
        let Some(raw) = self.client.kv_get_value(&full).await? else {
            return Ok(None);
        };
        let Some((value, expires)) = envelope::decode(&raw.decoded_value()) else {
            // A non-mcpg writer touched the key — treat as absent.
            return Ok(None);
        };
        Ok(entry_from_decoded(value, expires))
    }

    async fn put(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), ClusterError> {
        let full = self.full_key(key);
        let expires = ttl.map(expires_at_from_ttl);
        let wrapped = envelope::encode(&value, expires);
        // Unconditional last-writer-wins put.
        self.client.kv_put_value(&full, &wrapped, None).await?;
        Ok(())
    }

    async fn put_if_absent(
        &self,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let expires = ttl.map(expires_at_from_ttl);
        let wrapped = envelope::encode(&value, expires);
        // `cas=0` = create-only: the atomic cross-replica single-winner
        // claim. No two concurrent callers can both win.
        if self.client.kv_put_value(&full, &wrapped, Some(0)).await? {
            return Ok(true);
        }
        // The CAS failed because a key already exists. It may be
        // LOGICALLY expired, though — in which case the contract says it
        // counts as absent and this caller should be able to claim it.
        // Re-read; if it's expired, reclaim it with a CAS on its current
        // ModifyIndex (so a concurrent live writer still can't be
        // clobbered).
        let Some(existing) = self.client.kv_get_value(&full).await? else {
            // Vanished between the create attempt and the read — retry
            // the create once.
            return self.client.kv_put_value(&full, &wrapped, Some(0)).await;
        };
        let still_live = match envelope::decode(&existing.decoded_value()) {
            Some((_, Some(ms))) => ms > now_unix_ms(),
            // No-TTL value, or a foreign/garbled value: treat as live so
            // we don't stomp data this backend didn't write.
            Some((_, None)) => true,
            None => true,
        };
        if still_live {
            return Ok(false);
        }
        // Logically expired → reclaim via CAS on the stale index.
        self.client
            .kv_put_value(&full, &wrapped, Some(existing.modify_index))
            .await
    }

    async fn delete(&self, key: &str) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        // Consul's plain DELETE doesn't report prior existence, so probe
        // first to honour the `Ok(false) when absent` contract. A
        // logically-expired key counts as already-absent.
        let existed = match self.client.kv_get_value(&full).await? {
            Some(raw) => match envelope::decode(&raw.decoded_value()) {
                Some((value, expires)) => entry_from_decoded(value, expires).is_some(),
                None => false,
            },
            None => false,
        };
        self.client.kv_delete_value(&full).await?;
        Ok(existed)
    }

    async fn list_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Entry)>, ClusterError> {
        let full_prefix = self.full_key(prefix);
        let entries = self.client.kv_list_recurse(&full_prefix).await?;
        let mut out = Vec::new();
        for raw in entries {
            let Some((value, expires)) = envelope::decode(&raw.decoded_value()) else {
                continue;
            };
            let Some(entry) = entry_from_decoded(value, expires) else {
                continue; // logically expired → absent
            };
            out.push((self.logical_key(&raw.key), entry));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn expire(&self, key: &str, ttl: Option<Duration>) -> Result<bool, ClusterError> {
        let full = self.full_key(key);
        let Some(existing) = self.client.kv_get_value(&full).await? else {
            return Ok(false);
        };
        let Some((value, cur_expires)) = envelope::decode(&existing.decoded_value()) else {
            return Ok(false);
        };
        // Already logically expired → behaves as absent.
        if matches!(cur_expires, Some(ms) if ms <= now_unix_ms()) {
            return Ok(false);
        }
        let new_expires = ttl.map(expires_at_from_ttl);
        let wrapped = envelope::encode(&value, new_expires);
        // CAS on the current ModifyIndex so a concurrent writer between
        // our read and write isn't clobbered. A lost race surfaces as a
        // benign `false` — the key was mutated out from under us.
        self.client
            .kv_put_value(&full, &wrapped, Some(existing.modify_index))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_without_ttl() {
        let enc = envelope::encode(b"payload", None);
        let (value, expires) = envelope::decode(&enc).unwrap();
        assert_eq!(value, b"payload");
        assert_eq!(expires, None);
    }

    #[test]
    fn envelope_round_trips_with_ttl() {
        let enc = envelope::encode(b"payload", Some(1_700_000_000_123));
        let (value, expires) = envelope::decode(&enc).unwrap();
        assert_eq!(value, b"payload");
        assert_eq!(expires, Some(1_700_000_000_123));
    }

    #[test]
    fn envelope_empty_value_ok() {
        let enc = envelope::encode(b"", Some(42));
        let (value, expires) = envelope::decode(&enc).unwrap();
        assert!(value.is_empty());
        assert_eq!(expires, Some(42));
    }

    #[test]
    fn envelope_rejects_foreign_or_truncated() {
        assert!(envelope::decode(&[]).is_none());
        assert!(envelope::decode(&[0x01]).is_none());
        // Unknown version.
        assert!(envelope::decode(&[0x02, 0x00]).is_none());
        // Claims an 8-byte stamp but is truncated.
        assert!(envelope::decode(&[0x01, 0x01, 0x00, 0x00]).is_none());
    }

    #[test]
    fn entry_from_decoded_treats_past_expiry_as_absent() {
        // Far-past stamp → None (logically expired).
        assert!(entry_from_decoded(b"x".to_vec(), Some(1)).is_none());
        // No expiry → present.
        assert!(entry_from_decoded(b"x".to_vec(), None).is_some());
        // Far-future stamp → present.
        let future = now_unix_ms() + 60_000;
        assert!(entry_from_decoded(b"x".to_vec(), Some(future)).is_some());
    }
}
