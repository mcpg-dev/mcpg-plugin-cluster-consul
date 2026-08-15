//! Minimal Consul HTTP client used by the cluster_backend
//! plugin. Covers:
//!
//! - `GET /v1/catalog/service/<name>` — list peers (one-shot
//!   + long-poll variant for `watch_peers`).
//! - `PUT /v1/event/fire/<name>` — publish event.
//! - `GET /v1/event/list?index=N&wait=Ns` — long-poll subscribe.
//! - `PUT /v1/session/create` — Sessions API for leases.
//! - `PUT /v1/session/renew/<sid>` — extend lease TTL.
//! - `PUT /v1/session/destroy/<sid>` — revoke a session.
//! - `PUT /v1/kv/<key>?acquire=<sid>` — atomic lock acquire.
//! - `PUT /v1/kv/<key>?release=<sid>` — release lock.
//! - `GET /v1/kv/<key>` — read lock state (LockIndex for
//!   fencing).

use std::time::Duration;

use mcpg_cluster_api::ClusterError;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub(crate) struct ConsulClient {
    http: HttpClient,
    address: String,
    token: Option<String>,
    datacenter: Option<String>,
}

impl ConsulClient {
    pub fn new(
        address: String,
        token: Option<String>,
        datacenter: Option<String>,
    ) -> Result<Self, ClusterError> {
        let http = HttpClient::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ClusterError::Internal {
                reason: format!("reqwest client init: {e}"),
            })?;
        Ok(Self {
            http,
            address: address.trim_end_matches('/').to_owned(),
            token,
            datacenter,
        })
    }

    fn add_headers(&self, b: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(t) = &self.token {
            b.header("X-Consul-Token", t)
        } else {
            b
        }
    }

    /// `GET /v1/catalog/service/<service-name>` — every healthy
    /// instance of `service_name` registered with the Consul
    /// catalog.
    pub async fn catalog_service(
        &self,
        service_name: &str,
    ) -> Result<Vec<CatalogServiceEntry>, ClusterError> {
        let mut url = format!("{}/v1/catalog/service/{}", self.address, service_name);
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let req = self.add_headers(self.http.get(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("catalog request: {e}"),
            })?;
        if !resp.status().is_success() {
            return Err(ClusterError::Internal {
                reason: format!("catalog status {}", resp.status()),
            });
        }
        resp.json::<Vec<CatalogServiceEntry>>()
            .await
            .map_err(|e| ClusterError::Internal {
                reason: format!("catalog json decode: {e}"),
            })
    }

    /// `PUT /v1/event/fire/<topic>` — publish event payload to
    /// the Consul-cluster gossip layer. Best-effort delivery —
    /// Consul Events are not durable across subscriber
    /// disconnects.
    pub async fn fire_event(&self, topic: &str, payload: bytes::Bytes) -> Result<(), ClusterError> {
        let mut url = format!("{}/v1/event/fire/{}", self.address, topic);
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let req = self.add_headers(self.http.put(&url).body(payload));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("event fire: {e}"),
            })?;
        if !resp.status().is_success() {
            return Err(ClusterError::Internal {
                reason: format!("event fire status {}", resp.status()),
            });
        }
        Ok(())
    }

    /// `GET /v1/catalog/service/<name>?index=N&wait=Ns` —
    /// blocking variant of [`Self::catalog_service`]. Returns the
    /// catalog snapshot + the new index. The next call feeds
    /// `index = new_index` so Consul holds the request until the
    /// catalog changes (or `wait` elapses).
    pub async fn catalog_service_blocking(
        &self,
        service_name: &str,
        index: u64,
        wait_seconds: u64,
    ) -> Result<(Vec<CatalogServiceEntry>, u64), ClusterError> {
        let mut url = format!(
            "{}/v1/catalog/service/{service_name}?index={index}&wait={wait_seconds}s",
            self.address
        );
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("&dc={dc}"));
        }
        let req = self
            .add_headers(self.http.get(&url))
            .timeout(Duration::from_secs(wait_seconds + 30));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("catalog blocking: {e}"),
            })?;
        let new_index = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(index);
        if !resp.status().is_success() {
            return Err(ClusterError::Internal {
                reason: format!("catalog blocking status {}", resp.status()),
            });
        }
        let entries =
            resp.json::<Vec<CatalogServiceEntry>>()
                .await
                .map_err(|e| ClusterError::Internal {
                    reason: format!("catalog blocking json: {e}"),
                })?;
        Ok((entries, new_index))
    }

    /// `GET /v1/event/list?name=<topic>&index=N&wait=<seconds>s`
    /// — long-poll for events. Returns the (events, new_index)
    /// pair. The new index is fed into the next call's `index`
    /// query param so blocking-watch semantics are honored
    /// per Consul's docs.
    ///
    /// Per Consul's API: events are deduped by ID. Subscribers
    /// are responsible for tracking last-seen IDs and skipping
    /// replays — Consul's index parameter only suppresses
    /// already-known generations of the event list.
    pub async fn list_events(
        &self,
        topic: &str,
        index: u64,
        wait_seconds: u64,
    ) -> Result<(Vec<UserEvent>, u64), ClusterError> {
        let mut url = format!(
            "{}/v1/event/list?name={topic}&index={index}&wait={wait_seconds}s",
            self.address
        );
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("&dc={dc}"));
        }
        // Per Consul docs the long-poll holds for up to `wait`
        // before returning. Add a generous request timeout so
        // network blips don't tear down the subscriber.
        let req = self
            .add_headers(self.http.get(&url))
            .timeout(Duration::from_secs(wait_seconds + 30));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("event list: {e}"),
            })?;
        let new_index = resp
            .headers()
            .get("X-Consul-Index")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(index);
        if !resp.status().is_success() {
            return Err(ClusterError::Internal {
                reason: format!("event list status {}", resp.status()),
            });
        }
        let events: Vec<UserEvent> = resp.json().await.map_err(|e| ClusterError::Internal {
            reason: format!("event list json: {e}"),
        })?;
        Ok((events, new_index))
    }

    /// `PUT /v1/session/create` with body
    /// `{"Name": ..., "TTL": "30s", "Behavior": "release"|"delete",
    ///   "LockDelay": "0s"}`. Returns the new session id.
    pub async fn session_create(
        &self,
        name: &str,
        ttl_seconds: u64,
        behavior: SessionBehavior,
    ) -> Result<String, ClusterError> {
        let mut url = format!("{}/v1/session/create", self.address);
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let body = SessionCreateBody {
            name: name.to_owned(),
            ttl: format!("{ttl_seconds}s"),
            behavior: behavior.as_str().to_owned(),
            lock_delay: "0s".to_owned(),
        };
        let req = self.add_headers(self.http.put(&url).json(&body));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("session_create: {e}"),
            })?;
        if !resp.status().is_success() {
            return Err(ClusterError::BackendUnavailable {
                reason: format!("session_create status {}", resp.status()),
            });
        }
        let body: SessionCreateResponse =
            resp.json().await.map_err(|e| ClusterError::Internal {
                reason: format!("session_create json: {e}"),
            })?;
        Ok(body.id)
    }

    /// `PUT /v1/session/renew/<sid>`. Extends the session TTL.
    /// Returns `LeaseExpired` if Consul reports the session as
    /// gone (404 / empty array body).
    pub async fn session_renew(&self, session_id: &str) -> Result<(), ClusterError> {
        let mut url = format!("{}/v1/session/renew/{session_id}", self.address);
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let req = self.add_headers(self.http.put(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("session_renew: {e}"),
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ClusterError::LeaseExpired);
        }
        if !resp.status().is_success() {
            return Err(ClusterError::BackendUnavailable {
                reason: format!("session_renew status {}", resp.status()),
            });
        }
        // Body is `[ {session info} ]` or `[]`. Empty array =
        // session vanished between our send + Consul's
        // processing.
        let body_text = resp.text().await.map_err(|e| ClusterError::Internal {
            reason: format!("session_renew read body: {e}"),
        })?;
        let parsed: serde_json::Value =
            serde_json::from_str(&body_text).unwrap_or(serde_json::Value::Null);
        if parsed.as_array().map(|a| a.is_empty()).unwrap_or(true) {
            return Err(ClusterError::LeaseExpired);
        }
        Ok(())
    }

    /// `PUT /v1/session/destroy/<sid>`. Idempotent — Consul
    /// returns `true` even if the session is already gone.
    pub async fn session_destroy(&self, session_id: &str) -> Result<(), ClusterError> {
        let mut url = format!("{}/v1/session/destroy/{session_id}", self.address);
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let req = self.add_headers(self.http.put(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("session_destroy: {e}"),
            })?;
        if !resp.status().is_success() {
            // Treat any error as best-effort cleanup; the lease
            // is already torn down.
            tracing::debug!(
                session_id,
                status = %resp.status(),
                "consul cluster: session_destroy non-2xx (best-effort cleanup)"
            );
        }
        Ok(())
    }

    /// `PUT /v1/kv/<key>?acquire=<sid>` with body=value. Returns
    /// `Ok(true)` on acquired, `Ok(false)` on contention.
    pub async fn kv_acquire(
        &self,
        key: &str,
        session_id: &str,
        value: &[u8],
    ) -> Result<bool, ClusterError> {
        let mut url = format!(
            "{}/v1/kv/{key}?acquire={session_id}",
            self.address,
            key = url_escape(key),
        );
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("&dc={dc}"));
        }
        let req = self.add_headers(self.http.put(&url).body(value.to_vec()));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("kv_acquire: {e}"),
            })?;
        if !resp.status().is_success() {
            return Err(ClusterError::BackendUnavailable {
                reason: format!("kv_acquire status {}", resp.status()),
            });
        }
        let body = resp.text().await.map_err(|e| ClusterError::Internal {
            reason: format!("kv_acquire body: {e}"),
        })?;
        Ok(body.trim() == "true")
    }

    /// `PUT /v1/kv/<key>?release=<sid>`. Returns `Ok(true)` on
    /// release, `Ok(false)` if Consul reports the session
    /// doesn't hold the key (already expired / released).
    pub async fn kv_release(&self, key: &str, session_id: &str) -> Result<bool, ClusterError> {
        let mut url = format!(
            "{}/v1/kv/{key}?release={session_id}",
            self.address,
            key = url_escape(key),
        );
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("&dc={dc}"));
        }
        let req = self.add_headers(self.http.put(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("kv_release: {e}"),
            })?;
        if !resp.status().is_success() {
            return Ok(false);
        }
        let body = resp.text().await.map_err(|e| ClusterError::Internal {
            reason: format!("kv_release body: {e}"),
        })?;
        Ok(body.trim() == "true")
    }

    /// `GET /v1/kv/<key>` — full entry including the base64 `Value`
    /// and `ModifyIndex` (Consul's per-key monotonic counter used for
    /// compare-and-swap). Returns `Ok(None)` on 404.
    pub async fn kv_get_value(&self, key: &str) -> Result<Option<KvValueEntry>, ClusterError> {
        let mut url = format!("{}/v1/kv/{key}", self.address, key = url_escape(key));
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let req = self.add_headers(self.http.get(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("kv_get: {e}"),
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(ClusterError::Internal {
                reason: format!("kv_get status {}", resp.status()),
            });
        }
        let entries: Vec<KvValueEntry> = resp.json().await.map_err(|e| ClusterError::Internal {
            reason: format!("kv_get json: {e}"),
        })?;
        Ok(entries.into_iter().next())
    }

    /// `PUT /v1/kv/<key>` with the raw value bytes as the body. When
    /// `cas` is `Some(index)`, Consul applies the write only if the
    /// key's current `ModifyIndex` equals `index` (`cas=0` == create
    /// only). Returns `Ok(true)` when the write took effect, `Ok(false)`
    /// when a CAS check failed (lost the race). `cas == None` is an
    /// unconditional last-writer-wins put.
    pub async fn kv_put_value(
        &self,
        key: &str,
        value: &[u8],
        cas: Option<u64>,
    ) -> Result<bool, ClusterError> {
        let mut url = format!("{}/v1/kv/{key}", self.address, key = url_escape(key));
        let mut sep = '?';
        if let Some(idx) = cas {
            url.push_str(&format!("{sep}cas={idx}"));
            sep = '&';
        }
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("{sep}dc={dc}"));
        }
        let req = self.add_headers(self.http.put(&url).body(value.to_vec()));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("kv_put: {e}"),
            })?;
        if !resp.status().is_success() {
            return Err(ClusterError::BackendUnavailable {
                reason: format!("kv_put status {}", resp.status()),
            });
        }
        let body = resp.text().await.map_err(|e| ClusterError::Internal {
            reason: format!("kv_put body: {e}"),
        })?;
        Ok(body.trim() == "true")
    }

    /// `DELETE /v1/kv/<key>`. Returns `Ok(true)` unconditionally on a
    /// 2xx (Consul does not report whether the key existed for a plain
    /// delete — existence is checked by the caller via a prior `get`).
    pub async fn kv_delete_value(&self, key: &str) -> Result<(), ClusterError> {
        let mut url = format!("{}/v1/kv/{key}", self.address, key = url_escape(key));
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let req = self.add_headers(self.http.delete(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("kv_delete: {e}"),
            })?;
        if !resp.status().is_success() {
            return Err(ClusterError::BackendUnavailable {
                reason: format!("kv_delete status {}", resp.status()),
            });
        }
        Ok(())
    }

    /// `GET /v1/kv/<prefix>?recurse` — every entry whose key starts
    /// with `prefix`. Returns `Ok(vec![])` on 404 (no matches).
    pub async fn kv_list_recurse(&self, prefix: &str) -> Result<Vec<KvValueEntry>, ClusterError> {
        let mut url = format!(
            "{}/v1/kv/{key}?recurse",
            self.address,
            key = url_escape(prefix)
        );
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("&dc={dc}"));
        }
        let req = self.add_headers(self.http.get(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("kv_list: {e}"),
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(vec![]);
        }
        if !resp.status().is_success() {
            return Err(ClusterError::Internal {
                reason: format!("kv_list status {}", resp.status()),
            });
        }
        resp.json::<Vec<KvValueEntry>>()
            .await
            .map_err(|e| ClusterError::Internal {
                reason: format!("kv_list json: {e}"),
            })
    }

    /// `GET /v1/kv/<key>` — used after `kv_acquire` to read the
    /// `LockIndex` (Consul's per-key monotonic counter, used as
    /// the fencing token).
    pub async fn kv_read(&self, key: &str) -> Result<Option<KvEntry>, ClusterError> {
        let mut url = format!("{}/v1/kv/{key}", self.address, key = url_escape(key),);
        if let Some(dc) = &self.datacenter {
            url.push_str(&format!("?dc={dc}"));
        }
        let req = self.add_headers(self.http.get(&url));
        let resp = req
            .send()
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("kv_read: {e}"),
            })?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(ClusterError::Internal {
                reason: format!("kv_read status {}", resp.status()),
            });
        }
        let entries: Vec<KvEntry> = resp.json().await.map_err(|e| ClusterError::Internal {
            reason: format!("kv_read json: {e}"),
        })?;
        Ok(entries.into_iter().next())
    }
}

/// Encode `key` for use in a Consul KV URL path. Consul accepts
/// most characters unescaped; we just normalise leading slashes.
fn url_escape(key: &str) -> String {
    key.trim_start_matches('/').to_owned()
}

/// Consul session "Behavior" — what Consul does on session expiry.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum SessionBehavior {
    /// Release any locks held by the session (caller can re-
    /// acquire). The right choice for advisory locks.
    Release,
    /// Delete the keys held by the session. The right choice
    /// for "this entry only exists while the holder is alive"
    /// — peer membership entries. Reserved for future
    /// auto-registration support.
    Delete,
}

impl SessionBehavior {
    fn as_str(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Delete => "delete",
        }
    }
}

#[derive(Serialize)]
struct SessionCreateBody {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "TTL")]
    ttl: String,
    #[serde(rename = "Behavior")]
    behavior: String,
    #[serde(rename = "LockDelay")]
    lock_delay: String,
}

#[derive(Deserialize)]
struct SessionCreateResponse {
    #[serde(rename = "ID")]
    id: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct KvEntry {
    #[serde(rename = "LockIndex", default)]
    pub lock_index: u64,
    #[serde(rename = "Key", default)]
    pub key: String,
    #[serde(rename = "Session", default)]
    pub session: Option<String>,
}

/// A Consul KV entry with its base64-encoded value and `ModifyIndex`.
/// `ModifyIndex` is the per-key monotonic counter Consul uses for
/// compare-and-swap (`?cas=`); the `KeyValueStore` impl uses it as the
/// version anchor for `put_if_absent` (`cas=0`) and the read-modify-
/// write `expire` path.
#[derive(Debug, Clone, Deserialize)]
pub struct KvValueEntry {
    #[serde(rename = "Key", default)]
    pub key: String,
    #[serde(rename = "ModifyIndex", default)]
    pub modify_index: u64,
    /// Base64-encoded value as Consul transmits it. `None` when the
    /// key was written with an empty value.
    #[serde(rename = "Value", default)]
    pub value_b64: Option<String>,
}

impl KvValueEntry {
    /// Decode the base64 `Value` into raw bytes (empty when `None`).
    pub fn decoded_value(&self) -> Vec<u8> {
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD as B64;
        match &self.value_b64 {
            Some(s) => B64.decode(s.as_bytes()).unwrap_or_default(),
            None => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogServiceEntry {
    #[serde(rename = "Node")]
    pub node: String,
    #[serde(rename = "Address", default)]
    pub address: String,
    #[serde(rename = "ServiceID", default)]
    pub service_id: String,
    #[serde(rename = "ServiceName", default)]
    pub service_name: String,
    #[serde(rename = "ServicePort", default)]
    pub service_port: u16,
    #[serde(rename = "ServiceTags", default)]
    pub service_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEvent {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Name")]
    pub name: String,
    /// Base64-encoded payload as Consul transmits it. Decoded by
    /// the subscribe stream.
    #[serde(rename = "Payload", default)]
    pub payload_b64: Option<String>,
    #[serde(rename = "NodeFilter", default)]
    pub node_filter: Option<String>,
    #[serde(rename = "ServiceFilter", default)]
    pub service_filter: Option<String>,
    #[serde(rename = "TagFilter", default)]
    pub tag_filter: Option<String>,
    #[serde(rename = "Version", default)]
    pub version: u64,
    #[serde(rename = "LTime", default)]
    pub ltime: u64,
}
