//! Operator-supplied configuration schema for `dev.mcpg.cluster.consul`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsulConfig {
    /// Consul HTTP API base URL — e.g. `http://consul.svc:8500`.
    /// HTTPS requires the gateway's `network_outbound`
    /// capability + a Consul cluster with TLS configured.
    pub address: String,

    /// Consul ACL token. Optional. When present, sent as the
    /// `X-Consul-Token` header on every request.
    #[serde(default)]
    pub token: Option<String>,

    /// Consul datacenter for cross-DC requests (`?dc=` query
    /// parameter). When absent, Consul uses the agent's local
    /// DC.
    #[serde(default)]
    pub datacenter: Option<String>,

    /// Service name this gateway instance registers under (or
    /// looks up peers via). Operators register the gateway
    /// instances with their Consul agent; this plugin doesn't
    /// auto-register in v0.1 — operators handle that via Consul
    /// agent config or sidecar.
    pub service_name: String,

    /// This node's stable identifier (used for self-publish
    /// dedup in pub/sub events). Defaults to `service_name +
    /// hostname`.
    #[serde(default)]
    pub node_id: Option<String>,

    /// Long-poll wait duration for the subscribe path. Consul
    /// long-polls events with the `wait` query parameter; values
    /// up to 10m are honored.
    #[serde(default = "default_subscribe_wait_sec")]
    pub subscribe_wait_sec: u64,

    /// KV path prefix for plugin-managed state (sessions /
    /// locks). Operators running multiple MCPG deployments
    /// against one Consul cluster MUST set distinct prefixes per
    /// deployment. Default `mcpg/`.
    #[serde(default = "default_kv_prefix")]
    pub kv_prefix: String,

    /// Background renewal task fires every
    /// `ttl × (100 - pct) / 100`. Default 30 → renewal at 70% of
    /// TTL. Clamped to [1, 99] at runtime.
    #[serde(default = "default_renew_pct")]
    pub lease_renew_before_expiry_percent: u32,
}

fn default_subscribe_wait_sec() -> u64 {
    30
}

fn default_kv_prefix() -> String {
    "mcpg/".into()
}

fn default_renew_pct() -> u32 {
    30
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid cluster.consul config JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("cluster.consul: address is empty")]
    EmptyAddress,
    #[error("cluster.consul: address must start with http:// or https://")]
    InvalidAddressScheme,
    #[error("cluster.consul: service_name is empty")]
    EmptyServiceName,
    #[error("cluster.consul: subscribe_wait_sec must be in 1..=600 (Consul max 10 minutes)")]
    InvalidSubscribeWait,
}

impl ConsulConfig {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.address.trim().is_empty() {
            return Err(ConfigError::EmptyAddress);
        }
        if !self.address.starts_with("http://") && !self.address.starts_with("https://") {
            return Err(ConfigError::InvalidAddressScheme);
        }
        if self.service_name.trim().is_empty() {
            return Err(ConfigError::EmptyServiceName);
        }
        if self.subscribe_wait_sec == 0 || self.subscribe_wait_sec > 600 {
            return Err(ConfigError::InvalidSubscribeWait);
        }
        Ok(())
    }

    pub fn resolved_node_id(&self) -> String {
        self.node_id.clone().unwrap_or_else(|| {
            let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".into());
            format!("{}-{hostname}", self.service_name)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_minimal_config() {
        let cfg = json!({
            "address": "http://consul.svc:8500",
            "service_name": "mcpg"
        })
        .to_string();
        let parsed = ConsulConfig::parse(&cfg).unwrap();
        assert_eq!(parsed.address, "http://consul.svc:8500");
        assert_eq!(parsed.service_name, "mcpg");
        assert_eq!(parsed.subscribe_wait_sec, 30);
    }

    #[test]
    fn rejects_invalid_scheme() {
        let cfg = json!({
            "address": "consul.svc:8500",
            "service_name": "mcpg"
        })
        .to_string();
        let err = ConsulConfig::parse(&cfg).unwrap_err();
        matches!(err, ConfigError::InvalidAddressScheme);
    }

    #[test]
    fn rejects_empty_service_name() {
        let cfg = json!({
            "address": "http://x:8500",
            "service_name": ""
        })
        .to_string();
        let err = ConsulConfig::parse(&cfg).unwrap_err();
        matches!(err, ConfigError::EmptyServiceName);
    }

    #[test]
    fn rejects_oversize_wait() {
        let cfg = json!({
            "address": "http://x:8500",
            "service_name": "mcpg",
             "subscribe_wait_sec": 700
        })
        .to_string();
        let err = ConsulConfig::parse(&cfg).unwrap_err();
        matches!(err, ConfigError::InvalidSubscribeWait);
    }

    #[test]
    fn resolved_node_id_uses_explicit_when_set() {
        let cfg = ConsulConfig::parse(
            &json!({
                "address": "http://x:8500",
                "service_name": "mcpg",
                "node_id": "node-a"
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(cfg.resolved_node_id(), "node-a");
    }

    #[test]
    fn resolved_node_id_falls_back_to_service_name_hostname() {
        let cfg = ConsulConfig::parse(
            &json!({
                "address": "http://x:8500",
                "service_name": "mcpg"
            })
            .to_string(),
        )
        .unwrap();
        assert!(cfg.resolved_node_id().starts_with("mcpg-"));
    }
}
