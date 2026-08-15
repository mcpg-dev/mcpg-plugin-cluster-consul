# Consul Cluster Coordinator — `dev.mcpg.cluster.consul`

> class `cluster` · `native` · package `mcpg-plugin-cluster-consul` · artifact `libmcpg_plugin_cluster_consul.so` · BUSL-1.1

The cluster coordinator an MCPG gateway fleet uses when HashiCorp Consul is
already the source of truth for service discovery and shared configuration. It
speaks the Consul HTTP API for everything a multi-replica gateway needs: peers
from the catalog, leadership and distributed locks from Sessions plus KV
compare-and-swap for fencing tokens, cross-replica notifications from the Events
API, and a durable key/value store for capability state. Reach for it when your
gateway replicas run alongside a Consul agent and you would rather not introduce
a second coordination system.

## What it does
- Advertises the `kv` and `bus` coordinator roles, so gateway capabilities that
  inherit from the coordinator (sessions, tasks, subscriptions, delivery,
  cancellation) are backed by Consul rather than per-replica memory.
- Reports this replica's identity and lists peers from the Consul catalog
  service named by `service_name`.
- Acquires leadership and locks through Consul Sessions, using the per-key
  `LockIndex` as a strictly monotonic, non-zero fencing token, and renews each
  lease in the background before it expires.
- Watches the catalog by long-poll and emits peer `Joined` / `Left` events by
  diffing consecutive snapshots.
- Publishes and subscribes over the Consul Events API. Because Consul events
  carry no metadata channel, each payload is wrapped in a versioned envelope so
  subscribers can filter on a routing key; malformed events are dropped.
- Declares the `network_outbound` capability; the gateway refuses to load the
  plugin unless the `plugins[]` entry grants it.
- Refuses to register on invalid config — a misconfigured coordinator fails the
  gateway's boot rather than silently de-clustering.

The plugin does not register the gateway with the Consul agent. Registration
stays an operator concern, handled through agent configuration or a sidecar;
this plugin only reads the catalog.

## Configuration
Selected by the dedicated top-level `cluster:` block through `cluster.kind:
consul`. The kind-specific fields are written **flat** under `cluster:` and flow
to the plugin's factory as JSON, replacing any `config:` block on the matching
`plugins[]` entry — so the `plugins[]` entry keeps the artifact location and the
`cluster:` block keeps the operational knobs. The cdylib must still be declared
in `plugins[]`; if `cluster.kind` names a coordinator with no matching entry,
the gateway fails fast at boot.

```yaml
cluster:
  kind: consul
  address: https://consul.service.internal:8501
  service_name: mcpg-gateway
  kv_prefix: mcpg/prod/
  token: ${env.CONSUL_TOKEN}
  datacenter: eu-west-1
  node_id: ${env.HOSTNAME}
  subscribe_wait_sec: 30
  lease_renew_before_expiry_percent: 30

plugins:
  - id: dev.mcpg.cluster.consul
    class: cluster
    kind: native
    source:
      path: ./plugins/libmcpg_plugin_cluster_consul.so
      # or, platform-agnostic:
      # oci: ghcr.io/mcpg-dev/source-code/plugins/cluster-consul:protocol-1
    granted_capabilities:
      - network_outbound
```

| Field | Type | Default | Description |
|---|---|---|---|
| `address` | string | — (required) | Consul HTTP API base URL. Must start with `http://` or `https://`. |
| `service_name` | string | — (required) | Catalog service name this fleet is registered under; peers are read from it. |
| `token` | string | unset | Consul ACL token, sent as the `X-Consul-Token` header on every request. |
| `datacenter` | string | agent-local | Cross-datacenter `?dc=` query parameter. |
| `node_id` | string | `<service_name>-$HOSTNAME` | Stable node identity, used to skip this replica's own published events. |
| `subscribe_wait_sec` | integer | `30` | Event long-poll wait, in seconds. Must be within `1..=600` (Consul's ten-minute maximum). |
| `kv_prefix` | string | `mcpg/` | Path prefix for every key this plugin owns. Concatenated directly, so include the trailing `/`. |
| `lease_renew_before_expiry_percent` | integer | `30` | Background renewal fires after `100 − percent` of the TTL has elapsed. Clamped to `1..=99`. |

Unknown fields are rejected.

## Operations
Everything the plugin owns lives under `kv_prefix`, split into three
non-overlapping keyspaces, so the coordinator's own state can never collide with
the capability state the gateway stores through it:

| Keys | Consul API | Purpose |
|---|---|---|
| `<kv_prefix>leadership/<role>` | Sessions + KV | Leadership election per role. |
| `<kv_prefix>locks/<key>` | Sessions + KV | Named distributed locks. |
| `<kv_prefix>kv/<key>` | KV | The key/value primitive gateway capabilities inherit. |

Peers come from `GET /v1/catalog/service/<service_name>`; the watch variant
long-polls the same endpoint on its index. Pub/sub uses the Events API, so
delivery inherits Consul's gossip semantics — best-effort, not durable. Use it
for cross-replica notification, not for anything that must not be lost.

Consul KV has no native per-key TTL — the only TTL Consul exposes is on
Sessions, which this plugin uses for leases. TTLs on the key/value primitive are
therefore **logical**: each value is stored inside an envelope carrying an
absolute expiry, and a key past its expiry reads back as absent. Expiry is lazy,
not reaped: the bytes stay in Consul until the key is next read, overwritten, or
deleted, which matters if you are sizing Consul storage. Create-once writes use
`?cas=0` and read-modify-write uses `?cas=<ModifyIndex>`, so a contended write
has exactly one winner.

## Security
- Use an `https://` address in production. The gateway refuses to boot a
  non-`single_node` coordinator on a plaintext transport unless
  `cluster.allow_insecure_transport: true` is set explicitly, which is intended
  for local development and CI only.
- Supply the ACL token through the environment (`${env.CONSUL_TOKEN}`) or a
  secret provider rather than committing it to the config artifact, and scope
  the token to the `kv_prefix` this deployment owns.
- Run each deployment sharing one Consul cluster under a distinct `kv_prefix`,
  fenced by a Consul ACL path policy, so deployments cannot read or overwrite
  each other's coordination state.
- Coordinator-backed capability state can additionally be sealed at the
  application layer with `cluster.state_encryption_key_env`, which names the
  environment variable holding the key; keys and topics stay cleartext for
  routing while values are encrypted.

## Build
`cdylib-export` is enabled by default, so the plain build already produces the
loadable artifact. Disable the default features when linking this crate as an
rlib path dependency alongside other plugins, so the workspace build does not
link two `mcpg_plugin_register` exports.

```bash
cargo build -p mcpg-plugin-cluster-consul --features cdylib-export --release   # → target/release/libmcpg_plugin_cluster_consul.so
```

## Testing
The unit suite is offline:

```bash
cargo test -p mcpg-plugin-cluster-consul --lib
```

The integration suites need a Docker daemon. They boot a dev-mode Consul agent
and run both this plugin's own key/value tests and the shared coordinator
equivalence suite — the same suite every other coordinator runs, which is what
proves the backends behave identically:

```bash
cargo test -p mcpg-plugin-cluster-consul --features integration-tests
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- <https://mcpg.dev/docs/self-hosting/clustering> — the coordinator model, the primitive-inheritance rules, and every backend's keys.
- <https://mcpg.dev/docs/plugins/plugins-and-protocol> — plugin classes, the ABI, and how the gateway loads them.
- `libs/plugins/cluster/nats`, `libs/plugins/cluster/etcd`, `libs/plugins/cluster/redis` — the sibling coordinators.
