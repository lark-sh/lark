# Observability

How to get logs and metrics out of a self-hosted Lark deployment.

Lark gives you two observability primitives:

- **Logs** — both binaries write structured logs to **stdout**. Point `docker logs` / journald /
  your log aggregator at them.
- **Metrics** — `lark-server` tracks per-database usage (writes, reads, bytes, CCU, latency, on-disk
  size, etc.) and the admin dashboard charts it. In the bundled deployments this **works out of the box**;
  larger or bare-metal setups have a couple of options, covered below.

---

## Metrics work out of the box (containers + Fly quickstart)

In the bundled `docker-compose.yml` and the Fly quickstart, `lark-server` **pushes its metrics directly
to `lark-edge`** over the internal network.

```bash
make up
# → http://localhost:8080/admin/  → Monitoring
```

The flow:

```
 lark-server ──(every ~60s, per active database)──► lark-edge  POST /internal/metrics
   │  computes per-DB metrics, POSTs them straight                  │  aggregator: one bucket
   │  to the coordinator it already registers with                  │  per (project, database)
   │                                                                ▼
   │                                              flush every METRICS_FLUSH_INTERVAL (3m)
   │                                                                ▼
   │                                              database_metrics table (SQLite/Postgres)
   │                                                                ▼
   └─ also logs each sample to stdout (for logs)   Admin dashboard /admin/ (rolled up per project)
```

This is controlled by one environment variable on **lark-server**:

| Variable | Default | Effect |
|----------|---------|--------|
| `LARK_METRICS_PUSH` | `false` | `true` → push metrics to the coordinator (`LARK_COORDINATOR_URL`). Set to `true` in the bundled compose + Fly configs. |

`lark-server` opens a single background thread that batches metrics and POSTs them to
`<LARK_COORDINATOR_URL>/internal/metrics`. It's best-effort: if lark-edge is briefly unreachable the
samples are dropped (logged, never blocking the database).

Note that it may take ~5 minutes for the first metrics to show up in the Dashboard, this is expected. Also note that metrics are saved to the 
lark-edge backing database (SQLite or PostgreSQL), and these do add up over time.

---

## Logs

Both components log to stdout:

| Component | Format | Level control |
|-----------|--------|---------------|
| `lark-server` (Rust) | human-readable text | `RUST_LOG` (default `info`; e.g. `RUST_LOG=debug`) |
| `lark-edge` (Go) | JSON (`level`, `ts`, `message`, + fields) | `DEBUG=true` raises to debug |

---

## Larger / bare-metal setups: shipping metrics with Vector

`LARK_METRICS_PUSH` is the right answer for a single lark-server talking to a lark-edge. You may want a
log/metrics shipper like [Vector](https://vector.dev) instead when you want to:

- **Fan metrics out off-site** — into Prometheus/Grafana, Datadog, BetterStack, etc., not just Lark's
  own dashboard.
- **Run on bare metal / systemd** where you already operate a log pipeline and prefer one path for
  everything.
- **Decouple** metric delivery from the database process (buffering, retries, multiple sinks).

Because `lark-server` also writes every metric sample to **stdout** as a JSON line, a shipper can pick
them up there regardless of `LARK_METRICS_PUSH`. Each line looks like:

```json
{"type":"db_metrics","ts":1706011200,"server":"lark-server-1","core":3,
 "project":"acme","database":"production","writes":142,"reads":891,"transactions":12,
 "write_bytes":94021,"read_bytes":284729,"events_sent":2341,"ccu":23,"subscriptions":47,
 "data_size_bytes":52428800,"latency_avg_us":890,"latency_max_us":12300,
 "permission_denials":0,"size_rejections":0}
```

| Field | Meaning |
|-------|---------|
| `server`, `core`, `project`, `database` | dimensional fields; `(project, database)` identifies the database |
| `writes`, `reads`, `transactions` | operation counts in the window |
| `write_bytes` / `read_bytes` | inbound payload / outbound bytes (→ `bytes_in` / `bytes_out` once stored) |
| `events_sent` | events pushed to subscribers |
| `ccu`, `subscriptions` | concurrent connections / active subscriptions (gauges) |
| `data_size_bytes` | on-disk size of the database's blob (gauge) |
| `latency_avg_us`, `latency_max_us` | request latency (TCP receive → processing complete; excludes client network time) |
| `permission_denials`, `size_rejections` | rules rejections / oversized-payload rejections |

Samples are emitted every ~60s per **active** database (idle databases emit nothing).

### Option A — Vector to Lark's dashboard (instead of `LARK_METRICS_PUSH`)

If you'd rather Vector deliver metrics to the dashboard (e.g. for buffering across a flaky link), leave
`LARK_METRICS_PUSH=false` and have Vector POST the same payload to lark-edge's internal endpoint. The
contract: keep stdout lines where `type == "db_metrics"`, batch them into a JSON **array**, and POST to
`http://<edge-host>:<internal-port>/internal/metrics` (the internal listener — `:8081` in the bundled
compose; keep it off the public internet) with an `Authorization: Bearer <SERVER_SECRET>` header — the
endpoint is authenticated and rejects unauthenticated posts with `401`. Here's an example Vector config:

```toml
[sources.lark]
type = "journald"                 # or "docker_logs" / "file", per your setup
include_units = ["lark-server"]

[transforms.db_metrics]
type = "remap"
inputs = ["lark"]
source = '''
  parsed, err = parse_json(.message)
  if err != null || parsed.type != "db_metrics" { abort }
  . = parsed
'''

[sinks.edge]
type = "http"
inputs = ["db_metrics"]
uri = "http://lark-edge:8081/internal/metrics"
method = "post"
encoding.codec = "json"           # sends a JSON array per batch
request.headers.Authorization = "Bearer ${SERVER_SECRET}"  # required — /internal/* is authenticated
batch.max_events = 100
batch.timeout_secs = 5
```

### Option B — Vector to Prometheus / your own stack

Convert the same `db_metrics` lines into metrics tagged by `server` / `core` / `project` / `database`
and send them wherever you like, independent of Lark's dashboard:

```toml
[transforms.to_metrics]
type = "log_to_metric"
inputs = ["db_metrics"]
# … one metric per field (writes, reads, bytes, ccu, latency, data_size_bytes …),
#   with tags.server / tags.project / tags.database / tags.core

[sinks.prometheus]
type = "prometheus_exporter"      # or prometheus_remote_write to push
inputs = ["to_metrics"]
address = "0.0.0.0:9598"
```

---

## How stored values reduce (dashboard rollup)

lark-edge's aggregator keys incoming samples per `(project, database)` and, every
`METRICS_FLUSH_INTERVAL` (default `3m`, env-configurable on lark-edge), writes one `database_metrics`
row per active database. The dashboard rolls these up to project level on read. Per flush window:

| Stored field | Reduction |
|--------------|-----------|
| `bytes_in`/`bytes_out`, `writes`, `reads`, `events_sent`, `permission_denials` | **sum** |
| `peak_ccu` | **max** of the per-emit CCU samples |
| `ccu` | the **last** sample in the window |
| `data_size_bytes` | the **last** sample (gauge) |
| `p50/p99_latency_us` | averaged (approximate — display only) |

---

## Configuration summary

| Variable | Component | Default | Effect |
|----------|-----------|---------|--------|
| `LARK_METRICS_PUSH` | server | `false` | push metrics to the coordinator (set `true` in bundled deploys) |
| `RUST_LOG` | server | `info` | log verbosity/filter |
| `DEBUG` | edge | `false` | `true` enables debug logs |
| `METRICS_FLUSH_INTERVAL` | edge | `3m` | how often the aggregator writes `database_metrics` (Go duration string) |

See [DEPLOYMENT.md](./DEPLOYMENT.md) for the full environment-variable reference.
