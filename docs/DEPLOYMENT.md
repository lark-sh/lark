# Production Deployment

The [README quick start](../README.md#quick-start) (`make up`) is a good starting point for local
development and testing. However, if you want to run Lark in a production environment, you'll
need to do a little more work. There are three tiers of deployment, in increasing order of
complexity:

1. **[Tier 1 — Single host, hardened](#tier-1--single-host-hardened):** one machine,
  one `lark-edge` + one `lark-server`, real TLS, real secrets, persistent storage,
  backups. Suitable for small-to-medium production loads.
2. **[Tier 2 — Scale the gateway](#tier-2--scale-the-gateway):** *multiple
  `lark-edge` gateways, a single `lark-server`*, with a shared Postgres control
  plane. This scales client connections and gives you gateway-level redundancy
  while keeping the data tier simple — **one data directory**. This is the "larger
  deployment" most people actually need, and it's the ceiling this guide covers
  concretely. In practice with a large enough bare metal server you can handle up to 
  50k CCU with this tier.
3. **[Tier 3 — Scale the data tier](#tier-3--scale-the-data-tier):** *multiple
  `lark-server` nodes.* This is a step-change in operational complexity, because
  each `lark-server` owns its **own local data directory** and there is no shared
  storage. Covered conceptually only, beyond the scope of this document.

---

## Architecture

Lark is two main pieces plus a control plane. The diagram below shows a **Tier 2**
deployment — multiple `lark-edge` gateways in front of a single `lark-server` —
which is the shape most production deployments take:

```
                       Clients (browsers, SDKs)
                                │
                HTTPS (TCP) + WebTransport (UDP)
               ┌────────────────┴────────────────┐
               ▼                                  ▼
     ┌──────────────────┐    ···    ┌──────────────────┐
     │     lark-edge    │           │     lark-edge    │   Public tier — N gateways
     │   (Go gateway)   │           │   (Go gateway)   │   behind DNS RR / a load
     │   coordinator    │           │   coordinator    │   balancer, all sharing
     │  admin dashboard │           │  admin dashboard │   one SERVER_SECRET
     └─────────┬────────┘           └─────────┬────────┘
               │                              │
               └──────────────┬───────────────┘     private network
                              │
              ┌───────────────┴───────────────┐
              ▼                                ▼
    ┌──────────────────┐            ┌──────────────────┐
    │ Postgres / SQLite│            │    lark-server   │   Database tier — ONE
    │                  │            │      (Rust)      │   node (Tiers 1–2)
    │ control plane:   │            │     TCP 2727     │   - thread-per-core
    │ projects, users, │            │                  │   - WAL + blob, LOCAL disk
    │ routing, settings│            └─────────┬────────┘   - private network only
    └──────────────────┘                      ▼
     (Postgres once >1 gateway)            [ NVMe ]
```

Every `lark-edge` talks to both the shared control-plane store (for routing and
project config) and, over the private network, to the `lark-server` that owns each
database.

**`lark-edge` (the gateway / coordinator)** terminates TLS, upgrades client
connections (WebSocket / WebTransport), validates auth tokens, and routes each
database to the `lark-server` that owns it via consistent hashing. It also serves
the admin dashboard (`/admin/`) and holds all control-plane state (projects,
admin users, server routing, per-project settings) in its metadata store.

**`lark-server` (the engine)** holds the actual database contents (in-memory tree
+ WAL + blob on disk) and speaks Lark's wire protocol over plain TCP on its
`--proxy-port` (default 2727). On startup it **registers** itself with the
coordinator's internal endpoint; the coordinator then routes databases to it via
consistent hashing. Booting another `lark-server` does get it into the hash ring
automatically — but the *data does not move with it*, which is why care is needed
when moving to a Tier 3 setup.

### Storage: local disk only

This is the single most important operational fact about `lark-server`:

**Each `lark-server` reads and writes its `LARK_DATA_DIR` as exclusively-owned,
local storage — ideally local NVMe.** The blob format does many small random
reads and the engine assumes low-latency, exclusively-owned files.

- **Use local NVMe (or equivalent low-latency local block storage).**
- **Do _not_ put `LARK_DATA_DIR` on a shared/network filesystem** — CephFS, NFS,
  EBS multi-attach, GlusterFS, etc. You will have a bad time: the small-random-read
  access pattern is pathological over network storage, and Lark assumes a single
  process exclusively owns each data directory.
- A consequence: a database's data lives on exactly one node's local disk. There
  is no shared pool that any `lark-server` can serve any database from. This is
  precisely why adding/removing servers requires data migration.

### Components and ports

| Component | Setting | Default | Protocol | Exposure |
|---|---|---|---|---|
| `lark-edge` client listener | `HTTPS_LISTEN_ADDR` | `:443` | TCP | **Public** — REST + WebSocket |
| `lark-edge` WebTransport | `WT_LISTEN_ADDR` (+ `WT_PORTS`) | `:8444` | UDP | **Public** — WebTransport/QUIC |
| `lark-edge` internal listener | `INTERNAL_LISTEN_ADDR` | `:8080` | TCP | **Private** — server registration + metrics ingest |
| `lark-edge` admin dashboard | path `/admin/` on the client listener | — | TCP | protected by admin auth; restrict at network layer too |
| `lark-server` wire protocol | `--proxy-port` / `LARK_PROXY_PORT` | `2727` | TCP | **Private** — only `lark-edge` should reach it |

The two **private** rows are the critical firewall boundary: `lark-server`'s
`2727` and `lark-edge`'s internal listener must be reachable only from within your
trusted network, never from the internet. Clients only ever talk to the public
`lark-edge` listeners.

### DNS: clients connect to a per-database hostname

Lark clients connect **directly to a per-database hostname**, not to a single shared 
API endpoint. `lark-edge` figures out which project/database a connection is for by parsing the `Host` header
against `LARKDB_DOMAIN`:

- `<project>.<LARKDB_DOMAIN>` → that project's default database.
- `<database>--<project>.<LARKDB_DOMAIN>` → a specific database within a project
  (note the **double-hyphen** separator).

So with `LARKDB_DOMAIN=db.example.com`, a client that opens
`my-app.db.example.com` reaches project `my-app`. The consequence is a hard
requirement, from Tier 1 onward:

**Point a wildcard DNS record `*.<LARKDB_DOMAIN>` at your `lark-edge` gateway(s).**
The client resolves `<project>.<domain>` on its own and connects there, so that
name *must* land on a gateway. With a single gateway (Tier 1) that's one wildcard
A/AAAA record. With multiple gateways (Tier 2), use **round-robin DNS** — one
A/AAAA record per gateway public IP on the same `*.<LARKDB_DOMAIN>` name.

Your TLS certificate has to cover that wildcard too — but you normally don't
configure that separately. When CertMagic is enabled and you leave
`CERTMAGIC_DOMAINS` **unset**, it automatically manages certs for both
`*.<LARKDB_DOMAIN>` (the per-database client hostnames) **and** the apex
`<LARKDB_DOMAIN>` (where the admin dashboard is served, `<LARKDB_DOMAIN>/admin/`).
So in the common case you only set `LARKDB_DOMAIN`.

Set `CERTMAGIC_DOMAINS` explicitly only to manage *additional* names — e.g. a
separate API hostname. Note that doing so **replaces** the auto-derived list
rather than adding to it, so you must include `*.<LARKDB_DOMAIN>` and the apex
yourself. (Lark's own cloud does this: `LARKDB_DOMAIN=larkdb.net` with
`CERTMAGIC_DOMAINS="db.lark.sh,*.larkdb.net"` to add the `db.lark.sh` API host.)

Wildcard issuance uses the **DNS-01** challenge, and this build wires
**Cloudflare** specifically — `CLOUDFLARE_API_TOKEN` is **required** whenever
CertMagic is enabled. If your DNS isn't on Cloudflare, supply a wildcard cert via
`TLS_CERT_FILE` / `TLS_KEY_FILE` or terminate TLS at a load balancer instead.

### The admin dashboard

`lark-edge` serves an admin dashboard and API under `/admin/`, gated by
`ADMIN_API_ENABLED` (when off, the routes and SPA aren't mounted at all). Two
operational points:

- **Treat it as internal tooling.** The OSS admin panel is meant for operators —
  it is *not* designed as a self-service end-user control surface.
  Even though it has its own authentication, it's best to keep it off the public internet: put
  it behind your VPN / management network / an IP allowlist.
- **With multiple gateways, enable it on exactly one.** Run `ADMIN_API_ENABLED=true`
  on a single designated gateway — ideally one kept out of the public
  client-traffic rotation — and `false` on the rest. You don't lose anything by
  doing so: admin writes land in the shared metadata store and propagate to every
  gateway via Postgres `NOTIFY` (`project_config_changed` / `database_evicted`),
  so the other gateways pick up config and routing changes automatically.

### The metadata store: SQLite vs. Postgres

`lark-edge`'s control-plane state lives in `DATABASE_URL`:

- **SQLite** (`sqlite:///data/lark.db`) — the default. Fine for **a single
  `lark-edge` instance**. Simplest to operate; state is one file.
- **Postgres** (`postgres://…`) — **required once you run more than one
  `lark-edge`**, because the coordinators share project/routing/user state. The
  moment you want a second gateway (for HA or to spread client load), switch to
  Postgres. This is the single most important config change going from Tier 1 to
  Tier 2.

---

## Tier 1 — Single host, hardened

> **Fastest path:** if you just want a hosted Tier 1 deployment up quickly,
> [`deploy/fly/`](../deploy/fly/README.md) does all of this on Fly.io with one
> script. The rest of this section is the general single-host recipe for any host.

Start from the bundled `docker-compose.yml`, but change the three things that make
it a dev stack:

1. **Real `SERVER_SECRET`.** The shared secret authenticating `lark-edge` ↔
   `lark-server`. The default is `dev-secret-change-me`. Generate one
   (`openssl rand -hex 32`) and set it on **both** services.
2. **Real TLS.** The dev compose runs `DISABLE_TLS: "true"` (HTTP only). For
   production, either let `lark-edge` obtain certificates automatically via
   CertMagic/Let's Encrypt, or terminate TLS at a reverse proxy / load balancer in
   front of it.
3. **Durable volumes + backups.** Make sure both the `lark-server` data dir and
   the `lark-edge` metadata store are on persistent volumes, and wire up backups
   per [BACKUP.md](BACKUP.md).

### TLS via CertMagic (automatic Let's Encrypt)

This is the example setup for running a single host in production. Note that it assumes
you will run your DNS via Cloudflare, and provides a subdomain-wildcard via CertMagic.

Set on the `lark-edge` service instead of `DISABLE_TLS`:

```yaml
environment:
  HTTPS_LISTEN_ADDR: ":443"
  WT_LISTEN_ADDR: ":8444"
  CERTMAGIC_ENABLED: "true"
  CERTMAGIC_EMAIL: "you@example.com"
  LARKDB_DOMAIN: "db.example.com"        # CertMagic auto-manages *.db.example.com + the apex
  CLOUDFLARE_API_TOKEN: "${CLOUDFLARE_API_TOKEN:?required for CertMagic DNS-01}"
  SERVER_SECRET: "${SERVER_SECRET:?set a real secret}"
  ADMIN_API_ENABLED: "true"
  DATABASE_URL: "sqlite:///data/lark.db"
```

CertMagic stores certificates under `CERTMAGIC_STORAGE` (default `./certs`) — put
that on a persistent volume so you don't re-issue on every restart. (Use
`CERTMAGIC_STAGING: "true"` while testing to avoid Let's Encrypt rate limits.) The
wildcard cert is obtained via the DNS-01 challenge, which is why
`CLOUDFLARE_API_TOKEN` is required; if your DNS isn't on Cloudflare, use
`TLS_CERT_FILE` / `TLS_KEY_FILE` with a wildcard cert instead. And remember the
matching DNS: a wildcard `*.db.example.com` record pointing at this host.

---

## Tier 2 — Scale the gateway

Tier 2 keeps **one `lark-server`** and scales out the **`lark-edge` gateway tier** for connection
capacity and redundancy. The shape:

- **Multiple `lark-edge` gateways**, all backed by the **same Postgres**, all
  sharing the **same `SERVER_SECRET`**, fronted by DNS round-robin.
- **One `lark-server`** on a **private network**, registering with the coordinator.
- A firewall that exposes only the public `lark-edge` listeners.

At this tier, `lark-server` and `lark-edge` are best run **directly on the host via systemd, not in
a container** — `lark-server` uses Linux `io_uring` (Glommio thread-per-core) and benefits
from raw `memlock` limits and CPU affinity that are awkward to grant inside a
container.

### Host requirements (`lark-server`)

- **Linux kernel ≥ 5.8** — `io_uring` support is mandatory; the process will not
  run otherwise.
- **`memlock` unlimited** — `io_uring` registers locked memory. The systemd unit
  below sets `LimitMEMLOCK=infinity`.
- **High file-descriptor limits** — many concurrent connections (`LimitNOFILE`).
- Persistent, fast storage for `LARK_DATA_DIR` (local NVMe is ideal; the blob
  format does small random reads).

### Networking and discovery

1. Put all nodes on a private network (VLAN, VPC, WireGuard/Tailscale — your
   choice).
2. Give the `lark-server` a `--private-ip` on that network. It registers with the
   coordinator as `private_ip:proxy_port`, so the coordinator (and only the
   coordinator) can reach it on `2727`.
3. Point the `lark-server` at a coordinator's **internal** endpoint via
   `--coordinator` / `LARK_COORDINATOR_URL` (e.g. `http://10.0.0.20:8080`).
   Registration and heartbeats are persisted to the shared Postgres, so **every**
   `lark-edge` learns about the server from the database — you only need to point
   it at one gateway's internal endpoint (or an internal load balancer across
   them).
4. Firewall rules:
   - **Public, on `lark-edge` only:** `HTTPS_LISTEN_ADDR` (TCP) and
     `WT_LISTEN_ADDR` (+ `WT_PORTS`, UDP).
   - **Private only:** `lark-server` `2727`, and every `lark-edge`'s
     `INTERNAL_LISTEN_ADDR`.
   - Consider restricting `/admin/` to your VPN/management network even though it
     has its own authentication.

### Reference systemd unit — `lark-server`

`/etc/systemd/system/lark-server.service` (genericized; fill in the `Environment`
values for your host):

```ini
[Unit]
Description=Lark Realtime Database Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=lark
Group=lark

# Identity and networking
Environment="LARK_SERVER_ID=db-1"
Environment="LARK_HOSTNAME=db-1.example.com"
Environment="LARK_PUBLIC_IP=203.0.113.10"
Environment="LARK_PRIVATE_IP=10.0.0.10"
Environment="LARK_PROXY_PORT=2727"
Environment="LARK_CAPACITY=10000"
Environment="LARK_DATA_DIR=/var/lib/lark/data"
# Coordinator's INTERNAL endpoint (private network)
Environment="LARK_COORDINATOR_URL=http://10.0.0.20:8080"
Environment="RUST_LOG=info"

ExecStart=/usr/local/bin/lark-server \
    --id=${LARK_SERVER_ID} \
    --hostname=${LARK_HOSTNAME} \
    --public-ip=${LARK_PUBLIC_IP} \
    --private-ip=${LARK_PRIVATE_IP} \
    --proxy-port=${LARK_PROXY_PORT} \
    --capacity=${LARK_CAPACITY} \
    --data-dir=${LARK_DATA_DIR} \
    --coordinator=${LARK_COORDINATOR_URL}

Restart=always
RestartSec=5

# io_uring needs locked memory; many connections need many fds
LimitMEMLOCK=infinity
LimitNOFILE=1000000

# Hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/lark/data

StandardOutput=journal
StandardError=journal
SyslogIdentifier=lark-server

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lark-server
sudo journalctl -u lark-server -f
```

### Reference deployment — `lark-edge` (with Postgres)

Run `lark-edge` as a container or binary. The essential environment for a
production coordinator:

```bash
HTTPS_LISTEN_ADDR=":443"
WT_LISTEN_ADDR=":8444"
INTERNAL_LISTEN_ADDR=":8080"        # private; lark-server registers here
DATABASE_URL="postgres://lark:…@db.internal:5432/lark"
SERVER_SECRET="<shared secret, identical on every node>"
ADMIN_API_ENABLED="true"            # ONE designated gateway only; "false" on the rest
LARKDB_DOMAIN="db.example.com"      # CertMagic auto-manages *.db.example.com + the apex
CERTMAGIC_ENABLED="true"
CERTMAGIC_EMAIL="you@example.com"
CLOUDFLARE_API_TOKEN="<token>"      # required when CERTMAGIC_ENABLED (DNS-01)
```

Every `lark-edge` and `lark-server` in the deployment must share the **same**
`SERVER_SECRET`. Running a second `lark-edge` is just another instance with the
same env pointed at the same Postgres — they coordinate through it — **except
`ADMIN_API_ENABLED`, which should be `true` on only one gateway** (see
[The admin dashboard](#the-admin-dashboard)). Front the gateways with round-robin
DNS or a load balancer on the wildcard hostname (see
[DNS](#dns-clients-connect-to-a-per-database-hostname)).

> **Postgres schema:** `lark-edge` manages its own schema (see
> `edge/db/postgres_schema.sql`). Point `DATABASE_URL` at an empty database and it
> initializes on first boot.

---

## Tier 3 — Scale the data tier

> **Covered conceptually.** This section explains the model and the problems you
> must solve, not a turnkey recipe — the right answer is deployment-specific.
> Exhaust vertical scaling (a bigger `lark-server` box: more cores, more RAM, more
> NVMe) before going here.

Tier 3 is running **more than one `lark-server`**. The coordinator consistent-hashes
each `(project, database)` to one server, so adding nodes spreads databases across
them — in principle, linear scale-out of total data size and write throughput.

What makes this a different class of problem from Tier 2 is the
[local-disk-only](#storage-local-disk-only) reality:

- **Each database lives on exactly one node's local disk.** There is no shared
  pool. A server can only serve databases whose data directory is physically
  present on its own NVMe.
- **Changing the node count reshuffles the hash ring.** Consistent hashing
  minimizes how many databases move when you add or remove a node, but the ones
  that move get *reassigned to a node that doesn't have their files*. Until you
  migrate those data directories, the new owner has no data for them.
- **Therefore, scaling the server count is a data-migration operation, not just a
  config change.** Roughly: identify which `{project}/{database}` directories are
  reassigned by the new topology, back them up / quiesce them, copy them to the newly-assigned 
  node, then bring that node into the ring. Done wrong, a reassigned database appears empty on its 
  new owner while its data sits stranded on the old one.
- **Backups now span N hosts.** The [BACKUP.md](BACKUP.md) procedure is per-data-
  directory; at Tier 3 you run it across every node and need a strategy that
  captures all of them (and ideally tracks which databases live where).

---

## Operational concerns

- **Backups** — see [BACKUP.md](BACKUP.md). You need both the `lark-server`
  `LARK_DATA_DIR` (database contents) and the `lark-edge` metadata store
  (projects, routing, users). Backing up only one leaves an incomplete restore.
- **Compaction** — `lark-server`'s in-process storage worker keeps the blob
  reasonably current automatically. Full re-compaction (space reclamation) is the
  separate `lark-compact` tool, run when a blob has accumulated significant wasted
  space; it coordinates with the server via the `.compacting` marker (see
  [BACKUP.md](BACKUP.md) and the README's [Storage section](../README.md#storage)).
- **Observability** — `lark-server` instances ship metrics to `lark-edge`'s
  internal endpoint; the admin dashboard surfaces per-database and per-server
  stats. Enable `--debug-timing` / `LARK_DEBUG_TIMING` for latency breakdowns when
  diagnosing.
- **Upgrades** — deploy node-by-node. The on-disk blob carries a
  `blob.generation`; a server restart re-opens its data and replays WAL forward
  (same path as restore). Roll `lark-server` nodes one at a time so the coordinator
  reroutes around each during its brief restart.
