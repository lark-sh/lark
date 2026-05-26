# Quick start: Lark on Fly.io

The fastest way to get Lark running **on the public internet** — a real,
TLS-terminated, internet-reachable deployment you can point app clients at. (For
local development and contributing, use `make up` from the repo root instead; for
production hardening and scaling, see [`docs/DEPLOYMENT.md`](../../docs/DEPLOYMENT.md).)

This deploys a **Tier 1** stack: one `lark-edge` (public gateway) + one
`lark-server` (engine), on Fly Machines with local-NVMe volumes and Fly-terminated
wildcard TLS.

## What you need

- [`flyctl`](https://fly.io/docs/flyctl/install/) installed, and `fly auth login` done.
- **A domain you control the DNS for.** This is not optional: Lark routes each
  database by hostname (`<project>.your-domain`), so clients must be able to
  resolve a wildcard at your domain. (The free `*.fly.dev` hostname can't do
  per-database subdomains.)
- `openssl` (for generating the shared secret).

## Run it

```bash
deploy/fly/quickstart.sh
```

It prompts for (or reads from env vars) an app-name **prefix** (Fly app names are
globally unique, so pick your own — e.g. `acme-lark`), a **region**, and your
**domain**. Then it creates both apps, a shared `SERVER_SECRET`, the volumes, a
dedicated IPv4, deploys both services, and adds the TLS certs — echoing every
`fly` command as it goes. Non-interactive:

```bash
LARK_FLY_PREFIX=acme-lark LARK_FLY_REGION=iad LARK_FLY_DOMAIN=db.example.com \
  deploy/fly/quickstart.sh
```

The script then prints the **two DNS records** to add (it can't touch your DNS
provider) and the ACME validation step. Once the wildcard cert reports issued:

- **Dashboard:** `https://<your-domain>/admin/` (first-boot admin password:
  `fly logs -a <prefix>-edge | grep temporary_password`).
- **Read/write** against `https://default--default.<your-domain>/.json` — standard
  Firebase-style REST.
- **Connect via WS** to `https://default--default.<your-domain>` - standard Firebase or Lark SDK clients

Tear it all down with `fly apps destroy <prefix>-edge <prefix>-server`.

## What it does

```bash
PREFIX=acme-lark REGION=iad DOMAIN=db.example.com
EDGE=$PREFIX-edge SERVER=$PREFIX-server

# apps + shared secret
fly apps create "$SERVER" && fly apps create "$EDGE"
SECRET=$(openssl rand -hex 32)
fly secrets set SERVER_SECRET="$SECRET" -a "$SERVER"
fly secrets set SERVER_SECRET="$SECRET" -a "$EDGE"

# volumes (local NVMe), dedicated IPv4 for the edge
fly volumes create lark_server_data -a "$SERVER" --region "$REGION" --size 10 --yes
fly volumes create lark_edge_data   -a "$EDGE"   --region "$REGION" --size 1  --yes
fly ips allocate-v4 -a "$EDGE"

# deploy (edit the fly.tomls for your app names/domain first, or use the script)
fly deploy . --config deploy/fly/lark-server/fly.toml --dockerfile server/Dockerfile
fly deploy . --config deploy/fly/lark-edge/fly.toml   --dockerfile edge/Dockerfile

# TLS: wildcard for client DB hostnames + apex for the admin dashboard
fly certs add "*.$DOMAIN" -a "$EDGE"
fly certs add "$DOMAIN"   -a "$EDGE"
fly certs setup "*.$DOMAIN" -a "$EDGE"   # prints the DNS validation record
```

Then add the DNS records (`A $DOMAIN` and `A *.$DOMAIN` → the edge's IPv4, plus
the `_acme-challenge` record).

## How it works (and a few Fly-specific gotchas)

- **Two apps, not one.** `lark-edge` (Go) and `lark-server` (Rust) are different
  images, so they're separate Fly apps that talk over Fly's private network.
- **Storage is local NVMe.** Each app gets a Fly Volume — a slice of NVMe on the
  same host, exclusively owned, which is exactly what `lark-server` needs. 
  Fly encrypts volumes at rest.
- **IPv6 private network.** Fly app-to-app traffic is IPv6 (6PN). `lark-server`
  registers `lark-server.internal:2727` for the edge to dial, and listens on IPv6
  via `LARK_PROXY_BIND=[::]` (it defaults to `0.0.0.0`/IPv4-only, which the edge
  can't reach over 6PN). Both are set in `lark-server/fly.toml`.
- **TLS is Fly-terminated.** `lark-edge` runs `DISABLE_TLS=true`; Fly terminates
  TLS at its edge using the certs from `fly certs add` and forwards plain HTTP
  (WebSocket upgrades pass through). No CertMagic / Cloudflare token needed.
- **Wildcard cert.** Clients hit `<project>.<domain>`, so the cert must cover
  `*.<domain>`. Fly issues this via DNS-01. **If your DNS is on Cloudflare, keep
  the records DNS-only (grey cloud)**,
- **WebTransport/UDP is skipped** for simplicity — clients use WebSocket, which
  works fine through Fly's HTTP path. WebTransport on Fly needs a dedicated IPv4 +
  `fly-global-services` binding; add it later if you want it.
