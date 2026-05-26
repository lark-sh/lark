#!/usr/bin/env bash
#
# Lark on Fly.io
#
# Deploys a public, internet-accessible Lark stack: one lark-edge (gateway) +
# one lark-server (engine), on Fly Machines with local-NVMe volumes, a shared
# secret, a dedicated IPv4, and Fly-terminated wildcard TLS.
#
# Config — set as env vars, or the script prompts for anything missing:
#   LARK_FLY_PREFIX   App-name prefix. Fly app names are GLOBALLY unique, so
#                     pick something of your own (e.g. "acme-lark" → creates
#                     "acme-lark-edge" and "acme-lark-server").
#   LARK_FLY_REGION   Fly region (e.g. "iad"). See `fly platform regions`.
#   LARK_FLY_DOMAIN   The domain clients use (e.g. "db.example.com"). You must
#                     control its DNS. Clients connect to <project>.<domain>.
#   LARK_FLY_ORG      Fly org slug (optional; omit to use your personal org).
#
# Requires: flyctl installed and `fly auth login` done; a domain you control;
# `openssl`.

set -euo pipefail

# ── locate the repo root (two levels up from this script) ────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GEN_DIR="$SCRIPT_DIR/.gen"

step() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
run()  { printf '  \033[2m$ %s\033[0m\n' "$*"; "$@"; }

# ── preflight ────────────────────────────────────────────────────────────────
command -v fly >/dev/null 2>&1 || { echo "❌ flyctl not found — install: brew install flyctl"; exit 1; }
command -v openssl >/dev/null 2>&1 || { echo "❌ openssl not found"; exit 1; }
fly auth whoami >/dev/null 2>&1 || { echo "❌ not logged in — run: fly auth login"; exit 1; }

# ── gather config ────────────────────────────────────────────────────────────
PREFIX="${LARK_FLY_PREFIX:-}"
REGION="${LARK_FLY_REGION:-}"
DOMAIN="${LARK_FLY_DOMAIN:-}"
ORG="${LARK_FLY_ORG:-}"

[ -z "$PREFIX" ] && read -rp "App-name prefix (globally unique, e.g. acme-lark): " PREFIX
[ -z "$REGION" ] && read -rp "Fly region (e.g. iad): " REGION
[ -z "$DOMAIN" ] && read -rp "Your domain for clients/admin (e.g. db.example.com): " DOMAIN

EDGE_APP="${PREFIX}-edge"
SERVER_APP="${PREFIX}-server"
ORG_FLAG=(); [ -n "$ORG" ] && ORG_FLAG=(--org "$ORG")

cat <<EOF

  Edge app:    $EDGE_APP   (public gateway)
  Server app:  $SERVER_APP   (private engine)
  Region:      $REGION
  Domain:      $DOMAIN  → clients reach <project>.$DOMAIN, admin at $DOMAIN/admin/
EOF
read -rp $'\nProceed? [y/N] ' yn; [[ "$yn" =~ ^[Yy] ]] || { echo "aborted."; exit 1; }

# ── generate per-deployment fly.toml copies (substituting names/region/domain) ─
step "Generating config in $GEN_DIR"
mkdir -p "$GEN_DIR"
GEN_SERVER="$GEN_DIR/${SERVER_APP}.fly.toml"
GEN_EDGE="$GEN_DIR/${EDGE_APP}.fly.toml"
sed -e "s/lark-server/${SERVER_APP}/g" \
    -e "s/lark-edge/${EDGE_APP}/g" \
    -e "s/primary_region = \"iad\"/primary_region = \"${REGION}\"/" \
    "$SCRIPT_DIR/lark-server/fly.toml" > "$GEN_SERVER"
sed -e "s/lark-server/${SERVER_APP}/g" \
    -e "s/lark-edge/${EDGE_APP}/g" \
    -e "s/primary_region = \"iad\"/primary_region = \"${REGION}\"/" \
    -e "s/db\.example\.com/${DOMAIN}/g" \
    "$SCRIPT_DIR/lark-edge/fly.toml" > "$GEN_EDGE"
echo "  wrote $GEN_SERVER"
echo "  wrote $GEN_EDGE"

# small helpers so the script is re-runnable
app_exists() { fly apps list 2>/dev/null | awk '{print $1}' | grep -qx "$1"; }
vol_exists() { fly volumes list -a "$1" 2>/dev/null | grep -q "$2"; }
has_v4()     { fly ips list -a "$1" 2>/dev/null | grep -qiE 'v4'; }
cert_exists(){ fly certs list -a "$1" 2>/dev/null | awk '{print $1}' | grep -Fxq "$2"; }  # exact host: "*.d" != "d"

# Grab the first-boot admin password from the edge logs (only printed once, on
# the very first boot when the accounts table is empty). Best-effort: tails recent
# logs into a temp file, polls for the banner line, then stops.
capture_admin_password() {
  local app="$1" logf pid pw i
  logf="$(mktemp)"
  fly logs -a "$app" >"$logf" 2>&1 &
  pid=$!
  for i in $(seq 1 25); do
    if grep -q "Temporary password:" "$logf" 2>/dev/null; then break; fi
    sleep 1
  done
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
  # Plain-text banner first ("Temporary password:  <pw>"), then JSON field fallback.
  pw="$(grep "Temporary password:" "$logf" 2>/dev/null | head -1 | sed -E 's/.*Temporary password:[[:space:]]*//' | awk '{print $1}' || true)"
  [ -z "$pw" ] && pw="$(grep -o '"temporary_password":"[^"]*"' "$logf" 2>/dev/null | head -1 | sed -E 's/.*:"//; s/"//' || true)"
  rm -f "$logf"
  printf '%s' "$pw"
}

# ── apps ─────────────────────────────────────────────────────────────────────
step "Creating apps (if needed)"
app_exists "$SERVER_APP" || run fly apps create "$SERVER_APP" ${ORG_FLAG[@]+"${ORG_FLAG[@]}"}
app_exists "$EDGE_APP"   || run fly apps create "$EDGE_APP"   ${ORG_FLAG[@]+"${ORG_FLAG[@]}"}

# ── shared secret ─────────────────────────────────────────────────────────────
step "Setting a shared SERVER_SECRET on both apps"
SECRET="$(openssl rand -hex 32)"
run fly secrets set "SERVER_SECRET=$SECRET" -a "$SERVER_APP" --stage
run fly secrets set "SERVER_SECRET=$SECRET" -a "$EDGE_APP"   --stage

# ── volumes (local NVMe, one per app) ─────────────────────────────────────────
step "Creating volumes (local NVMe)"
vol_exists "$SERVER_APP" lark_server_data || run fly volumes create lark_server_data -a "$SERVER_APP" --region "$REGION" --size 10 --yes
vol_exists "$EDGE_APP"   lark_edge_data    || run fly volumes create lark_edge_data   -a "$EDGE_APP"   --region "$REGION" --size 1  --yes

# ── dedicated IPv4 for the public edge ────────────────────────────────────────
step "Allocating a dedicated IPv4 for the edge"
has_v4 "$EDGE_APP" || run fly ips allocate-v4 -a "$EDGE_APP"

# ── deploy (server first so it's up when the edge starts) ─────────────────────
step "Deploying $SERVER_APP (private engine)"
( cd "$REPO_ROOT" && run fly deploy . --config "$GEN_SERVER" --dockerfile server/Dockerfile )

step "Deploying $EDGE_APP (public gateway)"
( cd "$REPO_ROOT" && run fly deploy . --config "$GEN_EDGE" --dockerfile edge/Dockerfile )

# ── TLS certs (wildcard for client DB hostnames + apex for admin) ─────────────
step "Adding TLS certificates"
cert_exists "$EDGE_APP" "*.$DOMAIN" || run fly certs add "*.$DOMAIN" -a "$EDGE_APP"
cert_exists "$EDGE_APP" "$DOMAIN"   || run fly certs add "$DOMAIN"   -a "$EDGE_APP"

# ── first-boot admin credentials ──────────────────────────────────────────────
step "Reading the first-boot admin password from $EDGE_APP logs"
TEMP_PW="$(capture_admin_password "$EDGE_APP")"

# ── final instructions ────────────────────────────────────────────────────────
# Pull the dedicated IPv4 out of the (box-drawing) table by matching the dotted
# quad directly; skip any "shared" line so we get the dedicated address.
EDGE_IP="$(fly ips list -a "$EDGE_APP" 2>/dev/null | grep -iv shared | grep -oE '([0-9]{1,3}\.){3}[0-9]{1,3}' | head -1 || true)"
if [ -n "$TEMP_PW" ]; then
  CRED_BLOCK="$(printf '   Email:     admin@local\n   Password:  %s     ← only printed once; save it now' "$TEMP_PW")"
else
  CRED_BLOCK="   (couldn't read it automatically — run:
      fly logs -a ${EDGE_APP} | grep -i 'temporary password'
    It's only printed on the very first boot.)"
fi
cat <<EOF

✅ Apps deployed.

ADMIN LOGIN (for https://${DOMAIN}/admin/ once DNS + cert are ready):
${CRED_BLOCK}

Two manual steps remain (DNS — the script can't touch your DNS provider):

1. Add these records at your DNS host for ${DOMAIN}:
     A     ${DOMAIN}      → ${EDGE_IP:-<run: fly ips list -a $EDGE_APP>}
     A     *.${DOMAIN}    → ${EDGE_IP:-<same IP>}
   If your DNS is on Cloudflare, set these to DNS-only (grey cloud), not proxied.

2. Get the DNS records the wildcard cert needs, and add them:
     fly certs setup "*.${DOMAIN}" -a ${EDGE_APP}
   (follow the records it prints, including the _acme-challenge entry)

Then, once the cert is issued (re-run that 'fly certs setup' to check status):
   • Dashboard:  https://${DOMAIN}/admin/   (log in with the credentials above)
   • Create a project there, then read/write over REST, e.g.:
     curl -X PUT -d '{"hello":"world"}' https://<project>.${DOMAIN}/demo.json
     curl https://<project>.${DOMAIN}/demo.json

Tear down everything with:
   fly apps destroy ${EDGE_APP} ${SERVER_APP}
EOF
