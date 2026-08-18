#!/usr/bin/env bash
# R4 content-time activation runner (main ranked feeds) -- receipted, owner-gated apply of the four-row atomic catalog
# change, modelled on the R2 Phase-3 bulk mechanism. Runs ON THE HOST as root (sudo -n bash ...) because it needs
# /etc/newsflows/secrets/feedgen.env (never printed) and the bskyops tool. Every step writes into $E (root:newsflows 0640).
#   env: E TREE_CATALOG(candidate publishers.yml path == merged origin/main copy) PACKET_SHA EXPECTED_UPDATE_RKEYS(csv)
#        EXPECTED_CLOCK EXPECTED_EXPIRY EXPECTED_FLOOR EXPECTED_CONTRACT FEEDGEN_URL(http://127.0.0.1:3020)
#        EFFECTIVE_CONFIG_JSON(/opt/newsflows/blueskyranker_v2/logs/effective_config_readback.json)
#   sub: preview | apply | parity | rollback-dryrun | rollback | battery <label>
set -euo pipefail
: "${E:?}"; : "${TREE_CATALOG:?}"; : "${PACKET_SHA:?}"; : "${EXPECTED_UPDATE_RKEYS:?}"; : "${EXPECTED_CLOCK:=content_time_v1}"; : "${EXPECTED_EXPIRY:?}"; : "${EXPECTED_FLOOR:?}"; : "${EXPECTED_CONTRACT:=newsflows-content-time/v2}"
FEEDGEN_URL="${FEEDGEN_URL:-http://127.0.0.1:3020}"; EFFECTIVE_CONFIG_JSON="${EFFECTIVE_CONFIG_JSON:-/opt/newsflows/blueskyranker_v2/logs/effective_config_readback.json}"
BSKYOPS="${BSKYOPS:-bskyops}"; ENV_FILE="${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}"
log() { echo "[r4] $*" >&2; }; die() { echo "[r4] STOP: $*" >&2; exit 2; }; ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
emit() { local n=$1; [[ -e "$E/$n" ]] && die "refusing to overwrite $E/$n"; install -o root -g newsflows -m 640 /dev/stdin "$E/$n"; log "wrote $n"; }
[[ $(id -u) -eq 0 ]] || die "run as root (sudo -n)"
[[ -d "$E" ]] || install -d -o root -g newsflows -m 750 "$E"
load_keys() { set -a; . "$ENV_FILE"; set +a; : "${FEEDGEN_ADMIN_API_KEY:?}"; HDR=$(mktemp); chmod 600 "$HDR"; printf 'api-key: %s\n' "$FEEDGEN_ADMIN_API_KEY" > "$HDR"; unset FEEDGEN_ADMIN_API_KEY; }
psqlq() { docker exec -i feedgen-db psql -U feedgen -d feedgen-db -X -A -F '|' -v ON_ERROR_STOP=1 "$@"; }
sync_packet() {  # -> $1 (json path)
  "$BSKYOPS" ecosystem desired-state feedgen-sync-packet --active-only --catalog-yaml "$TREE_CATALOG" --feedgen-url "$FEEDGEN_URL" --bsr-effective-config-json "$EFFECTIVE_CONFIG_JSON" --json > "$1"
}
gate_packet() {  # <packet.json>: exactly the expected rows x fields, symmetric rollback
  local f=$1
  node -e '
const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const [rkeys,clock,expiry,floor,contract]=process.argv.slice(2);const exp=rkeys.split(",").sort();
const fail=(m)=>{console.error("gate: "+m);process.exit(2)};
if(j.status!=="ready-for-apply"||!(j.atomic_change_set&&j.atomic_change_set.atomic===true))fail("status "+j.status);
const b=j.atomic_change_set.request_body,rb=j.atomic_change_set.rollback_request_body;
const got=b.updates.map(u=>u.rkey).sort();if(JSON.stringify(got)!==JSON.stringify(exp))fail("rkeys "+got);
for(const u of b.updates){const k=Object.keys(u).filter(x=>!["op","rkey","if_current"].includes(x)).sort();
 if(JSON.stringify(k)!==JSON.stringify(["content_time_contract_version","content_time_cutover_min_valid_share","publisher_time_clock","publisher_time_transition_expires_at"]))fail(u.rkey+" fields "+k);
 if(u.publisher_time_clock!==clock||u.publisher_time_transition_expires_at!==expiry||Number(u.content_time_cutover_min_valid_share)!==Number(floor)||u.content_time_contract_version!==contract)fail(u.rkey+" values");
 if(u.if_current.publisher_time_clock!=="receipt_time")fail(u.rkey+" if_current clock");}
if(!rb||rb.updates.length!==b.updates.length)fail("rollback size");
for(const u of rb.updates){if(u.publisher_time_clock!=="receipt_time"||u.if_current.publisher_time_clock!==clock)fail("rollback "+u.rkey)}
if((j.blockers||[]).length||(j.findings||[]).length)fail("blockers/findings");
console.log("gate ok updates="+b.updates.length);' "$f" "$EXPECTED_UPDATE_RKEYS" "$EXPECTED_CLOCK" "$EXPECTED_EXPIRY" "$EXPECTED_FLOOR" "$EXPECTED_CONTRACT"
}
cmd_preview() {
  load_keys; local p; p=$(mktemp); sync_packet "$p"; gate_packet "$p"
  emit feedgen-sync-packet-preview.json < "$p"; sha256sum "$E/feedgen-sync-packet-preview.json" | emit feedgen-sync-packet-preview.sha256
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log(JSON.stringify(j.atomic_change_set.request_body))' "$p" | emit feedgen-bulk-request.json
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log(JSON.stringify(j.atomic_change_set.rollback_request_body))' "$p" | emit feedgen-bulk-rollback.json
  # per-row dry-run against the running feedgen (read-only)
  local n=0; while IFS= read -r u; do local rk; rk=$(node -e 'console.log(JSON.parse(process.argv[1]).rkey)' "$u"); curl --fail -sS --max-time 30 -H "@$HDR" -H 'content-type: application/json' --data-binary "$u" "$FEEDGEN_URL/api/admin/feed_catalog/dry-run" | emit "dry-run-$rk.json"; node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));if(!(j.mode==="dry-run"&&j.status==="dry-run"&&(j.blockers||[]).length===0&&j.would_write===false))process.exit(2)' "$E/dry-run-$rk.json" || die "dry-run $rk not clean"; n=$((n+1)); done < <(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));for(const u of j.updates)console.log(JSON.stringify(u))' "$E/feedgen-bulk-request.json")
  { echo "generated_at=$(ts)"; echo "packet_sha256=$PACKET_SHA"; echo "candidate_catalog_sha256=$(sha256sum "$TREE_CATALOG" | cut -d' ' -f1)"; echo "request_sha256=$(sha256sum "$E/feedgen-bulk-request.json" | cut -d' ' -f1)"; echo "rollback_sha256=$(sha256sum "$E/feedgen-bulk-rollback.json" | cut -d' ' -f1)"; echo "dry_runs=$n"; echo "feedgen_image=$(docker inspect feedgen --format '{{.Image}}')"; echo "expected=$EXPECTED_UPDATE_RKEYS clock=$EXPECTED_CLOCK expiry=$EXPECTED_EXPIRY floor=$EXPECTED_FLOOR contract=$EXPECTED_CONTRACT"; } | emit preview-summary.txt
  psqlq -c "SELECT rkey, catalog_revision, publisher_time_clock, publisher_time_transition_expires_at, content_time_cutover_min_valid_share, content_time_contract_version FROM feedgen_ops.feed_catalog WHERE enabled ORDER BY 1" | emit catalog-prestate.tsv
  rm -f "$p" "$HDR"; log "preview ok"
}
cmd_apply() {
  load_keys; [[ -f "$E/feedgen-bulk-request.json" && -f "$E/preview-summary.txt" ]] || die "run preview first"
  # the request body must be byte-identical to the previewed one and the live rows must still match if_current (fresh packet == same body)
  local p; p=$(mktemp); sync_packet "$p"; gate_packet "$p"
  local fresh; fresh=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log(JSON.stringify(j.atomic_change_set.request_body))' "$p" | sha256sum | cut -d' ' -f1); local prev; prev=$(sha256sum "$E/feedgen-bulk-request.json" | cut -d' ' -f1)
  [[ "$fresh" == "$prev" ]] || die "live rows changed since preview (request body hash $fresh != $prev) -- re-preview and re-approve"
  local rid; rid=$prev; local resp; resp=$(mktemp); local http
  http=$(curl -sS --max-time 60 -H "@$HDR" -H "x-feedgen-actor: approved-activation-packet" -H "x-feedgen-source: $PACKET_SHA" -H "x-feedgen-request-id: $rid" -H 'content-type: application/json' --data-binary "@$E/feedgen-bulk-request.json" --output "$resp" --write-out '%{http_code}' "$FEEDGEN_URL/api/admin/feed_catalog/bulk") || true
  emit feedgen-bulk-response.json < "$resp"; { echo "applied_at=$(ts)"; echo "http=$http"; echo "request_id=$rid"; echo "packet_sha256=$PACKET_SHA"; } | emit apply-transport.txt
  [[ "$http" == "200" ]] || die "bulk apply http=$http (see feedgen-bulk-response.json); if ambiguous, run parity before deciding"
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const n=Number(process.argv[2]);if(!(j.status==="applied"&&j.applied===true&&j.result_count===n&&j.results.length===n))process.exit(2)' "$resp" "$(echo "$EXPECTED_UPDATE_RKEYS" | tr ',' '\n' | wc -l | tr -d ' ')" || die "bulk response not a clean apply"
  psqlq -c "SELECT rkey, catalog_revision, publisher_time_clock, publisher_time_transition_expires_at, content_time_cutover_min_valid_share, content_time_contract_version FROM feedgen_ops.feed_catalog WHERE enabled ORDER BY 1" | emit catalog-poststate.tsv
  psqlq -c "SELECT rkey, catalog_revision, source, changed_at FROM feedgen_ops.feed_catalog_history WHERE rkey = ANY(string_to_array('$EXPECTED_UPDATE_RKEYS', ',')) ORDER BY changed_at DESC LIMIT 12" | emit catalog-history.tsv 2>/dev/null || true
  (docker logs feedgen --since 2m 2>&1 | grep -i "feed_catalog_changed\|catalog" | tail -20 || true) | emit feedgen-log-notify.txt
  rm -f "$p" "$resp" "$HDR"; log "apply ok"
}
cmd_parity() { load_keys; "$BSKYOPS" ecosystem desired-state feedgen-parity --active-only --catalog-yaml "$TREE_CATALOG" --feedgen-url "$FEEDGEN_URL" --bsr-effective-config-json "$EFFECTIVE_CONFIG_JSON" --json | emit "feedgen-parity-$(date -u +%H%M%S).json"; rm -f "$HDR"; }
cmd_rollback_dryrun() { load_keys; local n=0; while IFS= read -r u; do local rk; rk=$(node -e 'console.log(JSON.parse(process.argv[1]).rkey)' "$u"); curl --fail -sS --max-time 30 -H "@$HDR" -H 'content-type: application/json' --data-binary "$u" "$FEEDGEN_URL/api/admin/feed_catalog/dry-run" | emit "rollback-dry-run-$rk-$(date -u +%H%M%S).json"; n=$((n+1)); done < <(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));for(const u of j.updates)console.log(JSON.stringify(u))' "$E/feedgen-bulk-rollback.json"); rm -f "$HDR"; log "rollback dry-run ok ($n rows)"; }
cmd_rollback() { load_keys; local resp; resp=$(mktemp); local http; http=$(curl -sS --max-time 60 -H "@$HDR" -H "x-feedgen-actor: approved-activation-packet-rollback" -H "x-feedgen-source: $PACKET_SHA" -H "x-feedgen-request-id: rollback-$(sha256sum "$E/feedgen-bulk-rollback.json" | cut -c1-64)" -H 'content-type: application/json' --data-binary "@$E/feedgen-bulk-rollback.json" --output "$resp" --write-out '%{http_code}' "$FEEDGEN_URL/api/admin/feed_catalog/bulk") || true; emit "feedgen-bulk-rollback-response-$(date -u +%H%M%S).json" < "$resp"; echo "rolled_back_at=$(ts) http=$http" | emit "rollback-transport-$(date -u +%H%M%S).txt"; [[ "$http" == "200" ]] || die "rollback http=$http"; psqlq -c "SELECT rkey, catalog_revision, publisher_time_clock FROM feedgen_ops.feed_catalog WHERE enabled ORDER BY 1" | emit "catalog-after-rollback-$(date -u +%H%M%S).tsv"; rm -f "$resp" "$HDR"; log "rollback applied"; }
cmd_battery() {  # <label>: BSR contract per feed, health composite, served assumptions, served shadow, latency, conformance
  local L=${1:?label}
  psqlq -c "SELECT r.ranker_id, r.updated_at, (r.config_json::jsonb)->'feed_time_contract' AS feed_time_contract FROM ranker_prod.ranker r WHERE r.ranker_id IN ('main','dispatch:main') OR (r.config_json::jsonb)->'feed_time_contract' IS NOT NULL ORDER BY r.updated_at DESC LIMIT 6" 2>/dev/null | emit "battery-$L-bsr-contract.tsv" || true
  psqlq -v rkeys="$EXPECTED_UPDATE_RKEYS" -f "$(dirname "$0")/served_content_shadow.sql" | emit "battery-$L-served-shadow.tsv"
  DOCKER="docker" bash "$(dirname "$0")/served_latency_from_logs.sh" "${SINCE_ACTIVATION:-2h}" "$EXPECTED_UPDATE_RKEYS" | emit "battery-$L-latency.json"
  local rk; for rk in $(echo "$EXPECTED_UPDATE_RKEYS" | tr ',' ' '); do local did; did=$(psqlq -t -c "SELECT publisher_did FROM feedgen_ops.feed_catalog WHERE rkey='$rk'"); (python3 -m bskyhealth.checks.feed_served_assumptions --rkey "$rk" --publisher-did "$did" --algo-policy-id ranker-priority --window-hours 3 --json-out /dev/stdout 2>/dev/null || bskyhealth check feed_served_assumptions "--rkey $rk --publisher-did $did --algo-policy-id ranker-priority --window-hours 3" 2>/dev/null || echo '{"error":"served-assumptions invocation failed"}') | emit "battery-$L-served-assumptions-$rk.json"; done
  (bskyhealth status --all 2>&1 | grep -i "readiness\|served" || true) | emit "battery-$L-health-status.txt"
  cp /var/lib/newsflows/health-checker/feed_readiness_latest.json /tmp/fr.json 2>/dev/null && node -e 'const j=JSON.parse(require("fs").readFileSync("/tmp/fr.json"));const want=process.argv[1].split(",");for(const f of j.feeds){const id=f.catalog&&f.catalog.feed_id;if(!want.includes(id))continue;const tc=(f.components||{}).time_contract||{};console.log(id,f.status,"time_contract:",tc.status,(tc.summary||"").slice(0,120),"clock:",f.catalog.publisher_time_clock,"valid_share:",f.catalog.content_time_valid_share,"floor:",f.catalog.content_time_cutover_min_valid_share,"rev:",f.catalog.catalog_revision)}console.log("generated_at",j.generated_at)' "$EXPECTED_UPDATE_RKEYS" | emit "battery-$L-readiness.txt"; rm -f /tmp/fr.json
  ("$BSKYOPS" ecosystem conformance scan --json 2>/dev/null | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);console.log(JSON.stringify(j.summary))})' || echo "conformance unavailable") | emit "battery-$L-conformance.txt"
  log "battery $L written (read the receipts; the runner does not auto-decide)"
}
case "${1:-}" in preview) cmd_preview;; apply) cmd_apply;; parity) cmd_parity;; rollback-dryrun) cmd_rollback_dryrun;; rollback) cmd_rollback;; battery) cmd_battery "${2:?label}";; *) echo "usage: $0 preview|apply|parity|rollback-dryrun|rollback|battery <label>" >&2; exit 2;; esac
