#!/usr/bin/env bash
# R4 content-time activation runner (main ranked feeds) -- receipted, owner-gated apply of the four-row atomic catalog
# change, modelled on the R2 Phase-3 bulk mechanism. Runs ON THE HOST as root (sudo -n bash ...) because it needs
# /etc/newsflows/secrets/feedgen.env (never printed) and the bskyops tool. Every step writes into $E (root:newsflows 0640).
#   env: E PACKET_PATH (the approved packet file) EXPECTED_PACKET_SHA (from the ledger approval) TREE_CATALOG (the DEPLOYED
#        catalog /opt/newsflows/config/newsflows/catalogs/publishers.yml) CATALOG_SOURCE_SHA (merged origin/main commit whose
#        config/newsflows/catalogs/publishers.yml must be byte-identical to TREE_CATALOG; verified against ROOT_CHECKOUT)
#        EXPECTED_UPDATE_RKEYS(csv) EXPECTED_CLOCK EXPECTED_EXPIRY EXPECTED_FLOOR EXPECTED_CONTRACT (each must literally
#        appear in the approved packet) EXPECTED_TOOL_REFS (bsky-ops=<sha>,newsflows-bskyhealth=<sha>,blueskyranker=<sha>)
#        FEEDGEN_URL(http://127.0.0.1:3020) EFFECTIVE_CONFIG_JSON APPLIED_AT (battery: ISO time of the apply, from apply-transport.txt)
#   sub: preview | apply | parity | rollback-dryrun | rollback | battery <label>
set -euo pipefail
: "${E:?}"; : "${PACKET_PATH:?}"; : "${EXPECTED_PACKET_SHA:?}"; : "${TREE_CATALOG:?}"; : "${CATALOG_SOURCE_SHA:?}"; : "${EXPECTED_UPDATE_RKEYS:?}"; : "${EXPECTED_CLOCK:=content_time_v1}"; : "${EXPECTED_EXPIRY:?}"; : "${EXPECTED_FLOOR:?}"; : "${EXPECTED_CONTRACT:=newsflows-content-time/v2}"; : "${EXPECTED_TOOL_REFS:?}"
FEEDGEN_URL="${FEEDGEN_URL:-http://127.0.0.1:3020}"; EFFECTIVE_CONFIG_JSON="${EFFECTIVE_CONFIG_JSON:-/opt/newsflows/blueskyranker_v2/logs/effective_config_readback.json}"
BSKYOPS="${BSKYOPS:-bskyops}"; ENV_FILE="${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}"; ROOT_CHECKOUT="${ROOT_CHECKOUT:-/opt/newsflows/code/newsflows-bsky-ecosystem-07111009}"
HEALTH_PY="${HEALTH_PY:-/opt/newsflows/tools/uv/newsflows-bskyhealth/bin/python}"
log() { echo "[r4] $*" >&2; }; die() { echo "[r4] STOP: $*" >&2; exit 2; }; ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
emit() { local n=$1; [[ -e "$E/$n" ]] && die "refusing to overwrite $E/$n"; install -o root -g newsflows -m 640 /dev/stdin "$E/$n"; log "wrote $n"; }
[[ $(id -u) -eq 0 ]] || die "run as root (sudo -n)"
[[ -d "$E" ]] || install -d -o root -g newsflows -m 750 "$E"
HDR=""; trap '[[ -n "$HDR" ]] && rm -f "$HDR"' EXIT INT TERM HUP
load_keys() {  # extract ONLY the admin key into a 0600 header file (never exported to children); removed by the EXIT trap
  HDR=$(mktemp); chmod 600 "$HDR"
  ( set -a; . "$ENV_FILE"; set +a; : "${FEEDGEN_ADMIN_API_KEY:?}"; printf 'api-key: %s\n' "$FEEDGEN_ADMIN_API_KEY" ) > "$HDR"
  # bskyops readback needs the READ key in its environment: pass it via a scrubbed sub-environment (see bskyops_env)
}
bskyops_env() { ( set -a; . "$ENV_FILE"; set +a; env -i PATH="$PATH" HOME="$HOME" FEEDGEN_READ_API_KEY="${FEEDGEN_READ_API_KEY:-}" FEEDGEN_ADMIN_API_KEY="${FEEDGEN_ADMIN_API_KEY:-}" "$@" ); }
psqlq() { docker exec -i feedgen-db psql -U feedgen -d feedgen-db -X -A -F '|' -v ON_ERROR_STOP=1 "$@"; }
tool_ref() { node -e 'const fs=require("fs");const base=process.argv[1];let out="";for(const py of fs.readdirSync(base+"/lib").filter(d=>d.startsWith("python3"))){const sp=base+"/lib/"+py+"/site-packages";for(const d of fs.readdirSync(sp).filter(d=>d.endsWith(".dist-info")&&d.toLowerCase().replace(/_/g,"-").startsWith(process.argv[2]))){const f=sp+"/"+d+"/direct_url.json";if(fs.existsSync(f)){const j=JSON.parse(fs.readFileSync(f));out=(j.vcs_info&&j.vcs_info.commit_id)||""}}}console.log(out)' "/opt/newsflows/tools/uv/$1" "$1"; }
assert_bindings() {  # the packet file, its hash, the values it names, the catalog identity, the tool refs
  local ph; ph=$(sha256sum "$PACKET_PATH" | cut -d' ' -f1); [[ "$ph" == "$EXPECTED_PACKET_SHA" ]] || die "packet $PACKET_PATH sha256 $ph != EXPECTED_PACKET_SHA $EXPECTED_PACKET_SHA"
  local v; for v in "$EXPECTED_CLOCK" "$EXPECTED_EXPIRY" "$EXPECTED_FLOOR" "$EXPECTED_CONTRACT"; do grep -qF -- "$v" "$PACKET_PATH" || die "value '$v' does not appear in the approved packet"; done
  local rk; for rk in $(echo "$EXPECTED_UPDATE_RKEYS" | tr ',' ' '); do grep -qF -- "$rk" "$PACKET_PATH" || die "rkey $rk not named in the approved packet"; done
  local d1 d2 d3; d1=$(sha256sum "$TREE_CATALOG" | cut -d' ' -f1); d2=$(sha256sum /opt/newsflows/config/newsflows/catalogs/publishers.yml | cut -d' ' -f1)
  d3=$(git -c safe.directory="$ROOT_CHECKOUT" -C "$ROOT_CHECKOUT" show "$CATALOG_SOURCE_SHA:config/newsflows/catalogs/publishers.yml" | sha256sum | cut -d' ' -f1)
  [[ "$d1" == "$d2" && "$d2" == "$d3" ]] || die "catalog identity: TREE_CATALOG $d1 / deployed $d2 / origin@$CATALOG_SOURCE_SHA $d3 differ"
  git -c safe.directory="$ROOT_CHECKOUT" -C "$ROOT_CHECKOUT" fetch -q origin; git -c safe.directory="$ROOT_CHECKOUT" -C "$ROOT_CHECKOUT" merge-base --is-ancestor "$CATALOG_SOURCE_SHA" origin/main || die "CATALOG_SOURCE_SHA is not on origin/main"
  local t exp got; for t in bsky-ops newsflows-bskyhealth blueskyranker; do exp=$(echo "$EXPECTED_TOOL_REFS" | tr ',' '\n' | awk -F= -v k="$t" '$1==k{print $2}'); got=$(tool_ref "$t"); [[ -n "$exp" && "$got" == "$exp" ]] || die "installed tool $t is '$got', expected '$exp'"; done
  CATALOG_SHA=$d1
}
tool_refs_line() { echo "tool_refs bsky-ops=$(tool_ref bsky-ops) newsflows-bskyhealth=$(tool_ref newsflows-bskyhealth) blueskyranker=$(tool_ref blueskyranker) runner_sha256=$(sha256sum "${BASH_SOURCE[0]}" | cut -d' ' -f1) feedgen_tree=$(git -C "$(dirname "${BASH_SOURCE[0]}")/.." rev-parse HEAD 2>/dev/null || echo unknown) packet_sha256=$EXPECTED_PACKET_SHA catalog_sha256=${CATALOG_SHA:-} catalog_source_sha=$CATALOG_SOURCE_SHA"; }
sync_packet() {  # -> $1 (json path); the sync packet is generated from the DEPLOYED catalog (identity-checked against origin/main)
  bskyops_env "$BSKYOPS" ecosystem desired-state feedgen-sync-packet --active-only --catalog-yaml "$TREE_CATALOG" --feedgen-url "$FEEDGEN_URL" --bsr-effective-config-json "$EFFECTIVE_CONFIG_JSON" --json > "$1"
}
dry_run_gate() { node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));if(!(j.mode==="dry-run"&&j.status==="dry-run"&&(j.blockers||[]).length===0&&j.would_write===false))process.exit(2)' "$1"; }
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
  assert_bindings; load_keys; local p; p=$(mktemp); sync_packet "$p"; gate_packet "$p"
  emit feedgen-sync-packet-preview.json < "$p"; sha256sum "$E/feedgen-sync-packet-preview.json" | emit feedgen-sync-packet-preview.sha256
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log(JSON.stringify(j.atomic_change_set.request_body))' "$p" | emit feedgen-bulk-request.json
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log(JSON.stringify(j.atomic_change_set.rollback_request_body))' "$p" | emit feedgen-bulk-rollback.json
  # per-row dry-run against the running feedgen (read-only)
  local n=0; while IFS= read -r u; do local rk; rk=$(node -e 'console.log(JSON.parse(process.argv[1]).rkey)' "$u"); curl --fail -sS --max-time 30 -H "@$HDR" -H 'content-type: application/json' --data-binary "$u" "$FEEDGEN_URL/api/admin/feed_catalog/dry-run" | emit "dry-run-$rk.json"; dry_run_gate "$E/dry-run-$rk.json" || die "dry-run $rk not clean"; n=$((n+1)); done < <(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));for(const u of j.updates)console.log(JSON.stringify(u))' "$E/feedgen-bulk-request.json")
  { echo "generated_at=$(ts)"; echo "packet_path=$PACKET_PATH"; echo "packet_sha256=$EXPECTED_PACKET_SHA"; echo "catalog_sha256=$CATALOG_SHA (== deployed == origin@$CATALOG_SOURCE_SHA)"; echo "request_sha256=$(sha256sum "$E/feedgen-bulk-request.json" | cut -d' ' -f1)"; echo "rollback_sha256=$(sha256sum "$E/feedgen-bulk-rollback.json" | cut -d' ' -f1)"; echo "dry_runs=$n"; echo "feedgen_image=$(docker inspect feedgen --format '{{.Image}}')"; echo "expected=$EXPECTED_UPDATE_RKEYS clock=$EXPECTED_CLOCK expiry=$EXPECTED_EXPIRY floor=$EXPECTED_FLOOR contract=$EXPECTED_CONTRACT"; tool_refs_line; } | emit preview-summary.txt
  psqlq -c "SELECT rkey, catalog_revision, publisher_time_clock, publisher_time_transition_expires_at, content_time_cutover_min_valid_share, content_time_contract_version FROM feedgen_ops.feed_catalog WHERE enabled ORDER BY 1" | emit catalog-prestate.tsv
  rm -f "$p" "$HDR"; log "preview ok"
}
cmd_apply() {
  assert_bindings; load_keys; [[ -f "$E/feedgen-bulk-request.json" && -f "$E/preview-summary.txt" ]] || die "run preview first"
  local t0; t0=$(ts)
  # the request body must be byte-identical to the previewed one and the live rows must still match if_current (fresh packet == same body)
  local p; p=$(mktemp); sync_packet "$p"; gate_packet "$p"
  local fresh; fresh=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log(JSON.stringify(j.atomic_change_set.request_body))' "$p" | sha256sum | cut -d' ' -f1); local prev; prev=$(sha256sum "$E/feedgen-bulk-request.json" | cut -d' ' -f1)
  [[ "$fresh" == "$prev" ]] || die "live rows changed since preview (request body hash $fresh != $prev) -- re-preview and re-approve"
  local rid; rid=$prev; local resp; resp=$(mktemp); local http
  http=$(curl -sS --max-time 60 -H "@$HDR" -H "x-feedgen-actor: approved-activation-packet" -H "x-feedgen-source: $EXPECTED_PACKET_SHA" -H "x-feedgen-request-id: $rid" -H 'content-type: application/json' --data-binary "@$E/feedgen-bulk-request.json" --output "$resp" --write-out '%{http_code}' "$FEEDGEN_URL/api/admin/feed_catalog/bulk") || true
  emit feedgen-bulk-response.json < "$resp"; { echo "applied_at=$(ts)"; echo "http=$http"; echo "request_id=$rid"; echo "packet_sha256=$EXPECTED_PACKET_SHA"; tool_refs_line; } | emit apply-transport.txt
  [[ "$http" == "200" ]] || die "bulk apply http=$http (see feedgen-bulk-response.json); if ambiguous, run parity before deciding"
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const n=Number(process.argv[2]);if(!(j.status==="applied"&&j.applied===true&&j.result_count===n&&j.results.length===n))process.exit(2)' "$resp" "$(echo "$EXPECTED_UPDATE_RKEYS" | tr ',' '\n' | wc -l | tr -d ' ')" || die "bulk response not a clean apply"
  psqlq -c "SELECT rkey, catalog_revision, publisher_time_clock, publisher_time_transition_expires_at, content_time_cutover_min_valid_share, content_time_contract_version FROM feedgen_ops.feed_catalog WHERE enabled ORDER BY 1" | emit catalog-poststate.tsv
  local hist; hist=$(psqlq -c "SELECT rkey, catalog_revision, source, changed_at FROM feedgen_ops.feed_catalog_history WHERE rkey = ANY(string_to_array('$EXPECTED_UPDATE_RKEYS', ',')) AND source = '$rid' ORDER BY rkey"); echo "$hist" | emit catalog-history.tsv
  [[ $(echo "$hist" | grep -c "|$rid|") -eq $(echo "$EXPECTED_UPDATE_RKEYS" | tr ',' '\n' | wc -l | tr -d ' ') ]] || die "feed_catalog_history does not show one row per rkey with source=$rid"
  local nl; nl=$(docker logs feedgen --since "$t0" 2>&1 | grep -i "feed_catalog_changed\|catalog" | tail -40 || true); [[ -n "$nl" ]] || die "no feed_catalog_changed/catalog lines in feedgen logs since $t0"; echo "$nl" | emit feedgen-log-notify.txt
  rm -f "$p" "$resp" "$HDR"; log "apply ok"
}
cmd_parity() { assert_bindings; local f="feedgen-parity-$(date -u +%H%M%S).json"; bskyops_env "$BSKYOPS" ecosystem desired-state feedgen-parity --active-only --catalog-yaml "$TREE_CATALOG" --feedgen-url "$FEEDGEN_URL" --bsr-effective-config-json "$EFFECTIVE_CONFIG_JSON" --json | emit "$f"; node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const s=j.status||"";if(!/converged|already-converged|ok/i.test(s)||((j.summary||{}).mismatch_count||0)>0){console.error("parity status "+s+" mismatches "+JSON.stringify(j.summary||{}));process.exit(2)}console.log("parity "+s)' "$E/$f" || die "feedgen-parity not converged (see $f)"; }
cmd_rollback_dryrun() {  # only meaningful AFTER apply (the rollback CAS expects the activated values); every row gated like the forward dry-run
  load_keys; local n=0 stamp; stamp=$(date -u +%H%M%S); while IFS= read -r u; do local rk; rk=$(node -e 'console.log(JSON.parse(process.argv[1]).rkey)' "$u"); curl --fail -sS --max-time 30 -H "@$HDR" -H 'content-type: application/json' --data-binary "$u" "$FEEDGEN_URL/api/admin/feed_catalog/dry-run" | emit "rollback-dry-run-$rk-$stamp.json"; dry_run_gate "$E/rollback-dry-run-$rk-$stamp.json" || die "rollback dry-run $rk not clean (see rollback-dry-run-$rk-$stamp.json)"; n=$((n+1)); done < <(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));for(const u of j.updates)console.log(JSON.stringify(u))' "$E/feedgen-bulk-rollback.json"); log "rollback dry-run ok ($n rows, all would_write=false, no blockers)"; }
cmd_rollback() { assert_bindings; load_keys; local resp; resp=$(mktemp); local http; http=$(curl -sS --max-time 60 -H "@$HDR" -H "x-feedgen-actor: approved-activation-packet-rollback" -H "x-feedgen-source: $EXPECTED_PACKET_SHA" -H "x-feedgen-request-id: rollback-$(sha256sum "$E/feedgen-bulk-rollback.json" | cut -c1-64)" -H 'content-type: application/json' --data-binary "@$E/feedgen-bulk-rollback.json" --output "$resp" --write-out '%{http_code}' "$FEEDGEN_URL/api/admin/feed_catalog/bulk") || true; emit "feedgen-bulk-rollback-response-$(date -u +%H%M%S).json" < "$resp"; echo "rolled_back_at=$(ts) http=$http" | emit "rollback-transport-$(date -u +%H%M%S).txt"; [[ "$http" == "200" ]] || die "rollback http=$http"; psqlq -c "SELECT rkey, catalog_revision, publisher_time_clock FROM feedgen_ops.feed_catalog WHERE enabled ORDER BY 1" | emit "catalog-after-rollback-$(date -u +%H%M%S).tsv"; rm -f "$resp" "$HDR"; log "rollback applied"; }
cmd_battery() {  # <label>: per-invocation subdir; fresh authenticated requests first; every gate fails closed
  local L=${1:?label}; : "${APPLIED_AT:?APPLIED_AT (ISO, from apply-transport.txt; use the preview time for a pre-activation baseline) is required}"
  local B="battery-$L-$(date -u +%Y%m%dT%H%M%SZ)"; install -d -o root -g newsflows -m 750 "$E/$B"; local E0=$E; E="$E/$B"
  { echo "label=$L"; echo "started_at=$(ts)"; echo "applied_at=$APPLIED_AT"; tool_refs_line; } | emit meta.txt
  # (0) fresh AUTHENTICATED served requests for every feed: run the canonical appview feed-health check now (it fetches each
  #     feed via the AppView relay as the health bot account -> new request_log rows, e2e latency in feed_health_latest.json)
  systemctl start bsr-feed-health.service; local w=0; while systemctl is-active --quiet bsr-feed-health.service && (( w < 180 )); do sleep 2; w=$((w+2)); done
  cp /opt/newsflows/blueskyranker_v2/logs/feed_health_latest.json /tmp/fh.$$ && node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const want=process.argv[2].split(",");const fin=Date.parse(j.finished_at||j.started_at);if(!(fin>Date.parse(process.argv[3])))throw new Error("feed_health_latest predates APPLIED_AT");const arr=Array.isArray(j.feeds)?j.feeds:Object.entries(j.feeds).map(([k,v])=>({rkey:k,...v}));for(const f of arr){const rk=f.rkey||f.feed_id||f.k;if(!want.includes(rk))continue;const r=(f.results||[]).find(x=>x.name==="appview_authed_relay");console.log(rk,f.overall,"authed_relay:",r&&r.status,"elapsed_sec:",r&&r.detail&&r.detail.elapsed_sec)}console.log("finished_at",j.finished_at)' /tmp/fh.$$ "$EXPECTED_UPDATE_RKEYS" "$APPLIED_AT" | emit fresh-authenticated-requests.txt; rm -f /tmp/fh.$$
  # (1) BSR run evidence: journal lines of the main ranker after APPLIED_AT must show the expected clock/contract for all four handles
  journalctl -u bsr-ranker@main.service --since "$APPLIED_AT" --no-pager 2>/dev/null | grep -i "_query_posts called\|ENGAGEMENT_EXPORT_TRANSITIONAL\|time_contract\|Shared feeds have divergent" | tail -60 | emit bsr-main-journal.txt
  node -e 'const t=require("fs").readFileSync(process.argv[1],"utf8");const clock=process.argv[2];const want=clock==="content_time_v1"?/time_column=content_time_v1/:/time_column=createdAt/;const q=(t.match(/_query_posts called[^\n]*/g)||[]);const ex=(t.match(/ENGAGEMENT_EXPORT_TRANSITIONAL[^\n]*/g)||[]);const bad=q.filter(l=>!want.test(l));const div=/divergent effective time contracts/.test(t);console.log("query_posts_lines="+q.length+" wrong_clock="+bad.length+" export_lines="+ex.length+" divergent="+div);if(clock==="content_time_v1"){const exBad=ex.filter(l=>!/time_clock=.content_time_v1.*contract_version=newsflows-content-time\/v2/.test(l));console.log("export_wrong_clock="+exBad.length);if(q.length<4||bad.length||exBad.length||div)process.exit(2)}else{if(q.length<1||bad.length||div)process.exit(2)}' "$E/bsr-main-journal.txt" "$EXPECTED_CLOCK" | emit bsr-main-gate.txt || die "BSR run evidence not on the expected clock (see bsr-main-journal.txt); a run may still be in progress -- wait for the cycle, then re-run battery"
  # (2) served content-shadow (oracle = deployed ranker-priority ordering: coalesce(score,-1) desc, content_time_utc desc, indexedAt desc, uri desc)
  psqlq -v rkeys="$EXPECTED_UPDATE_RKEYS" -f "$(dirname "$0")/served_content_shadow.sql" | emit served-shadow.tsv
  node -e 'const rows=require("fs").readFileSync(process.argv[1],"utf8").split("\n").filter(l=>l&&!l.startsWith("[")&&!l.startsWith("rkey"));const [clock,applied]=process.argv.slice(2);let bad=[];for(const l of rows){const c=l.split("\t");const [rk,catClock,catRev,sClock,sRev,servedAt,n,notValid,outside,inv,scored,sha,unjoined,future]=c;if(catClock!==clock)bad.push(rk+" catalog_clock="+catClock);if(sClock!==clock)bad.push(rk+" served_clock="+sClock);if(sRev!==catRev)bad.push(rk+" served_revision "+sRev+"!="+catRev);if(!(Date.parse(servedAt)>Date.parse(applied)))bad.push(rk+" served_at "+servedAt+" not after applied_at");if(Number(n)===0)bad.push(rk+" no served publisher rows");if(clock==="content_time_v1"&&(Number(notValid)||Number(outside)||Number(future)))bad.push(rk+" not_valid="+notValid+" outside="+outside+" future="+future);if(Number(inv))bad.push(rk+" inversions="+inv);if(Number(scored)===0)bad.push(rk+" served_scored=0");if(Number(unjoined))bad.push(rk+" unjoined_request_posts="+unjoined);}console.log(bad.length?bad.join("\n"):"served-shadow ok rows="+rows.length);if(bad.length)process.exit(2)' "$E/served-shadow.tsv" "$EXPECTED_CLOCK" "$APPLIED_AT" | emit served-shadow-gate.txt || die "served shadow gate failed (see served-shadow-gate.txt)"
  # (3) served-assumptions per feed (0.80 score-backed @24h, time_contract, revision), fail closed on invocation failure
  local rk; for rk in $(echo "$EXPECTED_UPDATE_RKEYS" | tr ',' ' '); do local did; did=$(psqlq -t -c "SELECT publisher_did FROM feedgen_ops.feed_catalog WHERE rkey='$rk'"); "$HEALTH_PY" -m bskyhealth.checks.feed_served_assumptions --rkey "$rk" --publisher-did "$did" --algo-policy-id ranker-priority --window-hours 3 --json-out "/tmp/sa.$$.json" --quiet < /dev/null || die "served-assumptions invocation failed for $rk"; emit "served-assumptions-$rk.json" < "/tmp/sa.$$.json"; rm -f "/tmp/sa.$$.json"; node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const f=j.feeds[0];const rc=(f.results||[]).find(r=>r.name==="ranker_serving_contract")||{};const tc=(f.results||[]).find(r=>r.name==="time_contract")||{};const d=rc.detail||{};console.log(process.argv[2],"overall="+j.overall,"latest_rev="+(f.latest_request||{}).catalog_revision,"rsc="+rc.status,"share="+d.observed_score_backed_share,"floor="+d.minimum_score_backed_share,"time_contract="+tc.status);if(!(j.overall==="OK"&&rc.status==="OK"&&Number(d.observed_score_backed_share)+1e-9>=Number(d.minimum_score_backed_share)))process.exit(2)' "$E/served-assumptions-$rk.json" "$rk" | emit "served-assumptions-$rk-gate.txt" || die "served-assumptions gate failed for $rk"; done
  # (4) D5 latency: served-log pairs since APPLIED_AT (p95/max are the gate; the >=30-sample count is reported, not gated -- it accrues)
  DOCKER="docker" bash "$(dirname "$0")/served_latency_from_logs.sh" "$APPLIED_AT" "$EXPECTED_UPDATE_RKEYS" 5000 8000 1 | emit latency.json
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));let bad=[];for(const [k,v] of Object.entries(j.feeds)){if(v.samples===0)bad.push(k+" no samples yet");else if(!(v.p95_ms<=5000&&v.max_ms<=8000))bad.push(k+" p95="+v.p95_ms+" max="+v.max_ms);console.log(k,"samples="+v.samples,"p95="+v.p95_ms,"max="+v.max_ms)}if(bad.length){console.log(bad.join("\n"));process.exit(2)}' "$E/latency.json" | emit latency-gate.txt || die "latency gate failed (see latency-gate.txt)"
  # (5) readiness composite: the four feeds, time_contract component, floor, revision; artifact must be fresh
  cp /var/lib/newsflows/health-checker/feed_readiness_latest.json /tmp/fr.$$ || die "readiness artifact missing"
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const want=process.argv[2].split(",");const clock=process.argv[3];const age=(Date.now()-Date.parse(j.generated_at))/1000;if(age>2400)throw new Error("readiness artifact stale "+age+"s");let bad=[];for(const f of j.feeds){const id=f.catalog&&f.catalog.feed_id;if(!want.includes(id))continue;const tc=(f.components||{}).time_contract||{};console.log(id,f.status,"time_contract:",tc.status,(tc.summary||"").slice(0,100),"clock:",f.catalog.publisher_time_clock,"valid_share:",f.catalog.content_time_valid_share,"floor:",f.catalog.content_time_cutover_min_valid_share,"rev:",f.catalog.catalog_revision);if(f.status==="FAIL"||tc.status==="FAIL")bad.push(id+" "+f.status+"/"+tc.status);if(f.catalog.publisher_time_clock!==clock)bad.push(id+" clock="+f.catalog.publisher_time_clock)}console.log("generated_at",j.generated_at,"age_s",Math.round(age));if(bad.length){console.log(bad.join("\n"));process.exit(2)}' /tmp/fr.$$ "$EXPECTED_UPDATE_RKEYS" "$EXPECTED_CLOCK" | emit readiness-gate.txt || { rm -f /tmp/fr.$$; die "readiness gate failed (see readiness-gate.txt); note the composite is produced by the health timer -- if it merely predates the apply, wait for the next tick and re-run battery"; }; rm -f /tmp/fr.$$
  # (6) rollback dry-run (D3(v)): the restore body must be valid against the activated rows -- only after apply
  [[ "$EXPECTED_CLOCK" == "content_time_v1" && -f "$E0/feedgen-bulk-rollback.json" && -f "$E0/apply-transport.txt" ]] && { E=$E0 cmd_rollback_dryrun; }
  # (7) conformance summary (sudo scan) -- fail closed if the scan itself fails
  local cs; cs=$("$BSKYOPS" ecosystem conformance scan --json 2>/dev/null | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);console.log(JSON.stringify(j.summary))})') || die "conformance scan failed"; echo "$cs" | emit conformance.txt
  E=$E0; log "battery $L complete: all gates passed (receipts in $B)"
}
case "${1:-}" in preview) cmd_preview;; apply) cmd_apply;; parity) cmd_parity;; rollback-dryrun) cmd_rollback_dryrun;; rollback) cmd_rollback;; battery) cmd_battery "${2:?label}";; *) echo "usage: $0 preview|apply|parity|rollback-dryrun|rollback|battery <label>" >&2; exit 2;; esac
