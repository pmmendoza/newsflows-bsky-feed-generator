#!/usr/bin/env bash
# FT-FU-1 coordinated newsflows-content-time/v2 -> v3 production packet.
# This script is inert unless an explicit subcommand is supplied. It writes
# raw-free, non-overwriting receipts below the caller-bound evidence root.
set -euo pipefail

COMMAND=${1:-}
V2=newsflows-content-time/v2
V3=newsflows-content-time/v3
RKEYS=newsflow-nl-2,newsflow-fr-2,newsflow-cz-2,newsflow-ir-2,newsflow-be-k,newsflow-be-m
MAIN_DIDS=${MAIN_DIDS:-did:plc:toz4no26o2x4vsbum7cp4bxp,did:plc:kzmukwaf72iwepygposicgt3,did:plc:cegiy4pfghh4rjs7ks7pbnkm,did:plc:vzmnljt7otfbbgrmachtefxh}
BE_DID=${BE_DID:-did:plc:tlmi333azel2jcornp2qeolm}
IR_DID=${IR_DID:-did:plc:vzmnljt7otfbbgrmachtefxh}
TIMER_UNITS=${TIMER_UNITS:-bsr-ranker@main.timer,bsr-ranker@be-k.timer}
SERVICE_UNITS=${SERVICE_UNITS:-bsr-ranker@main.service,bsr-ranker@be-k.service}
FEEDGEN_URL=${FEEDGEN_URL:-http://127.0.0.1:3020}
ENV_FILE=${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}
DEPLOYED_CATALOG_ROOT=${DEPLOYED_CATALOG_ROOT:-/opt/newsflows/config/newsflows/catalogs}
BSKYOPS=${BSKYOPS:-bskyops}
DOCKER=${DOCKER:-sudo -n docker}
NETWORK=${NETWORK:-newsflows-bsky-feed-generator-v2_default}
IMG=${IMG:-}
HDR=
TIMERS_FENCED=0
MIGRATION_DRAIN_SECONDS=${MIGRATION_DRAIN_SECONDS:-60}

log() { echo "[ft-fu-1] $*" >&2; }
die() { echo "[ft-fu-1] STOP: $*" >&2; exit 2; }
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
sha() { sha256sum "$1" | cut -d' ' -f1; }
emit() {
  local name=$1 tmp
  [[ ! -e "$E/$name" ]] || die "refusing to overwrite $E/$name"
  tmp=$(mktemp); cat >"$tmp"
  if [[ ${FTFU1_TEST_MODE:-0} == 1 ]]; then install -m 600 "$tmp" "$E/$name"
  else sudo -n install -o root -g newsflows -m 640 "$tmp" "$E/$name"; fi
  rm -f "$tmp"
}
split_csv() { tr ',' '\n' <<<"$1"; }
assert_packet_tree() {
  local got; got=$(git -C "$TREE" rev-parse HEAD)
  [[ "$got" == "$PACKET_SOURCE_SHA" && -z $(git -C "$TREE" status --porcelain) ]] || die "packet execution tree is not the bound clean commit $PACKET_SOURCE_SHA"
}
assert_runtime_provenance() {
  local got; got=$(sudo -n docker image inspect "$EXPECTED_IMAGE" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')
  [[ "$FEEDGEN_SHA" =~ ^[0-9a-f]{40}$ && "$got" == "$FEEDGEN_SHA" ]] || die "runtime image revision is '$got', expected FEEDGEN_SHA=$FEEDGEN_SHA"
}

restore_timers() {
  local unit failed=0 state_file=${E:-}/timer-prestate-${COMMAND:-unknown}.tsv
  [[ $TIMERS_FENCED == 1 && -f $state_file ]] || return 0
  while IFS='|' read -r unit state; do
    if [[ $state == active ]]; then
      sudo -n systemctl start "$unit" || failed=1
      [[ $(systemctl is-active "$unit" 2>/dev/null || true) == active ]] || failed=1
    fi
  done <"$state_file"
  TIMERS_FENCED=0
  (( failed == 0 ))
}
cleanup() { local rc=$?; trap - EXIT INT TERM HUP; restore_timers || { log 'timer restoration failed'; rc=3; }; [[ -z $HDR ]] || rm -f "$HDR"; exit "$rc"; }
trap cleanup EXIT INT TERM HUP

fence_timers() {
  local unit state tmp
  tmp=$(mktemp)
  while IFS= read -r unit; do
    state=$(systemctl is-active "$unit" 2>/dev/null || true)
    [[ $state == active ]] || die "$unit is $state before fencing (expected active)"
    printf '%s|%s\n' "$unit" "$state" >>"$tmp"
  done < <(split_csv "$TIMER_UNITS")
  emit "timer-prestate-$COMMAND.tsv" <"$tmp"; rm -f "$tmp"
  TIMERS_FENCED=1
  while IFS= read -r unit; do sudo -n systemctl stop "$unit"; done < <(split_csv "$TIMER_UNITS")
  while IFS= read -r unit; do
    [[ $(systemctl is-active "$unit" 2>/dev/null || true) == inactive ]] || die "$unit did not stop"
  done < <(split_csv "$TIMER_UNITS")
  while IFS= read -r unit; do
    [[ $(systemctl is-active "$unit" 2>/dev/null || true) == inactive ]] || die "$unit is running; wait for dispatch completion before retry"
  done < <(split_csv "$SERVICE_UNITS")
}

if [[ $COMMAND == test-timer-restore && ${FTFU1_TEST_MODE:-0} == 1 ]]; then
  : "${E:?}"; mkdir -p "$E"; fence_timers; false
fi
if [[ $COMMAND == test-packet-tree-binding && ${FTFU1_TEST_MODE:-0} == 1 ]]; then
  : "${E:?}" "${TREE:?}" "${PACKET_SOURCE_SHA:?}"; assert_packet_tree; exit 0
fi
if [[ $COMMAND == test-runtime-provenance && ${FTFU1_TEST_MODE:-0} == 1 ]]; then
  : "${E:?}" "${EXPECTED_IMAGE:?}" "${FEEDGEN_SHA:?}"; assert_runtime_provenance; exit 0
fi

for v in E SOURCE_ROOT SOURCE_SHA SOURCE_CATALOG_SHA ROLLBACK_SOURCE_ROOT ROLLBACK_SOURCE_SHA ROLLBACK_CATALOG_SHA TREE PACKET_SOURCE_SHA FEEDGEN_SHA EXPECTED_DIST_SHA EXPECTED_CT_SHA EXPECTED_IMAGE_CT_SHA EXPECTED_IMAGE EXPECTED_TOOL_REFS EXPECTED_RUNNER_SHA EXPECTED_REVALIDATE_RUNNER_SHA PACKET_PATH PACKET_SHA SINCE_MAIN SINCE_BE SINCE_ENGAGEMENT; do
  [[ -n ${!v:-} ]] || die "$v is required"
done
[[ $E == /* ]] || die 'E must be an absolute evidence root'
[[ $COMMAND != preflight || ! -e $E ]] || die 'preflight requires a new evidence root'
[[ $SOURCE_SHA =~ ^[0-9a-f]{40}$ && $ROLLBACK_SOURCE_SHA =~ ^[0-9a-f]{40}$ && $PACKET_SOURCE_SHA =~ ^[0-9a-f]{40}$ && $FEEDGEN_SHA =~ ^[0-9a-f]{40}$ ]] || die 'source refs must be full commit SHAs'
for v in SOURCE_CATALOG_SHA ROLLBACK_CATALOG_SHA EXPECTED_DIST_SHA EXPECTED_CT_SHA EXPECTED_IMAGE_CT_SHA EXPECTED_RUNNER_SHA EXPECTED_REVALIDATE_RUNNER_SHA PACKET_SHA; do [[ ${!v} =~ ^[0-9a-f]{64}$ ]] || die "$v must be sha256"; done
for v in SINCE_MAIN SINCE_BE SINCE_ENGAGEMENT; do [[ ${!v} =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "$v must be fixed ISO UTC with milliseconds"; done
if [[ ${FTFU1_TEST_MODE:-0} == 1 ]]; then mkdir -p "$E"; else sudo -n install -d -o root -g newsflows -m 750 "$E"; fi
IMG=${IMG:-$EXPECTED_IMAGE}

CATALOG_REL=config/newsflows/catalogs/publishers.yml
DIST=$TREE/dist/tools/backfill-publisher-posts.js
CT_DIST=$TREE/dist/util/content-time.js
REVALIDATE=$TREE/scripts/content_time_revalidate_packet.sh

load_header() {
  HDR=$(mktemp); chmod 600 "$HDR"
  ( set -a; . "$ENV_FILE"; set +a; : "${FEEDGEN_ADMIN_API_KEY:?}"; printf 'api-key: %s\n' "$FEEDGEN_ADMIN_API_KEY" ) >"$HDR"
}
bskyops_env() {
  ( set -a; . "$ENV_FILE"; set +a; env -i PATH="$PATH" HOME="$HOME" FEEDGEN_READ_API_KEY="${FEEDGEN_READ_API_KEY:-}" FEEDGEN_ADMIN_API_KEY="${FEEDGEN_ADMIN_API_KEY:-}" "$@" )
}
tool_ref() {
  node -e 'const fs=require("fs");const [base,n]=process.argv.slice(1);let out="";for(const py of fs.readdirSync(base+"/lib").filter(x=>x.startsWith("python3"))){const sp=base+"/lib/"+py+"/site-packages";for(const d of fs.readdirSync(sp).filter(x=>x.endsWith(".dist-info")&&x.toLowerCase().replace(/_/g,"-").startsWith(n))){const f=sp+"/"+d+"/direct_url.json";if(fs.existsSync(f)){const j=JSON.parse(fs.readFileSync(f));out=j.vcs_info?.commit_id||""}}}console.log(out)' "/opt/newsflows/tools/uv/$1" "$1"
}
assert_bindings() {
  [[ $(git -C "$SOURCE_ROOT" rev-parse HEAD) == "$SOURCE_SHA" && -z $(git -C "$SOURCE_ROOT" status --porcelain) ]] || die 'forward source root is not the bound clean commit'
  [[ $(git -C "$ROLLBACK_SOURCE_ROOT" rev-parse HEAD) == "$ROLLBACK_SOURCE_SHA" && -z $(git -C "$ROLLBACK_SOURCE_ROOT" status --porcelain) ]] || die 'rollback source root is not the bound clean commit'
  assert_packet_tree
  [[ $(sha "$SOURCE_ROOT/$CATALOG_REL") == "$SOURCE_CATALOG_SHA" && $(sha "$ROLLBACK_SOURCE_ROOT/$CATALOG_REL") == "$ROLLBACK_CATALOG_SHA" ]] || die 'catalog hash differs'
  [[ $(sha "$DIST") == "$EXPECTED_DIST_SHA" && $(sha "$CT_DIST") == "$EXPECTED_CT_SHA" ]] || die 'built revalidation artifact hash differs'
  [[ $(sha "${BASH_SOURCE[0]}") == "$EXPECTED_RUNNER_SHA" ]] || die 'packet runner hash differs'
  [[ $(sha "$REVALIDATE") == "$EXPECTED_REVALIDATE_RUNNER_SHA" ]] || die 'revalidation runner hash differs'
  [[ $(sha "$PACKET_PATH") == "$PACKET_SHA" ]] || die 'approved packet hash differs'
  [[ $(sudo -n docker inspect feedgen --format '{{.Image}}') == "$EXPECTED_IMAGE" ]] || die 'running feedgen image differs'
  assert_runtime_provenance
  local tool expected actual
  for tool in bsky-ops blueskyranker newsflows-bskyhealth; do
    expected=$(split_csv "$EXPECTED_TOOL_REFS" | awk -F= -v k="$tool" '$1==k{print $2}')
    actual=$(tool_ref "$tool")
    [[ -n $expected && $actual == "$expected" ]] || die "$tool ref differs"
  done
}
catalog_packet() {
  bskyops_env "$BSKYOPS" ecosystem desired-state feedgen-sync-packet --active-only --catalog-yaml "${1:-$DEPLOYED_CATALOG_ROOT/publishers.yml}" --feedgen-url "$FEEDGEN_URL" --json
}
gate_body() {
  node - "$1" "$2" "$3" "$RKEYS" <<'NODE'
const fs=require('fs');const [f,from,to,rkeys]=process.argv.slice(2);const j=JSON.parse(fs.readFileSync(f));
if(j.status!=='ready-for-apply'||j.atomic_change_set?.atomic!==true)throw Error(`packet status ${j.status}`);
const expected=rkeys.split(',').sort(), check=(body,a,b)=>{const u=body.updates||[];if(JSON.stringify(u.map(x=>x.rkey).sort())!==JSON.stringify(expected))throw Error('not exact six');for(const x of u){const fields=Object.keys(x).filter(k=>!['op','rkey','if_current'].includes(k));if(fields.length!==1||fields[0]!=='content_time_contract_version')throw Error(`${x.rkey} not version-only`);if(x.content_time_contract_version!==b||x.if_current?.content_time_contract_version!==a)throw Error(`${x.rkey} asymmetric ${a}->${b}`)}};
check(j.atomic_change_set.request_body,from,to);check(j.atomic_change_set.rollback_request_body,to,from);
NODE
}
extract_bodies() {
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));process.stdout.write(JSON.stringify(j.atomic_change_set.request_body,null,2))' "$1" | emit feedgen-forward.json
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));process.stdout.write(JSON.stringify(j.atomic_change_set.rollback_request_body,null,2))' "$1" | emit feedgen-rollback.json
}
post_bulk() {
  local body=$1 name=$2 response http
  load_header; response=$(mktemp)
  http=$(curl -sS --max-time 60 -H "@$HDR" -H "x-feedgen-actor: ft-fu-1:$PACKET_SHA" -H "x-feedgen-source: $FEEDGEN_SHA" -H "x-feedgen-request-id: $name-$PACKET_SHA" -H 'content-type: application/json' --data-binary "@$body" -o "$response" -w '%{http_code}' "$FEEDGEN_URL/api/admin/feed_catalog/bulk" || true)
  emit "$name-response.json" <"$response"; rm -f "$response" "$HDR"; HDR=
  [[ $http == 200 ]] || die "$name returned HTTP $http"
}
active_version_gate() {
  local expected=$1 got
  got=$(sudo -n docker exec -i feedgen-db psql -U feedgen -d feedgen-db -X -qAt -v ON_ERROR_STOP=1 -c "SELECT CASE WHEN count(*)=6 AND count(DISTINCT content_time_contract_version)=1 THEN min(content_time_contract_version) ELSE 'bad:'||count(*)||':'||count(DISTINCT content_time_contract_version) END FROM feedgen_ops.feed_catalog WHERE enabled AND publisher_time_clock='content_time_v1';")
  [[ $got == "$expected" ]] || die "active catalog version is $got, expected $expected"
}
revalidate_runner() {
  EXPECTED_SHA=$PACKET_SOURCE_SHA EXPECTED_DIST_SHA256=$EXPECTED_DIST_SHA EXPECTED_CT_SHA256=$EXPECTED_CT_SHA EXPECTED_IMAGE_CT_SHA256=$EXPECTED_IMAGE_CT_SHA \
    FROM_VERSION=$V2 TO_VERSION=$V3 IMG=$IMG RUNNER=container MIGRATION_DRAIN_SECONDS=$MIGRATION_DRAIN_SECONDS "$REVALIDATE" "$@"
}
migration_complete() {
  local label=$1 target
  for target in post engagement; do
    [[ -f "$E/migrate-$target-apply-$label.json" ]] || return 1
    [[ $(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1])).revalidation.complete)' "$E/migrate-$target-apply-$label.json") == true ]] || return 1
  done
}
forward_label_used() { [[ -n $(find "$E" -maxdepth 1 -type f -name "*forward-$1*" -print -quit) ]]; }
forward_to_completion() {
  local attempt label
  for attempt in 1 2 3 4 5; do migration_complete "forward-$attempt" && return 0; done
  for attempt in 1 2 3 4 5; do
    forward_label_used "$attempt" && continue
    label="forward-$attempt"; revalidate_runner migrate-apply "$label"
    migration_complete "$label" && return 0
  done
  die 'forward revalidation remained incomplete after five resumable invocations'
}
catalog_sync_preview() { bskyops_env "$BSKYOPS" ecosystem catalog-sync-packet --source-root "$1/config/newsflows/catalogs" --deployed-root "$DEPLOYED_CATALOG_ROOT" --json; }
catalog_sync_apply() { bskyops_env "$BSKYOPS" ecosystem catalog-sync-apply --source-root "$1/config/newsflows/catalogs" --deployed-root "$DEPLOYED_CATALOG_ROOT" --confirm-live-host-catalog-sync --json; }
gate_catalog_sync() {
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));if(!["ready-for-apply","already-converged","applied"].includes(j.status))throw Error(`catalog sync status ${j.status}`);const bad=(j.actions||[]).filter(a=>a.filename!=="publishers.yml"&&a.action!=="no-op");if(bad.length)throw Error(`catalog sync escaped publishers.yml: ${JSON.stringify(bad)}`)' "$1"
}

cmd_preflight() {
  assert_bindings
  catalog_sync_preview "$SOURCE_ROOT" | emit catalog-sync-forward-preview.json; gate_catalog_sync "$E/catalog-sync-forward-preview.json"
  catalog_packet "$SOURCE_ROOT/$CATALOG_REL" | emit feedgen-sync-preview.json
  gate_body "$E/feedgen-sync-preview.json" "$V2" "$V3"; extract_bodies "$E/feedgen-sync-preview.json"
  # Prepare bounds/control while writes are still v2. The exact population is
  # deliberately not bound until the catalog switch makes new writes v3.
  revalidate_runner migrate-prepare
  { echo "generated_at=$(ts)"; echo "source_sha=$SOURCE_SHA"; echo "source_catalog_sha256=$SOURCE_CATALOG_SHA"; echo "rollback_source_sha=$ROLLBACK_SOURCE_SHA"; echo "rollback_catalog_sha256=$ROLLBACK_CATALOG_SHA"; echo "packet_source_sha=$PACKET_SOURCE_SHA"; echo "feedgen_runtime_sha=$FEEDGEN_SHA"; echo "feedgen_image=$EXPECTED_IMAGE"; echo "packet_sha256=$PACKET_SHA"; echo "runner_sha256=$EXPECTED_RUNNER_SHA"; echo "since_main=$SINCE_MAIN"; echo "since_be=$SINCE_BE"; echo "since_engagement=$SINCE_ENGAGEMENT"; echo "migration_drain_seconds=$MIGRATION_DRAIN_SECONDS"; echo "rkeys=$RKEYS"; echo "tool_refs=$EXPECTED_TOOL_REFS"; } | emit bindings.txt
}
cmd_preview() {
  assert_bindings
  [[ -f $E/feedgen-forward.json ]] || die 'preflight receipts missing'
  [[ -f $E/migrate-source-set.txt && -n $(ls -1 "$E"/pg-control-*.txt 2>/dev/null | head -1) ]] || die 'migration preparation receipts missing'
  catalog_sync_preview "$ROLLBACK_SOURCE_ROOT" | emit catalog-sync-rollback-preview.json; gate_catalog_sync "$E/catalog-sync-rollback-preview.json"
}
cmd_apply() {
  assert_bindings
  [[ -f $E/feedgen-forward.json ]] || die 'preflight receipts missing'
  fence_timers
  catalog_sync_apply "$SOURCE_ROOT" | emit 01-catalog-sync-forward-apply.json; gate_catalog_sync "$E/01-catalog-sync-forward-apply.json"
  catalog_packet | emit 02-feedgen-sync-apply-packet.json; gate_body "$E/02-feedgen-sync-apply-packet.json" "$V2" "$V3"
  node -e 'const fs=require("fs"),a=JSON.parse(fs.readFileSync(process.argv[1])),j=JSON.parse(fs.readFileSync(process.argv[2]));if(JSON.stringify(a)!==JSON.stringify(j.atomic_change_set.request_body))throw Error("fresh apply body differs from bound preview body")' "$E/feedgen-forward.json" "$E/02-feedgen-sync-apply-packet.json"
  post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply
  active_version_gate "$V3"
  revalidate_runner migrate-freeze
  forward_to_completion
  { echo '01 catalog-sync forward'; echo '02 feedgen sync packet gate'; echo '03 exact-six bulk v2->v3'; echo '04 drain and bind stable v2 population'; echo '05 global post'; echo '05 global engagement'; } | emit apply-order.txt
  restore_timers
}
cmd_revalidate() { assert_bindings; active_version_gate "$V3"; fence_timers; [[ -f $E/migrate-stable-population.txt ]] || revalidate_runner migrate-freeze; revalidate_runner migrate-stable-check; forward_to_completion; restore_timers; }
cmd_readback() {
  assert_bindings
  active_version_gate "$V3"
  revalidate_runner migrate-readback
  sleep "${DRAIN_INTERVAL_SECONDS:-60}"
  revalidate_runner migrate-readback
  local num den; num=$(awk -F= '$1=="ir_gt_5m_restored"{print $2}' "$E/migrate-stable-population.txt"); den=$(awk -F= '$1=="ir_denominator"{print $2}' "$E/migrate-stable-population.txt")
  { echo "irish_restored_gt_5m_numerator=$num"; echo "irish_in_horizon_v2_denominator=$den"; echo 'participant_exposure_not_inferred=true'; } | emit irish-restoration.txt
}
cmd_rollback_dryrun() {
  assert_bindings
  active_version_gate "$V3"; load_header
  catalog_sync_preview "$ROLLBACK_SOURCE_ROOT" | emit rollback-catalog-sync-dryrun.json; gate_catalog_sync "$E/rollback-catalog-sync-dryrun.json"
  node -e 'for(const u of JSON.parse(require("fs").readFileSync(process.argv[1])).updates)console.log(JSON.stringify(u))' "$E/feedgen-rollback.json" | while IFS= read -r row; do
    local rk; rk=$(node -e 'console.log(JSON.parse(process.argv[1]).rkey)' "$row")
    curl -fsS --max-time 30 -H "@$HDR" -H 'content-type: application/json' --data-binary "$row" "$FEEDGEN_URL/api/admin/feed_catalog/dry-run" | emit "rollback-dryrun-$rk.json"
    node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));if(j.mode!=="dry-run"||j.would_write!==false)throw Error("rollback dry-run gate failed")' "$E/rollback-dryrun-$rk.json"
  done
  rm -f "$HDR"; HDR=
  revalidate_runner migrate-rollback dry-run
}
cmd_rollback() {
  assert_bindings
  [[ -f $E/feedgen-rollback.json ]] || die 'rollback body missing'
  fence_timers
  post_bulk "$E/feedgen-rollback.json" 01-feedgen-rollback-v3-to-v2
  active_version_gate "$V2"
  catalog_sync_apply "$ROLLBACK_SOURCE_ROOT" | emit 02-catalog-sync-rollback-apply.json; gate_catalog_sync "$E/02-catalog-sync-rollback-apply.json"
  revalidate_runner migrate-rollback apply reverse
  { echo '01 exact-six bulk v3->v2'; echo '02 catalog desired-state rollback'; echo '03 reverse global post'; echo '03 reverse global engagement'; } | emit rollback-order.txt
  restore_timers
}
cmd_finalize() {
  assert_bindings
  revalidate_runner migrate-secret-scan
  revalidate_runner migrate-finalize "${2:?window start is required}" "${3:?window end is required}"
}

case "$COMMAND" in
  test-forward-resume) [[ ${FTFU1_TEST_MODE:-0} == 1 ]] || die 'test-only command'; forward_to_completion;;
  preflight) cmd_preflight;; preview) cmd_preview;; apply) cmd_apply;; revalidate) cmd_revalidate;; readback) cmd_readback;; rollback-dryrun) cmd_rollback_dryrun;; rollback) cmd_rollback;; finalize) cmd_finalize "$@";;
  *) echo 'usage: content_time_contract_upgrade_packet.sh preflight|preview|apply|revalidate|readback|rollback-dryrun|rollback|finalize <start> <end>' >&2; exit 2;;
esac
