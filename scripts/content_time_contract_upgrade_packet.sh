#!/usr/bin/env bash
# FT-FU-1 coordinated newsflows-content-time/v2 -> v3 production packet.
# The continuation-only normalize-overlap subcommand additionally requires
# ALLOW_FTFU1_OVERLAP_NORMALIZATION=1.
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
TIMER_STATE_LABEL=
KEEP_TIMERS_FENCED_ON_EXIT=0
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
  local unit failed=0 state_file=${E:-}/timer-prestate-${COMMAND:-unknown}${TIMER_STATE_LABEL:+-$TIMER_STATE_LABEL}.tsv
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
cleanup() { local rc=$?; trap - EXIT INT TERM HUP; if [[ $KEEP_TIMERS_FENCED_ON_EXIT == 0 ]]; then restore_timers || { log 'timer restoration failed'; rc=3; }; else log 'partial mutation: timers remain fenced'; fi; [[ -z $HDR ]] || rm -f "$HDR"; exit "$rc"; }
trap cleanup EXIT INT TERM HUP
complete_timer_window() { restore_timers; KEEP_TIMERS_FENCED_ON_EXIT=0; }

fence_timers() {
  local unit service state enabled service_state tmp i
  local -a timers services
  local state_file="$E/timer-prestate-$COMMAND${TIMER_STATE_LABEL:+-$TIMER_STATE_LABEL}.tsv"
  IFS=',' read -r -a timers <<<"$TIMER_UNITS"
  IFS=',' read -r -a services <<<"$SERVICE_UNITS"
  [[ ${ALLOW_PRE_FENCED_TIMERS:-0} == 0 || ${ALLOW_PRE_FENCED_TIMERS:-0} == 1 ]] || die 'ALLOW_PRE_FENCED_TIMERS must be 0 or 1'
  [[ ${ALLOW_PRE_FENCED_TIMERS:-0} == 0 || ${#timers[@]} == "${#services[@]}" ]] || die 'timer/service unit counts differ'
  tmp=$(mktemp)
  for i in "${!timers[@]}"; do
    unit=${timers[$i]}
    state=$(systemctl is-active "$unit" 2>/dev/null || true)
    if [[ $state == inactive && ${ALLOW_PRE_FENCED_TIMERS:-0} == 1 ]]; then
      service=${services[$i]}
      enabled=$(systemctl is-enabled "$unit" 2>/dev/null || true)
      service_state=$(systemctl is-active "$service" 2>/dev/null || true)
      [[ $enabled == enabled ]] || die "$unit is inactive but $enabled (expected enabled)"
      [[ $service_state == inactive ]] || die "$unit is inactive but $service is $service_state (expected inactive)"
    else
      [[ $state == active ]] || die "$unit is $state before fencing (expected active)"
    fi
    printf '%s|%s\n' "$unit" "$state" >>"$tmp"
  done
  emit "$(basename "$state_file")" <"$tmp"; rm -f "$tmp"
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
if [[ $COMMAND == test-timer-fenced-failure && ${FTFU1_TEST_MODE:-0} == 1 ]]; then
  : "${E:?}"; mkdir -p "$E"; fence_timers; KEEP_TIMERS_FENCED_ON_EXIT=1; false
fi
if [[ $COMMAND == test-packet-tree-binding && ${FTFU1_TEST_MODE:-0} == 1 ]]; then
  : "${E:?}" "${TREE:?}" "${PACKET_SOURCE_SHA:?}"; assert_packet_tree; exit 0
fi
if [[ $COMMAND == test-runtime-provenance && ${FTFU1_TEST_MODE:-0} == 1 ]]; then
  : "${E:?}" "${EXPECTED_IMAGE:?}" "${FEEDGEN_SHA:?}"; assert_runtime_provenance; exit 0
fi

for v in E SOURCE_ROOT SOURCE_SHA SOURCE_CATALOG_SHA ROLLBACK_SOURCE_ROOT ROLLBACK_SOURCE_SHA ROLLBACK_CATALOG_SHA TREE PACKET_SOURCE_SHA FEEDGEN_SHA EXPECTED_DIST_SHA EXPECTED_CT_SHA EXPECTED_IMAGE_CT_SHA EXPECTED_IMAGE EXPECTED_TOOL_REFS EXPECTED_RUNNER_SHA EXPECTED_REVALIDATE_RUNNER_SHA PACKET_PATH PACKET_SHA BSR_EFFECTIVE_CONFIG_JSON SINCE_MAIN SINCE_BE SINCE_ENGAGEMENT; do
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
load_read_header() {
  HDR=$(mktemp); chmod 600 "$HDR"
  ( set -a; . "$ENV_FILE"; set +a; : "${FEEDGEN_READ_API_KEY:?}"; printf 'api-key: %s\n' "$FEEDGEN_READ_API_KEY" ) >"$HDR"
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
  bskyops_env "$BSKYOPS" ecosystem desired-state feedgen-sync-packet --active-only --catalog-yaml "${1:-$DEPLOYED_CATALOG_ROOT/publishers.yml}" --bsr-effective-config-json "$BSR_EFFECTIVE_CONFIG_JSON" --feedgen-url "$FEEDGEN_URL" --json
}
gate_body() {
  node - "$1" "$2" "$3" "$RKEYS" <<'NODE'
const fs=require('fs');const [f,from,to,rkeys]=process.argv.slice(2);const j=JSON.parse(fs.readFileSync(f));
const expected=rkeys.split(',').sort();
const transitionBlocked=j.status==='blocked'&&j.blockers?.length===1&&j.blockers[0].code==='feedgen-projection-blocked'&&(j.findings||[]).length===expected.length&&JSON.stringify(j.findings.map(x=>x.field?.match(/^feeds\.(.+)\.publisher_time_clock$/)?.[1]).sort())===JSON.stringify(expected)&&j.findings.every(x=>x.code==='catalog-ranker-feed-time-contract-mismatch'&&x.actual?.clock==='content_time_v1'&&x.actual?.contract_version===from&&x.expected?.clock==='content_time_v1'&&x.expected?.contract_version===to);
if((j.status!=='ready-for-apply'&&!transitionBlocked)||j.atomic_change_set?.atomic!==true)throw Error(`packet status ${j.status}`);
const check=(body,a,b)=>{const u=body.updates||[];if(JSON.stringify(u.map(x=>x.rkey).sort())!==JSON.stringify(expected))throw Error('not exact six');for(const x of u){const fields=Object.keys(x).filter(k=>!['op','rkey','if_current'].includes(k));if(fields.length!==1||fields[0]!=='content_time_contract_version')throw Error(`${x.rkey} not version-only`);if(x.content_time_contract_version!==b||x.if_current?.content_time_contract_version!==a)throw Error(`${x.rkey} asymmetric ${a}->${b}`)}};
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
bind_activation_floor() {
  local row floor
  [[ ! -e $E/activation-floor.txt ]] || die 'activation floor already exists; use a fresh evidence root'
  row=$(sudo -n docker exec -i feedgen-db psql -U feedgen -d feedgen-db -X -qAt -v ON_ERROR_STOP=1 -c "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"');")
  floor=$row
  [[ $floor =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die 'activation floor readback is malformed'
  echo "activation_floor=$floor" | emit activation-floor.txt
}
gate_v2_engagement_projection() { # <receipt name> <since>
  local name=$1 since=$2 until response summary http
  until=$(date -u +%Y-%m-%dT%H:%M:%S.000Z); response=$(mktemp); summary=$(mktemp); load_read_header
  http=$(curl -sS --max-time 60 -H "@$HDR" --get \
    --data-urlencode feed_id=newsflow-be-k --data-urlencode since="$since" --data-urlencode until="$until" \
    --data-urlencode scope=publisher --data-urlencode types=repost,like,quote,comment \
    -o "$response" -w '%{http_code}' "$FEEDGEN_URL/api/compliance/engagement" || true)
  if [[ $http != 200 ]]; then rm -f "$response" "$summary" "$HDR"; HDR=; die "$name returned HTTP $http"; fi
  if ! node - "$response" "$summary" <<'NODE'
const fs=require('fs'),crypto=require('crypto');const raw=fs.readFileSync(process.argv[2]);const j=JSON.parse(raw);const v=j.validity||{};
for(const k of ['projected_v3_to_v2_valid','projected_v3_to_v2_invalid','semantic_incompatible'])if(!Object.prototype.hasOwnProperty.call(v,k)||!Number.isInteger(v[k])||v[k]<0)throw Error(`missing/non-numeric validity.${k}`);
if(j.content_time_contract_version!=='newsflows-content-time/v2'||j.science_eligible!==true||v.semantic_incompatible!==0)throw Error('v2 engagement compatibility projection gate failed');
fs.writeFileSync(process.argv[3],JSON.stringify({gate:'pass',content_time_contract_version:j.content_time_contract_version,science_eligible:j.science_eligible,projected_v3_to_v2_valid:v.projected_v3_to_v2_valid,projected_v3_to_v2_invalid:v.projected_v3_to_v2_invalid,semantic_incompatible:v.semantic_incompatible,response_sha256:crypto.createHash('sha256').update(raw).digest('hex')},null,2)+'\n');
NODE
  then rm -f "$response" "$summary" "$HDR"; HDR=; die "$name validation failed"; fi
  emit "$name.json" <"$summary"; rm -f "$response" "$summary" "$HDR"; HDR=
}
revalidate_runner() {
  EXPECTED_SHA=$PACKET_SOURCE_SHA EXPECTED_DIST_SHA256=$EXPECTED_DIST_SHA EXPECTED_CT_SHA256=$EXPECTED_CT_SHA EXPECTED_IMAGE_CT_SHA256=$EXPECTED_IMAGE_CT_SHA \
    FROM_VERSION=$V2 TO_VERSION=$V3 IMG=$IMG RUNNER=container MIGRATION_DRAIN_SECONDS=$MIGRATION_DRAIN_SECONDS \
    ALLOW_FTFU1_OVERLAP_NORMALIZATION="${ALLOW_FTFU1_OVERLAP_NORMALIZATION:-0}" "$REVALIDATE" "$@"
}
migration_complete() {
  local label=$1 target=post
  [[ -f "$E/migrate-$target-apply-$label.json" ]] || return 1
  [[ $(node -e 'const r=JSON.parse(require("fs").readFileSync(process.argv[1])).revalidation;console.log(r.complete===true&&r.skipped_cas===0)' "$E/migrate-$target-apply-$label.json") == true ]] || return 1
}
apply_receipts_have_cas() { # <filename prefix>
  local prefix=$1 receipt skipped
  for receipt in "$E"/"$prefix"-apply-*.json; do
    [[ -f "$receipt" ]] || continue
    skipped=$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1])).revalidation.skipped_cas)' "$receipt" 2>/dev/null || true)
    [[ "$skipped" =~ ^[0-9]+$ ]] || return 0
    (( skipped > 0 )) && return 0
  done
  return 1
}
forward_readback_complete() {
  local marker stable_sha
  [[ -f "$E/migrate-stable-population.txt" && ! -L "$E/migrate-stable-population.txt" ]] || return 1
  stable_sha=$(sha "$E/migrate-stable-population.txt")
  for marker in "$E"/migrate-readback-*.txt; do
    [[ -f "$marker" && ! -L "$marker" ]] || continue
    [[ $(grep -c '^status=' "$marker") == 1 && $(grep -c '^transition=' "$marker") == 1 && $(grep -c '^stable_marker_sha256=' "$marker") == 1 ]] || continue
    grep -Fxq 'status=complete' "$marker" && grep -Fxq "transition=$V2->$V3" "$marker" && grep -Fxq "stable_marker_sha256=$stable_sha" "$marker" && return 0
  done
  return 1
}
forward_label_used() { [[ -n $(find "$E" -maxdepth 1 -type f -name "*forward-$1*" -print -quit) ]]; }
forward_to_completion() {
  local attempt label rc
  apply_receipts_have_cas migrate-post && die 'forward evidence root contains a CAS-conflict receipt; start a fresh evidence root'
  forward_readback_complete && return 0
  for attempt in 1 2 3 4 5; do
    if migration_complete "forward-$attempt"; then
      revalidate_runner migrate-readback
      forward_readback_complete && return 0
      die 'forward migration marked complete in apply receipt but readback marker missing'
    fi
  done
  for attempt in 1 2 3 4 5; do
    forward_label_used "$attempt" && continue
    label="forward-$attempt"
    if revalidate_runner migrate-apply "$label"; then :; else rc=$?; [[ $rc == 3 ]] && continue; return "$rc"; fi
    if migration_complete "$label" || [[ ! -f "$E/migrate-post-apply-$label.json" ]]; then
      revalidate_runner migrate-readback
      forward_readback_complete && return 0
      die 'forward migration finished but readback marker missing'
    fi
  done
  die 'forward revalidation remained incomplete after five resumable invocations'
}
native_tail_to_completion() {
  local attempt label rc
  apply_receipts_have_cas native-post && die 'native-tail evidence root contains a CAS-conflict receipt; start a fresh evidence root'
  [[ -f $E/native-tail-readback.txt ]] && return 0
  for attempt in 1 2 3 4 5; do
    label="native-$attempt"
    [[ -e $E/native-post-apply-$label.json ]] && continue
    if revalidate_runner migrate-native-tail-rollback "$label"; then return 0; else rc=$?; [[ $rc == 3 ]] || return "$rc"; fi
  done
  die 'native-v3 tail rollback remained incomplete after five resumable invocations'
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
  gate_v2_engagement_projection engagement-v2-projection-preflight "$(date -u -d '12 hours ago' +%Y-%m-%dT%H:%M:%S.000Z)"
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
  KEEP_TIMERS_FENCED_ON_EXIT=1
  catalog_sync_apply "$SOURCE_ROOT" | emit 01-catalog-sync-forward-apply.json; gate_catalog_sync "$E/01-catalog-sync-forward-apply.json"
  catalog_packet | emit 02-feedgen-sync-apply-packet.json; gate_body "$E/02-feedgen-sync-apply-packet.json" "$V2" "$V3"
  node -e 'const fs=require("fs"),a=JSON.parse(fs.readFileSync(process.argv[1])),j=JSON.parse(fs.readFileSync(process.argv[2]));if(JSON.stringify(a)!==JSON.stringify(j.atomic_change_set.request_body))throw Error("fresh apply body differs from bound preview body")' "$E/feedgen-forward.json" "$E/02-feedgen-sync-apply-packet.json"
  gate_v2_engagement_projection engagement-v2-projection-apply "$(date -u -d '12 hours ago' +%Y-%m-%dT%H:%M:%S.000Z)"
  bind_activation_floor
  post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply
  active_version_gate "$V3"
  revalidate_runner migrate-native-tail-plan
  revalidate_runner migrate-freeze
  forward_to_completion
  { echo '01 catalog-sync forward'; echo '02 feedgen sync packet gate'; echo '03 exact-six bulk v2->v3'; echo '04 drain and bind stable v2 semantic-delta population'; echo '05 affected post rows'; echo '06 engagement projection (no historical writes)'; } | emit apply-order.txt
  complete_timer_window
}
cmd_revalidate() { assert_bindings; active_version_gate "$V3"; fence_timers; KEEP_TIMERS_FENCED_ON_EXIT=1; [[ -f $E/migrate-stable-population.txt ]] || revalidate_runner migrate-freeze; revalidate_runner migrate-stable-check; forward_to_completion; complete_timer_window; }
cmd_normalize_overlap() {
  local label=${2:?unique label is required} maxb=${3:-}
  [[ ${ALLOW_FTFU1_OVERLAP_NORMALIZATION:-0} == 1 ]] || die 'set ALLOW_FTFU1_OVERLAP_NORMALIZATION=1 for the explicit continuation path'
  [[ "$label" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || die 'normalization label is unsafe'
  TIMER_STATE_LABEL=$label
  assert_bindings; active_version_gate "$V2"; fence_timers
  KEEP_TIMERS_FENCED_ON_EXIT=1
  revalidate_runner migrate-normalize-overlap "$label" "$maxb"
  complete_timer_window
}
cmd_readback() {
  assert_bindings
  active_version_gate "$V3"
  revalidate_runner migrate-readback
  sleep "${DRAIN_INTERVAL_SECONDS:-60}"
  revalidate_runner migrate-readback
  local changed restored den
  changed=$(awk -F= '$1=="ir_semantic_changed"{print $2}' "$E/migrate-stable-population.txt")
  restored=$(awk -F= '$1=="ir_restored_valid"{print $2}' "$E/migrate-stable-population.txt")
  den=$(awk -F= '$1=="ir_total_denominator"{print $2}' "$E/migrate-stable-population.txt")
  { echo "irish_semantic_changed=$changed"; echo "irish_restored_valid=$restored"; echo "irish_in_horizon_v2_denominator=$den"; echo 'participant_exposure_not_inferred=true'; } | emit irish-restoration.txt
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
  KEEP_TIMERS_FENCED_ON_EXIT=1
  post_bulk "$E/feedgen-rollback.json" 01-feedgen-rollback-v3-to-v2
  active_version_gate "$V2"
  catalog_sync_apply "$ROLLBACK_SOURCE_ROOT" | emit 02-catalog-sync-rollback-apply.json; gate_catalog_sync "$E/02-catalog-sync-rollback-apply.json"
  revalidate_runner migrate-rollback apply reverse
  native_tail_to_completion
  gate_v2_engagement_projection engagement-v2-projection-rollback "$(awk -F= '$1=="activation_floor"{print $2}' "$E/activation-floor.txt")"
  { echo '01 exact-six bulk v3->v2'; echo '02 catalog desired-state rollback'; echo '03 reverse historical semantic-delta post rows below activation floor'; echo '04 after drain reverse native v3 post tail at/after activation floor'; echo '05 retain engagement provenance; require v2 compatibility projection'; } | emit rollback-order.txt
  complete_timer_window
}
cmd_finalize() {
  assert_bindings
  revalidate_runner migrate-secret-scan
  revalidate_runner migrate-finalize "${2:?window start is required}" "${3:?window end is required}"
}

case "$COMMAND" in
  test-forward-resume) [[ ${FTFU1_TEST_MODE:-0} == 1 ]] || die 'test-only command'; forward_to_completion;;
  test-native-tail-resume) [[ ${FTFU1_TEST_MODE:-0} == 1 ]] || die 'test-only command'; native_tail_to_completion;;
  preflight) cmd_preflight;; preview) cmd_preview;; apply) cmd_apply;; revalidate) cmd_revalidate;; normalize-overlap) cmd_normalize_overlap "$@";; readback) cmd_readback;; rollback-dryrun) cmd_rollback_dryrun;; rollback) cmd_rollback;; finalize) cmd_finalize "$@";;
  *) echo 'usage: content_time_contract_upgrade_packet.sh preflight|preview|apply|revalidate|normalize-overlap <unique-label> [max-batches]|readback|rollback-dryrun|rollback|finalize <start> <end>' >&2; exit 2;;
esac
