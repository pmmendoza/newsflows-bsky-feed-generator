#!/usr/bin/env bash
# Operator runner for the Belgium Step-2 legacy content-time recovery packet
# (dev/be-vlg-elif ... 2026-08-18 mission). Recovers the BE publisher's
# `legacy_unknown` rows (created_at_source_raw IS NULL) inside a rolling
# HORIZON_DAYS window from the Bluesky AppView via the tool's recover mode
# (`node dist/tools/backfill-publisher-posts.js --mode recover --plan-from-db
# --no-insert ...`). Sibling to scripts/content_time_revalidate_packet.sh:
# same style (set -euo pipefail; log/die/emit/psql_ro/psql_copy/run_tool/
# jsonq/assert_tree/take_control/control_rates/pgstat_read/latest_control
# copied verbatim), same raw-free posture (DSN composed in-container by
# scripts/compose_feedgen_dsn.js, never printed; secret-scan required before
# finalize), same evidence discipline ($E, root:newsflows 0640, never
# overwritten). Unlike the revalidate packet this operates on ONE publisher
# group (BE), so subcommands take no <group> argument and filenames say "be"
# literally rather than being group-parameterized.
#
# Subcommands:
#   prereg                read-only helper for the ledger approval; does NOT
#                          require $E (uses a scratch mktemp dir for the tool
#                          dry-run); prints per-rkey legacy counts in
#                          [SINCE,UNTIL), the all-time legacy total for the
#                          actors, and PREREG=... for binding
#   preflight              hard gates (tree/dist/image hashes, retention
#                          disabled, tool refs, catalog DIDs/horizon, BSR
#                          readback, SINCE/UNTIL bounds); records source-set,
#                          prestate scope/populations/rows, gates PREREG
#                          exactly against a dry-run, control read #1
#   control                an additional 60 s idle control read
#   preview                the tool dry-run again; same gates as preflight
#   apply <label> [max_batches]
#                          --apply --no-insert --checkpoint-file
#                          --packet-sha256; per-batch WAL/relation/dead-tuple
#                          ceilings identical to the revalidate packet v10
#   readback                re-runnable with attempt suffix; post-apply
#                          dry-run must be empty; diffs poststate against the
#                          prestate snapshot; blast-radius checks
#   restore                bounded CAS restore of recovered rows back to the
#                          prestate (all five content-time columns), keyset
#                          batches of 500, resumable by cursor
#   secret-scan             copied verbatim from the revalidate packet
#   finalize <start> <end> RESULT.txt + SHA256SUMS
#
# Required env: E (except for `prereg`) TREE EXPECTED_SHA EXPECTED_DIST_SHA256
#   EXPECTED_CT_SHA256 EXPECTED_IMAGE_CT_SHA256 PACKET_SHA EXPECTED_TOOL_REFS
#   RECOVER_DIDS (comma list of publisher DIDs) RECOVER_RKEY_PATTERN (regex
#   over feedgen_ops.feed_catalog.rkey) HORIZON_DAYS SINCE UNTIL (absolute
#   ISO-8601 with milliseconds and Z) PREREG (pre-registered cells
#   "legacy_in_window=N,unretrievable=N,would_recover=N,recover_source_valid=N,
#   recover_source_invalid=N,would_insert=0,conflict=0", exact-match)
# Optional env (production defaults): IMG NETWORK ENV_FILE DB_CONTAINER
#   PSQL_DB PSQL_USER DOCKER FEEDGEN_CONTAINER RUNNER (container|host; host =
#   rehearsal with HOST_DSN) API_BASE (default https://public.api.bsky.app)
#   CEIL_WAL_BASELINE_MULTIPLE CEIL_WAL_FLOOR_BYTES PAUSE_SECONDS
#   CEIL_REL_BYTES READBACK_JSON SKIP_LIVE_IMAGE_CHECKS SECRET_KEY_REGEX
set -euo pipefail

SUBCMD="${1:-}"

: "${TREE:?TREE (built feedgen tree) is required}"
: "${EXPECTED_SHA:?EXPECTED_SHA (full source SHA) is required}"
: "${EXPECTED_DIST_SHA256:?EXPECTED_DIST_SHA256 is required}"
: "${EXPECTED_CT_SHA256:?EXPECTED_CT_SHA256 is required}"
: "${EXPECTED_IMAGE_CT_SHA256:?EXPECTED_IMAGE_CT_SHA256 (validator module hash inside the live image) is required}"
: "${PACKET_SHA:?PACKET_SHA (approved packet SHA-256) is required}"
: "${EXPECTED_TOOL_REFS:?EXPECTED_TOOL_REFS (bsky-ops=<sha>,blueskyranker=<sha>,newsflows-bskyhealth=<sha>) is required}"
: "${EXPECTED_RUNNER_SHA256:?EXPECTED_RUNNER_SHA256 (SHA-256 of this runner script as frozen in the packet) is required}"
: "${EXPECTED_DSN_HELPER_SHA256:?EXPECTED_DSN_HELPER_SHA256 (SHA-256 of scripts/compose_feedgen_dsn.js) is required}"
: "${RECOVER_DIDS:?RECOVER_DIDS (comma list of publisher DIDs) is required}"
: "${RECOVER_RKEY_PATTERN:?RECOVER_RKEY_PATTERN (regex over feedgen_ops.feed_catalog.rkey) is required}"
: "${HORIZON_DAYS:?HORIZON_DAYS is required}"
: "${SINCE:?SINCE (absolute ISO-8601 Z lower bound) is required}"
: "${UNTIL:?UNTIL (absolute ISO-8601 Z upper bound) is required}"
: "${PREREG:?PREREG (pre-registered cells) is required}"
: "${EXPECTED_RKEYS:?EXPECTED_RKEYS (exact csv of enabled feed rkeys whose publisher is RECOVER_DIDS) is required}"
PERMIT_NEGATIVE_PROBE="${PERMIT_NEGATIVE_PROBE:-0}"   # rehearsal-only escape hatch for the deliberate ceiling probe; pinned 0 in production
[[ "$SUBCMD" == "prereg" ]] || : "${E:?E (evidence root) is required}"
[[ "$PACKET_SHA" =~ ^[0-9a-f]{64}$ ]] || { echo "PACKET_SHA must be 64 lowercase hex" >&2; exit 2; }

IMG="${IMG:-pmmendoza/bsky-feedgen@sha256:928c15aac77a8a842f60053eff8953e70cc9e4117c2fbe86f548e345c1a34711}"
NETWORK="${NETWORK:-newsflows-bsky-feed-generator-v2_default}"
ENV_FILE="${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}"
DB_CONTAINER="${DB_CONTAINER:-feedgen-db}"
PSQL_DB="${PSQL_DB:-feedgen-db}"
PSQL_USER="${PSQL_USER:-feedgen}"
FEEDGEN_CONTAINER="${FEEDGEN_CONTAINER:-feedgen}"
RUNNER="${RUNNER:-container}"
API_BASE="${API_BASE:-https://public.api.bsky.app}"
# WAL ceiling (D4 amended 2026-08-18, owner): identical rule and defaults to the revalidate packet.
CEIL_WAL_BASELINE_MULTIPLE="${CEIL_WAL_BASELINE_MULTIPLE:-1.0}"
CEIL_WAL_FLOOR_BYTES="${CEIL_WAL_FLOOR_BYTES:-614400}"
PAUSE_SECONDS="${PAUSE_SECONDS:-1}"
CEIL_REL_BYTES="${CEIL_REL_BYTES:-409600}"
READBACK_JSON="${READBACK_JSON:-/opt/newsflows/blueskyranker_v2/logs/effective_config_readback.json}"
SKIP_LIVE_IMAGE_CHECKS="${SKIP_LIVE_IMAGE_CHECKS:-0}"
SKIP_BSR_READBACK="${SKIP_BSR_READBACK:-0}"   # rehearsal-only; the BSR readback gates run whenever this is 0 (independent of the image checks)
SECRET_KEY_REGEX="${SECRET_KEY_REGEX:-(PASSWORD|SECRET|TOKEN|_KEY|APIKEY|PASS)}"
read -r -a DOCKER <<<"${DOCKER:-sudo -n docker}"
V1='newsflows-content-time/v1'
V2='newsflows-content-time/v2'

log() { echo "[recover-packet] $*" >&2; }
die() { echo "[recover-packet] STOP: $*" >&2; exit 2; }
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
sql_array() { echo "'{$1}'"; }

emit() {  # emit <name>  (stdin -> $E/<name>, root:newsflows 0640, never overwrite)
  local name=$1 tmp
  [[ -e "$E/$name" ]] && die "refusing to overwrite existing evidence file $E/$name"
  tmp=$(mktemp); cat >"$tmp"
  sudo -n install -o root -g newsflows -m 640 "$tmp" "$E/$name"; rm -f "$tmp"
  log "wrote $name"
}
psql_ro() {
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -A -F '|' -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '120s';" "$@"
}
psql_copy() {
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '120s';" -c "\\copy ($1) TO STDOUT WITH (FORMAT text)"
}
pgstat_read() {  # wal_bytes|relation_bytes|n_dead_tup|n_live_tup|epoch_seconds
  psql_ro -t -c "SELECT (SELECT wal_bytes FROM pg_stat_wal), pg_total_relation_size('public.post'), n_dead_tup, n_live_tup, extract(epoch FROM now())::bigint FROM pg_stat_user_tables WHERE relname='post';"
}
assert_tree() {  # hard integrity gate, run by preflight/apply/restore
  local sha; sha=$(git -C "$TREE" rev-parse HEAD)
  [[ "$sha" == "$EXPECTED_SHA" ]] || die "tree HEAD $sha != EXPECTED_SHA $EXPECTED_SHA"
  [[ -z "$(git -C "$TREE" status --porcelain)" ]] || die "tree is dirty"
  local d1 d2; d1=$(sha256sum "$TREE/dist/tools/backfill-publisher-posts.js" | cut -d' ' -f1); d2=$(sha256sum "$TREE/dist/util/content-time.js" | cut -d' ' -f1)
  [[ "$d1" == "$EXPECTED_DIST_SHA256" ]] || die "dist backfill hash $d1 != expected"
  [[ "$d2" == "$EXPECTED_CT_SHA256" ]] || die "dist content-time hash $d2 != expected"
  # the script that is actually executing (not merely the tree copy) and the DSN helper must be the frozen ones
  local r0 r1 h1; r0=$(sha256sum "${BASH_SOURCE[0]}" | cut -d' ' -f1); r1=$(sha256sum "$TREE/scripts/content_time_recover_packet.sh" | cut -d' ' -f1); h1=$(sha256sum "$TREE/scripts/compose_feedgen_dsn.js" | cut -d' ' -f1)
  [[ "$r0" == "$EXPECTED_RUNNER_SHA256" && "$r1" == "$EXPECTED_RUNNER_SHA256" ]] || die "runner script hash (executing $r0 / tree $r1) != EXPECTED_RUNNER_SHA256 $EXPECTED_RUNNER_SHA256"
  [[ "$h1" == "$EXPECTED_DSN_HELPER_SHA256" ]] || die "DSN helper hash $h1 != EXPECTED_DSN_HELPER_SHA256"
}
# every subcommand after preflight must run with the SAME bound values preflight recorded (no re-binding of SINCE/UNTIL/
# PREREG/PACKET_SHA/DIDs/pattern/horizon between invocations -- a readback with a narrowed window or re-bound cells is not a readback)
assert_bound_env() {
  local ss="$E/source-set.txt"; [[ -f "$ss" ]] || die "no source-set.txt in $E -- run preflight first"
  local k v exp
  for k in since until prereg packet_sha256 recover_dids recover_rkey_pattern horizon_days api_base runner image env_file skip_live_image_checks skip_bsr_readback secret_key_regex ceil_wal_floor_bytes ceil_wal_baseline_multiple pause_seconds ceil_rel_bytes tree source_sha dist_backfill_sha256 dist_content_time_sha256 image_validator_sha256_expected runner_script_sha256 dsn_helper_sha256 expected_tool_refs expected_rkeys permit_negative_probe readback_json db_container psql_db psql_user feedgen_container network; do
    case "$k" in since) v=$SINCE;; until) v=$UNTIL;; prereg) v=$PREREG;; packet_sha256) v=$PACKET_SHA;; recover_dids) v=$RECOVER_DIDS;; recover_rkey_pattern) v=$RECOVER_RKEY_PATTERN;; horizon_days) v=$HORIZON_DAYS;; api_base) v=$API_BASE;; runner) v=$RUNNER;; image) v=$IMG;; env_file) v=$ENV_FILE;; skip_live_image_checks) v=$SKIP_LIVE_IMAGE_CHECKS;; skip_bsr_readback) v=$SKIP_BSR_READBACK;; secret_key_regex) v=$SECRET_KEY_REGEX;; ceil_wal_floor_bytes) v=$CEIL_WAL_FLOOR_BYTES;; ceil_wal_baseline_multiple) v=$CEIL_WAL_BASELINE_MULTIPLE;; pause_seconds) v=$PAUSE_SECONDS;; ceil_rel_bytes) v=$CEIL_REL_BYTES;; tree) v=$TREE;; source_sha) v=$EXPECTED_SHA;; dist_backfill_sha256) v=$EXPECTED_DIST_SHA256;; dist_content_time_sha256) v=$EXPECTED_CT_SHA256;; image_validator_sha256_expected) v=$EXPECTED_IMAGE_CT_SHA256;; runner_script_sha256) v=$EXPECTED_RUNNER_SHA256;; dsn_helper_sha256) v=$EXPECTED_DSN_HELPER_SHA256;; expected_tool_refs) v=$EXPECTED_TOOL_REFS;; expected_rkeys) v=$EXPECTED_RKEYS;; permit_negative_probe) v=$PERMIT_NEGATIVE_PROBE;; readback_json) v=$READBACK_JSON;; db_container) v=$DB_CONTAINER;; psql_db) v=$PSQL_DB;; psql_user) v=$PSQL_USER;; feedgen_container) v=$FEEDGEN_CONTAINER;; network) v=$NETWORK;; esac
    # the deliberate NEGATIVE probe (label ^neg): permitted only when preflight pinned permit_negative_probe=1 (rehearsal), and only LOWERING the two WAL knobs
    if [[ "${ALLOW_CEILING_OVERRIDE:-0}" == "1" && ( "$k" == "ceil_wal_floor_bytes" || "$k" == "ceil_wal_baseline_multiple" ) ]]; then exp=$(sed -n "s/^$k=//p" "$ss" | head -1); awk -v a="$v" -v b="$exp" 'BEGIN{exit !(a+0 < b+0)}' || die "negative probe may only LOWER $k (recorded $exp, given $v)"; continue; fi
    exp=$(sed -n "s/^$k=//p" "$ss" | head -1)
    [[ -n "$exp" ]] || die "source-set.txt lacks $k"
    [[ "$exp" == "$v" ]] || die "bound-value mismatch for $k: preflight recorded '$exp', this invocation has '$v' -- STOP (values may not be re-bound between invocations)"
  done
}
latest_control() { ls -1 "$E"/pg-control-*.txt 2>/dev/null | sort -V | tail -1; }
control_rates() {  # prints "wal_per_s rel_per_s dead_per_s" from the latest control file (clamped at 0)
  local f; f=$(latest_control); [[ -n "$f" ]] || die "no control read yet"
  awk -F'|' '/^read1 /{sub(/^read1 /,""); w1=$1; r1=$2; d1=$3; t1=$5} /^read2 /{sub(/^read2 /,""); w2=$1; r2=$2; d2=$3; t2=$5}
    END{dt=t2-t1; if(dt<1)dt=1; w=(w2-w1)/dt; r=(r2-r1)/dt; d=(d2-d1)/dt; if(w<0)w=0; if(r<0)r=0; if(d<0)d=0; printf "%d %d %d\n", w, r, d}' "$f"
}
take_control() {  # take_control <name>
  { echo "read1 $(pgstat_read)"; sleep 60; echo "read2 $(pgstat_read)"; } | emit "$1"
}

run_tool() {  # run_tool <outname> <tool args...>  -> $E/<outname>.json + .err ; echoes exit code
  local out=$1; shift
  [[ -e "$E/$out.json" || -e "$E/$out.err" ]] && die "refusing to overwrite $E/$out.*"
  local so se; so=$(mktemp); se=$(mktemp); local rc=0
  if [[ "$RUNNER" == "host" ]]; then
    : "${HOST_DSN:?HOST_DSN required for RUNNER=host}"
    sudo -n env FEEDGEN_POSTGRES_URL="$HOST_DSN" node "$TREE/dist/tools/backfill-publisher-posts.js" "$@" >"$so" 2>"$se" || rc=$?
  else
    # explicit entrypoint; DSN composed in-container by the committed helper (no shell expansion of secrets)
    # NODE_OPTIONS pins the AppView calls to IPv4 (outbound IPv6 is deliberately off on the host — owner 2026-08-17)
    "${DOCKER[@]}" run --rm --network "$NETWORK" --env-file "$ENV_FILE" -e NODE_OPTIONS=--dns-result-order=ipv4first \
      -v "$TREE:/src:ro" -v "$E:/evidence" -w /src --entrypoint sh "$IMG" \
      -c 'export FEEDGEN_POSTGRES_URL="$(node /src/scripts/compose_feedgen_dsn.js)" || exit 97; exec node /src/dist/tools/backfill-publisher-posts.js "$@"' sh "$@" \
      >"$so" 2>"$se" || rc=$?
  fi
  emit "$out.json" <"$so"; emit "$out.err" <"$se"; rm -f "$so" "$se"
  echo "$rc"
}
run_tool_scratch() {  # run_tool_scratch <dir> <outname> <tool args...>  -> <dir>/<outname>.json + .err (plain files, no $E needed); echoes exit code
  local dir=$1 out=$2; shift 2
  local so="$dir/$out.json" se="$dir/$out.err" rc=0
  if [[ "$RUNNER" == "host" ]]; then
    : "${HOST_DSN:?HOST_DSN required for RUNNER=host}"
    sudo -n env FEEDGEN_POSTGRES_URL="$HOST_DSN" node "$TREE/dist/tools/backfill-publisher-posts.js" "$@" >"$so" 2>"$se" || rc=$?
  else
    "${DOCKER[@]}" run --rm --network "$NETWORK" --env-file "$ENV_FILE" -e NODE_OPTIONS=--dns-result-order=ipv4first \
      -v "$TREE:/src:ro" -v "$dir:/evidence" -w /src --entrypoint sh "$IMG" \
      -c 'export FEEDGEN_POSTGRES_URL="$(node /src/scripts/compose_feedgen_dsn.js)" || exit 97; exec node /src/dist/tools/backfill-publisher-posts.js "$@"' sh "$@" \
      >"$so" 2>"$se" || rc=$?
  fi
  echo "$rc"
}
jsonq() { node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));const v=process.argv[2].split(".").reduce((a,k)=>a==null?a:a[k],j);console.log(v===undefined||v===null?"":(typeof v==="object"?JSON.stringify(v):v))' "$1" "$2"; }

# by PUBLISHER DID (the mutation is scoped by author): every enabled feed whose publisher is one of RECOVER_DIDS must serve on receipt_time
CATALOG_SQL="SELECT rkey, publisher_did, publisher_post_max_age_days, publisher_time_clock FROM feedgen_ops.feed_catalog WHERE enabled AND publisher_did = ANY($(sql_array "$RECOVER_DIDS")::text[]) ORDER BY 1;"
POP_SQL="SELECT c.rkey,
 count(*) FILTER (WHERE p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL') AS total,
 count(*) FILTER (WHERE p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL' AND p.content_time_status='source_valid' AND p.content_time_validator_version='$V2') AS v2_valid,
 count(*) FILTER (WHERE p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL' AND p.content_time_status='source_invalid' AND p.content_time_validator_version='$V2') AS v2_invalid,
 count(*) FILTER (WHERE p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL' AND p.content_time_validator_version='$V1') AS v1,
 count(*) FILTER (WHERE p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL' AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status='legacy_unknown')) AS legacy
FROM feedgen_ops.feed_catalog c JOIN post p ON p.author = c.publisher_did
WHERE c.enabled AND c.rkey ~ '^($RECOVER_RKEY_PATTERN)$'
GROUP BY c.rkey ORDER BY c.rkey;"
SCOPE_SQL="SELECT 'legacy_in_window', count(*) FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL' AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status='legacy_unknown')
UNION ALL SELECT 'legacy_outside_window', count(*) FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND NOT (p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL') AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status='legacy_unknown')
UNION ALL SELECT 'legacy_all_time', count(*) FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status='legacy_unknown')
UNION ALL SELECT 'legacy_after_until', count(*) FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND p.\"indexedAt\" >= '$UNTIL' AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status='legacy_unknown')
UNION ALL SELECT 'wrong_version_in_rolling_horizon', count(*) FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND p.\"indexedAt\" >= to_char(now() at time zone 'utc' - interval '$HORIZON_DAYS days', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AND p.\"indexedAt\" < '$UNTIL' AND NOT (p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status='legacy_unknown')) AND (p.content_time_validator_version IS DISTINCT FROM 'newsflows-content-time/v2')
UNION ALL SELECT 'unclassified_or_wrong_version_in_rolling_horizon_after_until', count(*) FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND p.\"indexedAt\" >= '$UNTIL' AND (p.content_time_validator_version IS DISTINCT FROM 'newsflows-content-time/v2');"
PREREG_RKEY_SQL="SELECT c.rkey, count(*) AS legacy_in_window
FROM feedgen_ops.feed_catalog c JOIN post p ON p.author = c.publisher_did
WHERE c.enabled AND c.rkey ~ '^($RECOVER_RKEY_PATTERN)$'
  AND p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL'
  AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status = 'legacy_unknown')
GROUP BY c.rkey ORDER BY c.rkey;"
PREREG_ALLTIME_SQL="SELECT count(*) AS legacy_all_time FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status = 'legacy_unknown');"

snapshot_sql_legacy() {  # the legacy rows in [SINCE,UNTIL) for the actors, ordered by uri -- the prestate population
  echo "SELECT p.uri, p.author, p.\"indexedAt\", p.\"createdAt\", p.cid, encode(p.created_at_source_raw,'hex') AS raw_hex, p.content_time_utc, p.content_time_status, p.content_time_clamp_reason, p.content_time_validator_version FROM public.post p WHERE p.author = ANY($(sql_array "$RECOVER_DIDS")::text[]) AND p.\"indexedAt\" >= '$SINCE' AND p.\"indexedAt\" < '$UNTIL' AND p.created_at_source_raw IS NULL AND (p.content_time_status IS NULL OR p.content_time_status = 'legacy_unknown') ORDER BY p.uri"
}
plan_uris_path() {  # the immutable plan (URIs of the prestate legacy rows), as seen by the tool (container path or host path)
  if [[ "$RUNNER" == "host" ]]; then echo "$E/step1-be-plan-uris.txt"; else echo "/evidence/step1-be-plan-uris.txt"; fi
}
snapshot_by_uris() {  # snapshot_by_uris <urifile> -> stdout tab-text rows, SAME columns/order as snapshot_sql_legacy, for exactly the given URIs
  local urifile=$1
  { echo "BEGIN; SET statement_timeout = '120s';"  # (a temp table of URIs is created; the path issues no writes to public.* -- READ ONLY cannot be set because CREATE TEMP TABLE is refused in a read-only txn)
    echo "CREATE TEMP TABLE want_uris(uri text);"
    echo "COPY want_uris FROM STDIN WITH (FORMAT text);"
    cut -f1 "$urifile"
    echo '\.'
    echo "\\copy (SELECT p.uri, p.author, p.\"indexedAt\", p.\"createdAt\", p.cid, encode(p.created_at_source_raw,'hex') AS raw_hex, p.content_time_utc, p.content_time_status, p.content_time_clamp_reason, p.content_time_validator_version FROM public.post p JOIN want_uris w ON w.uri = p.uri ORDER BY p.uri) TO STDOUT WITH (FORMAT text)"
    echo "COMMIT;"
  } | "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -v ON_ERROR_STOP=1
}
prereg_value() {  # <key> from PREREG ("k=v,k=v,..."); a missing cell is a hard stop
  local v; v=$(echo "$PREREG" | tr ',' '\n' | awk -F= -v k="$1" '$1==k{print $2}')
  [[ "$v" =~ ^[0-9]+$ ]] || die "pre-registered cell $1 missing or non-numeric in PREREG"
  echo "$v"
}
gate_cell() {  # gate_cell <label> <expected> <actual>  (exact match only -- no tolerance)
  [[ "$2" == "$3" ]] || die "gate $1: expected $2 got $3"
  log "gate $1 ok (expected $2, got $3)"
}
gate_recover_preview() {  # gate_recover_preview <jsonfile> -- gates every PREREG cell EXACTLY against a recover dry-run
  local f=$1
  [[ "$(jsonq "$f" operation)" == "publisher-post-recover" ]] || die "$f: operation != publisher-post-recover"
  [[ "$(jsonq "$f" mode)" == "dry-run" ]] || die "$f: mode != dry-run"
  [[ "$(jsonq "$f" plan_source)" == "plan-uris-file+getPosts" ]] || die "$f: plan_source != plan-uris-file+getPosts (the frozen prestate plan)"
  gate_cell legacy_in_window "$(prereg_value legacy_in_window)" "$(jsonq "$f" db_legacy_in_window)"
  gate_cell unretrievable "$(prereg_value unretrievable)" "$(jsonq "$f" unretrievable)"
  gate_cell would_recover "$(prereg_value would_recover)" "$(jsonq "$f" preview.would_recover)"
  gate_cell recover_source_valid "$(prereg_value recover_source_valid)" "$(jsonq "$f" preview.recover_by_status.source_valid)"
  gate_cell recover_source_invalid "$(prereg_value recover_source_invalid)" "$(jsonq "$f" preview.recover_by_status.source_invalid)"
  gate_cell would_insert "$(prereg_value would_insert)" "$(jsonq "$f" preview.would_insert)"
  gate_cell conflict "$(prereg_value conflict)" "$(jsonq "$f" preview.conflict)"
  local candidates unretr legacy rows notinplan
  candidates=$(jsonq "$f" preview.candidates); unretr=$(jsonq "$f" unretrievable); legacy=$(jsonq "$f" db_legacy_in_window); rows=$(wc -l <"$E/step1-be-prestate-rows.tsv")
  [[ $(( candidates + unretr )) == "$legacy" && "$legacy" == "$rows" ]] || die "gate consistency: candidates($candidates)+unretrievable($unretr) must equal legacy_in_window($legacy) and prestate rows($rows)"
  [[ "$(jsonq "$f" plan_uris_not_in_db)" == "0" ]] || die "gate: plan_uris_not_in_db=$(jsonq "$f" plan_uris_not_in_db) (frozen plan URIs missing from the DB)"
  notinplan=$(jsonq "$f" preview.db_legacy_not_in_plan)
  [[ "$notinplan" == "$unretr" ]] || die "gate consistency: preview.db_legacy_not_in_plan($notinplan) != unretrievable($unretr)"
  log "gate recover preview ok: candidates=$candidates unretrievable=$unretr legacy_in_window=$legacy"
}

cmd_prereg() {  # read-only helper for the ledger approval: does NOT require $E
  assert_tree
  echo "since=$SINCE until=$UNTIL horizon_days=$HORIZON_DAYS"
  echo "--- per-rkey legacy count in [since,until) (rkey|legacy_in_window)"
  psql_ro -c "$PREREG_RKEY_SQL"
  echo "--- all-time legacy total for the actors (legacy_all_time)"
  psql_ro -c "$PREREG_ALLTIME_SQL"
  local dir; dir=$(mktemp -d)
  local rc; rc=$(run_tool_scratch "$dir" prereg-dry-run --mode recover --plan-from-db --no-insert --actors "$RECOVER_DIDS" --since "$SINCE" --until "$UNTIL" --api-base "$API_BASE" --packet-sha256 "$PACKET_SHA" --json)
  local f="$dir/prereg-dry-run.json"
  if [[ "$rc" != "0" ]]; then cat "$dir/prereg-dry-run.err" >&2; rm -rf "$dir"; die "prereg dry-run exit=$rc"; fi
  if [[ "$(jsonq "$f" operation)" != "publisher-post-recover" || "$(jsonq "$f" mode)" != "dry-run" ]]; then rm -rf "$dir"; die "prereg dry-run did not run the recover dry-run path"; fi
  local legacy unretr wr rsv rsi wi cf req
  legacy=$(jsonq "$f" db_legacy_in_window); unretr=$(jsonq "$f" unretrievable); wr=$(jsonq "$f" preview.would_recover)
  rsv=$(jsonq "$f" preview.recover_by_status.source_valid); rsi=$(jsonq "$f" preview.recover_by_status.source_invalid)
  wi=$(jsonq "$f" preview.would_insert); cf=$(jsonq "$f" preview.conflict); req=$(jsonq "$f" appview_requests)
  echo "PREREG=legacy_in_window=$legacy,unretrievable=$unretr,would_recover=$wr,recover_source_valid=$rsv,recover_source_invalid=$rsi,would_insert=$wi,conflict=$cf"
  echo "appview_requests=$req"
  rm -rf "$dir"
}

gate_catalog_clock() {  # <catalog-readback.tsv>: every enabled row of the pattern carries publisher_time_clock == receipt_time (empty/NULL is a STOP), a positive max-age, and the row set is exactly the expected rkeys
  local f=$1; local rows; rows=$(awk -F'|' 'NR>1 && NF>=4 && $1 !~ /^\(/ {print}' "$f")
  local n; n=$(echo "$rows" | grep -c . || true); (( n >= 1 )) || die "catalog readback returned no rows for '$RECOVER_RKEY_PATTERN'"
  local badclock; badclock=$(echo "$rows" | awk -F'|' '$4!="receipt_time"{print $1"="($4==""?"NULL":$4)}' | tr '\n' ',' ); [[ -z "$badclock" ]] || die "publisher_time_clock must be receipt_time on EVERY enabled row of the pattern; offending: $badclock -- a content-time (or unknown) clock makes this packet a serving change -- STOP"
  local badage; badage=$(echo "$rows" | awk -F'|' '$3=="" || $3+0<1 {print $1}' | tr '\n' ','); [[ -z "$badage" ]] || die "publisher_post_max_age_days NULL/invalid on: $badage"
  local got; got=$(echo "$rows" | cut -d'|' -f1 | sort | tr '\n' ','); [[ "$got" == "$(echo "$EXPECTED_RKEYS" | tr ',' '\n' | sort | tr '\n' ',')" ]] || die "enabled feeds carrying the publisher(s) = '$got' != EXPECTED_RKEYS '$EXPECTED_RKEYS' (a feed outside the expected set reads this publisher -- STOP)"
  local badpat; badpat=$(echo "$rows" | awk -F'|' -v pat="^($RECOVER_RKEY_PATTERN)\$" '$1 !~ pat {print $1}' | tr '\n' ','); [[ -z "$badpat" ]] || die "feeds carrying the publisher outside RECOVER_RKEY_PATTERN: $badpat"
}
cmd_preflight() {
  [[ -d "$E" ]] || sudo -n install -d -o root -g newsflows -m 750 "$E"
  assert_tree
  local sha; sha=$(git -C "$TREE" rev-parse HEAD)
  local runner_sha helper_sha; runner_sha=$(sha256sum "$TREE/scripts/content_time_recover_packet.sh" | cut -d' ' -f1); helper_sha=$(sha256sum "$TREE/scripts/compose_feedgen_dsn.js" | cut -d' ' -f1)
  local img_ct="skipped" ret="skipped"
  if [[ "$SKIP_LIVE_IMAGE_CHECKS" != "1" ]]; then
    img_ct=$("${DOCKER[@]}" run --rm --entrypoint sh "$IMG" -c 'sha256sum /app/dist/util/content-time.js' | cut -d' ' -f1)
    [[ "$img_ct" == "$EXPECTED_IMAGE_CT_SHA256" ]] || die "live image validator module hash $img_ct != EXPECTED_IMAGE_CT_SHA256"
    ret=$("${DOCKER[@]}" exec "$FEEDGEN_CONTAINER" sh -c 'echo "${FEEDGEN_RETENTION_ENABLED:-unset}"')
    [[ "$ret" == "unset" || "$ret" == "false" || "$ret" == "0" ]] || die "FEEDGEN_RETENTION_ENABLED=$ret in the running feedgen (must be disabled; D6)"
    local img_id run_img; img_id=$("${DOCKER[@]}" image inspect "$IMG" --format '{{.Id}}'); run_img=$("${DOCKER[@]}" inspect "$FEEDGEN_CONTAINER" --format '{{.Image}}')
    [[ "$img_id" == "$run_img" ]] || die "running $FEEDGEN_CONTAINER image ID $run_img != pinned IMG image ID $img_id"
    { "${DOCKER[@]}" inspect "$FEEDGEN_CONTAINER" --format '{{.Id}} {{.Image}} {{.Config.Image}} {{.State.StartedAt}} {{.RestartCount}}'; echo "pinned_image_id=$img_id"; } | emit feedgen-prestate.txt
    local t exp got tline=""
    for t in bsky-ops blueskyranker newsflows-bskyhealth; do
      exp=$(echo "$EXPECTED_TOOL_REFS" | tr ',' '\n' | awk -F= -v k="$t" '$1==k{print $2}')
      [[ -n "$exp" ]] || die "EXPECTED_TOOL_REFS lacks $t"
      got=$(sudo -n node -e 'const fs=require("fs");const g=require("path");const base=process.argv[1];let out="";for(const py of fs.readdirSync(base+"/lib").filter(d=>d.startsWith("python3"))){const sp=base+"/lib/"+py+"/site-packages";for(const d of fs.readdirSync(sp).filter(d=>d.endsWith(".dist-info")&&d.toLowerCase().replace(/-/g,"_").startsWith(process.argv[2].toLowerCase().replace(/-/g,"_")))){try{const j=JSON.parse(fs.readFileSync(sp+"/"+d+"/direct_url.json"));out=(j.vcs_info||{}).commit_id||"";}catch(e){}}}console.log(out)' "/opt/newsflows/tools/uv/$t" "$t" 2>/dev/null || true)
      [[ -n "$got" ]] || die "installed tool $t: commit id not found under /opt/newsflows/tools/uv/$t"
      [[ "$got" == "$exp" ]] || die "installed tool $t is at $got, expected $exp"
      tline+="$t $got"$'\n'
    done
    printf '%s' "$tline" | emit tools.txt
  fi
  psql_ro -c "$CATALOG_SQL" | emit catalog-readback.tsv
  local cat_dids cfg_dids
  cat_dids=$(awk -F'|' 'NR>1 && $2!="" {print $2}' "$E/catalog-readback.tsv" | sort -u | tr '\n' ',' | sed 's/,$//')
  cfg_dids=$(echo "$RECOVER_DIDS" | tr ',' '\n' | sort -u | tr '\n' ',' | sed 's/,$//')
  [[ "$cat_dids" == "$cfg_dids" ]] || die "catalog publisher DIDs ($cat_dids) != configured RECOVER_DIDS ($cfg_dids)"
  local max_age; max_age=$(awk -F'|' 'NR>1 {if($3+0>m)m=$3+0} END{print m+0}' "$E/catalog-readback.tsv")
  (( HORIZON_DAYS >= max_age )) || die "HORIZON_DAYS ($HORIZON_DAYS) below catalog publisher_post_max_age_days ($max_age)"
  # participant-safety gate: every enabled feed of the pattern must SERVE on receipt_time (the columns this packet writes are
  # then read by nothing that orders/filters/scores posts) -- verified from the feedgen-owned catalog value, not from prose
  gate_catalog_clock "$E/catalog-readback.tsv"
  local rb_push="n/a" rb_timecol="n/a"
  if [[ "$SKIP_BSR_READBACK" != "1" ]]; then
    [[ -f "$READBACK_JSON" ]] || die "READBACK_JSON $READBACK_JSON missing — BSR effective-config readback is a prerequisite"
    local rb_stale rb_now; rb_stale=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const v=(j.artifact_metadata||{}).stale_at;const n=typeof v==="number"?v:(typeof v==="string"&&/^\d+$/.test(v)?Number(v):(typeof v==="string"?Math.floor(Date.parse(v)/1000):NaN));console.log(Number.isFinite(n)?n:"unparseable")' "$READBACK_JSON"); rb_now=$(date -u +%s)
    [[ "$rb_stale" =~ ^[0-9]+$ ]] || die "BSR effective-config readback stale_at is unparseable ($rb_stale)"
    (( rb_stale > rb_now )) || die "BSR effective-config readback is stale (stale_at=$rb_stale now=$rb_now); the 5-min producer must be running"
    rb_push=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const re=new RegExp("^"+process.argv[2]+"$");let m=0;for(const b of Object.values(j.bindings||{})){if((b.feed_ids||[]).some(f=>re.test(f)))m=Math.max(m,b.push_window_days||0)}console.log(m)' "$READBACK_JSON" "$RECOVER_RKEY_PATTERN")
    [[ "$rb_push" =~ ^[0-9]+$ ]] && (( rb_push > 0 )) || die "BSR readback did not yield a positive push window for RECOVER_RKEY_PATTERN (got '$rb_push'): bindings/feed_ids shape mismatch"
    (( HORIZON_DAYS >= rb_push )) || die "HORIZON_DAYS ($HORIZON_DAYS) below BSR push window ($rb_push)"
    # the ranker side of the same claim: BSR must rank the pattern's feeds on createdAt (the receipt/record clock), not on content time
    rb_timecol=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const re=new RegExp("^"+process.argv[2]+"$");const cols=new Set();for(const b of Object.values(j.bindings||{})){if((b.feed_ids||[]).some(f=>re.test(f)))cols.add(String(b.time_column||""))}console.log([...cols].sort().join(","))' "$READBACK_JSON" "$RECOVER_RKEY_PATTERN")
    [[ "$rb_timecol" == "createdAt" ]] || die "BSR effective-config time_column for '$RECOVER_RKEY_PATTERN' is '$rb_timecol' (must be exactly createdAt); a content-time ranking clock makes this packet a ranking change -- STOP"
    sudo -n sha256sum "$READBACK_JSON" | emit effective-config-readback.sha256
  fi
  local now_iso; now_iso=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
  [[ "$SINCE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "SINCE must be ISO-8601 with milliseconds and Z"
  [[ "$UNTIL" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "UNTIL must be ISO-8601 with milliseconds and Z"
  [[ "$SINCE" < "$UNTIL" ]] || die "SINCE must be before UNTIL (population must be closed by construction)"
  { [[ "$UNTIL" < "$now_iso" ]] || [[ "$UNTIL" == "$now_iso" ]]; } || die "UNTIL is in the future (now=$now_iso)"
  local horizon_boundary; horizon_boundary=$(date -u -d "$HORIZON_DAYS days ago" +%Y-%m-%dT%H:%M:%S.000Z)
  { [[ "$SINCE" < "$horizon_boundary" ]] || [[ "$SINCE" == "$horizon_boundary" ]]; } || die "SINCE=$SINCE is later than the rolling $HORIZON_DAYS-day boundary $horizon_boundary"
  echo "$SINCE" | emit since.txt
  echo "$UNTIL" | emit until.txt
  { echo "generated_at=$(ts)"; echo "tree=$TREE"; echo "source_sha=$sha"; echo "runner_script_sha256=$runner_sha"; echo "dsn_helper_sha256=$helper_sha"; echo "dist_backfill_sha256=$EXPECTED_DIST_SHA256"; echo "dist_content_time_sha256=$EXPECTED_CT_SHA256"; echo "image=$IMG"; echo "image_validator_sha256=$img_ct"; echo "feedgen_retention_enabled=$ret"; echo "runner=$RUNNER"; echo "packet_sha256=$PACKET_SHA"; echo "network=$NETWORK"; echo "env_file=$ENV_FILE"; echo "db_container=$DB_CONTAINER"; echo "mode=recover"; echo "recover_dids=$RECOVER_DIDS"; echo "recover_rkey_pattern=$RECOVER_RKEY_PATTERN"; echo "horizon_days=$HORIZON_DAYS"; echo "catalog_max_age=$max_age readback_push_window=$rb_push"; echo "api_base=$API_BASE"; echo "ceilings wal<=max($CEIL_WAL_FLOOR_BYTES, $CEIL_WAL_BASELINE_MULTIPLE x baseline_rate x (batch_elapsed+${PAUSE_SECONDS}s)) relation<=$CEIL_REL_BYTES dead<=2 x recovered rows"; echo "since=$SINCE"; echo "until=$UNTIL"; echo "prereg=$PREREG"; echo "expected_tool_refs=$EXPECTED_TOOL_REFS"; echo "catalog_publisher_time_clock=receipt_time (gated: every row)"; echo "bsr_readback_time_column=$rb_timecol"; echo "expected_runner_sha256=$EXPECTED_RUNNER_SHA256"; echo "expected_dsn_helper_sha256=$EXPECTED_DSN_HELPER_SHA256"; echo "skip_live_image_checks=$SKIP_LIVE_IMAGE_CHECKS"; echo "skip_bsr_readback=$SKIP_BSR_READBACK"; echo "secret_key_regex=$SECRET_KEY_REGEX"; echo "ceil_wal_floor_bytes=$CEIL_WAL_FLOOR_BYTES"; echo "ceil_wal_baseline_multiple=$CEIL_WAL_BASELINE_MULTIPLE"; echo "pause_seconds=$PAUSE_SECONDS"; echo "ceil_rel_bytes=$CEIL_REL_BYTES"; echo "expected_rkeys=$EXPECTED_RKEYS"; echo "image_validator_sha256_expected=$EXPECTED_IMAGE_CT_SHA256"; echo "permit_negative_probe=$PERMIT_NEGATIVE_PROBE"; echo "readback_json=$READBACK_JSON"; echo "psql_db=$PSQL_DB"; echo "psql_user=$PSQL_USER"; echo "feedgen_container=$FEEDGEN_CONTAINER"; echo "wal_rule_note=at CEIL_WAL_BASELINE_MULTIPLE=1.0 with the adaptive pause paid the WAL ceiling is implied (ceiling >= LSN advance + baseline x elapsed >= attributed); the effective per-batch bound is the PAID pause (wal_batches_unpaid=0) plus the per-batch relation-growth and dead-tuple ceilings; the ceiling can only fail when the 30-min deadline clips a pause or the multiple is lowered"; } | emit source-set.txt
  psql_ro -c "$SCOPE_SQL" | emit prestate-scope.tsv
  local lau wv wvu; lau=$(awk -F'|' '$1=="legacy_after_until"{print $2}' "$E/prestate-scope.tsv"); wv=$(awk -F'|' '$1=="wrong_version_in_rolling_horizon"{print $2}' "$E/prestate-scope.tsv"); wvu=$(awk -F'|' '$1=="unclassified_or_wrong_version_in_rolling_horizon_after_until"{print $2}' "$E/prestate-scope.tsv")
  [[ "$lau" == "0" ]] || die "legacy rows at or after UNTIL: $lau (the fixed window must cover the whole legacy population inside the horizon; widen UNTIL and re-approve)"
  [[ "$wv" == "0" && "$wvu" == "0" ]] || die "non-legacy rows of the actors inside the rolling $HORIZON_DAYS-day horizon that are not validator v2: $wv (before UNTIL) / $wvu (at/after UNTIL) -- D1 readiness precondition broken; STOP"
  psql_ro -c "$POP_SQL" | emit prestate-populations.tsv
  psql_copy "$(snapshot_sql_legacy)" | emit step1-be-prestate-rows.tsv
  # the plan is frozen here: exactly the prestate URIs; every later invocation (preview/apply/readback) passes this file so
  # the plan hash the checkpoint binds stays identical across resumes (rows already recovered come back as already_current)
  cut -f1 "$E/step1-be-prestate-rows.tsv" | emit step1-be-plan-uris.txt
  local rc; rc=$(run_tool step1-be-preflight-preview --mode recover --plan-from-db --plan-uris-file "$(plan_uris_path)" --no-insert --actors "$RECOVER_DIDS" --since "$SINCE" --until "$UNTIL" --api-base "$API_BASE" --packet-sha256 "$PACKET_SHA" --json)
  [[ "$rc" == "0" ]] || die "preflight dry-run exit=$rc (see step1-be-preflight-preview.err)"
  gate_recover_preview "$E/step1-be-preflight-preview.json"
  take_control pg-control-1.txt
  pgstat_read | emit pg-prestate.txt
  log "preflight complete: be prestate rows=$(wc -l <"$E/step1-be-prestate-rows.tsv")"
}
cmd_control() { assert_tree; assert_bound_env; local n; n=$(( $(ls -1 "$E"/pg-control-*.txt 2>/dev/null | wc -l) + 1 )); take_control "pg-control-$n.txt"; }

cmd_preview() {
  assert_tree
  assert_bound_env
  local rc; rc=$(run_tool step1-be-preview --mode recover --plan-from-db --plan-uris-file "$(plan_uris_path)" --no-insert --actors "$RECOVER_DIDS" --since "$SINCE" --until "$UNTIL" --api-base "$API_BASE" --packet-sha256 "$PACKET_SHA" --json)
  [[ "$rc" == "0" ]] || die "preview exit=$rc (see step1-be-preview.err)"
  gate_recover_preview "$E/step1-be-preview.json"
  log "preview be ok"
}

cmd_apply() {
  local label=$1 maxb=${2:-}
  assert_tree
  local permit; permit=$(sed -n 's/^permit_negative_probe=//p' "$E/source-set.txt" 2>/dev/null | head -1)
  if [[ "$label" =~ ^neg ]]; then [[ "$permit" == "1" ]] || die "label '$label' is the negative probe, which preflight did not permit (permit_negative_probe=$permit) -- refused on the production path"; export ALLOW_CEILING_OVERRIDE=1; else export ALLOW_CEILING_OVERRIDE=0; fi
  assert_bound_env
  # breach cap (contract: one re-measure, a third repair attempt is prohibited): count prior BREACH receipts (neg receipts count too unless the probe is permitted)
  local nb_breach; if [[ "$permit" == "1" ]]; then nb_breach=$( { grep -l '^verdict=BREACH$' "$E"/ceiling-be-*.txt 2>/dev/null || true; } | { grep -v -- '-neg' || true; } | wc -l | tr -d ' '); else nb_breach=$( { grep -l '^verdict=BREACH$' "$E"/ceiling-be-*.txt 2>/dev/null || true; } | wc -l | tr -d ' '); fi
  if (( nb_breach >= 2 )); then die "two consecutive ceiling breaches already recorded in $E -- STOP FOR GOOD + escalation (a third repair attempt is prohibited)"; fi
  if (( nb_breach == 1 )); then [[ "$label" =~ r$ && "$maxb" == "1" ]] || die "after a BREACH the only permitted invocation is the single re-measure 'apply <label>r 1'"; fi
  # BEFORE THE MUTATION: re-read the participant-safety facts and the running image; any change since preflight is a STOP
  psql_ro -c "$CATALOG_SQL" | emit "catalog-readback-$label.tsv"; gate_catalog_clock "$E/catalog-readback-$label.tsv"
  if [[ "$SKIP_LIVE_IMAGE_CHECKS" != "1" ]]; then local run_img; run_img=$("${DOCKER[@]}" inspect "$FEEDGEN_CONTAINER" --format '{{.Image}}'); grep -q "^pinned_image_id=$run_img$" "$E/feedgen-prestate.txt" || die "running feedgen image $run_img != preflight-recorded image"; fi
  if [[ "$SKIP_BSR_READBACK" != "1" ]]; then local rbs rbn; rbs=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const v=(j.artifact_metadata||{}).stale_at;const n=typeof v==="number"?v:(typeof v==="string"&&/^\d+$/.test(v)?Number(v):(typeof v==="string"?Math.floor(Date.parse(v)/1000):NaN));console.log(Number.isFinite(n)?n:"NaN")' "$READBACK_JSON"); rbn=$(date -u +%s); [[ "$rbs" =~ ^[0-9]+$ ]] && (( rbs > rbn )) || die "BSR effective-config readback is stale at apply time (stale_at=$rbs now=$rbn)"; sudo -n sha256sum "$READBACK_JSON" | emit "effective-config-readback-$label.sha256"; local tc; tc=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const re=new RegExp("^"+process.argv[2]+"$");const cols=new Set();for(const b of Object.values(j.bindings||{})){if((b.feed_ids||[]).some(f=>re.test(f)))cols.add(String(b.time_column||""))}console.log([...cols].sort().join(","))' "$READBACK_JSON" "$RECOVER_RKEY_PATTERN"); [[ "$tc" == "createdAt" ]] || die "BSR time_column for the pattern is '$tc' at apply time (must be createdAt)"; fi
  local ckpt="/evidence/step1-be-checkpoint.json"; [[ "$RUNNER" == "host" ]] && ckpt="$E/step1-be-checkpoint.json"
  pgstat_read | emit "pg-be-$label-before.txt"
  # ADAPTIVE PAUSE (D4-b): identical rule to the revalidate packet -- the tool pauses after every batch for
  # max(1 s, batch LSN advance / baseline write rate); the baseline is the most recent control read.
  local wps0 rps0 dps0; read -r wps0 rps0 dps0 <<<"$(control_rates)"
  (( wps0 >= 1 )) || die "apply be/$label: control baseline write rate is $wps0 B/s (stalled or missing control read) -- run '$0 control' first"
  local rc; rc=$(run_tool "step1-be-apply-$label" --mode recover --plan-from-db --plan-uris-file "$(plan_uris_path)" --no-insert --apply --actors "$RECOVER_DIDS" --since "$SINCE" --until "$UNTIL" --api-base "$API_BASE" --checkpoint-file "$ckpt" --packet-sha256 "$PACKET_SHA" ${maxb:+--max-batches "$maxb"} --pause-baseline-bytes-per-s "$wps0" --json)
  pgstat_read | emit "pg-be-$label-after.txt"
  [[ -f "$E/step1-be-checkpoint.json" ]] && sudo -n chown root:newsflows "$E/step1-be-checkpoint.json" 2>/dev/null && sudo -n chmod 0640 "$E/step1-be-checkpoint.json" 2>/dev/null || true
  local f="$E/step1-be-apply-$label.json"
  [[ "$rc" == "0" || "$rc" == "3" ]] || die "apply be/$label exit=$rc (see .err); STOP + escalation"
  [[ "$(jsonq "$f" operation)" == "publisher-post-recover" && "$(jsonq "$f" mode)" == "apply" ]] || die "apply be/$label did not run the recover apply path"
  [[ "$(jsonq "$f" plan_source)" == "plan-uris-file+getPosts" && "$(jsonq "$f" plan_uris_not_in_db)" == "0" ]] || die "apply be/$label: plan_source=$(jsonq "$f" plan_source) plan_uris_not_in_db=$(jsonq "$f" plan_uris_not_in_db) (must be the frozen plan, all URIs present)"
  [[ "$(jsonq "$f" unretrievable)" == "$(prereg_value unretrievable)" && "$(jsonq "$f" preview)" == "" ]] || die "apply be/$label: unretrievable=$(jsonq "$f" unretrievable) != pre-registered $(prereg_value unretrievable)"
  local nb; nb=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));console.log((j.recovery&&j.recovery.batches||[]).length)' "$f")
  local b a; b=$(cat "$E/pg-be-$label-before.txt"); a=$(cat "$E/pg-be-$label-after.txt")
  local dw dr dd dur; dw=$(( $(echo "$a"|cut -d'|' -f1) - $(echo "$b"|cut -d'|' -f1) )); dr=$(( $(echo "$a"|cut -d'|' -f2) - $(echo "$b"|cut -d'|' -f2) )); dd=$(( $(echo "$a"|cut -d'|' -f3) - $(echo "$b"|cut -d'|' -f3) )); dur=$(( $(echo "$a"|cut -d'|' -f5) - $(echo "$b"|cut -d'|' -f5) )); (( dur < 1 )) && dur=1
  read -r wps rps dps <<<"$(control_rates)"
  local verdict="ok"
  if (( nb > 0 )); then
    local pw=$(( (dw - wps*dur) / nb )) pr=$(( (dr - rps*dur) / nb )) pd=$(( (dd - dps*dur) / nb ))
    # PER-BATCH enforcement, identical rule/format to the revalidate packet v10: each batch's attributed WAL (its own
    # LSN advance minus baseline x its own elapsed) must be <= max(floor, multiple x baseline x (its own elapsed + paid
    # pause)); the verdict is the conjunction over batches. j.recovery mirrors j.revalidation field-for-field.
    local perbatch; perbatch=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));const b=(j.recovery&&j.recovery.batches)||[];const [r,m,p,fl,rr,relCap]=process.argv.slice(2).map(Number);let worst=0,worstRatio=0,fail=0,unpaid=0,relFail=0,relWorst=0,lines=[];for(const x of b){if(typeof x.wal_bytes!=="number"){lines.push(`batch=${x.batch} wal_bytes=missing`);fail++;continue;}const ms=x.elapsed_ms||0;const paidMs=(typeof x.pause_ms==="number")?x.pause_ms:Math.round(p*1000);const owedMs=(x.candidates===0)?0:Math.max(Math.round(p*1000),Math.ceil(x.wal_bytes*1000/r));const paid=paidMs>=owedMs;if(!paid)unpaid++;const adj=Math.max(0,x.wal_bytes-Math.round(r*ms/1000));const ceil=Math.max(fl,Math.round(m*r*(ms/1000+paidMs/1000)));const okWal=adj<=ceil&&paid;if(!okWal)fail++;const ratio=ceil?adj/ceil:0;if(ratio>worstRatio){worstRatio=ratio;worst=adj;}const relDelta=(typeof x.relation_bytes_after==="number"&&typeof x.relation_bytes_before==="number")?(x.relation_bytes_after-x.relation_bytes_before):NaN;const relAdj=Number.isFinite(relDelta)?Math.max(0,relDelta-Math.round(rr*ms/1000)):NaN;const okRel=Number.isFinite(relAdj)&&relAdj<=relCap;if(!okRel)relFail++;if(Number.isFinite(relAdj)&&relAdj>relWorst)relWorst=relAdj;lines.push(`batch=${x.batch} elapsed_ms=${ms} lsn_advance=${x.wal_bytes} attributed=${adj} pause_owed_ms=${owedMs} pause_paid_ms=${paidMs} ceiling=${ceil} ratio=${ratio.toFixed(2)} ${paid?"paid":"UNPAID"} ${adj<=ceil?"ok":"BREACH"} relation_growth=${Number.isFinite(relAdj)?relAdj:"missing"} rel_cap=${relCap} ${okRel?"rel_ok":"REL_BREACH"}`);}console.log(JSON.stringify({fail,unpaid,relFail,relWorst,worst,worstRatio:worstRatio.toFixed(2),lines}))' "$f" "$wps0" "$CEIL_WAL_BASELINE_MULTIPLE" "$PAUSE_SECONDS" "$CEIL_WAL_FLOOR_BYTES" "$rps0" "$CEIL_REL_BYTES")
    local wal_fail wal_worst wal_ratio wal_lines wal_unpaid
    wal_fail=$(node -e 'console.log(JSON.parse(process.argv[1]).fail)' "$perbatch"); wal_unpaid=$(node -e 'console.log(JSON.parse(process.argv[1]).unpaid)' "$perbatch"); wal_worst=$(node -e 'console.log(JSON.parse(process.argv[1]).worst)' "$perbatch"); wal_ratio=$(node -e 'console.log(JSON.parse(process.argv[1]).worstRatio)' "$perbatch"); wal_lines=$(node -e 'console.log(JSON.parse(process.argv[1]).lines.join("\n"))' "$perbatch")
    local recovered_n; recovered_n=$(jsonq "$f" recovery.recovered)
    local dead_ceiling=$(( 2 * recovered_n / nb )); (( recovered_n > 0 )) || pd=0   # nothing written -> no dead tuples of ours to bound (already_current-only resume)
    local rel_fail rel_worst; rel_fail=$(node -e 'console.log(JSON.parse(process.argv[1]).relFail)' "$perbatch"); rel_worst=$(node -e 'console.log(JSON.parse(process.argv[1]).relWorst)' "$perbatch")
    # verdict = conjunction over batches for WAL (paid + ceiling) AND relation growth (per batch, baseline-adjusted); dead tuples
    # are only available as an invocation delta (pg_stat) and stay per-invocation-mean against 2 x recovered / batches
    (( wal_fail == 0 && rel_fail == 0 && pd <= dead_ceiling )) || verdict="BREACH"
    { echo "batches=$nb exit=$rc recovered=$(jsonq "$f" recovery.recovered) complete=$(jsonq "$f" recovery.complete) invocation_seconds=$dur"; echo "delta_wal_bytes=$dw delta_relation_bytes=$dr delta_dead_tuples=$dd"; echo "control_file=$(basename "$(latest_control)") control_rate_per_s wal=$wps relation=$rps dead=$dps (clamped at 0)"; echo "per_batch_relation_minus_control=$pr per_batch_dead_minus_control=$pd (invocation deltas minus baseline x duration, / batches)"; echo "wal_rule=per batch: attributed (LSN advance - baseline x elapsed) <= max(floor $CEIL_WAL_FLOOR_BYTES, $CEIL_WAL_BASELINE_MULTIPLE x baseline $wps0 B/s x (elapsed + paid pause)); adaptive pause owed = max(${PAUSE_SECONDS}s, LSN advance / baseline $wps0 B/s) and must be paid (tool --pause-baseline-bytes-per-s $wps0); cluster-wide LSN, concurrent writers included then subtracted"; echo "$wal_lines"; echo "wal_batches_failing=$wal_fail wal_batches_unpaid=$wal_unpaid wal_worst_attributed=$wal_worst wal_worst_ratio=$wal_ratio relation_batches_failing=$rel_fail relation_worst_growth=$rel_worst"; echo "wal_rule_note=at multiple $CEIL_WAL_BASELINE_MULTIPLE with the pause paid the WAL ceiling is implied by construction (ceiling >= LSN advance + baseline x elapsed >= attributed): the effective bound is the paid pause; it can only fail if the deadline clips a pause or the multiple is lowered"; echo "ceilings relation<=$CEIL_REL_BYTES dead<=$dead_ceiling (=2 x recovered rows per batch)"; echo "ceiling_override=${ALLOW_CEILING_OVERRIDE:-0} (1 only for the deliberate negative probe, label ^neg)"; echo "verdict=$verdict"; } | emit "ceiling-be-$label.txt"
  else
    echo "batches=0 exit=$rc verdict=no_batches" | emit "ceiling-be-$label.txt"
  fi
  [[ "$verdict" == "ok" ]] || die "ceiling breach on be/$label (see ceiling-be-$label.txt): run '$0 control', then re-measure once with '$0 apply ${label}r 1'; a second breach = stop for good + escalation"
  log "apply be/$label exit=$rc batches=$nb verdict=$verdict"
}

cmd_readback() {  # re-runnable: every attempt gets its own suffix
  assert_tree
  assert_bound_env
  local a=1; while [[ -e "$E/step1-be-preview-after-attempt-$a.json" || -e "$E/step1-be-preview-after-attempt-$a.err" ]]; do a=$(( a + 1 )); done; local sfx="attempt-$a"
  local rc; rc=$(run_tool "step1-be-preview-after-$sfx" --mode recover --plan-from-db --plan-uris-file "$(plan_uris_path)" --no-insert --actors "$RECOVER_DIDS" --since "$SINCE" --until "$UNTIL" --api-base "$API_BASE" --packet-sha256 "$PACKET_SHA" --json)
  local f="$E/step1-be-preview-after-$sfx.json"
  [[ "$rc" == "0" ]] || die "post-apply preview exit=$rc"
  [[ "$(jsonq "$f" operation)" == "publisher-post-recover" && "$(jsonq "$f" mode)" == "dry-run" ]] || die "post-apply preview did not run the recover dry-run path"
  local exp_unret; exp_unret=$(prereg_value unretrievable)
  [[ "$(jsonq "$f" db_legacy_in_window)" == "$exp_unret" ]] || die "readback: db_legacy_in_window=$(jsonq "$f" db_legacy_in_window), expected == pre-registered unretrievable ($exp_unret)"
  [[ "$(jsonq "$f" preview.would_recover)" == "0" ]] || die "readback: preview.would_recover=$(jsonq "$f" preview.would_recover), expected 0"
  snapshot_by_uris "$E/step1-be-prestate-rows.tsv" | emit "step1-be-poststate-rows-$sfx.tsv"
  local exp_valid exp_invalid; exp_valid=$(prereg_value recover_source_valid); exp_invalid=$(prereg_value recover_source_invalid)
  node -e '
const fs=require("fs");
const isNull=(v)=>v===undefined||v==="\\N"||v==="";
const [pre,post]=[process.argv[1],process.argv[2]].map(p=>fs.readFileSync(p,"utf8").split("\n").filter(Boolean).map(l=>l.split("\t")));
const [expValid,expInvalid,expUnret]=process.argv.slice(3).map(Number);
const m=new Map(post.map(r=>[r[0],r]));
let missing=0,cidChanged=0,indexedChanged=0,createdChanged=0,recoveredValid=0,recoveredInvalid=0,stillLegacy=0,badRecovered=0;
for(const r of pre){
  const q=m.get(r[0]); if(!q){missing++;continue;}
  if(q[4]!==r[4])cidChanged++; if(q[2]!==r[2])indexedChanged++; if(q[3]!==r[3])createdChanged++;
  const rawNonEmpty=!isNull(q[5]); const isV2=q[9]==="newsflows-content-time/v2";
  if(rawNonEmpty && isV2 && (q[7]==="source_valid"||q[7]==="source_invalid")){ if(q[7]==="source_valid")recoveredValid++; else recoveredInvalid++; }
  else if(!rawNonEmpty && isNull(q[9]) && (isNull(q[7])||q[7]==="legacy_unknown")) stillLegacy++;
  else badRecovered++;
}
console.log(`prestate_rows=${pre.length} poststate_rows=${post.length} missing=${missing} cid_changed=${cidChanged} indexedAt_changed=${indexedChanged} createdAt_changed=${createdChanged} recovered_valid=${recoveredValid} recovered_invalid=${recoveredInvalid} still_legacy=${stillLegacy} bad_recovered=${badRecovered} expected_valid=${expValid} expected_invalid=${expInvalid} expected_unretrievable=${expUnret}`);
const ok=missing===0 && cidChanged===0 && indexedChanged===0 && createdChanged===0 && badRecovered===0 && recoveredValid===expValid && recoveredInvalid===expInvalid && stillLegacy===expUnret;
process.exit(ok?0:2)' "$E/step1-be-prestate-rows.tsv" "$E/step1-be-poststate-rows-$sfx.tsv" "$exp_valid" "$exp_invalid" "$exp_unret" | emit "step1-be-diff-$sfx.txt" || die "readback: diff failed (see step1-be-diff-$sfx.txt)"
  local sum_recovered exp_recover; sum_recovered=$(for jf in "$E"/step1-be-apply-*.json; do jsonq "$jf" recovery.recovered; done | awk '{s+=$1} END{print s+0}'); exp_recover=$(prereg_value would_recover)
  [[ "$sum_recovered" == "$exp_recover" ]] || die "readback: sum(recovery.recovered)=$sum_recovered, expected would_recover=$exp_recover"
  psql_ro -c "$SCOPE_SQL" | emit "poststate-scope-$sfx.tsv"
  pgstat_read | emit "pg-poststate-$sfx.txt"
  local pre_out post_out pre_all post_all
  pre_out=$(awk -F'|' '$1=="legacy_outside_window"{print $2}' "$E/prestate-scope.tsv"); post_out=$(awk -F'|' '$1=="legacy_outside_window"{print $2}' "$E/poststate-scope-$sfx.tsv")
  pre_all=$(awk -F'|' '$1=="legacy_all_time"{print $2}' "$E/prestate-scope.tsv"); post_all=$(awk -F'|' '$1=="legacy_all_time"{print $2}' "$E/poststate-scope-$sfx.tsv")
  [[ "$pre_out" == "$post_out" ]] || die "readback: legacy_outside_window changed ($pre_out -> $post_out)"
  local post_lau post_wvu; post_lau=$(awk -F'|' '$1=="legacy_after_until"{print $2}' "$E/poststate-scope-$sfx.tsv"); post_wvu=$(awk -F'|' '$1=="unclassified_or_wrong_version_in_rolling_horizon_after_until"{print $2}' "$E/poststate-scope-$sfx.tsv")
  [[ "$post_lau" == "0" && "$post_wvu" == "0" ]] || die "readback: rows at/after UNTIL that are legacy ($post_lau) or not v2 ($post_wvu) -- the horizon is not 100% v2-classified"
  [[ $(( pre_all - post_all )) == "$exp_recover" ]] || die "readback: legacy_all_time moved by $(( pre_all - post_all )), expected $exp_recover"
  { echo "readback=$sfx"; cat "$E/step1-be-diff-$sfx.txt"; echo "sum_recovered=$sum_recovered would_recover=$exp_recover"; echo "legacy_outside_window before=$pre_out after=$post_out"; echo "legacy_all_time before=$pre_all after=$post_all"; } | emit "readback-$sfx.txt"
  log "readback ok ($sfx)"
}

cmd_restore() {  # bounded CAS restore from the prestate snapshot; keyset batches of 500; resumable; batch index derived from the cursor
  assert_tree
  assert_bound_env
  local rs; rs=$(date -u +%H%M%S); psql_ro -c "$CATALOG_SQL" | emit "catalog-readback-restore-$rs.tsv"; gate_catalog_clock "$E/catalog-readback-restore-$rs.tsv"
  assert_tree
  local pre="$E/step1-be-prestate-rows.tsv" cur="$E/restore-be-cursor.txt" total batch=0
  [[ -f "$pre" ]] || die "no prestate snapshot for be"
  total=$(wc -l <"$pre"); log "restore be: $total prestate rows"
  local start=1; [[ -f "$cur" ]] && start=$(cat "$cur")
  local ra=1; while [[ -e "$E/restore-be-result-attempt-$ra.txt" || -e "$E/restore-be-after-rows-attempt-$ra.tsv" ]]; do ra=$(( ra + 1 )); done
  while (( start <= total )); do
    local end=$(( start + 499 )); (( end > total )) && end=$total; batch=$(( (start - 1) / 500 + 1 ))
    local rtmp; rtmp=$(mktemp)
    local attempt=1 rname; while [[ -e "$E/restore-be-rows-$start-$end-attempt-$attempt.txt" ]]; do attempt=$(( attempt + 1 )); done; rname="restore-be-rows-$start-$end-attempt-$attempt.txt"
    (( attempt <= 2 )) || die "restore be rows $start-$end: a third attempt is prohibited (contract: a failed restore adapter is a P1 incident) -- STOP + escalation"
    { echo "BEGIN; SET lock_timeout='5s'; SET statement_timeout='30s';"; echo "CREATE TEMP TABLE prestate(uri text, author text, indexed_at text, created_at text, cid text, raw_hex text, content_time_utc text, status text, reason text, version text);"; echo "COPY prestate FROM STDIN WITH (FORMAT text);"; sed -n "${start},${end}p" "$pre"; echo '\.'; cat <<'SQL'
UPDATE public.post AS t SET created_at_source_raw=decode(s.raw_hex,'hex'), content_time_utc=s.content_time_utc, content_time_status=s.status, content_time_clamp_reason=s.reason, content_time_validator_version=s.version
FROM prestate s WHERE t.uri=s.uri AND t.content_time_validator_version='newsflows-content-time/v2' AND t.created_at_source_raw IS NOT NULL;
SELECT 'restored_in_batch', count(*) FROM post p JOIN prestate s ON s.uri=p.uri WHERE p.created_at_source_raw IS NOT DISTINCT FROM decode(s.raw_hex,'hex') AND p.content_time_utc IS NOT DISTINCT FROM s.content_time_utc AND p.content_time_status IS NOT DISTINCT FROM s.status AND p.content_time_clamp_reason IS NOT DISTINCT FROM s.reason AND p.content_time_validator_version IS NOT DISTINCT FROM s.version;
COMMIT;
SQL
    } | "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -A -F '|' -v ON_ERROR_STOP=1 >"$rtmp" 2>&1 || { emit "$rname" <"$rtmp"; die "restore be batch $batch (rows $start-$end) failed (see $rname); re-run 'restore' to resume from the cursor (next attempt gets its own receipt)"; }
    local restored expected=$(( end - start + 1 )); restored=$(awk -F'|' '$1=="restored_in_batch"{print $2}' "$rtmp")
    emit "$rname" <"$rtmp"; rm -f "$rtmp"
    [[ "$restored" == "$expected" ]] || die "restore be batch $batch restored $restored of $expected rows (CAS mismatch); stop + escalation"
    echo $(( end + 1 )) | sudo -n tee "$cur" >/dev/null
    sleep 1; start=$(( end + 1 ))
  done
  snapshot_by_uris "$pre" | emit "restore-be-after-rows-attempt-$ra.tsv"
  cmp -s "$pre" "$E/restore-be-after-rows-attempt-$ra.tsv" && echo "restore_be=identical_to_prestate rows=$total batches_this_run=$batch attempt=$ra" | emit "restore-be-result-attempt-$ra.txt" || { diff "$pre" "$E/restore-be-after-rows-attempt-$ra.tsv" | head -20 | emit "restore-be-result-attempt-$ra.txt"; die "restore be: after-restore snapshot differs from prestate"; }
  log "restore be complete: identical to prestate"
}

cmd_secret_scan() {
  assert_tree
  assert_bound_env
  # Only values of SECRET-bearing keys are hunted (fixed-string, len>=8); configuration values the runner records itself
  # (hosts, DB names, DIDs) are not secrets and would collide with source-set.txt / snapshots. The scan fails closed if no
  # secret-key value could be extracted. Each grep is failure-tolerant so the loop can never abort early.
  local vals hits=0 out="" n keys
  keys=$(sudo -n awk -F= -v re="$SECRET_KEY_REGEX" '/^[A-Za-z_][A-Za-z0-9_]*=/{k=$1; if(k ~ re){v=substr($0,index($0,"=")+1); gsub(/^["'"'"']|["'"'"']$/,"",v); if(length(v)>=8)print k}}' "$ENV_FILE" | tr '\n' ' ')
  vals=$(sudo -n awk -F= -v re="$SECRET_KEY_REGEX" '/^[A-Za-z_][A-Za-z0-9_]*=/{k=$1; if(k ~ re){v=substr($0,index($0,"=")+1); gsub(/^["'"'"']|["'"'"']$/,"",v); if(length(v)>=8)print v}}' "$ENV_FILE")
  n=$(printf '%s\n' "$vals" | grep -c . || true)
  (( n > 0 )) || die "secret scan: no secret-key values (keys matching $SECRET_KEY_REGEX, len>=8) could be extracted from $ENV_FILE — cannot certify raw-free"
  echo "$keys" | grep -q -E "PASSWORD" || die "secret scan: no PASSWORD-bearing key found in $ENV_FILE (keys: $keys)"
  local v f
  while IFS= read -r v; do
    [[ -n "$v" ]] || continue
    f=$(sudo -n grep -rlF -- "$v" "$E" 2>/dev/null || true); [[ -n "$f" ]] && out+="$f"$'\n'
  done <<<"$vals"
  f=$(sudo -n grep -rlE 'postgresql://[^ ]+:[^ ]+@|FEEDGEN_DB_PASSWORD=|app_password' "$E" 2>/dev/null | grep -v '/secret-scan.txt$' || true); [[ -n "$f" ]] && out+="$f"$'\n'
  out=$(printf '%s' "$out" | sort -u | grep . || true)
  if [[ -n "$out" ]]; then hits=$(echo "$out" | wc -l); fi
  { echo "generated_at=$(ts)"; echo "secret_keys_scanned=$keys"; echo "secret_values_scanned=$n"; echo "hits=$hits"; if [[ -n "$out" ]]; then echo "$out"; fi; } | emit secret-scan.txt
  (( hits == 0 )) || die "secret scan found $hits file(s) containing a secret value or DSN marker (names in secret-scan.txt); do NOT append the ledger row; incident: quarantine \$E (checkpoint files are written by the tool)"
  log "secret scan clean ($n secret values from keys: $keys)"
}

cmd_finalize() {
  assert_tree
  assert_bound_env
  local wstart=${1:-unset} wend=${2:-unset}
  { [[ -f "$E/secret-scan.txt" ]] && grep -q '^hits=0$' "$E/secret-scan.txt"; } || die "finalize requires a clean secret-scan first"
  local rtmp; rtmp=$(mktemp)
  { echo "status=complete"; echo "generated_at=$(ts)"; cat "$E/source-set.txt";
    echo "--- prestate-scope.tsv"; cat "$E/prestate-scope.tsv" 2>/dev/null || true
    echo "--- prestate-populations.tsv"; cat "$E/prestate-populations.tsv" 2>/dev/null || true
    for f in "$E"/ceiling-be-*.txt; do if [[ -f "$f" ]]; then echo "--- $(basename "$f")"; cat "$f"; fi; done
    echo "--- readback"; cat "$E"/readback-*.txt 2>/dev/null || true
    echo "--- secret-scan"; cat "$E/secret-scan.txt" 2>/dev/null || true
    echo "window_start_utc=$wstart"; echo "window_end_utc=$wend"
  } >"$rtmp"
  emit RESULT.txt <"$rtmp"; rm -f "$rtmp"
  ( cd "$E" && sudo -n sh -c 'find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS && chown root:newsflows SHA256SUMS && chmod 640 SHA256SUMS' )
  log "finalized: $E"
}

case "$SUBCMD" in
  prereg) cmd_prereg;;
  preflight) cmd_preflight;;
  control) cmd_control;;
  preview) cmd_preview;;
  apply) cmd_apply "${2:?label}" "${3:-}";;
  readback) cmd_readback;;
  restore) cmd_restore;;
  secret-scan) cmd_secret_scan;;
  finalize) cmd_finalize "${2:-}" "${3:-}";;
  *) echo "usage: $0 prereg|preflight|control|preview|apply <label> [max_batches]|readback|restore|secret-scan|finalize <start> <end>" >&2; exit 2;;
esac
