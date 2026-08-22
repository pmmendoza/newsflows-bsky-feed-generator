#!/usr/bin/env bash
# Operator runner for the content-time v1->v2 re-validation backfill packet
# and the FT-FU-1 semantic v2<->v3 post migration plus engagement projection.
# (dev/feedgen/2026-08-17_content_time_revalidation_backfill_packet.md in the
# BSKY root repo). Every production step is a subcommand here so nothing is
# hand-composed on the console. Raw-free by construction: the DSN is composed
# inside the container by scripts/compose_feedgen_dsn.js from the secrets
# env-file (URL-encoded) and is never printed; tool stdout and stderr are
# captured to SEPARATE files; `secret-scan` (every env-file value, fixed-string)
# must pass before `finalize`.
#
# Subcommands (each writes only into $E; every output is root:newsflows 0640;
# nothing is ever overwritten):
#   preflight            hard gates: tree HEAD/dirty, dist hashes, live-image validator hash,
#                        retention disabled, catalog DIDs == configured DIDs, horizons >= catalog/push
#                        windows; records source-set, filtered container readback, tool refs,
#                        population + transition prediction (gated against PREREG_* cells),
#                        bounded population previews (main/be), NULL-raw gate, pg_stat prestate,
#                        control read #1
#   control              an additional 60 s idle control read (pg-control-<n>.txt); the ceiling
#                        computation always uses the most recent one
#   preview  <group>     dry-run through the runner; per-outcome gate against the prediction
#                        (exact <50 rows, +/-2% above), unknown outcome keys stop, scanned ==
#                        sum(counts) == prestate rows
#   apply    <group> <label> [max_batches]
#                        re-asserts hashes; --apply --packet-sha256 --checkpoint-file; pg_stat
#                        before/after; per-batch ceilings from the tool's in-transaction WAL minus
#                        a duration-matched control rate; relation/dead deltas minus duration-
#                        matched control; distinct receipts per label
#   readback             previews empty; poststate populations + per-URI snapshots + diff; pg_stat
#   restore  <group>     bounded (500/keyset, lock 5 s, statement 30 s, 1 s pause, resumable by
#                        cursor; batch numbers derived from the cursor), CAS restore from the
#                        prestate snapshot; per-batch count check; byte-for-byte verification
#   secret-scan          every env-file value (len>=8) as fixed strings + DSN markers over $E
#                        (incl. checkpoints); the scan FAILS if no values could be extracted
#   finalize <start> <end>
#                        SHA256SUMS over every file (recursive), RESULT.txt last
#   migrate-prereg | migrate-prepare | migrate-preflight | migrate-freeze
#   migrate-normalize-overlap <label> [max_batches]  explicit continuation-only
#   migrate-native-tail-recovery-bind  import a prior failed activation floor as
#                        typed, hash-bound provenance into fresh evidence; regenerate
#                        the bounded post-tail plan under the restored v2 catalog
#   migrate-preview | migrate-apply <label>
#   migrate-readback | migrate-rollback <dry-run|apply> | migrate-secret-scan
#   migrate-finalize <start> <end>
#                        FT-FU-1 semantic post migration. Engagement history is
#                        projected at export time and is never rewritten.
#
# Required env (the runner refuses to start without them):
#   E TREE EXPECTED_SHA EXPECTED_DIST_SHA256 EXPECTED_CT_SHA256 EXPECTED_IMAGE_CT_SHA256 PACKET_SHA
#   SINCE_MAIN SINCE_BE (ABSOLUTE receipt-time lower bounds, ISO-8601 Z, bound in the ledger approval; the candidate set
#     {v1 rows with raw, indexedAt >= SINCE} is then stable because no v1 producer exists any more)
#   PREREG_MAIN PREREG_BE (pre-registered cells "v1_valid_to_v2_valid=N,v1_invalid_to_v2_valid=N,v1_to_v2_invalid=N",
#     computed by `prereg` at the same SINCE bounds; gated EXACTLY by preflight/preview/readback)
#   FT-FU-1 coordinated migration binds PREREG_POST/ENGAGEMENT/IR itself from
#     the stable post-switch snapshots; only legacy standalone migrate-preflight/preview accept caller values.
#   EXPECTED_TOOL_REFS ("bsky-ops=<sha>,blueskyranker=<sha>,newsflows-bskyhealth=<sha>": installed operator tools must match)
# `prereg` (read-only) prints the SINCE bounds for the current horizons plus the cells, for the ledger approval entry.
# Optional env (production defaults): IMG NETWORK ENV_FILE DB_CONTAINER PSQL_DB PSQL_USER
#   MAIN_DIDS BE_DID HORIZON_MAIN_DAYS HORIZON_BE_DAYS DOCKER FEEDGEN_CONTAINER RUNNER
#   (container|host; host = rehearsal with HOST_DSN) CEIL_WAL_BASELINE_MULTIPLE CEIL_WAL_FLOOR_BYTES CEIL_REL_BYTES
#   RANKED_RKEYS MAIN_RKEY_PATTERN BE_RKEY_PATTERN READBACK_JSON (BSR effective-config readback)
#   (READBACK_JSON is required to exist and be fresh: stale_at > now)
#   ALLOW_FTFU1_OVERLAP_NORMALIZATION=1 explicitly authorizes continuation-only
#   v3 future_skew_clamped -> v2 normalization while the catalog remains v2.
#   SECRET_KEY_REGEX (default: keys matching PASSWORD|SECRET|TOKEN|KEY|PASS) selects which env-file values the secret scan hunts for
#   PREREG_TOLERANCE_PCT (default 5) SKIP_LIVE_IMAGE_CHECKS=1 (rehearsal only) RESTORE_STOP_AFTER_BATCH (rehearsal only)
set -euo pipefail

: "${E:?E (evidence root) is required}"
COMMAND=${1:-}
MIGRATION_SEAL=0; [[ "$COMMAND" == migrate-secret-scan || "$COMMAND" == migrate-finalize ]] && MIGRATION_SEAL=1
if (( ! MIGRATION_SEAL )); then
  : "${TREE:?TREE (built feedgen tree) is required}"
  : "${EXPECTED_SHA:?EXPECTED_SHA (full source SHA) is required}"
  : "${EXPECTED_DIST_SHA256:?EXPECTED_DIST_SHA256 is required}"
  : "${EXPECTED_CT_SHA256:?EXPECTED_CT_SHA256 is required}"
  : "${EXPECTED_IMAGE_CT_SHA256:?EXPECTED_IMAGE_CT_SHA256 (validator module hash inside the live image) is required}"
  : "${PACKET_SHA:?PACKET_SHA (approved packet SHA-256) is required}"
  : "${EXPECTED_TOOL_REFS:?EXPECTED_TOOL_REFS (bsky-ops=<sha>,blueskyranker=<sha>,newsflows-bskyhealth=<sha>) is required}"
  [[ "$PACKET_SHA" =~ ^[0-9a-f]{64}$ ]] || { echo "PACKET_SHA must be 64 lowercase hex" >&2; exit 2; }
fi
if [[ "$COMMAND" == migrate-* && $MIGRATION_SEAL == 0 ]]; then
  : "${FROM_VERSION:?FROM_VERSION is required for migrate-* commands}"
  : "${TO_VERSION:?TO_VERSION is required for migrate-* commands}"
  : "${SINCE_MAIN:?SINCE_MAIN is required}"
  : "${SINCE_BE:?SINCE_BE is required}"
  SINCE_ENGAGEMENT="${SINCE_ENGAGEMENT:-$SINCE_BE}"
  if [[ "$COMMAND" == "migrate-preflight" || "$COMMAND" == "migrate-preview" ]]; then
    : "${PREREG_POST:?PREREG_POST is required}"
    : "${PREREG_IR:?PREREG_IR is required}"
  fi
elif [[ "$COMMAND" != migrate-* ]]; then
  : "${PREREG_MAIN:?PREREG_MAIN (pre-registered cells) is required}"
  : "${PREREG_BE:?PREREG_BE (pre-registered cells) is required}"
  : "${SINCE_MAIN:?SINCE_MAIN (absolute receipt-time lower bound, ISO Z, from the ledger approval) is required}"
  : "${SINCE_BE:?SINCE_BE (absolute receipt-time lower bound, ISO Z, from the ledger approval) is required}"
fi
# Keep legacy SQL constants parseable for seal-only invocations; they are not used.
SINCE_MAIN="${SINCE_MAIN:-1970-01-01T00:00:00.000Z}"; SINCE_BE="${SINCE_BE:-$SINCE_MAIN}"; SINCE_ENGAGEMENT="${SINCE_ENGAGEMENT:-$SINCE_BE}"
MIGRATION_MAX_PREVIEW_ROWS="${MIGRATION_MAX_PREVIEW_ROWS:-10000000}"
[[ "$MIGRATION_MAX_PREVIEW_ROWS" =~ ^[1-9][0-9]*$ ]] || { echo "MIGRATION_MAX_PREVIEW_ROWS must be a positive integer" >&2; exit 2; }
IMG="${IMG:-pmmendoza/bsky-feedgen@sha256:928c15aac77a8a842f60053eff8953e70cc9e4117c2fbe86f548e345c1a34711}"
NETWORK="${NETWORK:-newsflows-bsky-feed-generator-v2_default}"
ENV_FILE="${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}"
DB_CONTAINER="${DB_CONTAINER:-feedgen-db}"
PSQL_DB="${PSQL_DB:-feedgen-db}"
PSQL_USER="${PSQL_USER:-feedgen}"
MAIN_DIDS="${MAIN_DIDS:-did:plc:toz4no26o2x4vsbum7cp4bxp,did:plc:kzmukwaf72iwepygposicgt3,did:plc:cegiy4pfghh4rjs7ks7pbnkm,did:plc:vzmnljt7otfbbgrmachtefxh}"
BE_DID="${BE_DID:-did:plc:tlmi333azel2jcornp2qeolm}"
HORIZON_MAIN_DAYS="${HORIZON_MAIN_DAYS:-3}"
HORIZON_BE_DAYS="${HORIZON_BE_DAYS:-10}"
FEEDGEN_CONTAINER="${FEEDGEN_CONTAINER:-feedgen}"
RUNNER="${RUNNER:-container}"
# WAL ceiling (D4 amended 2026-08-18, owner): a batch's attributed WAL may not exceed the estate's own baseline write
# rate (fresh control read) x the batch's duration incl. the inter-batch pause, i.e. the backfill never writes faster
# than the estate already writes; a small absolute floor keeps idle systems (rehearsal) from tripping on noise.
CEIL_WAL_BASELINE_MULTIPLE="${CEIL_WAL_BASELINE_MULTIPLE:-1.0}"
CEIL_WAL_FLOOR_BYTES="${CEIL_WAL_FLOOR_BYTES:-614400}"
PAUSE_SECONDS="${PAUSE_SECONDS:-1}"
CEIL_REL_BYTES="${CEIL_REL_BYTES:-409600}"
READBACK_JSON="${READBACK_JSON:-/opt/newsflows/blueskyranker_v2/logs/effective_config_readback.json}"
SKIP_LIVE_IMAGE_CHECKS="${SKIP_LIVE_IMAGE_CHECKS:-0}"
SECRET_KEY_REGEX="${SECRET_KEY_REGEX:-(PASSWORD|SECRET|TOKEN|_KEY|APIKEY|PASS)}"
read -r -a DOCKER <<<"${DOCKER:-sudo -n docker}"
V1='newsflows-content-time/v1'
V2='newsflows-content-time/v2'
V3='newsflows-content-time/v3'
RANKED_RKEYS="${RANKED_RKEYS:-'newsflow-nl-2','newsflow-fr-2','newsflow-cz-2','newsflow-ir-2','newsflow-be-k','newsflow-be-m'}"
MAIN_RKEY_PATTERN="${MAIN_RKEY_PATTERN:-newsflow-(nl|fr|cz|ir)-2}"
BE_RKEY_PATTERN="${BE_RKEY_PATTERN:-newsflow-be-k}"   # be-k and be-m share one publisher: count once
OUTCOMES=(v1_valid_to_v2_valid v1_invalid_to_v2_valid v1_to_v2_invalid)
EXTRA_CELLS=(createdat_extra createdat_unchanged)   # pre-registered, not tool outcomes: valid->valid rows whose createdAt differs from the v2 rendering; flip rows whose createdAt already equals the v2 target
IR_DID="${IR_DID:-did:plc:vzmnljt7otfbbgrmachtefxh}"
EXPECTED_CONTRACT_ROWS="${EXPECTED_CONTRACT_ROWS:-6}"
MIGRATION_DRAIN_SECONDS="${MIGRATION_DRAIN_SECONDS:-60}"
PREREG_POST="${PREREG_POST:-}"; PREREG_IR="${PREREG_IR:-}"

log() { echo "[packet] $*" >&2; }
die() { echo "[packet] STOP: $*" >&2; exit 2; }
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

emit() {  # emit <name>  (stdin -> $E/<name>, root:newsflows 0640, never overwrite)
  local name=$1 tmp
  [[ -e "$E/$name" ]] && die "refusing to overwrite existing evidence file $E/$name"
  tmp=$(mktemp); cat >"$tmp"
  sudo -n install -o root -g newsflows -m 640 "$tmp" "$E/$name"; rm -f "$tmp"
  log "wrote $name"
}
seal_evidence() { ( cd "$E" && sudo -n sh -c 'find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS && chown root:newsflows SHA256SUMS && chmod 640 SHA256SUMS' ); }
psql_ro() {
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -A -F '|' -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '600s';" "$@"
}
psql_copy() {
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '600s';" -c "\\copy ($1) TO STDOUT WITH (FORMAT text)"
}
group_dids() { case "$1" in main) echo "$MAIN_DIDS";; be) echo "$BE_DID";; *) die "group must be main|be";; esac; }
group_horizon() { case "$1" in main) echo "$HORIZON_MAIN_DAYS";; be) echo "$HORIZON_BE_DAYS";; esac; }
group_since() { case "$1" in main) echo "$SINCE_MAIN";; be) echo "$SINCE_BE";; esac; }
horizon_since() { date -u -d "$(group_horizon "$1") days ago" +%Y-%m-%dT%H:%M:%S.000Z; }   # rolling boundary at this instant
since_file() { local g=$1; [[ -f "$E/since-$g.txt" ]] && cat "$E/since-$g.txt" || die "since-$g.txt missing: run preflight first"; }
sql_array() { echo "'{$1}'"; }
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
}
latest_control() { ls -1 "$E"/pg-control-*.txt 2>/dev/null | sort | tail -1; }
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
    "${DOCKER[@]}" run --rm --network "$NETWORK" --env-file "$ENV_FILE" \
      -v "$TREE:/src:ro" -v "$E:/evidence" -w /src --entrypoint sh "$IMG" \
      -c 'export NODE_PATH=/app/node_modules; export FEEDGEN_POSTGRES_URL="$(node /src/scripts/compose_feedgen_dsn.js)" || exit 97; exec node /src/dist/tools/backfill-publisher-posts.js "$@"' sh "$@" \
      >"$so" 2>"$se" || rc=$?
  fi
  emit "$out.json" <"$so"; emit "$out.err" <"$se"; rm -f "$so" "$se"
  echo "$rc"
}
jsonq() { node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));const v=process.argv[2].split(".").reduce((a,k)=>a==null?a:a[k],j);console.log(v===undefined||v===null?"":(typeof v==="object"?JSON.stringify(v):v))' "$1" "$2"; }

POP_SQL="WITH t AS (SELECT c.rkey, c.publisher_did, CASE WHEN c.rkey LIKE 'newsflow-be-%' THEN '$SINCE_BE' ELSE '$SINCE_MAIN' END AS since FROM feedgen_ops.feed_catalog c WHERE c.enabled AND c.rkey IN ($RANKED_RKEYS))
SELECT t.rkey, t.since AS since, count(*) AS total,
 count(*) FILTER (WHERE p.content_time_status='source_valid' AND p.content_time_validator_version='$V2') AS v2_valid,
 count(*) FILTER (WHERE p.content_time_status='source_invalid' AND p.content_time_validator_version='$V2') AS v2_invalid,
 count(*) FILTER (WHERE p.content_time_validator_version='$V1' AND p.created_at_source_raw IS NOT NULL) AS v1_rows_with_raw,
 count(*) FILTER (WHERE p.content_time_validator_version='$V1' AND p.created_at_source_raw IS NULL) AS v1_rows_null_raw,
 count(*) FILTER (WHERE p.content_time_status IS NULL OR p.content_time_status='legacy_unknown') AS legacy
FROM t JOIN post p ON p.author=t.publisher_did AND p.\"indexedAt\" >= t.since
GROUP BY 1,2 ORDER BY 1;"
# v1 rows of the six publishers OUTSIDE the recorded bounds (must be unchanged by the apply) and their all-time v1 total
SCOPE_SQL="WITH t AS (SELECT c.rkey, c.publisher_did, CASE WHEN c.rkey LIKE 'newsflow-be-%' THEN '$SINCE_BE' ELSE '$SINCE_MAIN' END AS since FROM feedgen_ops.feed_catalog c WHERE c.enabled AND c.rkey IN ($RANKED_RKEYS))
SELECT 'v1_outside_since', count(*) FROM (SELECT DISTINCT p.uri FROM t JOIN post p ON p.author=t.publisher_did AND p.\"indexedAt\" < t.since WHERE p.content_time_validator_version='$V1') x
UNION ALL SELECT 'v1_total_six_publishers', count(*) FROM (SELECT DISTINCT p.uri FROM t JOIN post p ON p.author=t.publisher_did WHERE p.content_time_validator_version='$V1') y;"
TRANS_SQL="WITH t AS (SELECT c.rkey, c.publisher_did, CASE WHEN c.rkey LIKE 'newsflow-be-%' THEN '$SINCE_BE' ELSE '$SINCE_MAIN' END AS since FROM feedgen_ops.feed_catalog c WHERE c.enabled AND c.rkey IN ($RANKED_RKEYS)),
v1 AS (SELECT t.rkey, p.content_time_status AS s1, convert_from(p.created_at_source_raw,'UTF8') AS raw, p.\"indexedAt\" AS ia
 FROM t JOIN post p ON p.author=t.publisher_did AND p.\"indexedAt\" >= t.since
 WHERE p.content_time_validator_version='$V1' AND p.created_at_source_raw IS NOT NULL),
pred AS (SELECT rkey, CASE
  WHEN raw IS NULL OR raw='' OR raw !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' THEN 'v1_to_v2_invalid'
  WHEN raw::timestamptz > ia::timestamptz + interval '5 minutes' THEN 'v1_to_v2_invalid'
  WHEN s1='source_valid' THEN 'v1_valid_to_v2_valid' ELSE 'v1_invalid_to_v2_valid' END AS outcome FROM v1)
SELECT rkey, outcome, count(*) FROM pred GROUP BY 1,2 ORDER BY 1,2;"
# among predicted valid->valid rows, those whose stored "createdAt" differs from the v2 rendering of the raw (per rkey) -> createdAt_changed pre-registration
CREATED_SQL="WITH t AS (SELECT c.rkey, c.publisher_did, CASE WHEN c.rkey LIKE 'newsflow-be-%' THEN '$SINCE_BE' ELSE '$SINCE_MAIN' END AS since FROM feedgen_ops.feed_catalog c WHERE c.enabled AND c.rkey IN ($RANKED_RKEYS)),
v0 AS (SELECT t.rkey, p.content_time_status AS s1, p.\"createdAt\" AS ca, convert_from(p.created_at_source_raw,'UTF8') AS raw, p.\"indexedAt\" AS ia
 FROM t JOIN post p ON p.author=t.publisher_did AND p.\"indexedAt\" >= t.since
 WHERE p.content_time_validator_version='$V1' AND p.created_at_source_raw IS NOT NULL),
cls AS (SELECT rkey, s1, ca, raw, ia,
  CASE WHEN raw IS NULL OR raw='' OR raw !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' THEN 'v1_to_v2_invalid'
       WHEN raw::timestamptz > ia::timestamptz + interval '5 minutes' THEN 'v1_to_v2_invalid'
       WHEN s1='source_valid' THEN 'v1_valid_to_v2_valid' ELSE 'v1_invalid_to_v2_valid' END AS outcome,
  CASE WHEN raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' AND NOT (raw::timestamptz > ia::timestamptz + interval '5 minutes')
       THEN to_char(raw::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') ELSE ia END AS target FROM v0)
SELECT rkey,
  count(*) FILTER (WHERE outcome='v1_valid_to_v2_valid' AND ca IS DISTINCT FROM target) AS createdat_extra,
  count(*) FILTER (WHERE outcome<>'v1_valid_to_v2_valid' AND ca IS NOT DISTINCT FROM target) AS createdat_unchanged
FROM cls GROUP BY 1 ORDER BY 1;"
CATALOG_SQL="SELECT rkey, publisher_did, publisher_post_max_age_days FROM feedgen_ops.feed_catalog WHERE enabled AND rkey IN ($RANKED_RKEYS) ORDER BY 1;"
snapshot_sql() {  # <dids-csv> <since> <version>
  echo "SELECT p.uri, p.author, p.\"indexedAt\", p.\"createdAt\", p.content_time_utc, p.content_time_status, p.content_time_clamp_reason, p.content_time_validator_version, encode(p.created_at_source_raw,'hex') AS raw_hex FROM post p WHERE p.author = ANY($(sql_array "$1")::text[]) AND p.content_time_validator_version = '$3' AND p.created_at_source_raw IS NOT NULL AND p.\"indexedAt\" >= '$2' ORDER BY p.author, p.uri"
}
predicted_outcome() {  # <group> <outcome> from prestate-transitions.tsv
  local pat; case "$1" in main) pat="$MAIN_RKEY_PATTERN";; be) pat="$BE_RKEY_PATTERN";; esac
  awk -F'|' -v pat="$pat" -v o="$2" '$1 ~ "^"pat"$" && $2==o {s+=$3} END{print s+0}' "$E/prestate-transitions.tsv"
}
predicted_extra() {  # <group> <createdat_extra|createdat_unchanged> from prestate-createdat.tsv
  local pat col; case "$1" in main) pat="$MAIN_RKEY_PATTERN";; be) pat="$BE_RKEY_PATTERN";; esac; case "$2" in createdat_extra) col=2;; createdat_unchanged) col=3;; esac
  awk -F'|' -v pat="$pat" -v c="$col" '$1 ~ "^"pat"$" {s+=$c} END{print s+0}' "$E/prestate-createdat.tsv"
}
prereg_value() {  # <group> <outcome> from PREREG_MAIN/PREREG_BE ("k=v,k=v"); a missing cell is a hard stop
  local spec v; case "$1" in main) spec="$PREREG_MAIN";; be) spec="$PREREG_BE";; esac
  v=$(echo "$spec" | tr ',' '\n' | awk -F= -v k="$2" '$1==k{print $2}')
  [[ "$v" =~ ^[0-9]+$ ]] || die "pre-registered cell $1/$2 missing or non-numeric in PREREG_${1^^}"
  echo "$v"
}
gate_cell() {  # gate_cell <label> <expected> <actual> [tolerance_pct_above_50]
  local exp=$2 act=$3 tol=${4:-2}
  if (( exp < 50 )); then [[ "$exp" == "$act" ]] || die "gate $1: expected $exp got $act (exact match required below 50)";
  else local lo=$(( exp*(100-tol)/100 )) hi=$(( exp*(100+tol)/100 + 1 )); (( act >= lo && act <= hi )) || die "gate $1: expected $exp±${tol}% got $act"; fi
  log "gate $1 ok (expected $exp, got $act)"
}

cmd_preflight() {
  [[ -d "$E" ]] || sudo -n install -d -o root -g newsflows -m 750 "$E"
  assert_tree
  local sha; sha=$(git -C "$TREE" rev-parse HEAD)
  local runner_sha helper_sha; runner_sha=$(sha256sum "$TREE/scripts/content_time_revalidate_packet.sh" | cut -d' ' -f1); helper_sha=$(sha256sum "$TREE/scripts/compose_feedgen_dsn.js" | cut -d' ' -f1)
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
  cfg_dids=$(echo "$MAIN_DIDS,$BE_DID" | tr ',' '\n' | sort -u | tr '\n' ',' | sed 's/,$//')
  [[ "$cat_dids" == "$cfg_dids" ]] || die "catalog publisher DIDs ($cat_dids) != configured MAIN_DIDS+BE_DID ($cfg_dids)"
  local max_main max_be; max_main=$(awk -F'|' -v pat="$MAIN_RKEY_PATTERN" 'NR>1 && $1 ~ "^"pat"$" {if($3+0>m)m=$3+0} END{print m+0}' "$E/catalog-readback.tsv"); max_be=$(awk -F'|' -v pat="$BE_RKEY_PATTERN" 'NR>1 && $1 ~ "^"pat"$" {if($3+0>m)m=$3+0} END{print m+0}' "$E/catalog-readback.tsv")
  (( HORIZON_MAIN_DAYS >= max_main && HORIZON_BE_DAYS >= max_be )) || die "horizon (main $HORIZON_MAIN_DAYS / be $HORIZON_BE_DAYS) below catalog publisher_post_max_age_days (main $max_main / be $max_be)"
  local rb_main="n/a" rb_be="n/a"
  if [[ "$SKIP_LIVE_IMAGE_CHECKS" != "1" ]]; then
    [[ -f "$READBACK_JSON" ]] || die "READBACK_JSON $READBACK_JSON missing — BSR effective-config readback is a prerequisite"
    local rb_stale rb_now; rb_stale=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const v=(j.artifact_metadata||{}).stale_at;const n=typeof v==="number"?v:(typeof v==="string"&&/^\d+$/.test(v)?Number(v):(typeof v==="string"?Math.floor(Date.parse(v)/1000):NaN));console.log(Number.isFinite(n)?n:"unparseable")' "$READBACK_JSON"); rb_now=$(date -u +%s)
    [[ "$rb_stale" =~ ^[0-9]+$ ]] || die "BSR effective-config readback stale_at is unparseable ($rb_stale)"
    (( rb_stale > rb_now )) || die "BSR effective-config readback is stale (stale_at=$rb_stale now=$rb_now); the 5-min producer must be running"
    rb_main=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const re=new RegExp("^"+process.argv[2]+"$");let m=0;for(const b of Object.values(j.bindings||{})){if((b.feed_ids||[]).some(f=>re.test(f)))m=Math.max(m,b.push_window_days||0)}console.log(m)' "$READBACK_JSON" "$MAIN_RKEY_PATTERN")
    rb_be=$(sudo -n node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const re=new RegExp("^"+process.argv[2]+"$");let m=0;for(const b of Object.values(j.bindings||{})){if((b.feed_ids||[]).some(f=>re.test(f)))m=Math.max(m,b.push_window_days||0)}console.log(m)' "$READBACK_JSON" "$BE_RKEY_PATTERN")
    [[ "$rb_main" =~ ^[0-9]+$ && "$rb_be" =~ ^[0-9]+$ ]] && (( rb_main > 0 && rb_be > 0 )) || die "BSR readback did not yield positive push windows for both groups (main '$rb_main' be '$rb_be'): bindings/feed_ids shape mismatch"
    (( HORIZON_MAIN_DAYS >= rb_main && HORIZON_BE_DAYS >= rb_be )) || die "horizon below BSR push window (readback main $rb_main / be $rb_be)"
    sudo -n sha256sum "$READBACK_JSON" | emit effective-config-readback.sha256
  fi
  { echo "generated_at=$(ts)"; echo "tree=$TREE"; echo "source_sha=$sha"; echo "runner_script_sha256=$runner_sha"; echo "dsn_helper_sha256=$helper_sha"; echo "dist_backfill_sha256=$EXPECTED_DIST_SHA256"; echo "dist_content_time_sha256=$EXPECTED_CT_SHA256"; echo "image=$IMG"; echo "image_validator_sha256=$img_ct"; echo "feedgen_retention_enabled=$ret"; echo "runner=$RUNNER"; echo "packet_sha256=$PACKET_SHA"; echo "network=$NETWORK"; echo "env_file=$ENV_FILE"; echo "db_container=$DB_CONTAINER"; echo "main_dids=$MAIN_DIDS"; echo "be_did=$BE_DID"; echo "horizon_main_days=$HORIZON_MAIN_DAYS"; echo "horizon_be_days=$HORIZON_BE_DAYS"; echo "catalog_max_age_main=$max_main catalog_max_age_be=$max_be readback_push_main=$rb_main readback_push_be=$rb_be"; echo "ceilings wal<=max($CEIL_WAL_FLOOR_BYTES, $CEIL_WAL_BASELINE_MULTIPLE x baseline_rate x (batch_elapsed+${PAUSE_SECONDS}s)) relation<=$CEIL_REL_BYTES dead<=2 x updated rows"; echo "since_main=$SINCE_MAIN since_be=$SINCE_BE"; echo "prereg_main=$PREREG_MAIN prereg_be=$PREREG_BE (exact)"; echo "expected_tool_refs=$EXPECTED_TOOL_REFS"; } | emit source-set.txt
  # the absolute bounds must be at or before the rolling horizon boundary of this instant (a fixed older bound is a superset)
  local g; for g in main be; do [[ "$(group_since "$g")" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "SINCE_${g^^} must be ISO-8601 with milliseconds and Z"; [[ "$(group_since "$g")" < "$(horizon_since "$g")" || "$(group_since "$g")" == "$(horizon_since "$g")" ]] || die "SINCE_${g^^}=$(group_since "$g") is later than the rolling $(group_horizon "$g")-day boundary $(horizon_since "$g")"; done
  echo "$(group_since main)" | emit since-main.txt
  echo "$(group_since be)" | emit since-be.txt
  psql_ro -c "$SCOPE_SQL" | emit prestate-scope.tsv
  psql_ro -c "$POP_SQL" | emit prestate-populations.tsv
  psql_ro -c "$TRANS_SQL" | emit prestate-transitions.tsv
  psql_ro -c "$CREATED_SQL" | emit prestate-createdat.tsv
  local nullraw; nullraw=$(awk -F'|' 'NR>1 && $1 !~ /^rkey$/ {s+=$7} END{print s+0}' "$E/prestate-populations.tsv")
  [[ "$nullraw" == "0" ]] || die "in-horizon v1 rows with NULL raw: $nullraw (expected 0)"
  local g o exp act
  # the candidate set is stable at absolute bounds (no v1 producer exists), so pre-registered cells must match EXACTLY
  for g in main be; do for o in "${OUTCOMES[@]}"; do exp=$(prereg_value "$g" "$o"); act=$(predicted_outcome "$g" "$o"); [[ "$exp" == "$act" ]] || die "prereg $g/$o: pre-registered $exp, live prediction $act (candidate set must be stable at the bound SINCE)"; log "prereg $g/$o ok ($exp)"; done
    for o in "${EXTRA_CELLS[@]}"; do exp=$(prereg_value "$g" "$o"); act=$(predicted_extra "$g" "$o"); [[ "$exp" == "$act" ]] || die "prereg $g/$o: pre-registered $exp, live prediction $act"; log "prereg $g/$o ok ($exp)"; done; done
  psql_copy "$(snapshot_sql "$MAIN_DIDS" "$(since_file main)" "$V1")" | emit step1-main-prestate-rows.tsv
  psql_copy "$(snapshot_sql "$BE_DID" "$(since_file be)" "$V1")" | emit step1-be-prestate-rows.tsv
  take_control pg-control-1.txt
  pgstat_read | emit pg-prestate.txt
  log "preflight complete: main prestate rows=$(wc -l <"$E/step1-main-prestate-rows.tsv") be prestate rows=$(wc -l <"$E/step1-be-prestate-rows.tsv")"
}
cmd_control() { local n; n=$(( $(ls -1 "$E"/pg-control-*.txt 2>/dev/null | wc -l) + 1 )); take_control "pg-control-$n.txt"; }

cmd_preview() {
  local g=$1 rc; rc=$(run_tool "step1-$g-preview" --mode revalidate --actors "$(group_dids "$g")" --since "$(since_file "$g")" --json --packet-sha256 "$PACKET_SHA")
  local f="$E/step1-$g-preview.json"
  [[ "$rc" == "0" ]] || die "preview $g exit=$rc (see step1-$g-preview.err)"
  [[ "$(jsonq "$f" operation)" == "content-time-revalidate" && "$(jsonq "$f" mode)" == "dry-run" ]] || die "preview $g did not run the revalidate dry-run path"
  local keys; keys=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log(Object.keys(j.preview.counts).filter(k=>k!=="by_v2_invalid_reason").join(" "))' "$f")
  local k known sum=0
  for k in $keys; do known=0; for o in "${OUTCOMES[@]}"; do [[ "$k" == "$o" ]] && known=1; done; (( known )) || die "preview $g reports unpredicted outcome class '$k'"; sum=$(( sum + $(jsonq "$f" "preview.counts.$k") )); done
  local o; for o in "${OUTCOMES[@]}"; do [[ "$(predicted_outcome "$g" "$o")" == "$(jsonq "$f" "preview.counts.$o")" ]] || die "preview $g/$o: predicted $(predicted_outcome "$g" "$o"), tool reports $(jsonq "$f" "preview.counts.$o")"; log "gate $g/$o ok"; done
  local scanned rows; scanned=$(jsonq "$f" preview.scanned); rows=$(wc -l <"$E/step1-$g-prestate-rows.tsv")
  [[ "$scanned" == "$sum" && "$scanned" == "$rows" ]] || die "preview $g: scanned=$scanned sum(counts)=$sum prestate_rows=$rows must all be equal"
  log "preview $g ok: scanned=$scanned"
}
cmd_apply() {
  local g=$1 label=$2 maxb=${3:-}
  assert_tree
  local ckpt="/evidence/step1-$g-checkpoint.json"; [[ "$RUNNER" == "host" ]] && ckpt="$E/step1-$g-checkpoint.json"
  pgstat_read | emit "pg-$g-$label-before.txt"
  # ADAPTIVE PAUSE (D4-b, owner 2026-08-18): the tool pauses after every batch for max(1 s, batch LSN advance / baseline
  # write rate) -- each batch pays its WAL back at the estate's own rate, so the backfill can never average faster than
  # the estate by construction. The baseline is the most recent control read (same file the ceiling uses).
  local wps0 rps0 dps0; read -r wps0 rps0 dps0 <<<"$(control_rates)"
  (( wps0 >= 1 )) || die "apply $g/$label: control baseline write rate is $wps0 B/s (stalled or missing control read) -- run '$0 control' first"
  local rc; rc=$(run_tool "step1-$g-apply-$label" --mode revalidate --apply --actors "$(group_dids "$g")" --since "$(since_file "$g")" --checkpoint-file "$ckpt" --packet-sha256 "$PACKET_SHA" ${maxb:+--max-batches "$maxb"} --pause-baseline-bytes-per-s "$wps0" --json)
  pgstat_read | emit "pg-$g-$label-after.txt"
  local f="$E/step1-$g-apply-$label.json"
  [[ "$rc" == "0" || "$rc" == "3" ]] || die "apply $g/$label exit=$rc (see .err); STOP + escalation"
  [[ "$(jsonq "$f" operation)" == "content-time-revalidate" && "$(jsonq "$f" mode)" == "apply" ]] || die "apply $g/$label did not run the revalidate apply path"
  local nb; nb=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log((j.revalidation&&j.revalidation.batches||[]).length)' "$f")
  local b a; b=$(cat "$E/pg-$g-$label-before.txt"); a=$(cat "$E/pg-$g-$label-after.txt")
  local dw dr dd dur; dw=$(( $(echo "$a"|cut -d'|' -f1) - $(echo "$b"|cut -d'|' -f1) )); dr=$(( $(echo "$a"|cut -d'|' -f2) - $(echo "$b"|cut -d'|' -f2) )); dd=$(( $(echo "$a"|cut -d'|' -f3) - $(echo "$b"|cut -d'|' -f3) )); dur=$(( $(echo "$a"|cut -d'|' -f5) - $(echo "$b"|cut -d'|' -f5) )); (( dur < 1 )) && dur=1
  read -r wps rps dps <<<"$(control_rates)"
  local verdict="ok"
  if (( nb > 0 )); then
    local pw=$(( (dw - wps*dur) / nb )) pr=$(( (dr - rps*dur) / nb )) pd=$(( (dd - dps*dur) / nb ))
    local twal; twal=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const b=(j.revalidation&&j.revalidation.batches)||[];const w=b.map(x=>[x.wal_bytes,x.elapsed_ms]).filter(x=>typeof x[0]==="number");if(!w.length){console.log("");process.exit(0)}const r=Number(process.argv[2]);const adj=w.map(([wb,ms])=>Math.max(0,wb-Math.round(r*(ms||0)/1000)));console.log(Math.max(...adj))' "$f" "$wps")
    # PER-BATCH enforcement: each batch's attributed WAL (its own LSN advance minus baseline x its own elapsed) must be
    # <= max(floor, multiple x baseline x (its own elapsed + pause)); the verdict is the conjunction over batches
    local perbatch; perbatch=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const b=(j.revalidation&&j.revalidation.batches)||[];const [r,m,p,fl]=process.argv.slice(2).map(Number);let worst=0,worstRatio=0,fail=0,unpaid=0,lines=[];for(const x of b){if(typeof x.wal_bytes!=="number"){lines.push(`batch=${x.batch} wal_bytes=missing`);fail++;continue;}const ms=x.elapsed_ms||0;const paidMs=(typeof x.pause_ms==="number")?x.pause_ms:Math.round(p*1000);const reqMs=(typeof x.pause_required_ms==="number")?x.pause_required_ms:Math.round(p*1000);const owedMs=(x.candidates===0)?0:Math.max(Math.round(p*1000),Math.ceil(x.wal_bytes*1000/r));const paid=paidMs>=owedMs;if(!paid)unpaid++;const adj=Math.max(0,x.wal_bytes-Math.round(r*ms/1000));const ceil=Math.max(fl,Math.round(m*r*(ms/1000+paidMs/1000)));const ok=adj<=ceil&&paid;if(!ok)fail++;const ratio=ceil?adj/ceil:0;if(ratio>worstRatio){worstRatio=ratio;worst=adj;}lines.push(`batch=${x.batch} elapsed_ms=${ms} lsn_advance=${x.wal_bytes} attributed=${adj} pause_owed_ms=${owedMs} pause_paid_ms=${paidMs} ceiling=${ceil} ratio=${ratio.toFixed(2)} ${paid?"paid":"UNPAID"} ${adj<=ceil?"ok":"BREACH"}`);}console.log(JSON.stringify({fail,unpaid,worst,worstRatio:worstRatio.toFixed(2),lines}))' "$f" "$wps0" "$CEIL_WAL_BASELINE_MULTIPLE" "$PAUSE_SECONDS" "$CEIL_WAL_FLOOR_BYTES")
    local wal_fail wal_worst wal_ratio wal_lines
    wal_fail=$(node -e 'console.log(JSON.parse(process.argv[1]).fail)' "$perbatch"); wal_worst=$(node -e 'console.log(JSON.parse(process.argv[1]).worst)' "$perbatch"); wal_ratio=$(node -e 'console.log(JSON.parse(process.argv[1]).worstRatio)' "$perbatch"); wal_lines=$(node -e 'console.log(JSON.parse(process.argv[1]).lines.join("\n"))' "$perbatch")
    local dead_ceiling=$(( 2 * $(jsonq "$f" revalidation.updated) / nb ))
    (( wal_fail == 0 && pr <= CEIL_REL_BYTES && pd <= dead_ceiling )) || verdict="BREACH"
    { echo "batches=$nb exit=$rc updated=$(jsonq "$f" revalidation.updated) complete=$(jsonq "$f" revalidation.complete) invocation_seconds=$dur"; echo "delta_wal_bytes=$dw delta_relation_bytes=$dr delta_dead_tuples=$dd"; echo "control_file=$(basename "$(latest_control)") control_rate_per_s wal=$wps relation=$rps dead=$dps (clamped at 0)"; echo "per_batch_relation_minus_control=$pr per_batch_dead_minus_control=$pd (invocation deltas minus baseline x duration, / batches)"; echo "wal_rule=per batch: attributed (LSN advance - baseline x elapsed) <= max(floor $CEIL_WAL_FLOOR_BYTES, $CEIL_WAL_BASELINE_MULTIPLE x baseline $wps0 B/s x (elapsed + paid pause)); adaptive pause owed = max(${PAUSE_SECONDS}s, LSN advance / baseline $wps0 B/s) and must be paid (tool --pause-baseline-bytes-per-s $wps0); cluster-wide LSN, concurrent writers included then subtracted"; echo "$wal_lines"; echo "wal_batches_failing=$wal_fail wal_batches_unpaid=$(node -e 'console.log(JSON.parse(process.argv[1]).unpaid)' "$perbatch") wal_worst_attributed=$wal_worst wal_worst_ratio=$wal_ratio"; echo "ceilings relation<=$CEIL_REL_BYTES dead<=$dead_ceiling (=2 x updated rows per batch)"; echo "verdict=$verdict"; } | emit "ceiling-$g-$label.txt"
  else
    echo "batches=0 exit=$rc verdict=no_batches" | emit "ceiling-$g-$label.txt"
  fi
  [[ "$verdict" == "ok" ]] || die "ceiling breach on $g/$label (see ceiling-$g-$label.txt): run '$0 control', then re-measure once with '$0 apply $g ${label}r 1'; a second breach = stop for good + escalation"
  log "apply $g/$label exit=$rc batches=$nb verdict=$verdict"
}
cmd_readback() {  # re-runnable: every attempt gets its own suffix
  local g rc a=1; while [[ -e "$E/step1-main-preview-after-attempt-$a.json" || -e "$E/step1-main-preview-after-attempt-$a.err" ]]; do a=$(( a + 1 )); done; local sfx="attempt-$a"
  local summary=""
  for g in main be; do
    rc=$(run_tool "step1-$g-preview-after-$sfx" --mode revalidate --actors "$(group_dids "$g")" --since "$(since_file "$g")" --json --packet-sha256 "$PACKET_SHA")
    [[ "$rc" == "0" && "$(jsonq "$E/step1-$g-preview-after-$sfx.json" preview.scanned)" == "0" ]] || die "post-apply preview $g not empty (exit=$rc)"
    psql_copy "$(snapshot_sql "$(group_dids "$g")" "$(since_file "$g")" "$V2")" | emit "step1-$g-poststate-rows-$sfx.tsv"
    node -e '
const fs=require("fs");const [pre,post]=[process.argv[1],process.argv[2]].map(p=>fs.readFileSync(p,"utf8").split("\n").filter(Boolean).map(l=>l.split("\t")));
const m=new Map(post.map(r=>[r[0],r]));let missing=0,createdChanged=0,statusChanged=0,rawMismatch=0;const byAuthor={};
for(const r of pre){const q=m.get(r[0]);if(!q){missing++;continue;}const A=byAuthor[r[1]]||(byAuthor[r[1]]={rows:0,to_invalid:0,to_valid:0});A.rows++;
 if(q[3]!==r[3])createdChanged++;if(q[5]!==r[5]){statusChanged++;if(q[5]==="source_invalid")A.to_invalid++;else A.to_valid++;}if(q[8]!==r[8])rawMismatch++;}
console.log(`prestate_rows=${pre.length} poststate_rows_v2=${post.length} prestate_missing_in_poststate=${missing} createdAt_changed=${createdChanged} status_changed=${statusChanged} raw_mismatch=${rawMismatch}`);
for(const [a,v] of Object.entries(byAuthor))console.log(`author=${a} rows=${v.rows} to_invalid=${v.to_invalid} to_valid=${v.to_valid}`);
process.exit(missing===0&&rawMismatch===0?0:2)' "$E/step1-$g-prestate-rows.tsv" "$E/step1-$g-poststate-rows-$sfx.tsv" | emit "step1-$g-diff-$sfx.txt" || die "readback $g: prestate rows missing from poststate or raw mismatch"
    local exp_changed act_status act_created
    exp_changed=$(( $(prereg_value "$g" v1_invalid_to_v2_valid) + $(prereg_value "$g" v1_to_v2_invalid) ))
    local exp_created=$(( exp_changed + $(prereg_value "$g" createdat_extra) - $(prereg_value "$g" createdat_unchanged) ))
    act_status=$(sed -n 's/^prestate_rows=.*status_changed=\([0-9]*\).*/\1/p' "$E/step1-$g-diff-$sfx.txt"); act_created=$(sed -n 's/^prestate_rows=.*createdAt_changed=\([0-9]*\).*/\1/p' "$E/step1-$g-diff-$sfx.txt")
    [[ "$act_status" == "$exp_changed" && "$act_created" == "$exp_created" ]] || die "realized $g: status_changed=$act_status (pre-registered $exp_changed) createdAt_changed=$act_created (pre-registered $exp_created)"
    # per publisher (= per feed for main; the shared BE publisher counts once): realized to_invalid / to_valid == predicted per rkey
    local line author rk pi pv ai av
    while read -r line; do
      author=$(echo "$line" | sed -n 's/^author=\([^ ]*\).*/\1/p'); [[ -n "$author" ]] || continue
      rk=$(awk -F'|' -v a="$author" 'NR>1 && $2==a {print $1; exit}' "$E/catalog-readback.tsv")
      pi=$(awk -F'|' -v r="$rk" '$1==r && $2=="v1_to_v2_invalid"{s+=$3} END{print s+0}' "$E/prestate-transitions.tsv"); pv=$(awk -F'|' -v r="$rk" '$1==r && $2=="v1_invalid_to_v2_valid"{s+=$3} END{print s+0}' "$E/prestate-transitions.tsv")
      ai=$(echo "$line" | sed -n 's/.*to_invalid=\([0-9]*\).*/\1/p'); av=$(echo "$line" | sed -n 's/.*to_valid=\([0-9]*\).*/\1/p')
      [[ "$ai" == "$pi" && "$av" == "$pv" ]] || die "realized per-publisher $rk: to_invalid=$ai (pred $pi) to_valid=$av (pred $pv)"
    done <"$E/step1-$g-diff-$sfx.txt"
    # every row the apply wrote is a prestate row: sum(updated) over this group's apply receipts == prestate rows
    local upd rows; upd=$(for f in "$E"/step1-$g-apply-*.json; do jsonq "$f" revalidation.updated; done | awk '{s+=$1} END{print s+0}'); rows=$(wc -l <"$E/step1-$g-prestate-rows.tsv")
    [[ "$upd" == "$rows" ]] || die "blast radius $g: apply receipts report updated=$upd, prestate rows=$rows"
    summary+="$g updated=$upd prestate_rows=$rows status_changed=$act_status"$'\n'
  done
  psql_ro -c "$POP_SQL" | emit "poststate-populations-$sfx.tsv"
  psql_ro -c "$SCOPE_SQL" | emit "poststate-scope-$sfx.tsv"
  pgstat_read | emit "pg-poststate-$sfx.txt"
  local rem; rem=$(awk -F'|' 'NR>1 && $1 !~ /^rkey$/ {s+=$6} END{print s+0}' "$E/poststate-populations-$sfx.tsv")
  [[ "$rem" == "0" ]] || die "v1_rows_with_raw remaining after apply: $rem"
  # out-of-scope v1 rows of the six publishers unchanged; all-time v1 total decreased by exactly the rows updated
  local pre_out post_out pre_tot post_tot tot_upd
  pre_out=$(awk -F'|' '$1=="v1_outside_since"{print $2}' "$E/prestate-scope.tsv"); post_out=$(awk -F'|' '$1=="v1_outside_since"{print $2}' "$E/poststate-scope-$sfx.tsv")
  pre_tot=$(awk -F'|' '$1=="v1_total_six_publishers"{print $2}' "$E/prestate-scope.tsv"); post_tot=$(awk -F'|' '$1=="v1_total_six_publishers"{print $2}' "$E/poststate-scope-$sfx.tsv")
  tot_upd=$(( $(wc -l <"$E/step1-main-prestate-rows.tsv") + $(wc -l <"$E/step1-be-prestate-rows.tsv") ))
  [[ "$pre_out" == "$post_out" ]] || die "blast radius: v1 rows outside the bounds changed ($pre_out -> $post_out)"
  [[ $(( pre_tot - post_tot )) == "$tot_upd" ]] || die "blast radius: six-publisher v1 total moved by $(( pre_tot - post_tot )), expected $tot_upd"
  { echo "readback=$sfx"; printf '%s' "$summary"; echo "v1_outside_since before=$pre_out after=$post_out"; echo "v1_total_six_publishers before=$pre_tot after=$post_tot"; } | emit "readback-$sfx.txt"
  log "readback ok ($sfx): v1_rows_with_raw=0, realized outcome == pre-registration, blast radius bounded"
}
cmd_restore() {  # bounded CAS restore from the prestate snapshot; keyset batches of 500; resumable; batch index derived from the cursor
  local g=$1
  assert_tree
  local pre="$E/step1-$g-prestate-rows.tsv" cur="$E/restore-$g-cursor.txt" total batch=0
  [[ -f "$pre" ]] || die "no prestate snapshot for $g"
  total=$(wc -l <"$pre"); log "restore $g: $total prestate rows"
  local start=1; [[ -f "$cur" ]] && start=$(cat "$cur")
  local ra=1; while [[ -e "$E/restore-$g-result-attempt-$ra.txt" || -e "$E/restore-$g-after-rows-attempt-$ra.tsv" ]]; do ra=$(( ra + 1 )); done
  while (( start <= total )); do
    local end=$(( start + 499 )); (( end > total )) && end=$total; batch=$(( (start - 1) / 500 + 1 ))
    local rtmp; rtmp=$(mktemp)
    local attempt=1 rname; while [[ -e "$E/restore-$g-rows-$start-$end-attempt-$attempt.txt" ]]; do attempt=$(( attempt + 1 )); done; rname="restore-$g-rows-$start-$end-attempt-$attempt.txt"
    { echo "BEGIN; SET lock_timeout='5s'; SET statement_timeout='30s';"; echo "CREATE TEMP TABLE prestate(uri text, author text, indexed_at text, created_at text, content_time_utc text, status text, reason text, version text, raw_hex text);"; echo "COPY prestate FROM STDIN WITH (FORMAT text);"; sed -n "${start},${end}p" "$pre"; echo '\.'; cat <<'SQL'
UPDATE public.post AS t SET "createdAt"=s.created_at, content_time_utc=s.content_time_utc, content_time_status=s.status, content_time_clamp_reason=s.reason, content_time_validator_version=s.version
FROM prestate s WHERE t.uri=s.uri AND t.content_time_validator_version='newsflows-content-time/v2' AND encode(t.created_at_source_raw,'hex')=s.raw_hex;
SELECT 'restored_in_batch', count(*) FROM post p JOIN prestate s ON s.uri=p.uri WHERE p."createdAt"=s.created_at AND p.content_time_utc IS NOT DISTINCT FROM s.content_time_utc AND p.content_time_status=s.status AND p.content_time_clamp_reason IS NOT DISTINCT FROM s.reason AND p.content_time_validator_version=s.version;
COMMIT;
SQL
    } | "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -A -F '|' -v ON_ERROR_STOP=1 >"$rtmp" 2>&1 || { emit "$rname" <"$rtmp"; die "restore $g batch $batch (rows $start-$end) failed (see $rname); re-run 'restore $g' to resume from the cursor (next attempt gets its own receipt)"; }
    local restored expected=$(( end - start + 1 )); restored=$(awk -F'|' '$1=="restored_in_batch"{print $2}' "$rtmp")
    emit "$rname" <"$rtmp"; rm -f "$rtmp"
    [[ "$restored" == "$expected" ]] || die "restore $g batch $batch restored $restored of $expected rows (CAS mismatch); stop + escalation"
    echo $(( end + 1 )) | sudo -n tee "$cur" >/dev/null
    if [[ -n "${RESTORE_STOP_AFTER_BATCH:-}" && "$batch" -ge "$RESTORE_STOP_AFTER_BATCH" && "$end" -lt "$total" ]]; then log "restore $g: rehearsal stop after batch $batch (resume by re-running)"; exit 3; fi
    sleep 1; start=$(( end + 1 ))
  done
  psql_copy "$(snapshot_sql "$(group_dids "$g")" "$(since_file "$g")" "$V1")" | emit "restore-$g-after-rows-attempt-$ra.tsv"
  cmp -s "$pre" "$E/restore-$g-after-rows-attempt-$ra.tsv" && echo "restore_$g=identical_to_prestate rows=$total batches_this_run=$batch attempt=$ra" | emit "restore-$g-result-attempt-$ra.txt" || { diff "$pre" "$E/restore-$g-after-rows-attempt-$ra.tsv" | head -20 | emit "restore-$g-result-attempt-$ra.txt"; die "restore $g: after-restore snapshot differs from prestate"; }
  log "restore $g complete: identical to prestate"
}
cmd_secret_scan() {
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
  local wstart=${1:-unset} wend=${2:-unset}
  { [[ -f "$E/secret-scan.txt" ]] && grep -q '^hits=0$' "$E/secret-scan.txt"; } || die "finalize requires a clean secret-scan first"
  local rtmp; rtmp=$(mktemp)
  { echo "status=complete"; echo "generated_at=$(ts)"; cat "$E/source-set.txt"; echo "window_start_utc=$wstart"; echo "window_end_utc=$wend"; for f in "$E"/ceiling-*.txt; do if [[ -f "$f" ]]; then echo "--- $(basename "$f")"; cat "$f"; fi; done; echo "--- diffs"; cat "$E"/step1-*-diff-*.txt "$E"/readback-*.txt 2>/dev/null || true; echo "--- populations after"; cat "$E/poststate-populations.tsv" 2>/dev/null || true; } >"$rtmp"
  emit RESULT.txt <"$rtmp"; rm -f "$rtmp"
  seal_evidence
  log "finalized: $E"
}

cmd_prereg() {  # read-only helper for the ledger approval: cells at the given SINCE bounds
  local g o; echo "since_main=$SINCE_MAIN since_be=$SINCE_BE"
  local tmp; tmp=$(mktemp); psql_ro -c "$TRANS_SQL" >"$tmp"
  for g in main be; do local pat; case "$g" in main) pat="$MAIN_RKEY_PATTERN";; be) pat="$BE_RKEY_PATTERN";; esac
    local cells=""; for o in "${OUTCOMES[@]}"; do cells+="$o=$(awk -F'|' -v pat="$pat" -v o="$o" '$1 ~ "^"pat"$" && $2==o {s+=$3} END{print s+0}' "$tmp"),"; done
    local ctmp; ctmp=$(mktemp); psql_ro -c "$CREATED_SQL" >"$ctmp"
    cells+="createdat_extra=$(awk -F'|' -v pat="$pat" '$1 ~ "^"pat"$" {s+=$2} END{print s+0}' "$ctmp"),createdat_unchanged=$(awk -F'|' -v pat="$pat" '$1 ~ "^"pat"$" {s+=$3} END{print s+0}' "$ctmp")"; rm -f "$ctmp"
    echo "PREREG_${g^^}=$cells"; done
  echo "--- per-feed population at the bounds (rkey|since|total|v2_valid|v2_invalid|v1_rows_with_raw|v1_rows_null_raw|legacy)"; psql_ro -c "$POP_SQL"
  echo "--- per-feed predicted transitions (rkey|outcome|count)"; cat "$tmp"
  psql_ro -c "$SCOPE_SQL"; rm -f "$tmp"
}

# FT-FU-1: one contract governs posts and projected engagement in the widest
# active horizon. Only semantic-delta post rows are physically rewritten. The stable
# denominator is one complete cutoff-bounded preview; immutable columns are
# protected by the runner's UPDATE shape and raw-byte CAS predicate.
migration_outcomes() {
  case "$1->$2" in
    "$V2->$V3") echo 'v2_valid_to_v3_valid v2_skew_to_v3_clamped v2_invalid_to_v3_clamped v2_to_v3_invalid gt_5m_restored zero_to_5m_clamped';;
    "$V3->$V2") echo 'v3_valid_to_v2_valid v3_clamped_to_v2_valid v3_clamped_to_v2_invalid v3_to_v2_invalid gt_5m_invalidated zero_to_5m_unclamped';;
    *) die "unsupported migration transition $1->$2";;
  esac
}
assert_migration_transition() { migration_outcomes "$FROM_VERSION" "$TO_VERSION" >/dev/null; }
migration_targets() { echo 'post'; }
migration_target_table() { case "$1" in post) echo "$1";; *) die "unknown migration target $1";; esac; }
migration_target_since() { case "$1" in post) echo "$SINCE_ENGAGEMENT";; *) die "unknown migration target $1";; esac; }
migration_target_actors() {
  case "$1" in
    # The contract is storage-global; never materialize a database-sized
    # author list into a shell argument.
    post) echo '';;
    *) die "unknown migration target $1";;
  esac
}
migration_semantic_predicate() { # <version>; canonical v2<->v3 delta cohort
  case "$1" in
    "$V2") echo "content_time_validator_version='$V2' AND ((content_time_status='source_valid' AND content_time_utc>\"indexedAt\") OR (content_time_status='source_invalid' AND content_time_clamp_reason='future_skew'))";;
    "$V3") echo "content_time_validator_version='$V3' AND content_time_clamp_reason='future_skew_clamped'";;
    *) die "unsupported semantic cohort version $1";;
  esac
}
migration_cutoff_sql() { # <target> -> index-probe max FROM, min TO, exclusive cutoff
  local target=$1 table since from_predicate to_predicate cutoff_expr
  table=$(migration_target_table "$target"); since=$(migration_target_since "$target")
  from_predicate=$(migration_semantic_predicate "$FROM_VERSION"); to_predicate=$(migration_semantic_predicate "$TO_VERSION")
  cutoff_expr="coalesce(min_to,to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'))"
  [[ ! -s "$E/activation-floor.txt" ]] || cutoff_expr="'$(catalog_commit_floor)'"
  cat <<SQL
WITH bounds AS (
  SELECT
    (SELECT max("indexedAt") FROM public.$table WHERE $from_predicate AND "indexedAt">='$since') AS max_from,
    (SELECT min("indexedAt") FROM public.$table WHERE $to_predicate AND "indexedAt">='$since') AS min_to
)
SELECT 'max_from='||coalesce(max_from,'')||
  '|min_to='||coalesce(min_to,'')||
  '|cutoff='||$cutoff_expr
FROM bounds;
SQL
}
migration_cutoff_field() { local file=$1 key=$2; awk -F'|' -v k="$key" '{for(i=1;i<=NF;i++)if($i ~ ("^" k "=")){sub("^" k "=","",$i);print $i;exit}}' "$file"; }
migration_cutoff_file() { echo "$E/migrate-freeze-$1-cutoff-$2.txt"; }
migration_stable_cutoff_file() { echo "$E/migrate-freeze-$1-cutoff-$(migration_stable_attempt).txt"; }
migration_cutoff_value() { migration_cutoff_field "$(migration_stable_cutoff_file "$1")" cutoff; }
migration_derive_cutoff() { # <target> <attempt> -> evidence file
  local target=$1 attempt=$2 file raw max_from min_to cutoff
  file=$(migration_cutoff_file "$target" "$attempt")
  raw=$(psql_ro -t -c "$(migration_cutoff_sql "$target")")
  max_from=$(migration_cutoff_field <(printf '%s\n' "$raw") max_from)
  min_to=$(migration_cutoff_field <(printf '%s\n' "$raw") min_to)
  cutoff=$(migration_cutoff_field <(printf '%s\n' "$raw") cutoff)
  [[ "$max_from" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "$target has no valid in-horizon $FROM_VERSION max indexedAt"
  [[ "$cutoff" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "$target migration cutoff evidence is malformed"
  if [[ -n "$min_to" ]]; then
    [[ "$min_to" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "$target TO cohort boundary is malformed"
    if [[ -s "$E/activation-floor.txt" ]]; then [[ "$min_to" > "$cutoff" || "$min_to" == "$cutoff" ]] || die "$target native v3 semantic rows precede the catalog commit floor"
    else [[ "$max_from" < "$min_to" ]] || die "$target FROM/TO indexedAt cohorts overlap or have no strict millisecond gap; replan"; fi
  fi
  [[ "$max_from" < "$cutoff" ]] || die "$target FROM cohort is not strictly below exclusive cutoff; replan"
  { echo "target=$target"; echo "max_from=$max_from"; echo "min_to=$min_to"; echo "cutoff=$cutoff"; } | emit "$(basename "$file")"
}
assert_from_before_cutoff() { # <target> <cutoff>
  local target=$1 cutoff=$2 table since rows predicate
  table=$(migration_target_table "$target"); since=$(migration_target_since "$target")
  predicate=$(migration_semantic_predicate "$FROM_VERSION")
  rows=$(psql_ro -t -c "SELECT count(*) FROM public.$table WHERE $predicate AND \"indexedAt\">='$since' AND \"indexedAt\">='$cutoff';")
  [[ "$rows" == 0 ]] || die "$target gained $rows in-horizon $FROM_VERSION semantic-delta rows at/after exclusive cutoff"
}
activation_floor() {
  local floor; floor=$(awk -F= '$1=="activation_floor"{print $2}' "$E/activation-floor.txt" 2>/dev/null || true)
  [[ "$floor" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die 'activation floor receipt is missing or malformed'
  echo "$floor"
}
catalog_commit_floor() {
  local floor; floor=$(awk -F= '$1=="catalog_commit_floor"{print $2}' "$E/catalog-commit-floor.txt" 2>/dev/null || true)
  [[ "$floor" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die 'catalog commit floor receipt is missing or malformed'
  echo "$floor"
}
assert_no_late_v2_rows() { # <target>
  local target=$1 table floor rows
  assert_active_catalog_version "$TO_VERSION"
  table=$(migration_target_table "$target")
  floor=$(catalog_commit_floor)
  rows=$(psql_ro -t -c "SELECT count(*) FROM public.$table WHERE content_time_validator_version='$FROM_VERSION' AND \"indexedAt\">='$floor';")
  [[ "$rows" =~ ^[0-9]+$ ]] || die "$target late $FROM_VERSION query failed"
  [[ "$rows" == 0 ]] || die "$target has $rows $FROM_VERSION rows at/after catalog commit floor $floor"
}
migration_scope_sql() { # <target> [exclusive cutoff]
  local target=$1 cutoff=${2:-} table since upper=""
  table=$(migration_target_table "$target"); since=$(migration_target_since "$target")
  [[ -z "$cutoff" ]] || upper=" AND \"indexedAt\"<'$cutoff'"
  echo "SELECT 'from_in_horizon',count(*) FROM public.$table WHERE content_time_validator_version='$FROM_VERSION' AND \"indexedAt\">='$since'$upper UNION ALL SELECT 'to_in_horizon',count(*) FROM public.$table WHERE content_time_validator_version='$TO_VERSION' AND \"indexedAt\">='$since'$upper ORDER BY 1"
}
pgstat_table_read() { # wal_bytes|relation_bytes|dead|live|epoch
  psql_ro -t -c "SELECT (SELECT wal_bytes FROM pg_stat_wal), pg_total_relation_size('public.$1'), n_dead_tup, n_live_tup, extract(epoch FROM now())::bigint FROM pg_stat_user_tables WHERE relname='$1';"
}
assert_active_catalog_version() {
  local got
  got=$(psql_ro -t -c "SELECT count(*)||'|'||count(DISTINCT content_time_contract_version)||'|'||coalesce(min(content_time_contract_version),'') FROM feedgen_ops.feed_catalog WHERE enabled AND content_time_contract_version IS NOT NULL;")
  [[ "$got" == "$EXPECTED_CONTRACT_ROWS|1|$1" ]] || die "active catalog content-time contract is '$got' (rows|versions|value), expected '$EXPECTED_CONTRACT_ROWS|1|$1'"
}
migration_run_tool() { # <artifact> <target> <from> <to> [extra args...]
  local out=$1 target=$2 from=$3 to=$4 table since actors cutoff=''; shift 4
  table=$(migration_target_table "$target"); since=${MIGRATION_SINCE_OVERRIDE:-$(migration_target_since "$target")}; actors=$(migration_target_actors "$target")
  local -a scope=(); [[ -n "$actors" ]] && scope=(--actors "$actors"); [[ "$table" == post && -z "$actors" ]] && scope+=(--all-authors)
  local -a until=() native=(); if [[ -s "$E/migrate-stable-population.txt" && ${MIGRATION_NATIVE_V3_TAIL:-0} == 0 ]]; then cutoff=$(migration_cutoff_value "$target"); until=(--until "$cutoff"); fi
  [[ ${MIGRATION_NATIVE_V3_TAIL:-0} == 0 ]] || native=(--native-v3-tail)
  run_tool "$out" --mode revalidate --table "$table" --since "$since" "${until[@]}" --max-preview-rows "$MIGRATION_MAX_PREVIEW_ROWS" "${scope[@]}" "${native[@]}" --from-version "$from" --to-version "$to" --packet-sha256 "$PACKET_SHA" --json "$@"
}
prereg_spec_value() { # <spec> <key>
  local v; v=$(echo "$1" | tr ',' '\n' | awk -F= -v k="$2" '$1==k{print $2}')
  [[ "$v" =~ ^[0-9]+$ ]] || die "pre-registered cell $2 missing or non-numeric"
  echo "$v"
}
migration_prereg_spec() {
  if [[ -f "$E/migrate-stable-population.txt" ]]; then migration_cells "$(migration_stable_preview_file "$1")"; return; fi
  case "$1" in post) echo "$PREREG_POST";; *) die "unknown migration target $1";; esac
}
migration_stable_attempt() { awk -F= '$1=="attempt"{print $2}' "$E/migrate-stable-population.txt"; }
migration_scope_file() { echo "$E/migrate-freeze-$1-scope-$(migration_stable_attempt).tsv"; }
migration_stable_preview_file() { echo "$E/migrate-freeze-$1-preview-$(migration_stable_attempt).json"; }
migration_ir_preview_file() { echo "$E/migrate-freeze-ir-preview-$(migration_stable_attempt).json"; }
migration_ir_total_denominator() { # <cutoff>; all in-horizon IR v2 rows, not only semantic deltas
  local cutoff=$1
  psql_ro -t -c "SELECT count(*) FROM public.post WHERE author='$IR_DID' AND content_time_validator_version='$FROM_VERSION' AND \"indexedAt\">='$SINCE_MAIN' AND \"indexedAt\"<'$cutoff';"
}
assert_stable_population_bound() {
  local f target expected actual attempt
  [[ -s "$E/migrate-stable-population.txt" ]] || die 'stable population marker missing'
  attempt=$(migration_stable_attempt); [[ "$attempt" =~ ^[0-9]+$ ]] || die 'stable population attempt is invalid'
  grep -Fxq "drain_seconds=$MIGRATION_DRAIN_SECONDS" "$E/migrate-stable-population.txt" || die 'stable population drain binding differs'
  for target in $(migration_targets); do
    for f in "$(migration_scope_file "$target")" "$(migration_stable_preview_file "$target")" "$(migration_stable_cutoff_file "$target")"; do [[ -s "$f" ]] || die "stable population artifact missing: $(basename "$f")"; done
    grep -Fxq "${target}_cutoff=$(migration_cutoff_value "$target")" "$E/migrate-stable-population.txt" || die "$target stable cutoff marker differs"
    expected=$(awk -F= -v k="${target}_preview_sha256" '$1==k{print $2}' "$E/migrate-stable-population.txt"); actual=$(sha256sum "$(migration_stable_preview_file "$target")" | cut -d' ' -f1); [[ -n "$expected" && "$actual" == "$expected" ]] || die "$target stable preview hash differs"
  done
  f=$(migration_ir_preview_file); [[ -s "$f" ]] || die "stable population artifact missing: $(basename "$f")"
  expected=$(awk -F= '$1=="ir_prereg_sha256"{print $2}' "$E/migrate-stable-population.txt"); actual=$(migration_cells "$f" | sha256sum | cut -d' ' -f1); [[ -n "$expected" && "$actual" == "$expected" ]] || die 'IR stable prereg hash differs'
}
gate_migration_preview() { # <file> <spec> <label> [from] [to]
  local file=$1 spec=$2 label=$3 from=${4:-$FROM_VERSION} to=${5:-$TO_VERSION} outcomes key expected actual sum=0 scanned aux1 aux2
  [[ "$(jsonq "$file" preview.truncated)" == false ]] || die "$label preview truncated; raise the reviewed MIGRATION_MAX_PREVIEW_ROWS and rerun"
  scanned=$(jsonq "$file" preview.scanned)
  [[ "$scanned" =~ ^[0-9]+$ ]] || die "$label preview has missing/malformed preview.scanned"
  outcomes=$(migration_outcomes "$from" "$to")
  for key in $outcomes; do
    expected=$(prereg_spec_value "$spec" "$key"); actual=$(jsonq "$file" "preview.counts.$key")
    [[ "$actual" =~ ^[0-9]+$ ]] || die "$label preview has missing/malformed preview.counts.$key"
    [[ "$expected" == "$actual" ]] || die "$label/$key: preregistered=$expected observed=$actual"
    sum=$((sum + actual))
  done
  node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const allowed=new Set(process.argv[2].split(" "));for(const [k,v] of Object.entries(j.preview.counts||{})){if(k.startsWith("by_")||allowed.has(k))continue;if(Number(v)!==0){console.error(`${k}=${v}`);process.exit(2)}}' "$file" "$outcomes" || die "$label returned a non-zero outcome outside transition $FROM_VERSION->$TO_VERSION"
  # Auxiliary category keys partition primary outcomes, so the primary outcome
  # denominator excludes them.
  case "$from->$to" in
    "$V2->$V3")
      aux1=$(jsonq "$file" preview.counts.gt_5m_restored); aux2=$(jsonq "$file" preview.counts.zero_to_5m_clamped)
      [[ "$aux1" =~ ^[0-9]+$ && "$aux2" =~ ^[0-9]+$ ]] || die "$label preview has missing/malformed auxiliary counts"
      sum=$((sum - aux1 - aux2));;
    "$V3->$V2")
      aux1=$(jsonq "$file" preview.counts.gt_5m_invalidated); aux2=$(jsonq "$file" preview.counts.zero_to_5m_unclamped)
      [[ "$aux1" =~ ^[0-9]+$ && "$aux2" =~ ^[0-9]+$ ]] || die "$label preview has missing/malformed auxiliary counts"
      sum=$((sum - aux1 - aux2));;
  esac
  [[ "$scanned" == "$sum" ]] || die "$label denominator mismatch: scanned=$scanned primary_outcomes=$sum"
}
migration_preview_one() { # <artifact> <target> <from> <to> [actor override] [since override] [cutoff override]
  local out=$1 target=$2 from=$3 to=$4 actors=${5:-} since_override=${6:-} cutoff=${7:-} rc
  local -a until=(); [[ -z "$cutoff" ]] || until=(--until "$cutoff")
  if [[ -n "$actors" ]]; then
    local table since; table=$(migration_target_table "$target"); since=${since_override:-$(migration_target_since "$target")}
    rc=$(run_tool "$out" --mode revalidate --table "$table" --since "$since" "${until[@]}" --max-preview-rows "$MIGRATION_MAX_PREVIEW_ROWS" --actors "$actors" --from-version "$from" --to-version "$to" --packet-sha256 "$PACKET_SHA" --json)
  elif [[ -n "$cutoff" ]]; then rc=$(migration_run_tool "$out" "$target" "$from" "$to" --until "$cutoff")
  else rc=$(migration_run_tool "$out" "$target" "$from" "$to"); fi
  [[ "$rc" == 0 ]] || die "$out preview failed (exit=$rc)"
  echo "$E/$out.json"
}
migration_cells() { # <preview-json> [from] [to]
  local file=$1 from=${2:-$FROM_VERSION} to=${3:-$TO_VERSION} key val cells=""
  for key in $(migration_outcomes "$from" "$to"); do
    val=$(jsonq "$file" "preview.counts.$key")
    [[ "$val" =~ ^[0-9]+$ ]] || die "$(basename "$file") has missing/malformed preview.counts.$key"
    cells+="$key=$val,"
  done
  echo "${cells%,}"
}
cmd_migrate_prereg() {
  assert_migration_transition
  [[ -d "$E" ]] || sudo -n install -d -o root -g newsflows -m 750 "$E"
  local target f
  echo "SINCE_MAIN=$SINCE_MAIN SINCE_BE=$SINCE_BE SINCE_ENGAGEMENT=$SINCE_ENGAGEMENT"
  for target in $(migration_targets); do f=$(migration_preview_one "migrate-prereg-$target" "$target" "$FROM_VERSION" "$TO_VERSION"); echo "PREREG_${target^^}=$(migration_cells "$f")"; done
  f=$(migration_preview_one migrate-prereg-ir post "$FROM_VERSION" "$TO_VERSION" "$IR_DID" "$SINCE_MAIN")
  echo "PREREG_IR=$(migration_cells "$f")"
}
validate_migration_inputs() {
  assert_migration_transition
  local bound; for bound in "$SINCE_MAIN" "$SINCE_BE" "$SINCE_ENGAGEMENT"; do [[ "$bound" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die "migration bound must be ISO-8601 with milliseconds and Z: $bound"; done
  [[ "$MIGRATION_DRAIN_SECONDS" =~ ^[0-9]+$ ]] || die 'MIGRATION_DRAIN_SECONDS must be a non-negative integer'
  [[ ${FTFU1_TEST_MODE:-0} == 1 || $MIGRATION_DRAIN_SECONDS -ge 60 ]] || die 'MIGRATION_DRAIN_SECONDS must be at least 60 in production'
}
emit_migration_source_set() {
  { echo "generated_at=$(ts)"; echo "migration_transition=$FROM_VERSION->$TO_VERSION"; echo "since_main=$SINCE_MAIN"; echo "since_be=$SINCE_BE"; echo "since_engagement=$SINCE_ENGAGEMENT"; echo "migration_drain_seconds=$MIGRATION_DRAIN_SECONDS"; echo "source_sha=$EXPECTED_SHA"; echo "packet_sha256=$PACKET_SHA"; echo "expected_tool_refs=$EXPECTED_TOOL_REFS"; echo "ir_did_sha256=$(printf %s "$IR_DID" | sha256sum | cut -d' ' -f1)"; } | emit migrate-source-set.txt
}
cmd_migrate_prepare() {
  validate_migration_inputs
  [[ -d "$E" ]] || sudo -n install -d -o root -g newsflows -m 750 "$E"
  assert_tree; assert_active_catalog_version "$FROM_VERSION"
  emit_migration_source_set
  take_control pg-control-1.txt
  log 'migration preparation complete; exact population intentionally deferred until the v3 catalog switch'
}
cmd_migrate_preflight() {
  validate_migration_inputs
  [[ -d "$E" ]] || sudo -n install -d -o root -g newsflows -m 750 "$E"
  assert_tree; assert_active_catalog_version "$FROM_VERSION"
  local target table since f
  emit_migration_source_set
  for target in $(migration_targets); do
    table=$(migration_target_table "$target"); since=$(migration_target_since "$target")
    psql_ro -c "$(migration_scope_sql "$target")" | emit "migrate-$target-prestate-scope.tsv"
    f=$(migration_preview_one "migrate-$target-preview-preflight" "$target" "$FROM_VERSION" "$TO_VERSION")
    gate_migration_preview "$f" "$(migration_prereg_spec "$target")" "$target"
  done
  f=$(migration_preview_one migrate-ir-preview-preflight post "$FROM_VERSION" "$TO_VERSION" "$IR_DID" "$SINCE_MAIN")
  gate_migration_preview "$f" "$PREREG_IR" ir
  take_control pg-control-1.txt
  log 'semantic post migration preflight complete; engagement remains projection-only'
}
cmd_migrate_freeze() {
  validate_migration_inputs
  assert_tree; assert_active_catalog_version "$TO_VERSION"
  [[ -f "$E/migrate-source-set.txt" && -n "$(latest_control)" ]] || die 'migrate-prepare receipts missing'
  [[ ! -f "$E/migrate-stable-population.txt" ]] || { assert_stable_population_bound; return; }
  local attempt=1 target table since f rows spec cutoff ir_changed ir_restored ir_total
  while [[ -e "$E/migrate-freeze-post-cutoff-$attempt.txt" ]]; do attempt=$((attempt+1)); done
  sleep "$MIGRATION_DRAIN_SECONDS"
  for target in $(migration_targets); do
    migration_derive_cutoff "$target" "$attempt"
  done
  for target in $(migration_targets); do
    cutoff=$(migration_cutoff_field "$(migration_cutoff_file "$target" "$attempt")" cutoff)
    f=$(migration_preview_one "migrate-freeze-$target-preview-$attempt" "$target" "$FROM_VERSION" "$TO_VERSION" "" "" "$cutoff")
    spec=$(migration_cells "$f"); gate_migration_preview "$f" "$spec" "$target"
    rows=$(jsonq "$f" preview.scanned); [[ "$rows" =~ ^[0-9]+$ && "$rows" -gt 0 ]] || die "$target stable bounded preview has no denominator"
  done
  cutoff=$(migration_cutoff_field "$(migration_cutoff_file post "$attempt")" cutoff)
  f=$(migration_preview_one "migrate-freeze-ir-preview-$attempt" post "$FROM_VERSION" "$TO_VERSION" "$IR_DID" "$SINCE_MAIN" "$cutoff")
  spec=$(migration_cells "$f"); gate_migration_preview "$f" "$spec" ir
  for target in $(migration_targets); do
    table=$(migration_target_table "$target"); since=$(migration_target_since "$target")
    cutoff=$(migration_cutoff_field "$(migration_cutoff_file "$target" "$attempt")" cutoff); f="$E/migrate-freeze-$target-preview-$attempt.json"
    psql_ro -c "$(migration_scope_sql "$target" "$cutoff")" | emit "migrate-freeze-$target-scope-$attempt.tsv" || die "$target scope snapshot failed"
    [[ -s "$E/migrate-freeze-$target-scope-$attempt.tsv" ]] || die "$target scope snapshot is empty"
  done
  for target in $(migration_targets); do
    cutoff=$(migration_cutoff_field "$(migration_cutoff_file "$target" "$attempt")" cutoff)
    assert_from_before_cutoff "$target" "$cutoff"
  done
  f="$E/migrate-freeze-ir-preview-$attempt.json"
  ir_changed=$(jsonq "$f" preview.scanned); ir_restored=$(jsonq "$f" preview.counts.gt_5m_restored); ir_total=$(migration_ir_total_denominator "$cutoff")
  [[ "$ir_changed" =~ ^[0-9]+$ && "$ir_restored" =~ ^[0-9]+$ && "$ir_total" =~ ^[0-9]+$ ]] || die 'IR migration metrics are malformed'
  (( ir_restored <= ir_changed && ir_changed <= ir_total )) || die 'IR migration metrics violate restored <= semantic-changed <= total denominator'
  { echo "attempt=$attempt"; echo "drain_seconds=$MIGRATION_DRAIN_SECONDS"; echo "max_preview_rows=$MIGRATION_MAX_PREVIEW_ROWS"; for target in $(migration_targets); do cutoff=$(migration_cutoff_field "$(migration_cutoff_file "$target" "$attempt")" cutoff); echo "${target}_cutoff=$cutoff"; echo "${target}_rows=$(jsonq "$E/migrate-freeze-$target-preview-$attempt.json" preview.scanned)"; echo "${target}_preview_sha256=$(sha256sum "$E/migrate-freeze-$target-preview-$attempt.json" | cut -d' ' -f1)"; done; echo "ir_prereg_sha256=$(migration_cells "$f" | sha256sum | cut -d' ' -f1)"; echo "ir_semantic_changed=$ir_changed"; echo "ir_restored_valid=$ir_restored"; echo "ir_total_denominator=$ir_total"; } | emit migrate-stable-population.txt
  assert_stable_population_bound
  log "stable post-switch v2 population bound (attempt $attempt)"
}
cmd_migrate_stable_check() { validate_migration_inputs; assert_tree; assert_active_catalog_version "$TO_VERSION"; assert_stable_population_bound; }
cmd_migrate_preview() {
  local target f
  for target in $(migration_targets); do
    f=$(migration_preview_one "migrate-$target-preview" "$target" "$FROM_VERSION" "$TO_VERSION")
    gate_migration_preview "$f" "$(migration_prereg_spec "$target")" "$target"
  done
}
migration_apply_one() { # <target> <label> <from> <to> <checkpoint-prefix> [max-batches]
  local target=$1 label=$2 from=$3 to=$4 prefix=$5 maxb=${6:-} table before after rc f nb wps _rps _dps verdict=ok
  table=$(migration_target_table "$target")
  before=$(pgstat_table_read "$table")
  read -r wps _rps _dps <<<"$(control_rates)"; (( wps >= 1 )) || die 'control WAL rate is zero; take another control read'
  local checkpoint="/evidence/$prefix-$target-checkpoint.json"; [[ "$RUNNER" == host ]] && checkpoint="$E/$prefix-$target-checkpoint.json"
  local -a extra=(--apply --checkpoint-file "$checkpoint" --pause-baseline-bytes-per-s "$wps")
  [[ -n "$maxb" ]] && extra+=(--max-batches "$maxb")
  rc=$(migration_run_tool "$prefix-$target-apply-$label" "$target" "$from" "$to" "${extra[@]}")
  after=$(pgstat_table_read "$table"); f="$E/$prefix-$target-apply-$label.json"
  [[ "$rc" == 0 || "$rc" == 3 ]] || die "$target apply failed (exit=$rc)"
  [[ "$(jsonq "$f" table)" == "$table" ]] || die "$table apply receipt has wrong table"
  nb=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));console.log((j.revalidation?.batches||[]).length)' "$f")
  local assessment; assessment=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const [r,m,floor]=process.argv.slice(2).map(Number);let fail=0,lines=[];for(const b of j.revalidation?.batches||[]){const ms=b.elapsed_ms||0,paid=b.pause_ms||0,owed=b.candidates===0?0:Math.max(1000,Math.ceil(b.wal_bytes*1000/r)),ceil=Math.max(floor,Math.round(m*r*(ms+paid)/1000)),ok=paid>=owed&&b.wal_bytes<=ceil;if(!ok)fail++;lines.push(`batch=${b.batch} wal=${b.wal_bytes} elapsed_ms=${ms} pause_owed_ms=${owed} pause_paid_ms=${paid} ceiling=${ceil} verdict=${ok?"ok":"BREACH"}`)}console.log(JSON.stringify({fail,lines}))' "$f" "$wps" "$CEIL_WAL_BASELINE_MULTIPLE" "$CEIL_WAL_FLOOR_BYTES")
  [[ "$(node -e 'console.log(JSON.parse(process.argv[1]).fail)' "$assessment")" == 0 ]] || verdict=BREACH
  { echo "target=$target table=$table transition=$from->$to batches=$nb exit=$rc updated=$(jsonq "$f" revalidation.updated)"; echo "pg_before=$before"; echo "pg_after=$after"; node -e 'console.log(JSON.parse(process.argv[1]).lines.join("\n"))' "$assessment"; echo "verdict=$verdict"; } | emit "$prefix-$target-ceiling-$label.txt"
  [[ "$verdict" == ok ]] || die "$target WAL ceiling breached"
}
migration_applied_value() { # <target> <json path>
  local target=$1 path=$2 f sum=0 value
  for f in "$E"/migrate-"$target"-apply-*.json; do
    [[ -e "$f" ]] || continue
    value=$(jsonq "$f" "$path")
    [[ "$value" =~ ^[0-9]+$ ]] || die "$target forward apply receipt $(basename "$f") has missing/malformed $path"
    sum=$((sum+value))
  done
  echo "$sum"
}
migration_receipt_value() { # <prefix> <target> <json path>
  local prefix=$1 target=$2 path=$3 f sum=0 value
  for f in "$E"/"$prefix"-"$target"-apply-*.json; do
    [[ -e "$f" ]] || continue
    value=$(jsonq "$f" "$path"); [[ "$value" =~ ^[0-9]+$ ]] || value=0; sum=$((sum+value))
  done
  echo "$sum"
}
assert_no_cas_conflicts() { # <prefix> <target>
  local prefix=$1 target=$2 f skipped
  for f in "$E"/"$prefix"-"$target"-apply-*.json; do
    [[ -e "$f" ]] || continue
    skipped=$(jsonq "$f" revalidation.skipped_cas)
    [[ "$skipped" =~ ^[0-9]+$ ]] || die "$target $(basename "$f") has missing/malformed revalidation.skipped_cas"
    (( skipped == 0 )) || die "$target $(basename "$f") recorded skipped_cas=$skipped; start a fresh evidence root"
  done
}
migration_converged_value() { # <prefix> <target> <field>  OR  <target> <field>
  local prefix=migrate target field
  if [[ $# -ge 3 ]]; then prefix=$1; target=$2; field=$3; else target=$1; field=$2; fi
  local f value sum=0
  for f in "$E"/"$prefix"-"$target"-convergence-*.txt; do
    [[ -e "$f" ]] || continue
    value=$(awk -F= -v k="$field" '$1==k{print $2}' "$f")
    [[ "$value" =~ ^[0-9]+$ ]] || die "$target $prefix convergence receipt has malformed $field"
    sum=$((sum+value))
  done
  echo "$sum"
}
migration_remaining_spec() {
  local target=$1 full key total applied converged cells=""
  full=$(migration_prereg_spec "$target")
  for key in $(migration_outcomes "$FROM_VERSION" "$TO_VERSION"); do
    total=$(prereg_spec_value "$full" "$key"); applied=$(migration_applied_value "$target" "revalidation.counts.$key")
    converged=$(migration_converged_value migrate "$target" "${key}_producer_converged")
    (( applied + converged <= total )) || die "$target prior receipts exceed preregistered $key"
    cells+="$key=$((total-applied-converged)),"
  done
  echo "${cells%,}"
}
migration_remaining_rows() {
  local target=$1 total applied converged
  total=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/migrate-stable-population.txt"); applied=$(migration_applied_value "$target" revalidation.updated)
  converged=$(migration_converged_value migrate "$target" producer_converged_rows)
  [[ "$total" =~ ^[0-9]+$ ]] || die "$target stable denominator missing or malformed"
  (( applied + converged <= total )) || die "$target prior receipts exceed stable denominator"
  echo $((total-applied-converged))
}
forward_convergence_receipt() { # <target> <expected spec> <observed spec> <expected rows> <observed rows>
  local target=$1 expected_spec=$2 observed_spec=$3 expected_rows=$4 observed_rows=$5 key expected observed delta sum=0 auxiliary=0 row_delta receipt
  [[ "$expected_rows" =~ ^[0-9]+$ && "$observed_rows" =~ ^[0-9]+$ ]] || die "$target forward convergence rows missing or malformed"
  (( observed_rows <= expected_rows )) || die "$target forward population increased after v3 catalog activation"
  row_delta=$((expected_rows-observed_rows)); receipt="producer_converged_rows=$row_delta"$'\n'
  for key in $(migration_outcomes "$FROM_VERSION" "$TO_VERSION"); do
    expected=$(prereg_spec_value "$expected_spec" "$key"); observed=$(prereg_spec_value "$observed_spec" "$key")
    [[ "$expected" =~ ^[0-9]+$ && "$observed" =~ ^[0-9]+$ ]] || die "$target forward $key missing or malformed"
    (( observed <= expected )) || die "$target forward $key increased after v3 catalog activation"
    delta=$((expected-observed)); sum=$((sum+delta)); receipt+="${key}_producer_converged=$delta"$'\n'
    [[ $key == gt_5m_restored || $key == zero_to_5m_clamped ]] && auxiliary=$((auxiliary+delta))
  done
  (( sum - auxiliary == row_delta )) || die "$target forward convergence primary cells do not sum to row delta"
  printf '%s' "$receipt"
}
emit_forward_convergence() { # <target> <label> <expected spec> <observed spec> <expected rows> <observed rows>
  local target=$1 label=$2 receipt
  receipt=$(forward_convergence_receipt "$target" "$3" "$4" "$5" "$6") || return $?
  printf '%s\n' "$receipt" | emit "migrate-$target-convergence-$label.txt"
}
rollback_remaining_spec() {
  local target=$1 full key total applied cells=""
  full=$(migration_cells "$E/rollback-$target-preview.json" "$TO_VERSION" "$FROM_VERSION")
  for key in $(migration_outcomes "$TO_VERSION" "$FROM_VERSION"); do
    total=$(prereg_spec_value "$full" "$key"); applied=$(migration_receipt_value rollback "$target" "revalidation.counts.$key")
    (( applied <= total )) || die "$target prior rollback receipts exceed dry-run $key"
    cells+="$key=$((total-applied)),"
  done
  echo "${cells%,}"
}
rollback_remaining_rows() {
  local target=$1 total applied
  total=$(jsonq "$E/rollback-$target-preview.json" preview.scanned); applied=$(migration_receipt_value rollback "$target" revalidation.updated)
  [[ "$total" =~ ^[0-9]+$ ]] || die "$target rollback denominator missing"
  (( applied <= total )) || die "$target prior rollback receipts exceed dry-run denominator"
  echo $((total-applied))
}
normalization_marker() { echo "$E/migrate-normalize-overlap.txt"; }
normalization_spec() { migration_cells "$E/migrate-normalize-overlap-preview.json" "$TO_VERSION" "$FROM_VERSION"; }
normalization_remaining_spec() {
  local key total applied cells=""
  for key in $(migration_outcomes "$TO_VERSION" "$FROM_VERSION"); do
    total=$(prereg_spec_value "$(normalization_spec)" "$key")
    applied=$(migration_receipt_value normalize post "revalidation.counts.$key")
    (( applied <= total )) || die "normalization receipts exceed bound $key"
    cells+="$key=$((total-applied)),"
  done
  echo "${cells%,}"
}
normalization_remaining_rows() {
  local total applied
  total=$(awk -F= '$1=="rows"{print $2}' "$(normalization_marker)")
  applied=$(migration_receipt_value normalize post revalidation.updated)
  [[ "$total" =~ ^[0-9]+$ ]] || die 'normalization denominator missing'
  (( applied <= total )) || die 'normalization receipts exceed bound denominator'
  echo $((total-applied))
}
cmd_migrate_normalize_overlap() { # <unique label> [max-batches]
  local label=${1:?label} maxb=${2:-} f rows updated skipped key expected actual
  [[ ${ALLOW_FTFU1_OVERLAP_NORMALIZATION:-0} == 1 ]] || die 'set ALLOW_FTFU1_OVERLAP_NORMALIZATION=1 for the explicit continuation path'
  [[ "$label" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || die 'normalization label is unsafe'
  validate_migration_inputs; assert_tree; assert_active_catalog_version "$FROM_VERSION"
  [[ ! -e "$E/migrate-stable-population.txt" ]] || die 'stable forward population already exists; overlap normalization is no longer applicable'
  if [[ ! -e "$(normalization_marker)" ]]; then
    sleep "$MIGRATION_DRAIN_SECONDS"
    f=$(migration_preview_one migrate-normalize-overlap-preview post "$TO_VERSION" "$FROM_VERSION")
    gate_migration_preview "$f" "$(migration_cells "$f" "$TO_VERSION" "$FROM_VERSION")" 'overlap normalization' "$TO_VERSION" "$FROM_VERSION"
    [[ $(jsonq "$f" preview.counts.v3_valid_to_v2_valid) == 0 && $(jsonq "$f" preview.counts.v3_to_v2_invalid) == 0 ]] || die 'normalization preview escaped future_skew_clamped rows'
    rows=$(jsonq "$f" preview.scanned); [[ "$rows" =~ ^[0-9]+$ ]] || die 'normalization denominator is malformed'
    { echo "transition=$TO_VERSION->$FROM_VERSION"; echo "drain_seconds=$MIGRATION_DRAIN_SECONDS"; echo "rows=$rows"; echo "preview_sha256=$(sha256sum "$f" | cut -d' ' -f1)"; } | emit "$(basename "$(normalization_marker)")"
  fi
  [[ $(awk -F= '$1=="drain_seconds"{print $2}' "$(normalization_marker)") == "$MIGRATION_DRAIN_SECONDS" ]] || die 'normalization drain binding differs'
  [[ $(awk -F= '$1=="preview_sha256"{print $2}' "$(normalization_marker)") == "$(sha256sum "$E/migrate-normalize-overlap-preview.json" | cut -d' ' -f1)" ]] || die 'normalization preview hash differs'
  rows=$(normalization_remaining_rows post)
  f=$(migration_preview_one "migrate-normalize-overlap-preview-before-$label" post "$TO_VERSION" "$FROM_VERSION")
  gate_migration_preview "$f" "$(normalization_remaining_spec)" 'overlap normalization remaining' "$TO_VERSION" "$FROM_VERSION"
  [[ $(jsonq "$f" preview.scanned) == "$rows" ]] || die 'normalization remaining population differs from bound denominator minus receipts'
  if (( rows > 0 )); then
    migration_apply_one post "$label" "$TO_VERSION" "$FROM_VERSION" normalize "$maxb"
    [[ $(jsonq "$E/normalize-post-apply-$label.json" revalidation.complete) == true ]] || return 3
  fi
  f=$(migration_preview_one "migrate-normalize-overlap-preview-after-$label" post "$TO_VERSION" "$FROM_VERSION")
  [[ $(jsonq "$f" preview.truncated) == false && $(jsonq "$f" preview.scanned) == 0 ]] || die 'overlap normalization is incomplete'
  rows=$(awk -F= '$1=="rows"{print $2}' "$(normalization_marker)")
  updated=$(migration_receipt_value normalize post revalidation.updated); [[ "$updated" == "$rows" ]] || die "normalization updated=$updated but bound denominator=$rows"
  skipped=$(migration_receipt_value normalize post revalidation.skipped_cas); [[ "$skipped" == 0 ]] || die "normalization recorded skipped_cas=$skipped"
  for key in $(migration_outcomes "$TO_VERSION" "$FROM_VERSION"); do
    expected=$(prereg_spec_value "$(normalization_spec)" "$key"); actual=$(migration_receipt_value normalize post "revalidation.counts.$key")
    [[ "$actual" == "$expected" ]] || die "normalization realized $key=$actual but bound=$expected"
  done
  { echo "normalized_rows=$rows"; echo 'bounded_v3_future_skew_clamped_residual=0'; echo 'skipped_cas=0'; } | emit migrate-normalize-overlap-readback.txt
}
native_tail_targets() { echo 'post'; }
native_tail_preview() { # <artifact> <target>
  local out=$1 target=$2 floor rc
  floor=$(activation_floor)
  rc=$(MIGRATION_NATIVE_V3_TAIL=1 MIGRATION_SINCE_OVERRIDE="$floor" migration_run_tool "$out" "$target" "$TO_VERSION" "$FROM_VERSION")
  [[ "$rc" == 0 ]] || die "$target native-tail preview failed (exit=$rc)"
  echo "$E/$out.json"
}
native_tail_spec() { migration_cells "$E/native-tail-$1-preview.json" "$TO_VERSION" "$FROM_VERSION"; }
native_tail_converged_value() { migration_converged_value native-tail "$1" "$2"; }
native_tail_remaining_spec() {
  local target=$1 key total applied converged cells=""
  for key in $(migration_outcomes "$TO_VERSION" "$FROM_VERSION"); do
    total=$(prereg_spec_value "$(native_tail_spec "$target")" "$key"); applied=$(migration_receipt_value native "$target" "revalidation.counts.$key")
    converged=$(native_tail_converged_value "$target" "${key}_producer_converged")
    (( applied + converged <= total )) || die "$target native-tail receipts exceed bound $key"
    cells+="$key=$((total-applied-converged)),"
  done
  echo "${cells%,}"
}
native_tail_remaining_rows() {
  local target=$1 total applied converged
  total=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/native-tail-stable.txt"); applied=$(migration_receipt_value native "$target" revalidation.updated)
  converged=$(native_tail_converged_value "$target" producer_converged_rows)
  [[ "$total" =~ ^[0-9]+$ ]] || die "$target native-tail denominator missing"
  (( applied + converged <= total )) || die "$target native-tail receipts exceed bound denominator"
  echo $((total-applied-converged))
}
native_tail_convergence_receipt() { # <target> <expected spec> <observed spec> <expected rows> <observed rows>
  local target=$1 expected_spec=$2 observed_spec=$3 expected_rows=$4 observed_rows=$5 key expected observed delta sum=0 auxiliary=0 row_delta receipt
  (( observed_rows <= expected_rows )) || die "$target native-tail population increased after v2 drain"
  row_delta=$((expected_rows-observed_rows)); receipt="producer_converged_rows=$row_delta"$'\n'
  for key in $(migration_outcomes "$TO_VERSION" "$FROM_VERSION"); do
    expected=$(prereg_spec_value "$expected_spec" "$key"); observed=$(prereg_spec_value "$observed_spec" "$key")
    (( observed <= expected )) || die "$target native-tail $key increased after v2 drain"
    delta=$((expected-observed)); sum=$((sum+delta)); receipt+="${key}_producer_converged=$delta"$'\n'
    [[ $key == gt_5m_invalidated || $key == zero_to_5m_unclamped ]] && auxiliary=$((auxiliary+delta))
  done
  (( sum - auxiliary == row_delta )) || die "$target native-tail convergence primary cells do not sum to row delta"
  printf '%s' "$receipt"
}
emit_native_tail_convergence() { # <target> <label> <expected spec> <observed spec> <expected rows> <observed rows>
  local target=$1 label=$2 receipt
  receipt=$(native_tail_convergence_receipt "$target" "$3" "$4" "$5" "$6") || return $?
  printf '%s\n' "$receipt" | emit "native-tail-$target-convergence-$label.txt"
}
emit_native_tail_plans() {
  local floor=$1 target table order
  for target in $(native_tail_targets); do
    table=$(migration_target_table "$target"); [[ $target == post ]] && order='author,uri' || order='uri'
    psql_ro -c "EXPLAIN SELECT uri FROM public.$table WHERE \"indexedAt\">='$floor' AND content_time_validator_version='$TO_VERSION' ORDER BY $order LIMIT 500;" | emit "native-tail-$target-plan.txt"
    ! grep -Fq 'Seq Scan' "$E/native-tail-$target-plan.txt" || die "$target native-tail plan is a broad sequential scan"
    grep -F 'Index Cond:' "$E/native-tail-$target-plan.txt" | grep -Fq 'indexedAt' || die "$target native-tail plan does not index-bound activation_floor"
  done
}
cmd_migrate_native_tail_plan() {
  local floor
  validate_migration_inputs; assert_tree; assert_active_catalog_version "$TO_VERSION"; floor=$(activation_floor)
  emit_native_tail_plans "$floor"
}
cmd_migrate_native_tail_recovery_bind() {
  local source_e source_set floor_file floor source_sha packet_sha
  : "${RECOVERY_SOURCE_E:?RECOVERY_SOURCE_E is required}"
  : "${RECOVERY_SOURCE_SET_SHA256:?RECOVERY_SOURCE_SET_SHA256 is required}"
  : "${RECOVERY_ACTIVATION_FLOOR_SHA256:?RECOVERY_ACTIVATION_FLOOR_SHA256 is required}"
  [[ "$RECOVERY_SOURCE_SET_SHA256" =~ ^[0-9a-f]{64}$ && "$RECOVERY_ACTIVATION_FLOOR_SHA256" =~ ^[0-9a-f]{64}$ ]] || die 'recovery source hashes must be 64 lowercase hex'
  validate_migration_inputs; assert_tree; assert_active_catalog_version "$FROM_VERSION"
  [[ "$RECOVERY_SOURCE_E" == /* && -d "$RECOVERY_SOURCE_E" ]] || die 'recovery source evidence root is not an absolute directory'
  source_e=$(cd -- "$RECOVERY_SOURCE_E" && pwd -P)
  source_set="$source_e/migrate-source-set.txt"; floor_file="$source_e/activation-floor.txt"
  [[ -f "$source_set" && ! -L "$source_set" && -f "$floor_file" && ! -L "$floor_file" ]] || die 'recovery source facts are missing or symlinked'
  [[ $(sha256sum "$source_set" | cut -d' ' -f1) == "$RECOVERY_SOURCE_SET_SHA256" ]] || die 'recovery source-set hash differs'
  [[ $(sha256sum "$floor_file" | cut -d' ' -f1) == "$RECOVERY_ACTIVATION_FLOOR_SHA256" ]] || die 'recovery activation-floor hash differs'
  grep -Fxq "migration_transition=$FROM_VERSION->$TO_VERSION" "$source_set" || die 'recovery source transition differs'
  floor=$(awk -F= '$1=="activation_floor"{print $2}' "$floor_file")
  [[ "$floor" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$ ]] || die 'recovery activation floor is malformed'
  source_sha=$(awk -F= '$1=="source_sha"{print $2}' "$source_set"); packet_sha=$(awk -F= '$1=="packet_sha256"{print $2}' "$source_set")
  [[ "$source_sha" =~ ^[0-9a-f]{40}$ && "$packet_sha" =~ ^[0-9a-f]{64}$ ]] || die 'recovery source identity is malformed'
  printf 'activation_floor=%s\n' "$floor" | emit activation-floor.txt
  { echo "bound_at=$(ts)"; echo "source_evidence_root=$source_e"; echo "source_set_sha256=$RECOVERY_SOURCE_SET_SHA256"; echo "activation_floor_sha256=$RECOVERY_ACTIVATION_FLOOR_SHA256"; echo "source_sha=$source_sha"; echo "source_packet_sha256=$packet_sha"; echo "recovery_sha=$EXPECTED_SHA"; echo "recovery_packet_sha256=$PACKET_SHA"; echo "catalog_version=$FROM_VERSION"; } | emit native-tail-recovery-binding.txt
  emit_native_tail_plans "$floor"
}
cmd_migrate_native_tail_rollback() { # <unique label> [max-batches]
  local label=${1:?label} maxb=${2:-} floor target f rows observed_rows updated converged skipped key expected actual expected_spec observed_spec candidate candidate_rows candidate_cell
  [[ "$label" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || die 'native-tail label is unsafe'
  validate_migration_inputs; assert_tree; assert_active_catalog_version "$FROM_VERSION"; floor=$(activation_floor)
  if [[ ! -e "$E/native-tail-stable.txt" ]]; then
    sleep "$MIGRATION_DRAIN_SECONDS"
    for target in $(native_tail_targets); do
      [[ -s "$E/native-tail-$target-plan.txt" ]] || die "$target native-tail activation-bound plan receipt is missing"
      f=$(native_tail_preview "native-tail-$target-preview" "$target")
      gate_migration_preview "$f" "$(migration_cells "$f" "$TO_VERSION" "$FROM_VERSION")" "$target native tail" "$TO_VERSION" "$FROM_VERSION"
    done
    { echo "activation_floor=$floor"; echo "drain_seconds=$MIGRATION_DRAIN_SECONDS"; for target in $(native_tail_targets); do echo "${target}_rows=$(jsonq "$E/native-tail-$target-preview.json" preview.scanned)"; echo "${target}_preview_sha256=$(sha256sum "$E/native-tail-$target-preview.json" | cut -d' ' -f1)"; done; } | emit native-tail-stable.txt
  fi
  grep -Fxq "activation_floor=$floor" "$E/native-tail-stable.txt" || die 'native-tail floor binding differs'
  grep -Fxq "drain_seconds=$MIGRATION_DRAIN_SECONDS" "$E/native-tail-stable.txt" || die 'native-tail drain binding differs'
  for target in $(native_tail_targets); do
    assert_no_cas_conflicts native "$target"
    [[ $(awk -F= -v k="${target}_preview_sha256" '$1==k{print $2}' "$E/native-tail-stable.txt") == "$(sha256sum "$E/native-tail-$target-preview.json" | cut -d' ' -f1)" ]] || die "$target native-tail preview hash differs"
    rows=$(native_tail_remaining_rows "$target"); expected_spec=$(native_tail_remaining_spec "$target")
    f=$(native_tail_preview "native-tail-$target-preview-before-$label" "$target")
    observed_rows=$(jsonq "$f" preview.scanned); observed_spec=$(migration_cells "$f" "$TO_VERSION" "$FROM_VERSION")
    gate_migration_preview "$f" "$observed_spec" "$target native-tail observed population" "$TO_VERSION" "$FROM_VERSION"
    assert_active_catalog_version "$FROM_VERSION"
    emit_native_tail_convergence "$target" "$label" "$expected_spec" "$observed_spec" "$rows" "$observed_rows"
    gate_migration_preview "$f" "$(native_tail_remaining_spec "$target")" "$target native-tail remaining" "$TO_VERSION" "$FROM_VERSION"
    rows=$(native_tail_remaining_rows "$target")
    [[ $(jsonq "$f" preview.scanned) == "$rows" ]] || die "$target native-tail population differs from bound denominator minus receipts"
    if (( rows > 0 )); then
      MIGRATION_NATIVE_V3_TAIL=1 MIGRATION_SINCE_OVERRIDE="$floor" migration_apply_one "$target" "$label" "$TO_VERSION" "$FROM_VERSION" native "$maxb"
      [[ $(jsonq "$E/native-$target-apply-$label.json" revalidation.complete) == true ]] || return 3
      assert_no_cas_conflicts native "$target"
    fi
  done
  for target in $(native_tail_targets); do
    f=$(native_tail_preview "native-tail-$target-preview-after-$label" "$target")
    observed_rows=$(jsonq "$f" preview.scanned); observed_spec=$(migration_cells "$f" "$TO_VERSION" "$FROM_VERSION")
    gate_migration_preview "$f" "$observed_spec" "$target native-tail final observed population" "$TO_VERSION" "$FROM_VERSION"
    [[ "$observed_rows" == 0 ]] || die "$target native-tail rollback is incomplete"
    expected_spec=$(native_tail_remaining_spec "$target")
    candidate=$(native_tail_convergence_receipt "$target" "$expected_spec" "$observed_spec" "$(native_tail_remaining_rows "$target")" "$observed_rows") || return $?
    candidate_rows=$(awk -F= '$1=="producer_converged_rows"{print $2}' <<<"$candidate")
    rows=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/native-tail-stable.txt"); updated=$(migration_receipt_value native "$target" revalidation.updated); converged=$(native_tail_converged_value "$target" producer_converged_rows); [[ $((updated+converged+candidate_rows)) == "$rows" ]] || die "$target native-tail updated=$updated + producer_converged=$converged + pending_converged=$candidate_rows but bound=$rows"
    skipped=$(migration_receipt_value native "$target" revalidation.skipped_cas); [[ "$skipped" == 0 ]] || die "$target native-tail skipped_cas=$skipped"
    for key in $(migration_outcomes "$TO_VERSION" "$FROM_VERSION"); do expected=$(prereg_spec_value "$(native_tail_spec "$target")" "$key"); actual=$(migration_receipt_value native "$target" "revalidation.counts.$key"); converged=$(native_tail_converged_value "$target" "${key}_producer_converged"); candidate_cell=$(awk -F= -v k="${key}_producer_converged" '$1==k{print $2}' <<<"$candidate"); [[ $((actual+converged+candidate_cell)) == "$expected" ]] || die "$target native-tail $key updated=$actual + producer_converged=$converged + pending_converged=$candidate_cell but bound=$expected"; done
    assert_active_catalog_version "$FROM_VERSION"
    if grep -Eq '=[1-9][0-9]*$' <<<"$candidate"; then printf '%s\n' "$candidate" | emit "native-tail-$target-convergence-$label-postapply.txt"; fi
  done
  { echo "activation_floor=$floor"; for target in $(native_tail_targets); do echo "$target updated_rows=$(migration_receipt_value native "$target" revalidation.updated) producer_converged_rows=$(native_tail_converged_value "$target" producer_converged_rows) bound_rows=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/native-tail-stable.txt") residual=0 skipped_cas=0"; done; echo 'historical_v3_before_floor_out_of_scope=true'; } | emit native-tail-readback.txt
}
cmd_migrate_apply() {
  local label=${1:?label} maxb=${2:-} target f rows spec cutoff attempt=1
  local expected_rows expected_spec observed_rows observed_spec
  [[ "$label" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || die 'forward migration label is unsafe'
  assert_tree; assert_active_catalog_version "$TO_VERSION"; assert_stable_population_bound
  if [[ ! -f "$E/migrate-apply-authorized.txt" ]]; then
    while [[ -e "$E/migrate-authorize-post-$attempt.json" ]]; do attempt=$((attempt+1)); done
    for target in $(migration_targets); do
      assert_no_late_v2_rows "$target"
      cutoff=$(migration_cutoff_value "$target")
      assert_from_before_cutoff "$target" "$cutoff"
      expected_rows=$(migration_remaining_rows "$target"); expected_spec=$(migration_remaining_spec "$target")
      f=$(migration_preview_one "migrate-authorize-$target-$attempt" "$target" "$FROM_VERSION" "$TO_VERSION")
      observed_rows=$(jsonq "$f" preview.scanned); observed_spec=$(migration_cells "$f" "$FROM_VERSION" "$TO_VERSION")
      [[ "$observed_rows" =~ ^[0-9]+$ ]] || die "$target preview scanned missing or malformed"
      gate_migration_preview "$f" "$observed_spec" "$target authorization observed population" "$FROM_VERSION" "$TO_VERSION"
      assert_active_catalog_version "$TO_VERSION"
      assert_from_before_cutoff "$target" "$cutoff"
      assert_no_late_v2_rows "$target"
      emit_forward_convergence "$target" "authorize-$attempt" "$expected_spec" "$observed_spec" "$expected_rows" "$observed_rows"
      gate_migration_preview "$f" "$(migration_remaining_spec "$target")" "$target authorization remaining" "$FROM_VERSION" "$TO_VERSION"
      rows=$(migration_remaining_rows "$target")
      [[ "$(jsonq "$f" preview.scanned)" == "$rows" ]] || die "$target bounded FROM population differs from stable denominator minus receipts"
    done
    { echo "attempt=$attempt"; echo "stable_marker_sha256=$(sha256sum "$E/migrate-stable-population.txt" | cut -d' ' -f1)"; } | emit migrate-apply-authorized.txt
  fi
  [[ $(awk -F= '$1=="stable_marker_sha256"{print $2}' "$E/migrate-apply-authorized.txt") == "$(sha256sum "$E/migrate-stable-population.txt" | cut -d' ' -f1)" ]] || die 'apply authorization does not bind the stable population'
  for target in $(migration_targets); do
    assert_no_cas_conflicts migrate "$target"
    assert_no_late_v2_rows "$target"
    cutoff=$(migration_cutoff_value "$target")
    assert_from_before_cutoff "$target" "$cutoff"
    expected_rows=$(migration_remaining_rows "$target"); expected_spec=$(migration_remaining_spec "$target")
    f=$(migration_preview_one "migrate-$target-preview-before-$label" "$target" "$FROM_VERSION" "$TO_VERSION")
    observed_rows=$(jsonq "$f" preview.scanned); observed_spec=$(migration_cells "$f" "$FROM_VERSION" "$TO_VERSION")
    [[ "$observed_rows" =~ ^[0-9]+$ ]] || die "$target preview scanned missing or malformed"
    gate_migration_preview "$f" "$observed_spec" "$target apply observed population" "$FROM_VERSION" "$TO_VERSION"
    assert_active_catalog_version "$TO_VERSION"
    assert_from_before_cutoff "$target" "$cutoff"
    assert_no_late_v2_rows "$target"
    emit_forward_convergence "$target" "$label" "$expected_spec" "$observed_spec" "$expected_rows" "$observed_rows"
    spec=$(migration_remaining_spec "$target"); gate_migration_preview "$f" "$spec" "$target"
    rows=$(migration_remaining_rows "$target")
    [[ "$(jsonq "$f" preview.scanned)" == "$rows" ]] || die "$target remaining bounded FROM population differs from stable denominator minus prior receipts"
    if (( rows > 0 )); then
      migration_apply_one "$target" "$label" "$FROM_VERSION" "$TO_VERSION" migrate "$maxb"
      [[ $(jsonq "$E/migrate-$target-apply-$label.json" revalidation.complete) == true ]] || return 3
      assert_no_cas_conflicts migrate "$target"
    fi
  done
}
cmd_migrate_readback() {
  local target table since cutoff f rows updated converged skipped before_in after_in attempt=1 key expected actual expected_rows expected_spec observed_rows observed_spec candidate candidate_rows candidate_cell
  assert_tree; assert_active_catalog_version "$TO_VERSION"; assert_stable_population_bound
  while [[ -e "$E/migrate-post-preview-after-$attempt.json" || -e "$E/migrate-post-preview-after-$attempt.err" ]]; do attempt=$((attempt+1)); done
  for target in $(migration_targets); do
    assert_no_cas_conflicts migrate "$target"
    assert_no_late_v2_rows "$target"
    table=$(migration_target_table "$target"); since=$(migration_target_since "$target")
    expected_rows=$(migration_remaining_rows "$target"); expected_spec=$(migration_remaining_spec "$target")
    f=$(migration_preview_one "migrate-$target-preview-after-$attempt" "$target" "$FROM_VERSION" "$TO_VERSION")
    observed_rows=$(jsonq "$f" preview.scanned); observed_spec=$(migration_cells "$f" "$FROM_VERSION" "$TO_VERSION")
    gate_migration_preview "$f" "$observed_spec" "$target residual observed population"
    [[ "$observed_rows" == 0 ]] || die "$target still has bounded $FROM_VERSION semantic-delta rows"
    candidate=$(forward_convergence_receipt "$target" "$expected_spec" "$observed_spec" "$expected_rows" "$observed_rows") || return $?
    candidate_rows=$(awk -F= '$1=="producer_converged_rows"{print $2}' <<<"$candidate")
    rows=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/migrate-stable-population.txt")
    [[ "$rows" =~ ^[0-9]+$ ]] || die "$target stable denominator missing or malformed"
    updated=$(migration_applied_value "$target" revalidation.updated); converged=$(migration_converged_value migrate "$target" producer_converged_rows)
    [[ $((updated+converged+candidate_rows)) == "$rows" ]] || die "$target apply updated=$updated + producer_converged=$converged + pending_converged=$candidate_rows but prestate denominator=$rows"
    skipped=$(migration_applied_value "$target" revalidation.skipped_cas)
    [[ "$skipped" == 0 ]] || die "$target apply recorded skipped_cas=$skipped"
    for key in $(migration_outcomes "$FROM_VERSION" "$TO_VERSION"); do
      expected=$(prereg_spec_value "$(migration_prereg_spec "$target")" "$key")
      actual=$(migration_applied_value "$target" "revalidation.counts.$key")
      converged=$(migration_converged_value migrate "$target" "${key}_producer_converged")
      candidate_cell=$(awk -F= -v k="${key}_producer_converged" '$1==k{print $2}' <<<"$candidate")
      [[ $((actual+converged+candidate_cell)) == "$expected" ]] || die "$target realized $key=$actual + producer_converged=$converged + pending_converged=$candidate_cell but preregistered=$expected"
    done
    cutoff=$(migration_cutoff_value "$target"); before_in=$(awk -F'|' '$1=="from_in_horizon"{print $2}' "$(migration_scope_file "$target")")
    [[ "$before_in" =~ ^[0-9]+$ ]] || die "$target scope from_in_horizon missing or malformed"
    after_in=$(psql_ro -t -c "SELECT count(*) FROM public.$table WHERE content_time_validator_version='$FROM_VERSION' AND \"indexedAt\">='$since' AND \"indexedAt\"<'$cutoff';")
    [[ "$after_in" =~ ^[0-9]+$ ]] || die "$target after_in count missing or malformed"
    [[ $((before_in-after_in)) == "$rows" ]] || die "$target migration changed a row outside the exact in-horizon snapshot"
    assert_active_catalog_version "$TO_VERSION"
    assert_no_late_v2_rows "$target"
    if grep -Eq '=[1-9][0-9]*$' <<<"$candidate"; then printf '%s\n' "$candidate" | emit "migrate-$target-convergence-readback-$attempt.txt"; fi
  done
  { echo 'status=complete'; echo "transition=$FROM_VERSION->$TO_VERSION"; echo "stable_marker_sha256=$(sha256sum "$E/migrate-stable-population.txt" | cut -d' ' -f1)"; for target in $(migration_targets); do echo "$target rows=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/migrate-stable-population.txt")"; done; echo "ir_semantic_changed=$(awk -F= '$1=="ir_semantic_changed"{print $2}' "$E/migrate-stable-population.txt")"; echo "ir_restored_valid=$(awk -F= '$1=="ir_restored_valid"{print $2}' "$E/migrate-stable-population.txt")"; echo "ir_total_denominator=$(awk -F= '$1=="ir_total_denominator"{print $2}' "$E/migrate-stable-population.txt")"; } | emit "migrate-readback-$attempt.txt"
}
cmd_migrate_rollback() { # dry-run | apply <label> [max-batches]
  local mode=${1:?dry-run|apply} label=${2:-rollback} maxb=${3:-} target f spec rows updated skipped key expected actual cutoff before_in after_in incomplete=0 attempt=1
  assert_migration_transition
  case "$mode" in
    dry-run)
      assert_stable_population_bound
      for target in $(migration_targets); do
        f=$(migration_preview_one "rollback-$target-preview" "$target" "$TO_VERSION" "$FROM_VERSION")
        spec=$(migration_cells "$f" "$TO_VERSION" "$FROM_VERSION"); gate_migration_preview "$f" "$spec" "$target rollback" "$TO_VERSION" "$FROM_VERSION"
        rows=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/migrate-stable-population.txt")
        [[ "$(jsonq "$f" preview.scanned)" == "$rows" ]] || die "$target rollback preview does not cover the stable bounded cohort"
      done;;
    apply)
      assert_tree; assert_active_catalog_version "$FROM_VERSION"; assert_stable_population_bound
      for target in $(migration_targets); do
        [[ -s "$E/rollback-$target-preview.json" ]] || die "$target rollback dry-run receipt missing"
        spec=$(rollback_remaining_spec "$target"); rows=$(rollback_remaining_rows "$target")
        f=$(migration_preview_one "rollback-$target-preview-before-$label" "$target" "$TO_VERSION" "$FROM_VERSION")
        gate_migration_preview "$f" "$spec" "$target rollback authorization" "$TO_VERSION" "$FROM_VERSION"
        [[ "$(jsonq "$f" preview.scanned)" == "$rows" ]] || die "$target remaining rollback population differs from dry-run minus prior receipts"
        (( rows > 0 )) || continue
        migration_apply_one "$target" "$label" "$TO_VERSION" "$FROM_VERSION" rollback "$maxb"
        [[ "$(jsonq "$E/rollback-$target-apply-$label.json" revalidation.complete)" == true ]] || incomplete=1
      done
      (( incomplete == 0 )) || return 3
      while [[ -e "$E/rollback-post-preview-after-$attempt.json" ]]; do attempt=$((attempt+1)); done
      for target in $(migration_targets); do
        f=$(migration_preview_one "rollback-$target-preview-after-$attempt" "$target" "$TO_VERSION" "$FROM_VERSION")
        [[ "$(jsonq "$f" preview.truncated)" == false && "$(jsonq "$f" preview.scanned)" == 0 ]] || die "$target reverse migration incomplete"
        rows=$(awk -F= -v k="${target}_rows" '$1==k{print $2}' "$E/migrate-stable-population.txt")
        updated=$(migration_receipt_value rollback "$target" revalidation.updated); [[ "$updated" == "$rows" ]] || die "$target rollback updated=$updated but stable denominator=$rows"
        skipped=$(migration_receipt_value rollback "$target" revalidation.skipped_cas); [[ "$skipped" == 0 ]] || die "$target rollback recorded skipped_cas=$skipped"
        spec=$(migration_cells "$E/rollback-$target-preview.json" "$TO_VERSION" "$FROM_VERSION")
        for key in $(migration_outcomes "$TO_VERSION" "$FROM_VERSION"); do
          expected=$(prereg_spec_value "$spec" "$key"); actual=$(migration_receipt_value rollback "$target" "revalidation.counts.$key")
          [[ "$actual" == "$expected" ]] || die "$target rollback realized $key=$actual but dry-run bound $expected"
        done
        f=$(migration_preview_one "rollback-$target-restored-$attempt" "$target" "$FROM_VERSION" "$TO_VERSION")
        gate_migration_preview "$f" "$(migration_prereg_spec "$target")" "$target restored cohort"
        [[ "$(jsonq "$f" preview.scanned)" == "$rows" ]] || die "$target restored cohort denominator differs"
        cutoff=$(migration_cutoff_value "$target"); before_in=$(awk -F'|' '$1=="from_in_horizon"{print $2}' "$(migration_scope_file "$target")")
        after_in=$(psql_ro -t -c "SELECT count(*) FROM public.$(migration_target_table "$target") WHERE content_time_validator_version='$FROM_VERSION' AND \"indexedAt\">='$(migration_target_since "$target")' AND \"indexedAt\"<'$cutoff';")
        [[ "$before_in" == "$after_in" ]] || die "$target rollback did not restore the exact in-horizon snapshot"
        { echo "restored_rows=$rows"; echo "bounded_to_residual=0"; echo "skipped_cas=0"; } | emit "rollback-$target-diff-$attempt.txt"
      done;;
    *) die 'migrate-rollback mode must be dry-run or apply';;
  esac
}
cmd_migrate_finalize() {
  local wstart=${1:-unset} wend=${2:-unset} tmp
  [[ -f "$E/secret-scan.txt" ]] && grep -q '^hits=0$' "$E/secret-scan.txt" || die 'migrate-finalize requires migrate-secret-scan first'
  tmp=$(mktemp)
  { echo 'status=complete'; echo "generated_at=$(ts)"; cat "$E/migrate-source-set.txt"; echo "window_start_utc=$wstart"; echo "window_end_utc=$wend"; echo '--- readback'; cat "$E"/migrate-readback-*.txt; echo '--- ceilings'; cat "$E"/migrate-*-ceiling-*.txt; } >"$tmp"
  emit RESULT.txt <"$tmp"; rm -f "$tmp"
  seal_evidence
}

if [[ "${CONTENT_TIME_PACKET_SOURCE_ONLY:-0}" == 1 ]]; then return 0; fi

case "${1:-}" in
  prereg) cmd_prereg;;
  preflight) cmd_preflight;;
  control) cmd_control;;
  preview) cmd_preview "${2:?group}";;
  apply) cmd_apply "${2:?group}" "${3:?label}" "${4:-}";;
  readback) cmd_readback;;
  restore) cmd_restore "${2:?group}";;
  secret-scan) cmd_secret_scan;;
  finalize) cmd_finalize "${2:-}" "${3:-}";;
  migrate-prereg) cmd_migrate_prereg;;
  migrate-prepare) cmd_migrate_prepare;;
  migrate-preflight) cmd_migrate_preflight;;
  migrate-freeze) cmd_migrate_freeze;;
  migrate-stable-check) cmd_migrate_stable_check;;
  migrate-normalize-overlap) cmd_migrate_normalize_overlap "${2:?label}" "${3:-}";;
  migrate-native-tail-plan) cmd_migrate_native_tail_plan;;
  migrate-native-tail-recovery-bind) cmd_migrate_native_tail_recovery_bind;;
  migrate-native-tail-rollback) cmd_migrate_native_tail_rollback "${2:?label}" "${3:-}";;
  migrate-preview) cmd_migrate_preview;;
  migrate-apply) cmd_migrate_apply "${2:?label}" "${3:-}";;
  migrate-readback) cmd_migrate_readback;;
  migrate-rollback) cmd_migrate_rollback "${2:?dry-run|apply}" "${3:-}" "${4:-}";;
  migrate-secret-scan) cmd_secret_scan;;
  migrate-finalize) cmd_migrate_finalize "${2:-}" "${3:-}";;
  *) echo "usage: $0 prereg|preflight|control|preview <group>|apply <group> <label> [max_batches]|readback|restore <group>|secret-scan|finalize <start> <end>|migrate-prereg|migrate-prepare|migrate-preflight|migrate-freeze|migrate-stable-check|migrate-normalize-overlap <label> [max_batches]|migrate-native-tail-recovery-bind|migrate-native-tail-rollback <label> [max_batches]|migrate-preview|migrate-apply <label> [max_batches]|migrate-readback|migrate-rollback <dry-run|apply> [label] [max_batches]|migrate-secret-scan|migrate-finalize <start> <end>" >&2; exit 2;;
esac
