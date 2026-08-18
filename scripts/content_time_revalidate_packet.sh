#!/usr/bin/env bash
# Operator runner for the content-time v1->v2 re-validation backfill packet
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
#                        per-URI prestate snapshots (main/be), NULL-raw gate, pg_stat prestate,
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
#
# Required env (the runner refuses to start without them):
#   E TREE EXPECTED_SHA EXPECTED_DIST_SHA256 EXPECTED_CT_SHA256 EXPECTED_IMAGE_CT_SHA256 PACKET_SHA
#   SINCE_MAIN SINCE_BE (ABSOLUTE receipt-time lower bounds, ISO-8601 Z, bound in the ledger approval; the candidate set
#     {v1 rows with raw, indexedAt >= SINCE} is then stable because no v1 producer exists any more)
#   PREREG_MAIN PREREG_BE (pre-registered cells "v1_valid_to_v2_valid=N,v1_invalid_to_v2_valid=N,v1_to_v2_invalid=N",
#     computed by `prereg` at the same SINCE bounds; gated EXACTLY by preflight/preview/readback)
#   EXPECTED_TOOL_REFS ("bsky-ops=<sha>,blueskyranker=<sha>,newsflows-bskyhealth=<sha>": installed operator tools must match)
# `prereg` (read-only) prints the SINCE bounds for the current horizons plus the cells, for the ledger approval entry.
# Optional env (production defaults): IMG NETWORK ENV_FILE DB_CONTAINER PSQL_DB PSQL_USER
#   MAIN_DIDS BE_DID HORIZON_MAIN_DAYS HORIZON_BE_DAYS DOCKER FEEDGEN_CONTAINER RUNNER
#   (container|host; host = rehearsal with HOST_DSN) CEIL_WAL_BASELINE_MULTIPLE CEIL_WAL_FLOOR_BYTES CEIL_REL_BYTES
#   RANKED_RKEYS MAIN_RKEY_PATTERN BE_RKEY_PATTERN READBACK_JSON (BSR effective-config readback)
#   (READBACK_JSON is required to exist and be fresh: stale_at > now)
#   SECRET_KEY_REGEX (default: keys matching PASSWORD|SECRET|TOKEN|KEY|PASS) selects which env-file values the secret scan hunts for
#   PREREG_TOLERANCE_PCT (default 5) SKIP_LIVE_IMAGE_CHECKS=1 (rehearsal only) RESTORE_STOP_AFTER_BATCH (rehearsal only)
set -euo pipefail

: "${E:?E (evidence root) is required}"
: "${TREE:?TREE (built feedgen tree) is required}"
: "${EXPECTED_SHA:?EXPECTED_SHA (full source SHA) is required}"
: "${EXPECTED_DIST_SHA256:?EXPECTED_DIST_SHA256 is required}"
: "${EXPECTED_CT_SHA256:?EXPECTED_CT_SHA256 is required}"
: "${EXPECTED_IMAGE_CT_SHA256:?EXPECTED_IMAGE_CT_SHA256 (validator module hash inside the live image) is required}"
: "${PACKET_SHA:?PACKET_SHA (approved packet SHA-256) is required}"
: "${PREREG_MAIN:?PREREG_MAIN (pre-registered cells) is required}"
: "${PREREG_BE:?PREREG_BE (pre-registered cells) is required}"
: "${SINCE_MAIN:?SINCE_MAIN (absolute receipt-time lower bound, ISO Z, from the ledger approval) is required}"
: "${SINCE_BE:?SINCE_BE (absolute receipt-time lower bound, ISO Z, from the ledger approval) is required}"
: "${EXPECTED_TOOL_REFS:?EXPECTED_TOOL_REFS (bsky-ops=<sha>,blueskyranker=<sha>,newsflows-bskyhealth=<sha>) is required}"
[[ "$PACKET_SHA" =~ ^[0-9a-f]{64}$ ]] || { echo "PACKET_SHA must be 64 lowercase hex" >&2; exit 2; }
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
RANKED_RKEYS="${RANKED_RKEYS:-'newsflow-nl-2','newsflow-fr-2','newsflow-cz-2','newsflow-ir-2','newsflow-be-k','newsflow-be-m'}"
MAIN_RKEY_PATTERN="${MAIN_RKEY_PATTERN:-newsflow-(nl|fr|cz|ir)-2}"
BE_RKEY_PATTERN="${BE_RKEY_PATTERN:-newsflow-be-k}"   # be-k and be-m share one publisher: count once
OUTCOMES=(v1_valid_to_v2_valid v1_invalid_to_v2_valid v1_to_v2_invalid)
EXTRA_CELLS=(createdat_extra createdat_unchanged)   # pre-registered, not tool outcomes: valid->valid rows whose createdAt differs from the v2 rendering; flip rows whose createdAt already equals the v2 target

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
psql_ro() {
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -A -F '|' -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '120s';" "$@"
}
psql_copy() {
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '120s';" -c "\\copy ($1) TO STDOUT WITH (FORMAT text)"
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
      -c 'export FEEDGEN_POSTGRES_URL="$(node /src/scripts/compose_feedgen_dsn.js)" || exit 97; exec node /src/dist/tools/backfill-publisher-posts.js "$@"' sh "$@" \
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
  local rc; rc=$(run_tool "step1-$g-apply-$label" --mode revalidate --apply --actors "$(group_dids "$g")" --since "$(since_file "$g")" --checkpoint-file "$ckpt" --packet-sha256 "$PACKET_SHA" ${maxb:+--max-batches "$maxb"} --json)
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
    local wal_used=$pw wal_source="pg_stat_wal_delta_minus_control_rate_x_duration"
    [[ -n "$twal" ]] && { wal_used=$twal; wal_source="tool_in_transaction_lsn_advance_minus_control_rate_x_batch_elapsed (cluster-wide insert LSN; concurrent writers included then subtracted)"; }
    # baseline-scaled WAL ceiling: multiple x baseline rate x (max batch elapsed + pause), floored at CEIL_WAL_FLOOR_BYTES
    local max_ms; max_ms=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1]));const b=(j.revalidation&&j.revalidation.batches)||[];console.log(Math.max(0,...b.map(x=>x.elapsed_ms||0)))' "$f")
    local wal_ceiling; wal_ceiling=$(node -e 'const [m,r,ms,p,fl]=process.argv.slice(1).map(Number);console.log(Math.max(fl,Math.round(m*r*(ms/1000+p))))' "$CEIL_WAL_BASELINE_MULTIPLE" "$wps" "$max_ms" "$PAUSE_SECONDS" "$CEIL_WAL_FLOOR_BYTES")
    local dead_ceiling=$(( 2 * $(jsonq "$f" revalidation.updated) / nb ))
    (( wal_used <= wal_ceiling && pr <= CEIL_REL_BYTES && pd <= dead_ceiling )) || verdict="BREACH"
    { echo "batches=$nb exit=$rc updated=$(jsonq "$f" revalidation.updated) complete=$(jsonq "$f" revalidation.complete) invocation_seconds=$dur max_batch_elapsed_ms=$max_ms"; echo "delta_wal_bytes=$dw delta_relation_bytes=$dr delta_dead_tuples=$dd"; echo "control_file=$(basename "$(latest_control)") control_rate_per_s wal=$wps relation=$rps dead=$dps (clamped at 0)"; echo "per_batch_wal_minus_control=$pw per_batch_relation_minus_control=$pr per_batch_dead_minus_control=$pd"; echo "wal_used_for_verdict=$wal_used wal_source=$wal_source"; echo "ceilings wal<=$wal_ceiling (=max(floor $CEIL_WAL_FLOOR_BYTES, $CEIL_WAL_BASELINE_MULTIPLE x baseline $wps B/s x ($max_ms ms + ${PAUSE_SECONDS}s))) relation<=$CEIL_REL_BYTES dead<=$dead_ceiling (=2 x updated rows per batch)"; echo "verdict=$verdict"; } | emit "ceiling-$g-$label.txt"
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
  ( cd "$E" && sudo -n sh -c 'find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS && chown root:newsflows SHA256SUMS && chmod 640 SHA256SUMS' )
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
  psql_ro -c "$SCOPE_SQL"; rm -f "$tmp"
}

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
  *) echo "usage: $0 prereg|preflight|control|preview <group>|apply <group> <label> [max_batches]|readback|restore <group>|secret-scan|finalize <start> <end>" >&2; exit 2;;
esac
