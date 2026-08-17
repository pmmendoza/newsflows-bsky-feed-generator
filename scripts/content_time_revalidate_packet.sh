#!/usr/bin/env bash
# Operator runner for the content-time v1->v2 re-validation backfill packet
# (dev/feedgen/2026-08-17_content_time_revalidation_backfill_packet.md in the
# BSKY root repo). Every production step of that packet is a subcommand here so
# nothing is hand-composed on the console. Raw-free by construction: the DSN is
# composed inside the container from the secrets env-file (URL-encoded) and is
# never printed; stdout and stderr of the tool are captured to SEPARATE files;
# `secret-scan` must pass before `finalize`.
#
# Subcommands (each writes only into $E; every output is root:newsflows 0640 and
# refuses to overwrite an existing file):
#   preflight            source/dist hashes, filtered container readback, tool refs,
#                        population + transition-prediction tables, per-URI prestate
#                        snapshots (main/be), NULL-raw gate, pg_stat prestate,
#                        60 s idle control read
#   preview  <group>     dry-run through the container runner; gate against the
#                        prediction table (exact for cells < 50, +/-2% above)
#   apply    <group> <label> [max_batches]
#                        --apply --packet-sha256 --checkpoint-file; pg_stat before/
#                        after; per-batch deltas vs ceilings (minus control baseline)
#   readback             previews must be empty; poststate populations + per-URI
#                        snapshots + diff summary; pg_stat poststate
#   restore  <group>     bounded (500/keyset, lock 5 s, statement 30 s, 1 s pause,
#                        cursor file) CAS restore from the prestate snapshot, then
#                        byte-for-byte verification
#   secret-scan          fail if the DB password or generic DSN markers appear in $E
#   finalize <start> <end>
#                        RESULT.txt + SHA256SUMS regeneration
#
# Config (env): E, TREE, EXPECTED_SHA, EXPECTED_DIST_SHA256, EXPECTED_CT_SHA256,
#   IMG, NETWORK, ENV_FILE, DB_CONTAINER, PSQL_DB, PSQL_USER, PACKET_SHA,
#   MAIN_DIDS, BE_DID, HORIZON_MAIN_DAYS, HORIZON_BE_DAYS, DOCKER,
#   RUNNER=container|host (host = rehearsal: node from $TREE with HOST_DSN),
#   FEEDGEN_CONTAINER (default feedgen), CEIL_WAL_BYTES, CEIL_REL_BYTES, CEIL_DEAD,
#   RANKED_RKEYS / MAIN_RKEY_PATTERN / BE_RKEY_PATTERN (rehearsal catalogs).
set -euo pipefail

: "${E:?E (evidence root) is required}"
: "${TREE:?TREE (built feedgen tree) is required}"
EXPECTED_SHA="${EXPECTED_SHA:-}"
EXPECTED_DIST_SHA256="${EXPECTED_DIST_SHA256:-}"
EXPECTED_CT_SHA256="${EXPECTED_CT_SHA256:-}"
IMG="${IMG:-pmmendoza/bsky-feedgen@sha256:928c15aac77a8a842f60053eff8953e70cc9e4117c2fbe86f548e345c1a34711}"
NETWORK="${NETWORK:-newsflows-bsky-feed-generator-v2_default}"
ENV_FILE="${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}"
DB_CONTAINER="${DB_CONTAINER:-feedgen-db}"
PSQL_DB="${PSQL_DB:-feedgen-db}"
PSQL_USER="${PSQL_USER:-feedgen}"
PACKET_SHA="${PACKET_SHA:-}"
MAIN_DIDS="${MAIN_DIDS:-did:plc:toz4no26o2x4vsbum7cp4bxp,did:plc:kzmukwaf72iwepygposicgt3,did:plc:cegiy4pfghh4rjs7ks7pbnkm,did:plc:vzmnljt7otfbbgrmachtefxh}"
BE_DID="${BE_DID:-did:plc:tlmi333azel2jcornp2qeolm}"
HORIZON_MAIN_DAYS="${HORIZON_MAIN_DAYS:-3}"
HORIZON_BE_DAYS="${HORIZON_BE_DAYS:-10}"
FEEDGEN_CONTAINER="${FEEDGEN_CONTAINER:-feedgen}"
RUNNER="${RUNNER:-container}"
CEIL_WAL_BYTES="${CEIL_WAL_BYTES:-614400}"
CEIL_REL_BYTES="${CEIL_REL_BYTES:-409600}"
CEIL_DEAD="${CEIL_DEAD:-1000}"
read -r -a DOCKER <<<"${DOCKER:-sudo -n docker}"
V1='newsflows-content-time/v1'
V2='newsflows-content-time/v2'
RANKED_RKEYS="${RANKED_RKEYS:-'newsflow-nl-2','newsflow-fr-2','newsflow-cz-2','newsflow-ir-2','newsflow-be-k','newsflow-be-m'}"
MAIN_RKEY_PATTERN="${MAIN_RKEY_PATTERN:-newsflow-(nl|fr|cz|ir)-2}"
BE_RKEY_PATTERN="${BE_RKEY_PATTERN:-newsflow-be-k}"   # be-k and be-m share one publisher: count once

log() { echo "[packet] $*" >&2; }
die() { echo "[packet] STOP: $*" >&2; exit 2; }
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# emit <name>  (stdin -> $E/<name>, root:newsflows 0640, never overwrite)
emit() {
  local name=$1 tmp
  [[ -e "$E/$name" ]] && die "refusing to overwrite existing evidence file $E/$name"
  tmp=$(mktemp); cat >"$tmp"
  sudo -n install -o root -g newsflows -m 640 "$tmp" "$E/$name"; rm -f "$tmp"
  log "wrote $name"
}
psql_ro() {  # read-only psql, pipe-separated, no header decorations
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -A -F '|' -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '120s';" "$@"
}
psql_copy() {  # \copy (query) TO STDOUT
  "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -q -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on; SET statement_timeout = '120s';" -c "\\copy ($1) TO STDOUT WITH (FORMAT text)"
}
group_dids() { case "$1" in main) echo "$MAIN_DIDS";; be) echo "$BE_DID";; *) die "group must be main|be";; esac; }
group_since() {
  case "$1" in
    main) date -u -d "$HORIZON_MAIN_DAYS days ago" +%Y-%m-%dT%H:%M:%S.000Z;;
    be)   date -u -d "$HORIZON_BE_DAYS days ago" +%Y-%m-%dT%H:%M:%S.000Z;;
  esac
}
since_file() { local g=$1; [[ -f "$E/since-$g.txt" ]] && cat "$E/since-$g.txt" || die "since-$g.txt missing: run preflight first"; }
sql_array() { echo "'{$1}'"; }   # csv -> postgres text[] literal

pgstat_read() {  # one-line pg_stat snapshot for post
  psql_ro -t -c "SELECT (SELECT wal_bytes FROM pg_stat_wal) AS wal_bytes, pg_total_relation_size('public.post') AS relation_bytes, n_dead_tup, n_live_tup, now() FROM pg_stat_user_tables WHERE relname='post';"
}

# ---------------------------------------------------------------- tool runner
run_tool() {  # run_tool <outname> <tool args...>  -> $E/<outname>.json + .err
  local out=$1; shift
  [[ -e "$E/$out.json" || -e "$E/$out.err" ]] && die "refusing to overwrite $E/$out.*"
  local so se; so=$(mktemp); se=$(mktemp); local rc=0
  if [[ "$RUNNER" == "host" ]]; then
    : "${HOST_DSN:?HOST_DSN required for RUNNER=host}"
    # host mode (rehearsal): run as root like the production container does, so the checkpoint in $E (root:newsflows 0750) is writable
    sudo -n env FEEDGEN_POSTGRES_URL="$HOST_DSN" node "$TREE/dist/tools/backfill-publisher-posts.js" "$@" >"$so" 2>"$se" || rc=$?
  else
    "${DOCKER[@]}" run --rm --network "$NETWORK" --env-file "$ENV_FILE" \
      -v "$TREE:/src:ro" -v "$E:/evidence" -w /src "$IMG" \
      sh -c 'export FEEDGEN_POSTGRES_URL="$(node -p "const u=encodeURIComponent;\`postgresql://${u(process.env.FEEDGEN_DB_USER)}:${u(process.env.FEEDGEN_DB_PASSWORD)}@${process.env.FEEDGEN_DB_HOST}:${process.env.FEEDGEN_DB_PORT}/${process.env.FEEDGEN_DB_DATABASE}\`")"; exec node /src/dist/tools/backfill-publisher-posts.js "$@"' sh "$@" \
      >"$so" 2>"$se" || rc=$?
  fi
  emit "$out.json" <"$so"; emit "$out.err" <"$se"; rm -f "$so" "$se"
  echo "$rc"
}
jsonq() { node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));const v=process.argv[2].split(".").reduce((a,k)=>a==null?a:a[k],j);console.log(v===undefined?"":(typeof v==="object"?JSON.stringify(v):v))' "$1" "$2"; }

# ---------------------------------------------------------------- SQL blocks
POP_SQL="WITH t AS (SELECT c.rkey, c.publisher_did, GREATEST(c.publisher_post_max_age_days, CASE WHEN c.rkey LIKE 'newsflow-be-%' THEN $HORIZON_BE_DAYS ELSE $HORIZON_MAIN_DAYS END) AS h FROM feedgen_ops.feed_catalog c WHERE c.enabled AND c.rkey IN ($RANKED_RKEYS))
SELECT t.rkey, t.h AS horizon_days, count(*) AS total,
 count(*) FILTER (WHERE p.content_time_status='source_valid' AND p.content_time_validator_version='$V2') AS v2_valid,
 count(*) FILTER (WHERE p.content_time_status='source_invalid' AND p.content_time_validator_version='$V2') AS v2_invalid,
 count(*) FILTER (WHERE p.content_time_validator_version='$V1' AND p.created_at_source_raw IS NOT NULL) AS v1_rows_with_raw,
 count(*) FILTER (WHERE p.content_time_validator_version='$V1' AND p.created_at_source_raw IS NULL) AS v1_rows_null_raw,
 count(*) FILTER (WHERE p.content_time_status IS NULL OR p.content_time_status='legacy_unknown') AS legacy
FROM t JOIN post p ON p.author=t.publisher_did AND p.\"indexedAt\" >= to_char(now() AT TIME ZONE 'UTC' - make_interval(days=>t.h), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')
GROUP BY 1,2 ORDER BY 1;"

# per-feed predicted v1->v2 transitions (v2 policy: raw <= indexedAt + 5 min is valid; missing/unparseable invalid)
TRANS_SQL="WITH t AS (SELECT c.rkey, c.publisher_did, GREATEST(c.publisher_post_max_age_days, CASE WHEN c.rkey LIKE 'newsflow-be-%' THEN $HORIZON_BE_DAYS ELSE $HORIZON_MAIN_DAYS END) AS h FROM feedgen_ops.feed_catalog c WHERE c.enabled AND c.rkey IN ($RANKED_RKEYS)),
v1 AS (SELECT t.rkey, p.content_time_status AS s1, convert_from(p.created_at_source_raw,'UTF8') AS raw, p.\"indexedAt\" AS ia
 FROM t JOIN post p ON p.author=t.publisher_did AND p.\"indexedAt\" >= to_char(now() AT TIME ZONE 'UTC' - make_interval(days=>t.h), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')
 WHERE p.content_time_validator_version='$V1' AND p.created_at_source_raw IS NOT NULL),
pred AS (SELECT rkey, CASE
  WHEN raw IS NULL OR raw='' OR raw !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' THEN 'v1_to_v2_invalid'
  WHEN raw::timestamptz > ia::timestamptz + interval '5 minutes' THEN 'v1_to_v2_invalid'
  WHEN s1='source_valid' THEN 'v1_valid_to_v2_valid' ELSE 'v1_invalid_to_v2_valid' END AS outcome FROM v1)
SELECT rkey, outcome, count(*) FROM pred GROUP BY 1,2 ORDER BY 1,2;"

snapshot_sql() {  # snapshot_sql <dids-csv> <since> <version>
  echo "SELECT p.uri, p.author, p.\"indexedAt\", p.\"createdAt\", p.content_time_utc, p.content_time_status, p.content_time_clamp_reason, p.content_time_validator_version, encode(p.created_at_source_raw,'hex') AS raw_hex FROM post p WHERE p.author = ANY($(sql_array "$1")::text[]) AND p.content_time_validator_version = '$3' AND p.created_at_source_raw IS NOT NULL AND p.\"indexedAt\" >= '$2' ORDER BY p.author, p.uri"
}

# ---------------------------------------------------------------- subcommands
cmd_preflight() {
  [[ -d "$E" ]] || sudo -n install -d -o root -g newsflows -m 750 "$E"
  local sha; sha=$(git -C "$TREE" rev-parse HEAD)
  [[ -z "$EXPECTED_SHA" || "$sha" == "$EXPECTED_SHA" ]] || die "tree HEAD $sha != EXPECTED_SHA $EXPECTED_SHA"
  [[ -z "$(git -C "$TREE" status --porcelain)" ]] || die "tree is dirty"
  local d1 d2; d1=$(sha256sum "$TREE/dist/tools/backfill-publisher-posts.js" | cut -d' ' -f1); d2=$(sha256sum "$TREE/dist/util/content-time.js" | cut -d' ' -f1)
  [[ -z "$EXPECTED_DIST_SHA256" || "$d1" == "$EXPECTED_DIST_SHA256" ]] || die "dist backfill hash $d1 != expected"
  [[ -z "$EXPECTED_CT_SHA256" || "$d2" == "$EXPECTED_CT_SHA256" ]] || die "dist content-time hash $d2 != expected"
  { echo "generated_at=$(ts)"; echo "tree=$TREE"; echo "source_sha=$sha"; echo "dist_backfill_sha256=$d1"; echo "dist_content_time_sha256=$d2"; echo "image=$IMG"; echo "runner=$RUNNER"; echo "packet_sha256=${PACKET_SHA:-unset}"; } | emit source-set.txt
  if [[ "$RUNNER" == "container" ]]; then
    "${DOCKER[@]}" inspect "$FEEDGEN_CONTAINER" --format '{{.Id}} {{.Image}} {{.Config.Image}} {{.State.StartedAt}} {{.RestartCount}}' | emit feedgen-prestate.txt
    { for t in bsky-ops blueskyranker newsflows-bskyhealth; do f=$(ls /opt/newsflows/tools/uv/$t/lib/python3.*/site-packages/*.dist-info/direct_url.json 2>/dev/null | head -1); echo "$t $(grep -o '"commit_id":"[0-9a-f]*"' "$f" 2>/dev/null | head -1)"; done; } | emit tools.txt
  fi
  echo "$(group_since main)" | emit since-main.txt
  echo "$(group_since be)" | emit since-be.txt
  psql_ro -c "$POP_SQL" | emit prestate-populations.tsv
  psql_ro -c "$TRANS_SQL" | emit prestate-transitions.tsv
  # NULL-raw gate
  local nullraw; nullraw=$(awk -F'|' 'NR>1 && $1 ~ /^newsflow/ {s+=$7} END{print s+0}' "$E/prestate-populations.tsv")
  [[ "$nullraw" == "0" ]] || die "in-horizon v1 rows with NULL raw: $nullraw (expected 0)"
  psql_copy "$(snapshot_sql "$MAIN_DIDS" "$(since_file main)" "$V1")" | emit step1-main-prestate-rows.tsv
  psql_copy "$(snapshot_sql "$BE_DID" "$(since_file be)" "$V1")" | emit step1-be-prestate-rows.tsv
  { echo "read1 $(pgstat_read)"; sleep 60; echo "read2 $(pgstat_read)"; } | emit pg-control-60s.txt
  pgstat_read | emit pg-prestate.txt
  log "preflight complete: main prestate rows=$(wc -l <"$E/step1-main-prestate-rows.tsv") be prestate rows=$(wc -l <"$E/step1-be-prestate-rows.tsv")"
}

expected_outcome() {  # expected_outcome <group> <outcome> -> predicted count summed over the group's feeds
  local g=$1 o=$2 pat
  case "$g" in main) pat="$MAIN_RKEY_PATTERN";; be) pat="$BE_RKEY_PATTERN";; esac
  awk -F'|' -v pat="$pat" -v o="$o" '$1 ~ "^"pat"$" && $2==o {s+=$3} END{print s+0}' "$E/prestate-transitions.tsv"
}
gate_cell() {  # gate_cell <label> <expected> <actual>
  local exp=$2 act=$3
  if (( exp < 50 )); then [[ "$exp" == "$act" ]] || die "gate $1: expected $exp got $act (exact match required below 50)";
  else local lo=$(( exp*98/100 )) hi=$(( exp*102/100 + 1 )); (( act >= lo && act <= hi )) || die "gate $1: expected $exp±2% got $act"; fi
  log "gate $1 ok (expected $exp, got $act)"
}
cmd_preview() {
  local g=$1 rc; rc=$(run_tool "step1-$g-preview" --mode revalidate --actors "$(group_dids "$g")" --since "$(since_file "$g")" --json ${PACKET_SHA:+--packet-sha256 "$PACKET_SHA"})
  local f="$E/step1-$g-preview.json"
  [[ "$rc" == "0" ]] || die "preview $g exit=$rc (see step1-$g-preview.err)"
  [[ "$(jsonq "$f" operation)" == "content-time-revalidate" && "$(jsonq "$f" mode)" == "dry-run" ]] || die "preview $g did not run the revalidate dry-run path"
  local o; for o in v1_valid_to_v2_valid v1_invalid_to_v2_valid v1_to_v2_invalid; do gate_cell "$g/$o" "$(expected_outcome "$g" "$o")" "$(jsonq "$f" "preview.counts.$o")"; done
  log "preview $g ok: scanned=$(jsonq "$f" preview.scanned)"
}
cmd_apply() {
  local g=$1 label=$2 maxb=${3:-}
  [[ -n "$PACKET_SHA" ]] || die "PACKET_SHA required for apply"
  local ckpt="/evidence/step1-$g-checkpoint.json"; [[ "$RUNNER" == "host" ]] && ckpt="$E/step1-$g-checkpoint.json"
  pgstat_read | emit "pg-$g-$label-before.txt"
  local rc; rc=$(run_tool "step1-$g-apply-$label" --mode revalidate --apply --actors "$(group_dids "$g")" --since "$(since_file "$g")" --checkpoint-file "$ckpt" --packet-sha256 "$PACKET_SHA" ${maxb:+--max-batches "$maxb"} --json)
  pgstat_read | emit "pg-$g-$label-after.txt"
  local f="$E/step1-$g-apply-$label.json"
  [[ "$rc" == "0" || "$rc" == "3" ]] || die "apply $g/$label exit=$rc (see .err); STOP + escalation"
  [[ "$(jsonq "$f" operation)" == "content-time-revalidate" && "$(jsonq "$f" mode)" == "apply" ]] || die "apply $g/$label did not run the revalidate apply path"
  local nb; nb=$(node -e 'const j=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));console.log((j.revalidation&&j.revalidation.batches||[]).length)' "$f")
  local b a; b=$(cut -d'|' -f1-3 "$E/pg-$g-$label-before.txt"); a=$(cut -d'|' -f1-3 "$E/pg-$g-$label-after.txt")
  local c1 c2; c1=$(sed -n 's/^read1 //p' "$E/pg-control-60s.txt" | cut -d'|' -f1-3); c2=$(sed -n 's/^read2 //p' "$E/pg-control-60s.txt" | cut -d'|' -f1-3)
  local dw dr dd cw cr cd
  dw=$(( $(echo "$a"|cut -d'|' -f1) - $(echo "$b"|cut -d'|' -f1) )); dr=$(( $(echo "$a"|cut -d'|' -f2) - $(echo "$b"|cut -d'|' -f2) )); dd=$(( $(echo "$a"|cut -d'|' -f3) - $(echo "$b"|cut -d'|' -f3) ))
  cw=$(( $(echo "$c2"|cut -d'|' -f1) - $(echo "$c1"|cut -d'|' -f1) )); cr=$(( $(echo "$c2"|cut -d'|' -f2) - $(echo "$c1"|cut -d'|' -f2) )); cd=$(( $(echo "$c2"|cut -d'|' -f3) - $(echo "$c1"|cut -d'|' -f3) ))
  local verdict="ok"
  if (( nb > 0 )); then
    local pw=$(( (dw - cw) / nb )) pr=$(( (dr - cr) / nb )) pd=$(( (dd - cd) / nb ))
    (( pw <= CEIL_WAL_BYTES && pr <= CEIL_REL_BYTES && pd <= CEIL_DEAD )) || verdict="BREACH"
    { echo "batches=$nb exit=$rc updated=$(jsonq "$f" revalidation.updated) complete=$(jsonq "$f" revalidation.complete)"; echo "delta_wal_bytes=$dw delta_relation_bytes=$dr delta_dead_tuples=$dd"; echo "control_60s_wal=$cw control_60s_relation=$cr control_60s_dead=$cd"; echo "per_batch_wal_minus_control=$pw per_batch_relation_minus_control=$pr per_batch_dead_minus_control=$pd"; echo "ceilings wal<=$CEIL_WAL_BYTES relation<=$CEIL_REL_BYTES dead<=$CEIL_DEAD"; echo "verdict=$verdict"; } | emit "ceiling-$g-$label.txt"
  else
    echo "batches=0 exit=$rc verdict=no_batches" | emit "ceiling-$g-$label.txt"
  fi
  [[ "$verdict" == "ok" ]] || die "ceiling breach on $g/$label (see ceiling-$g-$label.txt): stop, take a fresh control read, re-measure once; second breach = stop for good"
  log "apply $g/$label exit=$rc batches=$nb verdict=$verdict"
  return 0
}
cmd_readback() {
  local g rc
  for g in main be; do
    rc=$(run_tool "step1-$g-preview-after" --mode revalidate --actors "$(group_dids "$g")" --since "$(since_file "$g")" --json ${PACKET_SHA:+--packet-sha256 "$PACKET_SHA"})
    [[ "$rc" == "0" && "$(jsonq "$E/step1-$g-preview-after.json" preview.scanned)" == "0" ]] || die "post-apply preview $g not empty (exit=$rc)"
    psql_copy "$(snapshot_sql "$(group_dids "$g")" "$(since_file "$g")" "$V2")" | emit "step1-$g-poststate-rows.tsv"
    node -e '
const fs=require("fs");const [pre,post]=[process.argv[1],process.argv[2]].map(p=>fs.readFileSync(p,"utf8").split("\n").filter(Boolean).map(l=>l.split("\t")));
const m=new Map(post.map(r=>[r[0],r]));let missing=0,createdChanged=0,statusChanged=0,rawMismatch=0;
for(const r of pre){const q=m.get(r[0]);if(!q){missing++;continue;}if(q[3]!==r[3])createdChanged++;if(q[5]!==r[5])statusChanged++;if(q[8]!==r[8])rawMismatch++;}
console.log(`prestate_rows=${pre.length} poststate_rows_v2=${post.length} prestate_missing_in_poststate=${missing} createdAt_changed=${createdChanged} status_changed=${statusChanged} raw_mismatch=${rawMismatch}`);
process.exit(missing===0&&rawMismatch===0?0:2)' "$E/step1-$g-prestate-rows.tsv" "$E/step1-$g-poststate-rows.tsv" | emit "step1-$g-diff.txt" || die "readback $g: prestate rows missing from poststate or raw mismatch"
  done
  psql_ro -c "$POP_SQL" | emit poststate-populations.tsv
  pgstat_read | emit pg-poststate.txt
  local rem; rem=$(awk -F'|' 'NR>1 && $1 ~ /^newsflow/ {s+=$6} END{print s+0}' "$E/poststate-populations.tsv")
  [[ "$rem" == "0" ]] || die "v1_rows_with_raw remaining after apply: $rem"
  log "readback ok: v1_rows_with_raw=0 for all six ranked feeds"
}
cmd_restore() {  # bounded CAS restore from the prestate snapshot; keyset batches of 500
  local g=$1 pre="$E/step1-$g-prestate-rows.tsv" cur="$E/restore-$g-cursor.txt" total done_rows=0 batch=0
  [[ -f "$pre" ]] || die "no prestate snapshot for $g"
  total=$(wc -l <"$pre"); log "restore $g: $total prestate rows"
  local start=1; [[ -f "$cur" ]] && start=$(cat "$cur")
  while (( start <= total )); do
    local end=$(( start + 499 )); (( end > total )) && end=$total; batch=$(( batch + 1 ))
    { echo "BEGIN; SET lock_timeout='5s'; SET statement_timeout='30s';"; echo "CREATE TEMP TABLE prestate(uri text, author text, indexed_at text, created_at text, content_time_utc text, status text, reason text, version text, raw_hex text);"; echo "COPY prestate FROM STDIN WITH (FORMAT text);"; sed -n "${start},${end}p" "$pre"; echo '\.'; cat <<'SQL'
UPDATE public.post AS t SET "createdAt"=s.created_at, content_time_utc=s.content_time_utc, content_time_status=s.status, content_time_clamp_reason=s.reason, content_time_validator_version=s.version
FROM prestate s WHERE t.uri=s.uri AND t.content_time_validator_version='newsflows-content-time/v2' AND encode(t.created_at_source_raw,'hex')=s.raw_hex;
SELECT 'restored_in_batch', count(*) FROM post p JOIN prestate s ON s.uri=p.uri WHERE p."createdAt"=s.created_at AND p.content_time_utc IS NOT DISTINCT FROM s.content_time_utc AND p.content_time_status=s.status AND p.content_time_clamp_reason IS NOT DISTINCT FROM s.reason AND p.content_time_validator_version=s.version;
COMMIT;
SQL
    } | "${DOCKER[@]}" exec -i "$DB_CONTAINER" psql -U "$PSQL_USER" -d "$PSQL_DB" -X -A -F '|' -v ON_ERROR_STOP=1 >"$E/.restore-$g-batch-$batch.tmp" 2>&1 || die "restore $g batch $batch failed (see .restore-$g-batch-$batch.tmp)"
    sudo -n install -o root -g newsflows -m 640 "$E/.restore-$g-batch-$batch.tmp" "$E/restore-$g-batch-$batch.txt"; rm -f "$E/.restore-$g-batch-$batch.tmp"
    done_rows=$end; echo $(( end + 1 )) >"$cur"; sleep 1; start=$(( end + 1 ))
  done
  psql_copy "$(snapshot_sql "$(group_dids "$g")" "$(since_file "$g")" "$V1")" | emit "restore-$g-after-rows.tsv"
  cmp -s "$pre" "$E/restore-$g-after-rows.tsv" && echo "restore_$g=identical_to_prestate rows=$total batches=$batch" | emit "restore-$g-result.txt" || { diff "$pre" "$E/restore-$g-after-rows.tsv" | head -20 | emit "restore-$g-result.txt"; die "restore $g: after-restore snapshot differs from prestate"; }
  log "restore $g complete: identical to prestate"
}
cmd_secret_scan() {
  local hits=0 out
  out=$( ( set +e; pw=$(sudo -n grep -m1 '^FEEDGEN_DB_PASSWORD=' "$ENV_FILE" | cut -d= -f2- | tr -d "\"'"); [[ -n "$pw" ]] && sudo -n grep -rlF -- "$pw" "$E" ; sudo -n grep -rlE 'postgresql://[^ ]+:[^ ]+@|FEEDGEN_DB_PASSWORD=|app_password' "$E" | grep -v '/secret-scan.txt$' ) | sort -u )
  [[ -n "$out" ]] && hits=$(echo "$out" | wc -l)
  { echo "generated_at=$(ts)"; echo "hits=$hits"; [[ -n "$out" ]] && echo "$out"; } | emit secret-scan.txt
  (( hits == 0 )) || die "secret scan found $hits file(s) with secret markers; do NOT append the ledger row"
  log "secret scan clean"
}
cmd_finalize() {
  local wstart=${1:-unset} wend=${2:-unset}
  { echo "status=complete"; echo "generated_at=$(ts)"; cat "$E/source-set.txt"; echo "window_start_utc=$wstart"; echo "window_end_utc=$wend"; for f in "$E"/ceiling-*.txt; do [[ -f "$f" ]] && { echo "--- $(basename "$f")"; cat "$f"; }; done; echo "--- diffs"; cat "$E"/step1-*-diff.txt 2>/dev/null; echo "--- populations after"; cat "$E/poststate-populations.tsv"; } | emit RESULT.txt
  ( cd "$E" && sudo -n sh -c 'sha256sum $(ls | grep -v "^SHA256SUMS$") > SHA256SUMS && chown root:newsflows SHA256SUMS && chmod 640 SHA256SUMS' )
  log "finalized: $E"
}

case "${1:-}" in
  preflight) cmd_preflight;;
  preview) cmd_preview "${2:?group}";;
  apply) cmd_apply "${2:?group}" "${3:?label}" "${4:-}";;
  readback) cmd_readback;;
  restore) cmd_restore "${2:?group}";;
  secret-scan) cmd_secret_scan;;
  finalize) cmd_finalize "${2:-}" "${3:-}";;
  *) echo "usage: $0 preflight|preview <group>|apply <group> <label> [max_batches]|readback|restore <group>|secret-scan|finalize <start> <end>" >&2; exit 2;;
esac
