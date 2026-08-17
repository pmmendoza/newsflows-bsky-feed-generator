#!/usr/bin/env bash
# Disposable-Postgres-17 rehearsal for the content-time v1->v2 revalidation
# mode (src/tools/backfill-publisher-posts.ts --mode revalidate). Operator-run
# ON THE SERVER (or any host with docker + a built feedgen checkout). Mirrors
# the shape of the "V7" bounded-recovery rehearsal
# (scripts/test_content_time_recovery_execute.ts, added in commit 60561d1):
# same disposable-DB posture, same bounded-batch assertions, same WAL bytes /
# relation growth / dead tuple capture.
#
# What it does, in order:
#   1. Starts a disposable postgres:17 container (no volume, port on
#      127.0.0.1 only, torn down on exit).
#   2. Bootstraps the one table (feedgen_ops.feed_catalog) that the Kysely
#      migration chain assumes already exists (see NOTE below), then applies
#      every committed migration in src/db/migrations.ts via the already
#      built dist/scripts/migrate.js.
#   3. Seeds feed_catalog with two enabled publisher DIDs and one disabled
#      publisher DID, plus a transition matrix of public.post rows (v1-valid,
#      v1-invalid/past_bound, v1-valid/future_skew-under-v2, an
#      already-v2 control, a legacy_unknown control, a disabled-publisher
#      control, and an out-of-window control), plus 502 cheap generated
#      v1-valid rows so the CLI phase has more than one 500-row batch of work.
#   4. Exercises the *built CLI* end-to-end: dry-run preview (default
#      publisher-DID resolution from feed_catalog, --packet-sha256 accepted
#      and echoed), apply --max-batches 1 (stops after exactly one 500-row
#      batch, exit 3, WAL bytes / relation growth / dead tuples captured
#      around that single bounded call -- the "one batch, check the D4
#      ceilings, continue" workflow), apply resume with the same
#      --checkpoint-file and no --max-batches (finishes the remaining rows,
#      exit 0), a packet-hash-mismatch rejection against that checkpoint, a
#      missing---packet-sha256 rejection, and an idempotent re-apply --
#      proving the catalog-driven selection and the checkpoint/receipt/
#      packet-binding contract exactly as an operator would invoke them.
#   5. Runs scripts/test_content_time_revalidate_execute.ts against the same
#      container for the parts that need direct control over batch size/
#      timing: forced-stop + resume, checkpoint-mismatch rejection, packet-
#      sha256 format/binding rejection, the CAS predicate proof, duration
#      stop, inter-batch pause, lock timeout, the per-batch `batches[]`
#      breakdown, and the WAL bytes / relation growth / dead tuple capture
#      at 500-row scale.
#   6. Tears the container down.
#
# NOTE on feedgen_ops.feed_catalog: no migration in src/db/migrations.ts
# creates this table -- migration 008_feed_catalog_ranker_score_source
# explicitly RAISEs if it is missing, because in every real environment the
# table is provisioned once by the bsky-ops catalog-sync-apply path, outside
# the feedgen migration chain (see AGENTS.md: "feedgen_ops.feed_catalog is
# feedgen-owned serving/readback state"). This script's step 2 recreates the
# same minimal bootstrap shape used by
# rehearsal/feedgen-serving-no-archive/schema_bootstrap.sql for exactly this
# reason. This is not a production migration and must never run against a
# non-disposable database.
#
# Requires on PATH: docker (or passwordless sudo docker), psql, node. Needs
# nothing else: it runs the already-built dist/ output, it does not compile
# TypeScript itself (except for step 5, which uses ts-node from the
# already-installed node_modules devDependency, exactly like
# `yarn test:content-time-revalidate` does).
#
# Usage (run from the feedgen repo root, after `yarn install && yarn build`):
#   bash scripts/rehearse_content_time_revalidate.sh
#
# Env overrides:
#   FEEDGEN_REVALIDATE_REHEARSAL_DOCKER   docker invocation prefix
#                                         (default: auto-detect plain "docker"
#                                         vs "sudo -n docker")
#   FEEDGEN_REVALIDATE_REHEARSAL_KEEP=1   skip teardown for post-mortem
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ ! -f dist/scripts/migrate.js || ! -f dist/tools/backfill-publisher-posts.js ]]; then
  echo "ERROR: dist/ is missing the built migrate/backfill-publisher-posts output." >&2
  echo "Run 'yarn install && yarn build' in $repo_root first." >&2
  exit 2
fi

for tool in psql node; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: required tool not found: $tool" >&2
    exit 2
  fi
done

if [[ -n "${FEEDGEN_REVALIDATE_REHEARSAL_DOCKER:-}" ]]; then
  read -r -a DOCKER <<<"$FEEDGEN_REVALIDATE_REHEARSAL_DOCKER"
elif docker info >/dev/null 2>&1; then
  DOCKER=(docker)
elif sudo -n docker info >/dev/null 2>&1; then
  DOCKER=(sudo -n docker)
else
  echo "ERROR: docker is not reachable (tried plain 'docker' and 'sudo -n docker')." >&2
  exit 2
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)-$$"
container="feedgen-revalidate-rehearsal-${stamp}"
port="$((35000 + ($$ % 1000)))"
db_name="feedgen_revalidate_rehearsal"
db_user="feedgen"
db_password="feedgen"
dsn="postgresql://${db_user}:${db_password}@127.0.0.1:${port}/${db_name}"
workdir="$(mktemp -d "${TMPDIR:-/tmp}/feedgen-revalidate-rehearsal-XXXXXX")"
checkpoint_file="${workdir}/checkpoint.json"
# Fixed dummy packet hash for rehearsal, same convention as the V7 rehearsal's
# PACKET_SHA = '1'.repeat(64) in test_content_time_recovery_execute.ts. Not a
# real approved production packet -- this script never touches production.
packet_sha256="$(printf '1%.0s' $(seq 1 64))"

cleanup() {
  if [[ "${FEEDGEN_REVALIDATE_REHEARSAL_KEEP:-}" == "1" ]]; then
    echo "FEEDGEN_REVALIDATE_REHEARSAL_KEEP=1: leaving container ${container} and ${workdir} up for inspection." >&2
    return
  fi
  "${DOCKER[@]}" rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$workdir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "status=starting container=${container} port=${port}"

"${DOCKER[@]}" run -d --name "$container" \
  -p "127.0.0.1:${port}:5432" \
  -e POSTGRES_USER="$db_user" \
  -e POSTGRES_PASSWORD="$db_password" \
  -e POSTGRES_DB="$db_name" \
  postgres:17 >/dev/null

db_ready=false
for _ in $(seq 1 60); do
  if "${DOCKER[@]}" exec "$container" pg_isready -U "$db_user" -d "$db_name" >/dev/null 2>&1; then
    db_ready=true
    break
  fi
  sleep 1
done
if [[ "$db_ready" != true ]]; then
  echo "ERROR: disposable Postgres did not become ready within 60s" >&2
  exit 1
fi
echo "status=db_ready"

# --- Step 2: bootstrap feedgen_ops.feed_catalog, then apply migrations ----

"${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE SCHEMA IF NOT EXISTS feedgen_ops;

CREATE TABLE feedgen_ops.feed_catalog (
  feed_id text PRIMARY KEY,
  rkey text NOT NULL UNIQUE,
  display_name text NOT NULL,
  country text,
  publisher_did text,
  study_id text,
  algo_policy_id text NOT NULL,
  ranker_policy_id text,
  access_policy_id text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  retired_at timestamptz
);
SQL
echo "status=feed_catalog_bootstrapped"

FEEDGEN_POSTGRES_URL="$dsn" node dist/scripts/migrate.js >"${workdir}/migrate.log" 2>&1
echo "status=migrations_applied"

# --- Step 3: seed the publisher catalog and the post transition matrix ----

publisher_a="did:plc:revalidate-rehearsal-publisher-a"
publisher_b="did:plc:revalidate-rehearsal-publisher-b"
publisher_disabled="did:plc:revalidate-rehearsal-publisher-disabled"

"${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 >/dev/null <<SQL
INSERT INTO feedgen_ops.feed_catalog
  (feed_id, rkey, display_name, country, publisher_did, algo_policy_id, access_policy_id, enabled)
VALUES
  ('newsflow-zz-1-a', 'newsflow-zz-1a', 'Rehearsal A', 'ZZ', '${publisher_a}', 'chronological', 'subscriber-default', true),
  ('newsflow-zz-1-b', 'newsflow-zz-1b', 'Rehearsal B', 'ZZ', '${publisher_b}', 'chronological', 'subscriber-default', true),
  ('newsflow-zz-1-d', 'newsflow-zz-1d', 'Rehearsal Disabled', 'ZZ', '${publisher_disabled}', 'chronological', 'subscriber-default', false);
SQL

# Content-time transition matrix. Columns match migration 011's additions to
# public.post (created_at_source_raw bytea, content_time_utc/status/
# clamp_reason/validator_version text). Window floor for --since is
# 2026-08-01T00:00:00.000Z; the out-of-window row predates it.
"${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 >/dev/null <<SQL
INSERT INTO public.post (
  uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
  link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
  created_at_source_raw, content_time_utc, content_time_status,
  content_time_clamp_reason, content_time_validator_version
) VALUES
  -- v1-valid, stays valid under v2; content_time_utc must not change.
  ('at://${publisher_a}/app.bsky.feed.post/valid-stays-valid', 'cid-1',
   '2026-08-12T12:00:00.000Z', '2026-08-02T12:00:00.000Z', '${publisher_a}', 'valid-stays-valid', '', '',
   '', '', '', '', '', '',
   convert_to('2026-08-02T12:00:00+00:00', 'UTF8'), '2026-08-02T12:00:00.000Z', 'source_valid',
   NULL, 'newsflows-content-time/v1'),
  -- v1-invalid (past_bound); v2 has no past bound, must flip valid.
  ('at://${publisher_a}/app.bsky.feed.post/past-bound-becomes-valid', 'cid-2',
   '2026-08-12T12:00:00.000Z', '2026-08-12T12:00:00.000Z', '${publisher_a}', 'past-bound-becomes-valid', '', '',
   '', '', '', '', '', '',
   convert_to('2020-01-01T00:00:00Z', 'UTF8'), NULL, 'source_invalid',
   'past_bound', 'newsflows-content-time/v1'),
  -- v1-valid but 1h in the future: inside v1's 24h skew, outside v2's 5min.
  ('at://${publisher_b}/app.bsky.feed.post/future-skew-becomes-invalid', 'cid-3',
   '2026-08-12T12:00:00.000Z', '2026-08-12T13:00:00.000Z', '${publisher_b}', 'future-skew-becomes-invalid', '', '',
   '', '', '', '', '', '',
   convert_to('2026-08-12T13:00:00.000Z', 'UTF8'), '2026-08-12T13:00:00.000Z', 'source_valid',
   NULL, 'newsflows-content-time/v1'),
  -- Control: already on v2. Must never be touched.
  ('at://${publisher_a}/app.bsky.feed.post/already-v2-control', 'cid-4',
   '2026-08-12T12:00:00.000Z', '2026-08-02T12:00:00.000Z', '${publisher_a}', 'already-v2-control', '', '',
   '', '', '', '', '', '',
   convert_to('2026-08-02T12:00:00.000Z', 'UTF8'), '2026-08-02T12:00:00.000Z', 'source_valid',
   NULL, 'newsflows-content-time/v2'),
  -- Control: legacy_unknown (raw NULL). Must never be touched.
  ('at://${publisher_a}/app.bsky.feed.post/legacy-unknown-control', 'cid-5',
   '2026-08-12T12:00:00.000Z', '2026-08-12T12:00:00.000Z', '${publisher_a}', 'legacy-unknown-control', '', '',
   '', '', '', '', '', '',
   NULL, NULL, NULL, NULL, NULL),
  -- Control: disabled publisher. Must never be selected by default resolution.
  ('at://${publisher_disabled}/app.bsky.feed.post/disabled-publisher-control', 'cid-6',
   '2026-08-12T12:00:00.000Z', '2026-08-02T12:00:00.000Z', '${publisher_disabled}', 'disabled-publisher-control', '', '',
   '', '', '', '', '', '',
   convert_to('2026-08-02T12:00:00+00:00', 'UTF8'), '2026-08-02T12:00:00.000Z', 'source_valid',
   NULL, 'newsflows-content-time/v1'),
  -- Control: v1 but before the --since window. Must never be touched.
  ('at://${publisher_a}/app.bsky.feed.post/out-of-window-control', 'cid-7',
   '2026-07-01T00:00:00.000Z', '2026-06-01T12:00:00.000Z', '${publisher_a}', 'out-of-window-control', '', '',
   '', '', '', '', '', '',
   convert_to('2026-06-01T12:00:00+00:00', 'UTF8'), '2026-06-01T12:00:00.000Z', 'source_valid',
   NULL, 'newsflows-content-time/v1');
SQL

# 502 extra cheap v1-valid in-window rows for publisher A, generated instead
# of listed, so the CLI phase below has more than one batch's worth of work
# (REVALIDATION_LIMITS.batchSize=500 is not CLI-overridable) and can prove
# --max-batches 1 stopping with real work remaining, then resuming with the
# same --checkpoint-file. Combined with the 3-row transition matrix above,
# this brings the total in-window v1 candidate count to 505 (500 + 5).
"${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 >/dev/null <<SQL
INSERT INTO public.post (
  uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
  link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
  created_at_source_raw, content_time_utc, content_time_status,
  content_time_clamp_reason, content_time_validator_version
)
SELECT
  'at://${publisher_a}/app.bsky.feed.post/bulk-cli-' || lpad(g::text, 4, '0'),
  'cid-bulk-cli-' || g::text,
  '2026-08-12T12:00:00.000Z',
  '2026-08-02T12:00:00.000Z',
  '${publisher_a}',
  'bulk-cli-' || g::text,
  '', '', '', '', '', '', '', '',
  convert_to('2026-08-02T12:00:00+00:00', 'UTF8'),
  '2026-08-02T12:00:00.000Z',
  'source_valid',
  NULL,
  'newsflows-content-time/v1'
FROM generate_series(1, 502) AS g;
SQL
echo "status=seed_complete"

# --- Step 4: exercise the built CLI end-to-end -----------------------------

preview_json="${workdir}/preview.json"
FEEDGEN_POSTGRES_URL="$dsn" node dist/tools/backfill-publisher-posts.js --mode revalidate \
  --since 2026-08-01T00:00:00.000Z --json --packet-sha256 "$packet_sha256" \
  >"$preview_json" 2>"${workdir}/preview.err"

scanned="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).preview.scanned)" "$preview_json")"
valid_to_valid="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).preview.counts.v1_valid_to_v2_valid)" "$preview_json")"
invalid_to_valid="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).preview.counts.v1_invalid_to_v2_valid)" "$preview_json")"
to_invalid="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).preview.counts.v1_to_v2_invalid)" "$preview_json")"
actor_count="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).actor_count)" "$preview_json")"
preview_packet_echo="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).packet_sha256)" "$preview_json")"

preview_ok=false
if [[ "$scanned" == "505" && "$valid_to_valid" == "503" && "$invalid_to_valid" == "1" && "$to_invalid" == "1" \
  && "$actor_count" == "2" && "$preview_packet_echo" == "$packet_sha256" ]]; then
  preview_ok=true
fi
echo "status=cli_preview scanned=${scanned} v1_valid_to_v2_valid=${valid_to_valid} v1_invalid_to_v2_valid=${invalid_to_valid} v1_to_v2_invalid=${to_invalid} actor_count=${actor_count} packet_echoed=$([ "$preview_packet_echo" == "$packet_sha256" ] && echo true || echo false) preview_ok=${preview_ok}"

# --max-batches 1: apply must stop after exactly one 500-row batch, report
# complete=false, and exit 3 -- with 5 rows still pending. This is the
# "run one batch, measure deltas against the D4 ceilings" workflow: capture
# WAL bytes / relation growth / dead tuples around this single bounded call.
"${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
ANALYZE public.post;
SQL
batch1_stats_before="$("${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -At <<'SQL'
SELECT pg_current_wal_lsn()::text || '|' || pg_total_relation_size('public.post')::text || '|' || n_dead_tup::text
FROM pg_stat_user_tables WHERE schemaname = 'public' AND relname = 'post';
SQL
)"
batch1_wal_before="${batch1_stats_before%%|*}"
batch1_rest="${batch1_stats_before#*|}"
batch1_bytes_before="${batch1_rest%%|*}"
batch1_dead_before="${batch1_rest#*|}"

apply1_json="${workdir}/apply_batch1.json"
set +e
FEEDGEN_POSTGRES_URL="$dsn" node dist/tools/backfill-publisher-posts.js --mode revalidate --apply \
  --since 2026-08-01T00:00:00.000Z --json --packet-sha256 "$packet_sha256" \
  --checkpoint-file "$checkpoint_file" --max-batches 1 \
  >"$apply1_json" 2>"${workdir}/apply_batch1.err"
apply1_exit=$?
set -e

"${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
SELECT pg_stat_force_next_flush();
ANALYZE public.post;
SQL
batch1_stats_after="$("${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -At <<SQL
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '${batch1_wal_before}')::text || '|' || pg_total_relation_size('public.post')::text || '|' || n_dead_tup::text
FROM pg_stat_user_tables WHERE schemaname = 'public' AND relname = 'post';
SQL
)"
batch1_wal_bytes="${batch1_stats_after%%|*}"
batch1_rest_after="${batch1_stats_after#*|}"
batch1_bytes_after="${batch1_rest_after%%|*}"
batch1_dead_after="${batch1_rest_after#*|}"

apply1_updated="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.updated)" "$apply1_json")"
apply1_complete="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.complete)" "$apply1_json")"
apply1_batches_len="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.batches.length)" "$apply1_json")"
apply1_packet="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.packet_sha256)" "$apply1_json")"
echo "status=cli_apply_batch1 exit=${apply1_exit} updated=${apply1_updated} complete=${apply1_complete} batches_len=${apply1_batches_len} packet=$([ "$apply1_packet" == "$packet_sha256" ] && echo ok || echo mismatch) wal_bytes=${batch1_wal_bytes} relation_bytes_before=${batch1_bytes_before} relation_bytes_after=${batch1_bytes_after} dead_tuples_before=${batch1_dead_before} dead_tuples_after=${batch1_dead_after}"

# Resume with the SAME checkpoint file, no --max-batches (unlimited): must
# finish the remaining 5 rows, complete=true, exit 0.
apply2_json="${workdir}/apply_batch2.json"
FEEDGEN_POSTGRES_URL="$dsn" node dist/tools/backfill-publisher-posts.js --mode revalidate --apply \
  --since 2026-08-01T00:00:00.000Z --json --packet-sha256 "$packet_sha256" \
  --checkpoint-file "$checkpoint_file" \
  >"$apply2_json" 2>"${workdir}/apply_batch2.err"
apply2_exit=$?
apply2_updated="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.updated)" "$apply2_json")"
apply2_complete="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.complete)" "$apply2_json")"
echo "status=cli_apply_resume exit=${apply2_exit} updated=${apply2_updated} complete=${apply2_complete}"

# Mismatched packet against the same checkpoint file must be rejected.
mismatch_err=0
FEEDGEN_POSTGRES_URL="$dsn" node dist/tools/backfill-publisher-posts.js --mode revalidate --apply \
  --since 2026-08-01T00:00:00.000Z --json --packet-sha256 "$(printf '2%.0s' $(seq 1 64))" \
  --checkpoint-file "$checkpoint_file" \
  >"${workdir}/apply_packet_mismatch.json" 2>"${workdir}/apply_packet_mismatch.err" || mismatch_err=$?
packet_mismatch_rejected=false
if [[ "$mismatch_err" -ne 0 ]] && grep -q 'does not match the approved revalidation packet' "${workdir}/apply_packet_mismatch.err"; then
  packet_mismatch_rejected=true
fi
echo "status=cli_packet_mismatch rejected=${packet_mismatch_rejected}"

# Apply without --packet-sha256 at all must be rejected outright.
missing_packet_err=0
FEEDGEN_POSTGRES_URL="$dsn" node dist/tools/backfill-publisher-posts.js --mode revalidate --apply \
  --since 2026-08-01T00:00:00.000Z --json \
  --checkpoint-file "${workdir}/no-packet-checkpoint.json" \
  >"${workdir}/apply_missing_packet.json" 2>"${workdir}/apply_missing_packet.err" || missing_packet_err=$?
missing_packet_rejected=false
if [[ "$missing_packet_err" -ne 0 ]] && grep -q -- '--packet-sha256 is required with --apply' "${workdir}/apply_missing_packet.err"; then
  missing_packet_rejected=true
fi
echo "status=cli_missing_packet rejected=${missing_packet_rejected}"

rerun_json="${workdir}/rerun.json"
FEEDGEN_POSTGRES_URL="$dsn" node dist/tools/backfill-publisher-posts.js --mode revalidate --apply \
  --since 2026-08-01T00:00:00.000Z --json --packet-sha256 "$packet_sha256" \
  --checkpoint-file "${workdir}/rerun-checkpoint.json" \
  >"$rerun_json" 2>"${workdir}/rerun.err"
rerun_updated="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.updated)" "$rerun_json")"
rerun_scanned="$(node -e "console.log(JSON.parse(require('fs').readFileSync(process.argv[1],'utf8')).revalidation.scanned)" "$rerun_json")"
echo "status=cli_idempotent_rerun updated=${rerun_updated} scanned=${rerun_scanned}"

cli_ok=false
if [[ "$preview_ok" == true \
  && "$apply1_exit" == "3" && "$apply1_updated" == "500" && "$apply1_complete" == "false" && "$apply1_batches_len" == "1" \
  && "$apply2_exit" == "0" && "$apply2_updated" == "5" && "$apply2_complete" == "true" \
  && "$packet_mismatch_rejected" == true && "$missing_packet_rejected" == true \
  && "$rerun_updated" == "0" && "$rerun_scanned" == "0" ]]; then
  cli_ok=true
fi

readback="$("${DOCKER[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -At <<SQL
SELECT 'disabled_publisher_untouched=' || (content_time_validator_version = 'newsflows-content-time/v1')
FROM public.post WHERE uri = 'at://${publisher_disabled}/app.bsky.feed.post/disabled-publisher-control';
SELECT 'out_of_window_untouched=' || (content_time_validator_version = 'newsflows-content-time/v1')
FROM public.post WHERE uri = 'at://${publisher_a}/app.bsky.feed.post/out-of-window-control';
SELECT 'legacy_unknown_untouched=' || (content_time_validator_version IS NULL)
FROM public.post WHERE uri = 'at://${publisher_a}/app.bsky.feed.post/legacy-unknown-control';
SELECT 'v2_control_untouched=' || (content_time_utc = '2026-08-02T12:00:00.000Z')
FROM public.post WHERE uri = 'at://${publisher_a}/app.bsky.feed.post/already-v2-control';
SELECT 'future_skew_reason=' || content_time_clamp_reason
FROM public.post WHERE uri = 'at://${publisher_b}/app.bsky.feed.post/future-skew-becomes-invalid';
SQL
)"
echo "$readback"
if ! grep -q 'disabled_publisher_untouched=t' <<<"$readback" \
  || ! grep -q 'out_of_window_untouched=t' <<<"$readback" \
  || ! grep -q 'legacy_unknown_untouched=t' <<<"$readback" \
  || ! grep -q 'v2_control_untouched=t' <<<"$readback" \
  || ! grep -q 'future_skew_reason=future_skew' <<<"$readback"; then
  cli_ok=false
fi

echo "status=cli_phase cli_ok=${cli_ok}"

# --- Step 5: TS execute rehearsal (resume/CAS/bounds/WAL on the same DB) --

set +e
FEEDGEN_TEST_DSN="$dsn" \
FEEDGEN_CONTENT_TIME_REVALIDATE_REHEARSAL=1 \
  npx ts-node scripts/test_content_time_revalidate_execute.ts >"${workdir}/execute.log" 2>&1
execute_rc=$?
set -e
tail -n 5 "${workdir}/execute.log" || true
execute_ok=false
if [[ "$execute_rc" -eq 0 ]] && grep -q '"status":"pass"' "${workdir}/execute.log"; then
  execute_ok=true
fi
echo "status=execute_phase execute_rc=${execute_rc} execute_ok=${execute_ok}"

# --- Summary ----------------------------------------------------------------

raw_values=false
if grep -Eiq '(postgres(ql)?://[^[:space:]]+:[^[:space:]]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----)' \
  "${workdir}"/*.log "${workdir}"/*.json 2>/dev/null; then
  raw_values=true
fi

overall=partial
if [[ "$cli_ok" == true && "$execute_ok" == true && "$raw_values" == false ]]; then
  overall=ok
fi

echo "status=${overall}"
echo "container=${container}"
echo "cli_ok=${cli_ok}"
echo "execute_ok=${execute_ok}"
echo "raw_values_in_output=${raw_values}"
echo "workdir=${workdir}"

if [[ "$overall" != ok ]]; then
  echo "See ${workdir}/*.log and ${workdir}/*.json for details (set FEEDGEN_REVALIDATE_REHEARSAL_KEEP=1 to keep them)." >&2
  exit 1
fi
