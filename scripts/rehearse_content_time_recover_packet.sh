#!/usr/bin/env bash
# Rehearses scripts/content_time_recover_packet.sh end-to-end, in RUNNER=host
# mode (a local `node` process talking directly to the disposable postgres via
# HOST_DSN; SKIP_LIVE_IMAGE_CHECKS=1 so no live feedgen image/container is
# needed), against a disposable postgres:17 container previously prepared by
# scripts/rehearse_content_time_revalidate.sh with
# FEEDGEN_REVALIDATE_REHEARSAL_KEEP=1. A tiny local Node HTTP server stands in
# for the Bluesky AppView's app.bsky.feed.getPosts, so no network egress is
# needed. Seeds 1,260 legacy rows for a rehearsal publisher (1,240
# retrievable -- 1,230 valid + 10 future-skew -- and 20 not retrievable),
# then drives: prereg -> preflight -> preview -> batch-gated apply (1 batch,
# a negative BREACH probe, then the rest) -> readback (incl. a forced-failure
# re-run) -> restore (incl. a forced lock-timeout + resume, then a no-op
# re-run) -> secret-scan -> finalize.
#
# Usage (server, from the built feedgen tree; the disposable postgres
# container/port come from a prior
# FEEDGEN_REVALIDATE_REHEARSAL_KEEP=1 bash scripts/rehearse_content_time_revalidate.sh run):
#   bash scripts/rehearse_content_time_recover_packet.sh <container> <port> [evidence_root]
set -euo pipefail
container=${1:?container}; port=${2:?port}
E=${3:-/tmp/recover-rehearsal-$(date -u +%Y%m%dT%H%M%SZ)}
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner="$repo_root/scripts/content_time_recover_packet.sh"
DOCKER=${FEEDGEN_REVALIDATE_REHEARSAL_DOCKER:-sudo -n docker}
read -r -a D <<<"$DOCKER"
pub_did="did:plc:recover-rehearsal-publisher"
rehearsal_pw="recover-rehearsal-not-a-real-secret-4b1a"
db_name="feedgen_revalidate_rehearsal"
db_user="feedgen"
psql() { "${D[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -X -A -F '|' -v ON_ERROR_STOP=1 "$@"; }

workdir="$(mktemp -d "${TMPDIR:-/tmp}/recover-rehearsal-work-XXXXXX")"
mock_port="$((45000 + ($$ % 1000)))"
mock_pid=""
cleanup() {
  [[ -n "$mock_pid" ]] && kill "$mock_pid" >/dev/null 2>&1 || true
  rm -rf "$workdir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- window + timestamps -----------------------------------------------------
# HORIZON_DAYS=10; SINCE must be at/before the rolling now-10d boundary, UNTIL <= now.
SINCE="$(date -u -d '10 days ago' +%Y-%m-%dT%H:%M:%S.000Z)"
UNTIL="$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S.000Z)"
row_indexed_iso="$(date -u -d '5 days ago' +%Y-%m-%dT%H:%M:%S.000Z)"           # comfortably inside [SINCE,UNTIL)
valid_created_iso="$(date -u -d "$row_indexed_iso -1 hour" +%Y-%m-%dT%H:%M:%S.000Z)"   # well inside the v2 window (no past bound)
skew_created_iso="$(date -u -d "$row_indexed_iso +6 minutes" +%Y-%m-%dT%H:%M:%S.000Z)" # > indexedAt + 5min -> future_skew

echo "status=window since=$SINCE until=$UNTIL row_indexed=$row_indexed_iso"

# --- neutralize the base's own zz-1a/1b/1d catalog rows, then seed ours -----
# RECOVER_RKEY_PATTERN below is a broad 'newsflow-zz-.*' wildcard; the base script
# (rehearse_content_time_revalidate.sh) seeds enabled rows newsflow-zz-1a/1b under
# different publisher DIDs that would also match that wildcard and break the
# catalog-DID-equality gate, so they are disabled here before adding our own row.
psql <<SQL >/dev/null
ALTER USER "$db_user" PASSWORD '$rehearsal_pw';
UPDATE feedgen_ops.feed_catalog SET enabled = false WHERE rkey IN ('newsflow-zz-1a', 'newsflow-zz-1b');
DELETE FROM feedgen_ops.feed_catalog WHERE rkey = 'newsflow-zz-2a';
INSERT INTO feedgen_ops.feed_catalog
  (feed_id, rkey, display_name, country, publisher_did, algo_policy_id, access_policy_id, enabled)
VALUES
  ('newsflow-zz-2-a', 'newsflow-zz-2a', 'Rehearsal Recover', 'ZZ', '$pub_did', 'chronological', 'subscriber-default', true);
UPDATE feedgen_ops.feed_catalog SET publisher_post_max_age_days = 10, publisher_post_max_age_source = 'study_default' WHERE rkey = 'newsflow-zz-2a';
DELETE FROM public.post WHERE uri LIKE 'at://%/app.bsky.feed.post/rec-%';
INSERT INTO public.post (
  uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
  link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
  created_at_source_raw, content_time_utc, content_time_status,
  content_time_clamp_reason, content_time_validator_version
)
SELECT
  'at://$pub_did/app.bsky.feed.post/rec-' || lpad(g::text, 4, '0'),
  'cid-rec-' || g::text,
  '$row_indexed_iso',
  '$row_indexed_iso',
  '$pub_did',
  'rec-' || g::text,
  '', '', '', '', '', '', '', '',
  NULL, NULL, NULL, NULL, NULL
FROM generate_series(1, 1260) AS g;
SQL
echo "status=seeded legacy_rows=1260 retrievable=1240 unretrievable=20"

# --- mock AppView (app.bsky.feed.getPosts) -----------------------------------
manifest="$workdir/manifest.json"
{
  echo '['
  for i in $(seq 1 1240); do
    uri="at://$pub_did/app.bsky.feed.post/rec-$(printf '%04d' "$i")"
    cid="cid-rec-$i"
    if (( i <= 1230 )); then created="$valid_created_iso"; else created="$skew_created_iso"; fi
    sep=","; [[ "$i" == "1" ]] && sep=""
    printf '%s{"uri":"%s","cid":"%s","author":{"did":"%s"},"indexedAt":"%s","record":{"text":"rec-%d","createdAt":"%s"}}\n' \
      "$sep" "$uri" "$cid" "$pub_did" "$row_indexed_iso" "$i" "$created"
  done
  echo ']'
} >"$manifest"

mockjs="$workdir/mock_appview.js"
cat >"$mockjs" <<'JS'
const http = require('http')
const fs = require('fs')
const manifest = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'))
const byUri = new Map(manifest.map((p) => [p.uri, p]))
const port = Number(process.argv[3])
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${port}`)
  if (url.pathname !== '/xrpc/app.bsky.feed.getPosts') {
    res.writeHead(404); res.end(); return
  }
  const uris = url.searchParams.getAll('uris')
  const posts = uris.map((u) => byUri.get(u)).filter(Boolean)
  res.writeHead(200, { 'content-type': 'application/json' })
  res.end(JSON.stringify({ posts }))
})
server.listen(port, '127.0.0.1', () => console.log(`status=mock_appview_listening port=${port}`))
JS
node "$mockjs" "$manifest" "$mock_port" >"$workdir/mock_appview.log" 2>&1 &
mock_pid=$!
mock_ready=false
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$mock_port/xrpc/app.bsky.feed.getPosts?uris=probe" >/dev/null 2>&1; then mock_ready=true; break; fi
  sleep 0.5
done
[[ "$mock_ready" == true ]] || { cat "$workdir/mock_appview.log" >&2; echo "mock AppView did not become ready"; exit 1; }
echo "status=mock_appview_ready port=$mock_port"

# cmd_secret_scan always reads $ENV_FILE (default: the REAL production secrets
# file) to build its hunt-list -- a rehearsal must never point that at
# /etc/newsflows/secrets/feedgen.env, so give it a scratch env-file containing
# only the rehearsal DB password under a PASSWORD-bearing key.
envfile="$workdir/envfile"
{ echo "FEEDGEN_DB_USER=$db_user"; echo "FEEDGEN_DB_PASSWORD=$rehearsal_pw"; echo "FEEDGEN_DB_HOST=127.0.0.1"; echo "FEEDGEN_DB_PORT=$port"; echo "FEEDGEN_DB_DATABASE=$db_name"; echo "SOME_HOSTNAME_CONFIG=$container"; } >"$envfile"
chmod 600 "$envfile"

# --- runner env (RUNNER=host: a local node process against HOST_DSN; the
# psql_ro/psql_copy helpers always docker-exec into DB_CONTAINER regardless
# of RUNNER, so DOCKER/DB_CONTAINER/PSQL_* must still be set) ----------------
export E TREE="$repo_root" RUNNER=host \
  HOST_DSN="postgresql://${db_user}:${rehearsal_pw}@127.0.0.1:${port}/${db_name}" \
  DOCKER="$DOCKER" DB_CONTAINER="$container" PSQL_DB="$db_name" PSQL_USER="$db_user" \
  ENV_FILE="$envfile" \
  EXPECTED_SHA="$(git -C "$repo_root" rev-parse HEAD)" \
  EXPECTED_DIST_SHA256="$(sha256sum "$repo_root/dist/tools/backfill-publisher-posts.js" | cut -d' ' -f1)" \
  EXPECTED_CT_SHA256="$(sha256sum "$repo_root/dist/util/content-time.js" | cut -d' ' -f1)" \
  EXPECTED_RUNNER_SHA256="$(sha256sum "$repo_root/scripts/content_time_recover_packet.sh" | cut -d' ' -f1)" \
  EXPECTED_DSN_HELPER_SHA256="$(sha256sum "$repo_root/scripts/compose_feedgen_dsn.js" | cut -d' ' -f1)"
export EXPECTED_IMAGE_CT_SHA256="$EXPECTED_CT_SHA256"   # unused: SKIP_LIVE_IMAGE_CHECKS=1 never reads this
export SKIP_LIVE_IMAGE_CHECKS=1
export EXPECTED_TOOL_REFS="bsky-ops=rehearsal,blueskyranker=rehearsal,newsflows-bskyhealth=rehearsal"   # unused under SKIP_LIVE_IMAGE_CHECKS=1
export RECOVER_DIDS="$pub_did" RECOVER_RKEY_PATTERN='newsflow-zz-.*' HORIZON_DAYS=10 SINCE UNTIL EXPECTED_RKEYS=newsflow-zz-2a PERMIT_NEGATIVE_PROBE=1
export API_BASE="http://127.0.0.1:${mock_port}"
export PACKET_SHA="$(printf '2%.0s' $(seq 1 64))"
rb=$(mktemp); now=$(date -u +%s)
printf '{"schema_version":"bsr.ops.effective_config.readback.v1","raw_values_in_output":false,"artifact_metadata":{"generated_at":%s,"stale_at":%s},"bindings":{"reh-recover":{"feed_ids":["newsflow-zz-2a"],"push_window_days":10,"time_column":"createdAt"}}}\n' "$now" "$((now+3600))" >"$rb"
export READBACK_JSON="$rb"

# --- pre-registration flow: PREREG is the REVIEWED table's values (here: the seeded ground truth), never re-bound from the
# read-only prereg output; prereg must merely REPRODUCE them (a difference = STOP, not a re-bind)
export PREREG="legacy_in_window=1260,unretrievable=20,would_recover=1240,recover_source_valid=1230,recover_source_invalid=10,would_insert=0,conflict=0"
prereg_out=$(bash "$runner" prereg); echo "$prereg_out" | sed 's/^/prereg: /'
[[ "$(echo "$prereg_out" | sed -n 's/^PREREG=//p')" == "$PREREG" ]] \
  || { echo "prereg did not reproduce the pre-registered cells: $(echo "$prereg_out" | sed -n 's/^PREREG=//p') vs $PREREG"; exit 1; }

step() { echo "status=$1"; shift; "$@"; }
# NEGATIVE legs of the participant-safety gates (each must stop preflight before writing prestate/plan artifacts):
psql -c "UPDATE feedgen_ops.feed_catalog SET publisher_time_clock='content_time_v1', publisher_time_transition_expires_at='2030-01-01T00:00:00Z', content_time_cutover_min_valid_share=0.8, content_time_contract_version='newsflows-content-time/v2' WHERE rkey='newsflow-zz-2a';" >/dev/null
set +e; E="$E-neg-clock" bash "$runner" preflight >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 && ! -f "$E-neg-clock/step1-be-prestate-rows.tsv" ]] || { echo "content-time clock on the target feed did not stop preflight pre-artifact (rc=$rc)"; exit 1; }
psql -c "UPDATE feedgen_ops.feed_catalog SET publisher_time_clock='receipt_time', publisher_time_transition_expires_at=NULL, content_time_cutover_min_valid_share=NULL, content_time_contract_version=NULL WHERE rkey='newsflow-zz-2a';" >/dev/null
rb_bad=$(mktemp); sed 's/"time_column":"createdAt"/"time_column":"content_time_utc"/' "$rb" > "$rb_bad"; grep -q content_time_utc "$rb_bad" || { echo "could not build the bad readback"; exit 1; }
set +e; E="$E-neg-timecol" READBACK_JSON="$rb_bad" bash "$runner" preflight >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 && ! -f "$E-neg-timecol/step1-be-prestate-rows.tsv" ]] || { echo "BSR time_column=content_time_utc did not stop preflight pre-artifact (rc=$rc)"; exit 1; }
rm -f "$rb_bad"; sudo -n rm -rf "$E-neg-clock" "$E-neg-timecol"; echo "status=participant_safety_negatives ok"
# production-path negative: with permit_negative_probe=0 pinned at preflight, a ^neg apply must be refused before any receipt
set +e; E="$E-neg-permit" PERMIT_NEGATIVE_PROBE=0 bash "$runner" preflight >/dev/null 2>&1; rc=$?; set -e
[[ $rc -eq 0 ]] || { echo "preflight with permit_negative_probe=0 should pass (rc=$rc)"; exit 1; }
set +e; E="$E-neg-permit" PERMIT_NEGATIVE_PROBE=0 CEIL_WAL_FLOOR_BYTES=1 CEIL_WAL_BASELINE_MULTIPLE=0.5 bash "$runner" apply neg 1 >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 && ! -f "$E-neg-permit/step1-be-apply-neg.json" && ! -f "$E-neg-permit/ceiling-be-neg.txt" ]] || { echo "neg apply was not refused pre-artifact under permit_negative_probe=0 (rc=$rc)"; exit 1; }
sudo -n rm -rf "$E-neg-permit"; echo "status=negative_probe_refused_on_production_path ok"
step preflight bash "$runner" preflight
[[ $(wc -l <"$E/step1-be-prestate-rows.tsv") -eq 1260 ]] || { echo "prestate snapshot count unexpected"; exit 1; }
grep -q "^mode=recover$" "$E/source-set.txt" || { echo "mode=recover not recorded"; exit 1; }
step preview bash "$runner" preview
step apply_b1 bash "$runner" apply b1 1
grep -q "batches=1 exit=3" "$E/ceiling-be-b1.txt" || { echo "b1 did not stop after one batch with exit 3"; exit 1; }
grep -q "wal_batches_failing=0" "$E/ceiling-be-b1.txt" && grep -q "wal_batches_unpaid=0" "$E/ceiling-be-b1.txt" \
  && grep -q "^batch=1 elapsed_ms=.* pause_owed_ms=[0-9]* pause_paid_ms=[0-9]* .* paid ok relation_growth=[0-9]* rel_cap=[0-9]* rel_ok$" "$E/ceiling-be-b1.txt" \
  || { echo "b1 per-batch WAL rule / adaptive pause not evaluated"; cat "$E/ceiling-be-b1.txt"; exit 1; }
# the adaptive pause must have been PAID: pause_paid_ms >= pause_owed_ms = max(1 s, LSN advance / idle baseline) -- on the idle disposable DB that is ~2 minutes per batch
[[ $(sed -n 's/^batch=1 .* pause_owed_ms=\([0-9]*\) pause_paid_ms=\([0-9]*\) .*/\1 \2/p' "$E/ceiling-be-b1.txt" | awk '{print ($2>=$1 && $1>1000)?"yes":"no"}') == "yes" ]] \
  || { echo "b1 adaptive pause not paid / not adaptive"; cat "$E/ceiling-be-b1.txt"; exit 1; }
# NEGATIVE: floor 1 B and multiple 0.5 -> the ceiling is half the estate's rate x (elapsed + paid pause) while the batch wrote ~1x -> BREACH must stop the invocation (non-zero exit)
set +e; CEIL_WAL_FLOOR_BYTES=1 CEIL_WAL_BASELINE_MULTIPLE=0.5 bash "$runner" apply neg 1; rc=$?; set -e
[[ $rc -ne 0 ]] || { echo "negative apply (floor=1) should have stopped on BREACH"; exit 1; }
grep -q "verdict=BREACH" "$E/ceiling-be-neg.txt" && grep -q "wal_batches_failing=1" "$E/ceiling-be-neg.txt" && grep -q "^batch=1 .* paid BREACH relation_growth=.*$" "$E/ceiling-be-neg.txt" \
  || { echo "negative apply receipt does not show the per-batch BREACH"; cat "$E/ceiling-be-neg.txt"; exit 1; }
step apply_full bash "$runner" apply full
# the remaining 240 rows (1,240 - 500 - 500) fit in one batch of this invocation; recovery.recovered is per-invocation, not cumulative
grep -q "batches=1 exit=0 recovered=240 complete=true" "$E/ceiling-be-full.txt" || { echo "full apply did not finish the remaining 240 rows"; cat "$E/ceiling-be-full.txt"; exit 1; }
[[ -f "$E/step1-be-checkpoint.json" ]] || { echo "checkpoint not written into the 0750 evidence root by the host-mode tool run"; exit 1; }
step readback bash "$runner" readback
grep -q "missing=0" "$E/step1-be-diff-attempt-1.txt" && grep -q "recovered_valid=1230 recovered_invalid=10.*still_legacy=20" "$E/step1-be-diff-attempt-1.txt" \
  || { echo "readback diff not closed / counts unexpected"; cat "$E/step1-be-diff-attempt-1.txt"; exit 1; }
# re-binding PREREG (or SINCE/UNTIL/...) between invocations must be REJECTED before any artifact is written (bound-env gate)
set +e; PREREG="legacy_in_window=1260,unretrievable=20,would_recover=1240,recover_source_valid=1230,recover_source_invalid=11,would_insert=0,conflict=0" bash "$runner" readback >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 && ! -f "$E/step1-be-preview-after-attempt-2.json" && ! -f "$E/readback-attempt-2.txt" ]] || { echo "re-bound PREREG was not rejected pre-artifact (rc=$rc)"; exit 1; }
set +e; UNTIL="$(date -u -d '4 days ago' +%Y-%m-%dT%H:%M:%S.000Z)" bash "$runner" readback >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 && ! -f "$E/step1-be-preview-after-attempt-2.json" ]] || { echo "re-bound UNTIL was not rejected pre-artifact (rc=$rc)"; exit 1; }
# readback stays re-runnable: a second clean run gets its own attempt suffix
step readback_rerun bash "$runner" readback
[[ -f "$E/readback-attempt-2.txt" && -f "$E/step1-be-preview-after-attempt-2.json" ]] || { echo "readback attempt-2 receipt missing"; exit 1; }
# genuine mid-restore failure: hold a row lock on a recovered row of batch 2 (rows 501-1000) so lock_timeout=5s fires, then resume after release
lock_uri=$(sed -n '510p' "$E/step1-be-prestate-rows.tsv" | cut -f1)
( "${D[@]}" exec -i "$container" psql -U "$db_user" -d "$db_name" -X -q -v ON_ERROR_STOP=1 -c "BEGIN; SELECT uri FROM post WHERE uri='$lock_uri' FOR UPDATE; SELECT pg_sleep(25); COMMIT;" >/dev/null 2>&1 & ); sleep 2
set +e; bash "$runner" restore; rc=$?; set -e
[[ $rc -ne 0 ]] || { echo "restore should have failed on the held lock"; exit 1; }
[[ -f "$E/restore-be-rows-1-500-attempt-1.txt" && -f "$E/restore-be-rows-501-1000-attempt-1.txt" ]] || { echo "restore receipts by row range missing after failure"; ls "$E" | grep restore || true; exit 1; }
[[ "$(cat "$E/restore-be-cursor.txt")" == "501" ]] || { echo "cursor should point at 501 after the failed batch"; exit 1; }
sleep 26
step restore_resume bash "$runner" restore
[[ -f "$E/restore-be-rows-501-1000-attempt-2.txt" && -f "$E/restore-be-rows-1001-1260-attempt-1.txt" ]] || { echo "resume did not produce attempt-2/1001-1260 receipts"; exit 1; }
grep -q "identical_to_prestate" "$E/restore-be-result-attempt-1.txt" || { echo "restore not identical to prestate"; exit 1; }
# a no-op re-run of a completed restore must still produce a fresh attempt-2 verification receipt (post-loop path resume-safe)
step restore_noop_rerun bash "$runner" restore
[[ -f "$E/restore-be-result-attempt-2.txt" ]] && grep -q "identical_to_prestate rows=1260 batches_this_run=0 attempt=2" "$E/restore-be-result-attempt-2.txt" \
  || { echo "no-op restore re-run receipt missing/wrong"; exit 1; }
step control bash "$runner" control
[[ -f "$E/pg-control-2.txt" ]] || { echo "second control read missing"; exit 1; }
step secret_scan bash "$runner" secret-scan
grep -q "^hits=0$" "$E/secret-scan.txt" || { echo "secret scan not clean"; cat "$E/secret-scan.txt"; exit 1; }
grep -q "SOME_HOSTNAME_CONFIG" "$E/secret-scan.txt" && { echo "secret scan must not treat config keys as secrets"; exit 1; }
# negative test: a scratch evidence root containing the rehearsal DB password must FAIL the scan
E2=$(mktemp -d); sudo -n install -d -o root -g newsflows -m 750 "$E2/leak"; echo "leak $rehearsal_pw" | sudo -n tee "$E2/leak/step1-x.err" >/dev/null
sudo -n install -o root -g newsflows -m 640 "$E/source-set.txt" "$E2/leak/source-set.txt"   # same bound values -> the DETECTOR runs, not the bindings gate
sudo -n cp "$E/tree-manifest.txt" "$E2/leak/" 2>/dev/null || true
set +e; E="$E2/leak" bash "$runner" secret-scan >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 ]] || { echo "secret scan failed to detect a leaked password"; exit 1; }
sudo -n grep -q "^hits=1$" "$E2/leak/secret-scan.txt" && sudo -n grep -q "step1-x.err" "$E2/leak/secret-scan.txt" || { echo "secret scan negative: expected hits=1 naming step1-x.err"; sudo -n cat "$E2/leak/secret-scan.txt"; exit 1; }
sudo -n rm -rf "$E2"; echo "status=secret_scan_negative_test ok (hits=1 step1-x.err)"
step finalize bash "$runner" finalize 2026-08-18T00:00:00Z 2026-08-18T01:00:00Z
sudo -n grep -q "RESULT.txt" "$E/SHA256SUMS" || { echo "SHA256SUMS incomplete"; exit 1; }
rm -f "$rb"
echo "status=ok evidence=$E"
