#!/usr/bin/env bash
# Rehearses scripts/content_time_revalidate_packet.sh end-to-end, in the
# PRODUCTION runner mode (RUNNER=container: the live feedgen image as node
# runtime, DSN composed in-container by scripts/compose_feedgen_dsn.js from a
# scratch env-file, /evidence mounted from a 0750 root:newsflows evidence root),
# against a disposable postgres:17 container previously prepared by
# scripts/rehearse_content_time_revalidate.sh with
# FEEDGEN_REVALIDATE_REHEARSAL_KEEP=1. Asserts every gate: preflight (hashes,
# scope, horizons, NULL-raw, pre-registration cells), preview gates, batch-gated
# apply (--max-batches 1 then full), readback, forced mid-restore stop + resume,
# bounded restore, an extra control read, secret-scan, finalize.
#
# Usage (server, from the built feedgen tree; the running production feedgen
# container is only READ (inspect + env echo) for the live-image checks):
#   bash scripts/rehearse_content_time_revalidate_packet.sh <container> <port> [evidence_root]
set -euo pipefail
container=${1:?container}; port=${2:?port}
E=${3:-/tmp/packet-rehearsal-$(date -u +%Y%m%dT%H%M%SZ)}
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner="$repo_root/scripts/content_time_revalidate_packet.sh"
DOCKER=${FEEDGEN_REVALIDATE_REHEARSAL_DOCKER:-sudo -n docker}
read -r -a D <<<"$DOCKER"
pub_a="did:plc:revalidate-rehearsal-publisher-a"; pub_b="did:plc:revalidate-rehearsal-publisher-b"
rehearsal_pw="rehearsal-not-a-real-secret-8f2c"
psql() { "${D[@]}" exec -i "$container" psql -U feedgen -d feedgen_revalidate_rehearsal -X -A -F '|' -v ON_ERROR_STOP=1 "$@"; }

# Seed fresh in-window v1 rows: 2,120 for publisher A (2,100 valid + 10 past_bound + 10 future-skew) so one apply invocation
# has >= 2 batches of 500 (V11 round-10 P2-1), 40 for publisher B (30/5/5);
# give the disposable DB a distinct password so the secret scan is meaningful.
psql <<SQL >/dev/null
ALTER USER feedgen PASSWORD '$rehearsal_pw';
DELETE FROM public.post WHERE uri LIKE 'at://%/app.bsky.feed.post/pk-%';
INSERT INTO public.post (uri,cid,"indexedAt","createdAt",author,text,"rootUri","rootCid",link_uri,link_title,link_description,"linkUrl","linkTitle","linkDescription",created_at_source_raw,content_time_utc,content_time_status,content_time_clamp_reason,content_time_validator_version)
SELECT 'at://'||a||'/app.bsky.feed.post/pk-'||lpad(g::text,4,'0'),'cid-pk-'||a||g::text,'2026-08-13T10:00:00.000Z',
 CASE WHEN k='pb' THEN '2026-08-13T10:00:00.000Z' ELSE '2026-08-13T09:00:00.000Z' END,a,'pk-'||g::text,'','','','','','','','',
 CASE k WHEN 'v' THEN convert_to('2026-08-13T09:00:00+00:00','UTF8') WHEN 'pb' THEN convert_to('2020-01-01T00:00:00+00:00','UTF8') ELSE convert_to('2026-08-13T11:00:00+00:00','UTF8') END,
 CASE k WHEN 'v' THEN '2026-08-13T09:00:00.000Z' WHEN 'pb' THEN NULL ELSE '2026-08-13T11:00:00.000Z' END,
 CASE k WHEN 'pb' THEN 'source_invalid' ELSE 'source_valid' END, CASE k WHEN 'pb' THEN 'past_bound' ELSE NULL END,'newsflows-content-time/v1'
FROM (SELECT '$pub_a' AS a, g, CASE WHEN g<=2100 THEN 'v' WHEN g<=2110 THEN 'pb' ELSE 'fs' END AS k FROM generate_series(1,2120) g
      UNION ALL SELECT '$pub_b', g, CASE WHEN g<=30 THEN 'v' WHEN g<=35 THEN 'pb' ELSE 'fs' END FROM generate_series(1,40) g) x;
SQL
echo "status=seeded a=2120 b=40"

db_ip=$("${D[@]}" inspect "$container" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
envfile=$(mktemp); chmod 600 "$envfile"
{ echo "FEEDGEN_DB_USER=feedgen"; echo "FEEDGEN_DB_PASSWORD=$rehearsal_pw"; echo "FEEDGEN_DB_HOST=$db_ip"; echo "FEEDGEN_DB_PORT=5432"; echo "FEEDGEN_DB_DATABASE=feedgen_revalidate_rehearsal"; echo "SOME_HOSTNAME_CONFIG=$container"; echo "OTHER_APP_PASSWORD=decoy-secret-value-not-in-evidence-91c3"; } >"$envfile"
IMG=${REHEARSAL_IMG:-pmmendoza/bsky-feedgen@sha256:928c15aac77a8a842f60053eff8953e70cc9e4117c2fbe86f548e345c1a34711}
img_ct=$("${D[@]}" run --rm --entrypoint sh "$IMG" -c 'sha256sum /app/dist/util/content-time.js' | cut -d' ' -f1)

export E TREE="$repo_root" RUNNER=container IMG NETWORK=bridge ENV_FILE="$envfile" \
  DB_CONTAINER="$container" PSQL_DB=feedgen_revalidate_rehearsal PSQL_USER=feedgen DOCKER="$DOCKER" \
  EXPECTED_SHA="$(git -C "$repo_root" rev-parse HEAD)" \
  EXPECTED_DIST_SHA256="$(sha256sum "$repo_root/dist/tools/backfill-publisher-posts.js" | cut -d' ' -f1)" \
  EXPECTED_CT_SHA256="$(sha256sum "$repo_root/dist/util/content-time.js" | cut -d' ' -f1)" \
  EXPECTED_IMAGE_CT_SHA256="$img_ct" \
  MAIN_DIDS="$pub_a" BE_DID="$pub_b" HORIZON_MAIN_DAYS=10 HORIZON_BE_DAYS=10 \
  RANKED_RKEYS="'newsflow-zz-1a','newsflow-zz-1b'" MAIN_RKEY_PATTERN='newsflow-zz-1a' BE_RKEY_PATTERN='newsflow-zz-1b' \
  SINCE_MAIN=2026-08-07T00:00:00.000Z SINCE_BE=2026-08-07T00:00:00.000Z \
  PACKET_SHA="$(printf '1%.0s' $(seq 1 64))"
# installed operator tool refs (the runner gates against them): read them the same way the runner does
tool_ref() { sudo -n node -e 'const fs=require("fs");const base=process.argv[1];let out="";for(const py of fs.readdirSync(base+"/lib").filter(d=>d.startsWith("python3"))){const sp=base+"/lib/"+py+"/site-packages";for(const d of fs.readdirSync(sp).filter(d=>d.endsWith(".dist-info")&&d.toLowerCase().replace(/-/g,"_").startsWith(process.argv[2].toLowerCase().replace(/-/g,"_")))){try{const j=JSON.parse(fs.readFileSync(sp+"/"+d+"/direct_url.json"));out=(j.vcs_info||{}).commit_id||"";}catch(e){}}}console.log(out)' "/opt/newsflows/tools/uv/$1" "$1"; }
export EXPECTED_TOOL_REFS="bsky-ops=$(tool_ref bsky-ops),blueskyranker=$(tool_ref blueskyranker),newsflows-bskyhealth=$(tool_ref newsflows-bskyhealth)"
# pre-registration flow: read-only prereg at the bound SINCE, then bind the cells (as the ledger approval would)
export PREREG_MAIN=placeholder PREREG_BE=placeholder
prereg_out=$(bash "$runner" prereg); echo "$prereg_out" | sed 's/^/prereg: /'
export PREREG_MAIN="$(echo "$prereg_out" | sed -n 's/^PREREG_MAIN=//p')" PREREG_BE="$(echo "$prereg_out" | sed -n 's/^PREREG_BE=//p')"
[[ "$PREREG_MAIN" == "v1_valid_to_v2_valid=2100,v1_invalid_to_v2_valid=10,v1_to_v2_invalid=10,createdat_extra=0,createdat_unchanged=0" ]] || { echo "prereg main cells unexpected: $PREREG_MAIN"; exit 1; }
[[ "$PREREG_BE" == "v1_valid_to_v2_valid=30,v1_invalid_to_v2_valid=5,v1_to_v2_invalid=5,createdat_extra=0,createdat_unchanged=0" ]] || { echo "prereg be cells unexpected: $PREREG_BE"; exit 1; }
rb=$(mktemp); now=$(date -u +%s); printf '{"schema_version":"bsr.ops.effective_config.readback.v1","raw_values_in_output":false,"artifact_metadata":{"generated_at":%s,"stale_at":%s},"bindings":{"reh-a":{"feed_ids":["newsflow-zz-1a"],"push_window_days":10,"time_column":"createdAt"},"reh-b":{"feed_ids":["newsflow-zz-1b"],"push_window_days":10,"time_column":"createdAt"}}}\n' "$now" "$((now+3600))" >"$rb"
export READBACK_JSON="$rb"

step() { echo "status=$1"; shift; "$@"; }
step preflight bash "$runner" preflight
[[ $(wc -l <"$E/step1-main-prestate-rows.tsv") -eq 2120 && $(wc -l <"$E/step1-be-prestate-rows.tsv") -eq 40 ]] || { echo "prestate snapshot counts unexpected"; exit 1; }
grep -q "^feedgen_retention_enabled=unset$\|^feedgen_retention_enabled=false$\|^feedgen_retention_enabled=0$" "$E/source-set.txt" || { echo "retention gate not recorded as disabled"; exit 1; }
step preview_main bash "$runner" preview main
step preview_be bash "$runner" preview be
step apply_main_b1 bash "$runner" apply main b1 1
grep -q "batches=1 exit=3" "$E/ceiling-main-b1.txt" || { echo "b1 did not stop after one batch with exit 3"; exit 1; }
grep -q "wal_batches_failing=0" "$E/ceiling-main-b1.txt" && grep -q "wal_batches_unpaid=0" "$E/ceiling-main-b1.txt" && grep -q "^batch=1 elapsed_ms=.* pause_owed_ms=[0-9]* pause_paid_ms=[0-9]* .* paid ok$" "$E/ceiling-main-b1.txt" || { echo "b1 per-batch WAL rule / adaptive pause not evaluated"; cat "$E/ceiling-main-b1.txt"; exit 1; }
# the adaptive pause must have been PAID: pause_paid_ms >= pause_owed_ms = max(1 s, LSN advance / idle baseline) -- on the idle disposable DB that is ~2 minutes per batch
[[ $(sed -n 's/^batch=1 .* pause_owed_ms=\([0-9]*\) pause_paid_ms=\([0-9]*\) .*/\1 \2/p' "$E/ceiling-main-b1.txt" | awk '{print ($2>=$1 && $1>1000)?"yes":"no"}') == "yes" ]] || { echo "b1 adaptive pause not paid / not adaptive"; cat "$E/ceiling-main-b1.txt"; exit 1; }
# NEGATIVE: floor 1 B and multiple 0.5 -> the ceiling is half the estate's rate x (elapsed + paid pause) while the batch wrote ~1x -> BREACH must stop the invocation (non-zero exit)
set +e; CEIL_WAL_FLOOR_BYTES=1 CEIL_WAL_BASELINE_MULTIPLE=0.5 bash "$runner" apply main neg 1; rc=$?; set -e
[[ $rc -ne 0 ]] || { echo "negative apply (floor=1) should have stopped on BREACH"; exit 1; }
grep -q "verdict=BREACH" "$E/ceiling-main-neg.txt" && grep -q "wal_batches_failing=1" "$E/ceiling-main-neg.txt" && grep -q "^batch=1 .* paid BREACH$" "$E/ceiling-main-neg.txt" || { echo "negative apply receipt does not show the per-batch BREACH"; cat "$E/ceiling-main-neg.txt"; exit 1; }
# MULTI-BATCH + baseline-scaled branch governing: floor 1 B, multiple 1.0 -> ceiling = idle baseline x (elapsed + the paid adaptive pause) >= per-batch attributed WAL by construction; 2 batches
step apply_main_b2 env CEIL_WAL_FLOOR_BYTES=1 bash "$runner" apply main b2 2
grep -q "batches=2 exit=3" "$E/ceiling-main-b2.txt" && [[ $(grep -c "^batch=[0-9]* elapsed_ms=" "$E/ceiling-main-b2.txt") -eq 2 ]] || { echo "b2 did not run exactly two batches"; cat "$E/ceiling-main-b2.txt"; exit 1; }
grep -q "wal_batches_failing=0" "$E/ceiling-main-b2.txt" && grep -q "wal_batches_unpaid=0" "$E/ceiling-main-b2.txt" && ! grep -q "ceiling=614400 " "$E/ceiling-main-b2.txt" && ! grep -q "ceiling=1 " "$E/ceiling-main-b2.txt" || { echo "b2 baseline-scaled branch did not govern both batches"; cat "$E/ceiling-main-b2.txt"; exit 1; }
[[ $(grep "^batch=" "$E/ceiling-main-b2.txt" | sed 's/.*ceiling=\([0-9]*\).*/\1/' | sort -u | wc -l) -eq 2 ]] || { echo "b2 batches did not get distinct ceilings from their own durations"; cat "$E/ceiling-main-b2.txt"; exit 1; }
step apply_main_full bash "$runner" apply main full
grep -q "exit=0" "$E/ceiling-main-full.txt" || { echo "full apply did not exit 0"; exit 1; }
step apply_be_full bash "$runner" apply be full
[[ -f "$E/step1-main-checkpoint.json" && -f "$E/step1-be-checkpoint.json" ]] || { echo "checkpoints not written into the 0750 evidence root by the container"; exit 1; }
step readback bash "$runner" readback
grep -q "prestate_missing_in_poststate=0" "$E/step1-main-diff-attempt-1.txt" || { echo "readback diff main not closed"; exit 1; }
# readback must be re-runnable even after a FAILED attempt (files of the failed attempt stay; the next attempt gets a new suffix)
set +e; PREREG_MAIN="v1_valid_to_v2_valid=2100,v1_invalid_to_v2_valid=10,v1_to_v2_invalid=11,createdat_extra=0,createdat_unchanged=0" bash "$runner" readback >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 && -f "$E/step1-main-preview-after-attempt-2.json" && ! -f "$E/readback-attempt-2.txt" ]] || { echo "forced readback failure did not behave (rc=$rc)"; exit 1; }
step readback_after_failure bash "$runner" readback
[[ -f "$E/readback-attempt-3.txt" ]] || { echo "readback attempt-3 receipt missing"; exit 1; }
# genuine mid-restore failure: hold a row lock on a row of batch 2 (rows 501-1000) so lock_timeout=5s fires, then resume after release
lock_uri=$(sed -n '510p' "$E/step1-main-prestate-rows.tsv" | cut -f1)
( "${D[@]}" exec -i "$container" psql -U feedgen -d feedgen_revalidate_rehearsal -X -q -v ON_ERROR_STOP=1 -c "BEGIN; SELECT uri FROM post WHERE uri='$lock_uri' FOR UPDATE; SELECT pg_sleep(25); COMMIT;" >/dev/null 2>&1 & ) ; sleep 2
set +e; bash "$runner" restore main; rc=$?; set -e
[[ $rc -ne 0 ]] || { echo "restore should have failed on the held lock"; exit 1; }
[[ -f "$E/restore-main-rows-1-500-attempt-1.txt" && -f "$E/restore-main-rows-501-1000-attempt-1.txt" ]] || { echo "restore receipts by row range missing after failure"; ls "$E" | grep restore || true; exit 1; }
[[ "$(cat "$E/restore-main-cursor.txt")" == "501" ]] || { echo "cursor should point at 501 after the failed batch"; exit 1; }
sleep 26
step restore_main_resume bash "$runner" restore main
[[ -f "$E/restore-main-rows-501-1000-attempt-2.txt" && -f "$E/restore-main-rows-2001-2120-attempt-1.txt" ]] || { echo "resume did not produce attempt-2 receipts for rows 501-1000 … 2001-2120"; exit 1; }
step restore_be bash "$runner" restore be
grep -q "identical_to_prestate" "$E/restore-main-result-attempt-1.txt" && grep -q "identical_to_prestate" "$E/restore-be-result-attempt-1.txt" || { echo "restore not identical"; exit 1; }
# a no-op re-run of a completed restore must still produce a fresh attempt-2 verification receipt (post-loop path resume-safe)
step restore_main_noop_rerun bash "$runner" restore main
[[ -f "$E/restore-main-result-attempt-2.txt" ]] && grep -q "identical_to_prestate rows=2120 batches_this_run=0 attempt=2" "$E/restore-main-result-attempt-2.txt" || { echo "no-op restore re-run receipt missing/wrong"; exit 1; }
step control bash "$runner" control
[[ -f "$E/pg-control-2.txt" ]] || { echo "second control read missing"; exit 1; }
step secret_scan bash "$runner" secret-scan
grep -q "^hits=0$" "$E/secret-scan.txt" || { echo "secret scan not clean"; exit 1; }
grep -q "SOME_HOSTNAME_CONFIG" "$E/secret-scan.txt" && { echo "secret scan must not treat config keys as secrets"; exit 1; }
# negative test: a scratch evidence root containing the real DB password must FAIL the scan
E2=$(mktemp -d); sudo -n install -d -o root -g newsflows -m 750 "$E2/leak"; echo "leak $rehearsal_pw" | sudo -n tee "$E2/leak/step1-x.err" >/dev/null
set +e; E="$E2/leak" bash "$runner" secret-scan >/dev/null 2>&1; rc=$?; set -e
[[ $rc -ne 0 ]] || { echo "secret scan failed to detect a leaked password"; exit 1; }
sudo -n rm -rf "$E2"; echo "status=secret_scan_negative_test ok"
step finalize bash "$runner" finalize 2026-08-17T00:00:00Z 2026-08-17T00:30:00Z
sudo -n grep -q "RESULT.txt" "$E/SHA256SUMS" || { echo "SHA256SUMS incomplete"; exit 1; }
rm -f "$envfile" "$rb"
echo "status=ok evidence=$E"
