#!/usr/bin/env bash
# Rehearses scripts/content_time_revalidate_packet.sh end-to-end against a
# disposable postgres:17 container previously prepared by
# scripts/rehearse_content_time_revalidate.sh with
# FEEDGEN_REVALIDATE_REHEARSAL_KEEP=1 (that script bootstraps feed_catalog,
# applies the migrations and leaves the container up). Runs the packet runner in
# RUNNER=host mode (same tool flags/argv as the container path; only the DSN
# source differs) and asserts every gate: preflight, preview gate, batch-gated
# apply (--max-batches 1 then full), readback, bounded restore, secret-scan,
# finalize.
#
# Usage (server, from the built feedgen tree):
#   bash scripts/rehearse_content_time_revalidate_packet.sh <container> <port> [evidence_root]
set -euo pipefail
container=${1:?container}; port=${2:?port}
E=${3:-/tmp/packet-rehearsal-$(date -u +%Y%m%dT%H%M%SZ)}
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner="$repo_root/scripts/content_time_revalidate_packet.sh"
DOCKER=${FEEDGEN_REVALIDATE_REHEARSAL_DOCKER:-sudo -n docker}
read -r -a D <<<"$DOCKER"
pub_a="did:plc:revalidate-rehearsal-publisher-a"; pub_b="did:plc:revalidate-rehearsal-publisher-b"
psql() { "${D[@]}" exec -i "$container" psql -U feedgen -d feedgen_revalidate_rehearsal -X -A -F '|' -v ON_ERROR_STOP=1 "$@"; }

# Seed fresh in-window v1 rows: 620 for publisher A (600 valid + 10 past_bound + 10 future-skew), 40 for publisher B (30/5/5).
psql <<SQL >/dev/null
DELETE FROM public.post WHERE uri LIKE 'at://%/app.bsky.feed.post/pk-%';
INSERT INTO public.post (uri,cid,"indexedAt","createdAt",author,text,"rootUri","rootCid",link_uri,link_title,link_description,"linkUrl","linkTitle","linkDescription",created_at_source_raw,content_time_utc,content_time_status,content_time_clamp_reason,content_time_validator_version)
SELECT 'at://'||a||'/app.bsky.feed.post/pk-'||lpad(g::text,4,'0'),'cid-pk-'||a||g::text,'2026-08-13T10:00:00.000Z',
 CASE WHEN k='pb' THEN '2026-08-13T10:00:00.000Z' ELSE '2026-08-13T09:00:00.000Z' END,a,'pk-'||g::text,'','','','','','','','',
 CASE k WHEN 'v' THEN convert_to('2026-08-13T09:00:00+00:00','UTF8') WHEN 'pb' THEN convert_to('2020-01-01T00:00:00+00:00','UTF8') ELSE convert_to('2026-08-13T11:00:00+00:00','UTF8') END,
 CASE k WHEN 'v' THEN '2026-08-13T09:00:00.000Z' WHEN 'pb' THEN NULL ELSE '2026-08-13T11:00:00.000Z' END,
 CASE k WHEN 'pb' THEN 'source_invalid' ELSE 'source_valid' END, CASE k WHEN 'pb' THEN 'past_bound' ELSE NULL END,'newsflows-content-time/v1'
FROM (SELECT '$pub_a' AS a, g, CASE WHEN g<=600 THEN 'v' WHEN g<=610 THEN 'pb' ELSE 'fs' END AS k FROM generate_series(1,620) g
      UNION ALL SELECT '$pub_b', g, CASE WHEN g<=30 THEN 'v' WHEN g<=35 THEN 'pb' ELSE 'fs' END FROM generate_series(1,40) g) x;
SQL
echo "status=seeded a=620 b=40"

envfile=$(mktemp); echo 'FEEDGEN_DB_PASSWORD=rehearsal-not-a-real-secret-8f2c' >"$envfile"
export E TREE="$repo_root" RUNNER=host HOST_DSN="postgresql://feedgen:feedgen@127.0.0.1:$port/feedgen_revalidate_rehearsal" \
  DB_CONTAINER="$container" PSQL_DB=feedgen_revalidate_rehearsal PSQL_USER=feedgen ENV_FILE="$envfile" DOCKER="$DOCKER" \
  MAIN_DIDS="$pub_a" BE_DID="$pub_b" HORIZON_MAIN_DAYS=10 HORIZON_BE_DAYS=10 \
  RANKED_RKEYS="'newsflow-zz-1a','newsflow-zz-1b'" MAIN_RKEY_PATTERN='newsflow-zz-1a' BE_RKEY_PATTERN='newsflow-zz-1b' \
  PACKET_SHA="$(printf '1%.0s' $(seq 1 64))"

step() { echo "status=$1"; shift; "$@"; }
step preflight bash "$runner" preflight
[[ -s "$E/step1-main-prestate-rows.tsv" && $(wc -l <"$E/step1-main-prestate-rows.tsv") -eq 620 ]] || { echo "preflight main snapshot rows != 620"; exit 1; }
step preview_main bash "$runner" preview main
step preview_be bash "$runner" preview be
step apply_main_b1 bash "$runner" apply main b1 1
grep -q "batches=1 exit=3" "$E/ceiling-main-b1.txt" || { echo "b1 did not stop after one batch with exit 3"; exit 1; }
step apply_main_full bash "$runner" apply main full
grep -q "exit=0" "$E/ceiling-main-full.txt" || { echo "full apply did not exit 0"; exit 1; }
step apply_be_full bash "$runner" apply be full
step readback bash "$runner" readback
grep -q "prestate_missing_in_poststate=0" "$E/step1-main-diff.txt" || { echo "readback diff main not closed"; exit 1; }
step restore_main bash "$runner" restore main
step restore_be bash "$runner" restore be
grep -q "identical_to_prestate" "$E/restore-main-result.txt" && grep -q "identical_to_prestate" "$E/restore-be-result.txt" || { echo "restore not identical"; exit 1; }
# after restore, a fresh preview must again see the full v1 population (restore is exact)
step secret_scan bash "$runner" secret-scan
step finalize bash "$runner" finalize 2026-08-17T00:00:00Z 2026-08-17T00:30:00Z
rm -f "$envfile"
echo "status=ok evidence=$E"
