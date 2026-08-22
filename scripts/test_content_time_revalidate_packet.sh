#!/usr/bin/env bash
set -euo pipefail

packet="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/content_time_revalidate_packet.sh"
bash -n "$packet"

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export CONTENT_TIME_PACKET_SOURCE_ONLY=1 E="$scratch/evidence" TREE="$scratch/tree" EXPECTED_SHA=x \
  EXPECTED_DIST_SHA256=x EXPECTED_CT_SHA256=x EXPECTED_IMAGE_CT_SHA256=x EXPECTED_TOOL_REFS=x \
  PACKET_SHA=1111111111111111111111111111111111111111111111111111111111111111 \
  FROM_VERSION=newsflows-content-time/v2 TO_VERSION=newsflows-content-time/v3 \
  SINCE_MAIN=2026-08-18T00:00:00.000Z SINCE_BE=2026-08-11T00:00:00.000Z \
  SINCE_ENGAGEMENT=2026-08-11T00:00:00.000Z PREREG_POST=x PREREG_ENGAGEMENT=x PREREG_IR=x \
  MIGRATION_MAX_PREVIEW_ROWS=123456
set -- migrate-preflight
# shellcheck source=content_time_revalidate_packet.sh
. "$packet"
mkdir -p "$E"
emit() { mkdir -p "$E"; dd of="$E/$1" status=none; }

# The cutoff uses independent min/max probes and fails closed on overlap.
cutoff_bounds='max_from=2026-08-20T00:00:00.000Z|min_to=2026-08-21T00:00:00.000Z|cutoff=2026-08-21T00:00:00.000Z'
psql_ro() { [[ "$*" == *'WITH bounds'* ]] && printf '%s\n' "$cutoff_bounds" || printf '0\n'; }
migration_derive_cutoff post 1
grep -Fxq 'cutoff=2026-08-21T00:00:00.000Z' "$E/migrate-freeze-post-cutoff-1.txt"
[[ $(migration_cutoff_sql post) == *'(SELECT max("indexedAt")'* ]]
[[ $(migration_cutoff_sql post) == *'(SELECT min("indexedAt")'* ]]
[[ $(migration_cutoff_sql post) != *'FILTER ('* ]]
cutoff_bounds='max_from=2026-08-21T00:00:00.000Z|min_to=2026-08-21T00:00:00.000Z|cutoff=2026-08-21T00:00:00.000Z'
if ( migration_derive_cutoff post 2 ); then echo 'overlapping cohorts were accepted' >&2; exit 1; fi

# Complete previews pass; truncated previews fail before outcome cells can authorize work.
good="$scratch/good.json"
printf '%s\n' '{"preview":{"scanned":1,"truncated":false,"counts":{"v2_valid_to_v3_valid":1,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}' >"$good"
spec='v2_valid_to_v3_valid=1,v2_skew_to_v3_clamped=0,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=0'
gate_migration_preview "$good" "$spec" complete
sed 's/"truncated":false/"truncated":true/' "$good" >"$scratch/truncated.json"
if ( gate_migration_preview "$scratch/truncated.json" "$spec" truncated ); then echo 'truncated preview was accepted' >&2; exit 1; fi

# Before freeze the legacy scope is lower-bounded only; once the stable marker
# exists, every forward and reverse invocation carries the same exclusive cutoff.
captured="$scratch/args"
run_tool() { printf '%s\n' "$*" >"$captured"; echo 0; }
migration_run_tool prefreeze post "$FROM_VERSION" "$TO_VERSION" >/dev/null
grep -Fq -- "--max-preview-rows 123456" "$captured"
if grep -Fq -- '--until' "$captured"; then echo 'prefreeze invocation unexpectedly had a cutoff' >&2; exit 1; fi
printf 'attempt=1\ndrain_seconds=60\nmax_preview_rows=123456\npost_cutoff=2026-08-21T00:00:00.000Z\npost_rows=1\npost_preview_sha256=x\nengagement_cutoff=2026-08-21T00:00:00.000Z\nengagement_rows=1\nengagement_preview_sha256=x\nir_prereg_sha256=x\n' >"$E/migrate-stable-population.txt"
migration_run_tool forward post "$FROM_VERSION" "$TO_VERSION" --apply >/dev/null
[[ $(grep -o -- '--until' "$captured" | wc -l | tr -d ' ') == 1 ]]
grep -Fq -- '--until 2026-08-21T00:00:00.000Z' "$captured"
migration_run_tool rollback post "$TO_VERSION" "$FROM_VERSION" --apply >/dev/null
[[ $(grep -o -- '--until' "$captured" | wc -l | tr -d ' ') == 1 ]]
grep -Fq -- "--from-version $TO_VERSION --to-version $FROM_VERSION" "$captured"

echo 'content-time revalidation packet contract ok'
