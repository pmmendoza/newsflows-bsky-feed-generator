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
[[ $(migration_targets) == post ]]

# The cutoff uses only the semantic transition cohorts and fails closed on
# genuine overlap; compatible native v2/v3 history is irrelevant.
cutoff_bounds='max_from=2026-08-20T00:00:00.000Z|min_to=2026-08-21T00:00:00.000Z|cutoff=2026-08-21T00:00:00.000Z'
psql_ro() { [[ "$*" == *'WITH bounds'* ]] && printf '%s\n' "$cutoff_bounds" || printf '0\n'; }
migration_derive_cutoff post 1
grep -Fxq 'cutoff=2026-08-21T00:00:00.000Z' "$E/migrate-freeze-post-cutoff-1.txt"
cutoff_sql=$(migration_cutoff_sql post)
[[ $cutoff_sql == *'(SELECT max("indexedAt")'* ]]
[[ $cutoff_sql == *'(SELECT min("indexedAt")'* ]]
[[ $cutoff_sql == *"content_time_clamp_reason='future_skew_clamped'"* ]]
[[ $cutoff_sql == *"content_time_status='source_invalid' AND content_time_clamp_reason='future_skew'"* ]]
cutoff_bounds='max_from=2026-08-21T00:00:00.000Z|min_to=2026-08-21T00:00:00.000Z|cutoff=2026-08-21T00:00:00.000Z'
if ( migration_derive_cutoff post 2 ); then echo 'overlapping cohorts were accepted' >&2; exit 1; fi
printf 'activation_floor=2026-08-21T12:00:00.000Z\n' >"$E/activation-floor.txt"
cutoff_bounds='max_from=2026-08-21T11:59:59.999Z|min_to=2026-08-21T12:00:00.000Z|cutoff=2026-08-21T12:00:00.000Z'
migration_derive_cutoff post 3
grep -Fxq 'cutoff=2026-08-21T12:00:00.000Z' "$E/migrate-freeze-post-cutoff-3.txt"

# The explicit overlap-normalization path binds its reverse preview, resumes
# from receipts, and publishes a zero-residual readback without changing the
# later forward/rollback denominator.
ALLOW_FTFU1_OVERLAP_NORMALIZATION=1
assert_tree() { :; }
assert_active_catalog_version() { [[ $1 == "$FROM_VERSION" ]]; }
sleep() { :; }
control_rates() { echo '1 0 0'; }
pgstat_table_read() { echo '1|1|0|1|1'; }
normalization_calls=0
migration_preview_one() {
  local out=$1 from=$3 n=2
  [[ $from == "$TO_VERSION" ]] || { echo "unexpected normalization transition $from" >&2; return 2; }
  (( normalization_calls > 0 )) && n=1
  (( normalization_calls > 1 )) && n=0
  printf '{"preview":{"scanned":%s,"truncated":false,"counts":{"v3_valid_to_v2_valid":0,"v3_clamped_to_v2_valid":%s,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0}}}\n' "$n" "$n" >"$E/$out.json"
  echo "$E/$out.json"
}
migration_apply_one() {
  local target=$1 label=$2 prefix=$5
  [[ $prefix == normalize && $target == post ]]
  printf '{"revalidation":{"updated":1,"skipped_cas":0,"complete":%s,"counts":{"v3_valid_to_v2_valid":0,"v3_clamped_to_v2_valid":1,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0}}}\n' "$([[ $label == overlap-2 ]] && echo true || echo false)" >"$E/normalize-post-apply-$label.json"
  normalization_calls=$((normalization_calls+1))
}
set +e; cmd_migrate_normalize_overlap overlap-1; normalize_rc=$?; set -e
[[ $normalize_rc == 3 ]]
[[ $(normalization_remaining_rows post) == 1 ]]
cmd_migrate_normalize_overlap overlap-2
[[ $(normalization_remaining_rows post) == 0 ]]
grep -Fxq 'normalized_rows=2' "$E/migrate-normalize-overlap-readback.txt"
grep -Fxq 'skipped_cas=0' "$E/migrate-normalize-overlap-readback.txt"

# The activation-bound native-tail plan is proven only after the catalog CAS,
# so its runtime contract must already be v3 and its predicate index-bounded.
assert_active_catalog_version() { [[ $1 == "$TO_VERSION" ]]; }
psql_ro() { printf '%s\n' 'Index Scan using ftfu1_post_contract_indexedat_tmp' '  Index Cond: ("indexedAt" >= '\''2026-08-21T12:00:00.000Z'\''::text)'; }
cmd_migrate_native_tail_plan
grep -Fq 'Index Cond:' "$E/native-tail-post-plan.txt"

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
printf 'attempt=1\ndrain_seconds=60\nmax_preview_rows=123456\npost_cutoff=2026-08-21T00:00:00.000Z\npost_rows=1\npost_preview_sha256=x\nir_prereg_sha256=x\n' >"$E/migrate-stable-population.txt"
printf '{"preview":{"scanned":1}}\n' >"$E/rollback-post-preview.json"
[[ $(rollback_remaining_rows post) == 1 ]] # normalization receipts are not rollback receipts
migration_run_tool forward post "$FROM_VERSION" "$TO_VERSION" --apply >/dev/null
[[ $(grep -o -- '--until' "$captured" | wc -l | tr -d ' ') == 1 ]]
grep -Fq -- '--until 2026-08-21T00:00:00.000Z' "$captured"
migration_run_tool rollback post "$TO_VERSION" "$FROM_VERSION" --apply >/dev/null
[[ $(grep -o -- '--until' "$captured" | wc -l | tr -d ' ') == 1 ]]
grep -Fq -- "--from-version $TO_VERSION --to-version $FROM_VERSION" "$captured"
MIGRATION_NATIVE_V3_TAIL=1 MIGRATION_SINCE_OVERRIDE=2026-08-21T12:00:00.000Z migration_run_tool native post "$TO_VERSION" "$FROM_VERSION" >/dev/null
grep -Fq -- '--table post --since 2026-08-21T12:00:00.000Z' "$captured"
grep -Fq -- '--all-authors' "$captured"
grep -Fq -- '--native-v3-tail' "$captured"
if grep -Fq -- '--until' "$captured"; then echo 'native tail unexpectedly inherited historical cutoff' >&2; exit 1; fi

echo 'content-time revalidation packet contract ok'
