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
sleep() { :; }
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
scope_sql=$(migration_scope_sql post 2026-08-21T12:00:00.000Z)
[[ "$scope_sql" == *'"indexedAt">='\''2026-08-11T00:00:00.000Z'\'''* ]]
[[ "$scope_sql" == *'"indexedAt"<'\''2026-08-21T12:00:00.000Z'\'''* ]]
[[ "$scope_sql" != *from_outside_horizon* && "$scope_sql" != *from_total* ]]

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

# Recovery never copies old receipts. It imports only a typed activation-floor
# fact after explicit source hashes match, and regenerates the plan under v2.
prior_e="$scratch/prior-e"; mkdir -p "$prior_e"
printf '%s\n' 'activation_floor=2026-08-21T12:00:00.000Z' >"$prior_e/activation-floor.txt"
printf '%s\n' "migration_transition=$FROM_VERSION->$TO_VERSION" \
  'source_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
  'packet_sha256=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' >"$prior_e/migrate-source-set.txt"
export RECOVERY_SOURCE_E="$prior_e" \
  RECOVERY_SOURCE_SET_SHA256="$(sha256sum "$prior_e/migrate-source-set.txt" | cut -d' ' -f1)" \
  RECOVERY_ACTIVATION_FLOOR_SHA256="$(sha256sum "$prior_e/activation-floor.txt" | cut -d' ' -f1)"
main_e=$E
E="$scratch/recovery-e"; mkdir -p "$E"
assert_active_catalog_version() { [[ $1 == "$FROM_VERSION" ]]; }
cmd_migrate_native_tail_recovery_bind
grep -Fxq 'activation_floor=2026-08-21T12:00:00.000Z' "$E/activation-floor.txt"
grep -Fxq "source_set_sha256=$RECOVERY_SOURCE_SET_SHA256" "$E/native-tail-recovery-binding.txt"
grep -Fxq "recovery_packet_sha256=$PACKET_SHA" "$E/native-tail-recovery-binding.txt"
grep -Fq 'Index Cond:' "$E/native-tail-post-plan.txt"
if ( E="$scratch/bad-recovery-e"; mkdir -p "$E"; RECOVERY_ACTIVATION_FLOOR_SHA256=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff cmd_migrate_native_tail_recovery_bind ); then
  echo 'recovery accepted a mismatched activation-floor hash' >&2; exit 1
fi

# Under an active v2 producer the native-v3 tail may only shrink. Account both
# pre-apply and late convergence explicitly; never weaken the residual/CAS gate.
assert_active_catalog_version() { [[ $1 == "$FROM_VERSION" ]]; }
growth_spec='v3_valid_to_v2_valid=1,v3_clamped_to_v2_valid=0,v3_clamped_to_v2_invalid=0,v3_to_v2_invalid=0,gt_5m_invalidated=0,zero_to_5m_unclamped=0'
if ( emit_native_tail_convergence post growth "$growth_spec" 'v3_valid_to_v2_valid=2,v3_clamped_to_v2_valid=0,v3_clamped_to_v2_invalid=0,v3_to_v2_invalid=0,gt_5m_invalidated=0,zero_to_5m_unclamped=0' 1 2 ); then
  echo 'native-tail convergence accepted population growth' >&2; exit 1
fi
aux_spec='v3_valid_to_v2_valid=0,v3_clamped_to_v2_valid=0,v3_clamped_to_v2_invalid=1,v3_to_v2_invalid=0,gt_5m_invalidated=1,zero_to_5m_unclamped=0'
( E="$scratch/aux-e"; mkdir -p "$E"; emit_native_tail_convergence post aux "$aux_spec" 'v3_valid_to_v2_valid=0,v3_clamped_to_v2_valid=0,v3_clamped_to_v2_invalid=0,v3_to_v2_invalid=0,gt_5m_invalidated=0,zero_to_5m_unclamped=0' 1 0; grep -Fxq 'gt_5m_invalidated_producer_converged=1' "$E/native-tail-post-convergence-aux.txt" )
bad_receipt="$scratch/bad-convergence-e/native-tail-post-convergence-bad-cell.txt"
if ( E="$(dirname "$bad_receipt")"; mkdir -p "$E"; emit_native_tail_convergence post bad-cell "$growth_spec" 'v3_valid_to_v2_valid=2,v3_clamped_to_v2_valid=0,v3_clamped_to_v2_invalid=0,v3_to_v2_invalid=0,gt_5m_invalidated=0,zero_to_5m_unclamped=0' 1 1 ); then
  echo 'native-tail convergence accepted a growing outcome cell' >&2; exit 1
fi
[[ ! -e "$bad_receipt" ]]
native_tail_preview() {
  local out=$1 scanned valid
  case "$out" in
    native-tail-post-preview) scanned=3; valid=3;;
    native-tail-post-preview-before-native-convergence) scanned=2; valid=2;;
    native-tail-post-preview-after-native-convergence) scanned=0; valid=0;;
    *) echo "unexpected native preview $out" >&2; return 2;;
  esac
  printf '{"preview":{"scanned":%s,"truncated":false,"counts":{"v3_valid_to_v2_valid":%s,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0}}}\n' "$scanned" "$valid" >"$E/$out.json"
  echo "$E/$out.json"
}
migration_apply_one() {
  local target=$1 label=$2 prefix=$5
  [[ $target == post && $prefix == native ]]
  [[ $label == native-convergence ]]
  printf '{"revalidation":{"updated":1,"skipped_cas":1,"complete":true,"counts":{"v3_valid_to_v2_valid":1,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0}}}\n' >"$E/native-post-apply-$label.json"
}
set +e; ( cmd_migrate_native_tail_rollback native-convergence ) >/dev/null 2>&1; native_cas_rc=$?; set -e
[[ $native_cas_rc == 2 ]]
[[ ! -e "$E/native-tail-post-convergence-native-convergence-postapply.txt" ]]
[[ ! -e "$E/native-tail-readback.txt" ]]
[[ $(jsonq "$E/native-post-apply-native-convergence.json" revalidation.skipped_cas) == 1 ]]
set +e; ( cmd_migrate_native_tail_rollback native-2 ) >/dev/null 2>&1; native_retry_rc=$?; set -e
[[ $native_retry_rc == 2 && ! -e "$E/native-post-apply-native-2.json" ]]
truncated_e="$scratch/truncated-native-e"; mkdir -p "$truncated_e"
printf '%s\n' 'activation_floor=2026-08-21T12:00:00.000Z' >"$truncated_e/activation-floor.txt"
printf '%s\n' 'Index Scan' >"$truncated_e/native-tail-post-plan.txt"
set +e
(
  E=$truncated_e
  native_tail_preview() {
    local out=$1 truncated=false
    [[ $out == native-tail-post-preview-before-truncated ]] && truncated=true
    printf '{"preview":{"scanned":1,"truncated":%s,"counts":{"v3_valid_to_v2_valid":1,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0}}}\n' "$truncated" >"$E/$out.json"
    echo "$E/$out.json"
  }
  cmd_migrate_native_tail_rollback truncated
)
truncated_rc=$?; set -e
[[ $truncated_rc == 2 ]]
[[ ! -e "$truncated_e/native-tail-post-convergence-truncated.txt" ]]
E=$main_e

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
# Forward producer-convergence: under an active v3 catalog, the bounded FROM
# population may monotonically decrease. Validate every gate fails closed
# and that multi-attempt and partial-apply resumption reconciles to zero residual.
assert_active_catalog_version() { [[ $1 == "$TO_VERSION" ]]; }
fwd_growth_spec='v2_valid_to_v3_valid=2,v2_skew_to_v3_clamped=1,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=1'
if ( emit_forward_convergence post growth "$fwd_growth_spec" 'v2_valid_to_v3_valid=3,v2_skew_to_v3_clamped=1,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=1' 3 4 ); then
  echo 'forward convergence accepted population growth' >&2; exit 1
fi
if ( emit_forward_convergence post bad-cell "$fwd_growth_spec" 'v2_valid_to_v3_valid=3,v2_skew_to_v3_clamped=0,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=0' 3 3 ); then
  echo 'forward convergence accepted a growing outcome cell' >&2; exit 1
fi
if ( emit_forward_convergence post mismatch "$fwd_growth_spec" 'v2_valid_to_v3_valid=1,v2_skew_to_v3_clamped=1,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=1' 3 1 ); then
  echo 'forward convergence accepted mismatched cell sum vs row delta' >&2; exit 1
fi
fwd_bad_receipt="$scratch/bad-fwd-receipt.txt"
[[ ! -e "$fwd_bad_receipt" ]]
(
  E="$scratch/fwd-aux-e"; mkdir -p "$E"
  emit_forward_convergence post aux "$fwd_growth_spec" 'v2_valid_to_v3_valid=2,v2_skew_to_v3_clamped=0,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=0' 3 2
  grep -Fxq 'producer_converged_rows=1' "$E/migrate-post-convergence-aux.txt"
  grep -Fxq 'v2_skew_to_v3_clamped_producer_converged=1' "$E/migrate-post-convergence-aux.txt"
  grep -Fxq 'zero_to_5m_clamped_producer_converged=1' "$E/migrate-post-convergence-aux.txt"
)

# Resumable multi-attempt forward apply, authorization gates, and readback reconciliation
fwd_e="$scratch/fwd-flow-e"; mkdir -p "$fwd_e"
(
  E="$fwd_e"
  preview_content='{"preview":{"scanned":10,"truncated":false,"counts":{"v2_valid_to_v3_valid":5,"v2_skew_to_v3_clamped":5,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":5}}}'
  printf '%s\n' "$preview_content" >"$E/migrate-freeze-post-preview-1.json"
  post_preview_sha=$(printf '%s\n' "$preview_content" | sha256sum | cut -d' ' -f1)

  ir_content='{"preview":{"scanned":1,"truncated":false,"counts":{"v2_valid_to_v3_valid":1,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}'
  printf '%s\n' "$ir_content" >"$E/migrate-freeze-ir-preview-1.json"
  ir_sha=$(migration_cells "$E/migrate-freeze-ir-preview-1.json" | sha256sum | cut -d' ' -f1)

  {
    echo "attempt=1"
    echo "drain_seconds=60"
    echo "max_preview_rows=123456"
    echo "post_cutoff=2026-08-21T00:00:00.000Z"
    echo "post_rows=10"
    echo "post_preview_sha256=$post_preview_sha"
    echo "ir_prereg_sha256=$ir_sha"
    echo "ir_semantic_changed=1"
    echo "ir_restored_valid=0"
    echo "ir_total_denominator=1"
  } >"$E/migrate-stable-population.txt"
  printf 'cutoff=2026-08-21T00:00:00.000Z\n' >"$E/migrate-freeze-post-cutoff-1.txt"
  printf 'from_in_horizon|10\nto_in_horizon|0\n' >"$E/migrate-freeze-post-scope-1.tsv"
  printf 'activation_floor=2026-08-20T23:59:59.000Z\n' >"$E/activation-floor.txt"

  assert_tree() { :; }
  assert_active_catalog_version() { [[ $1 == "$TO_VERSION" ]]; }
  sleep() { :; }
  psql_ro() {
    [[ "$*" == *'feedgen_ops.feed_catalog'* ]] && { echo "$EXPECTED_CONTRACT_ROWS|1|$TO_VERSION"; return; }
    [[ "$*" == *'content_time_validator_version='\''newsflows-content-time/v2'\'''* && "$*" == *'2026-08-20T23:59:59.000Z'* ]] && { echo "${INJECT_LATE_V2_ALL:-0}"; return; }
    [[ "$*" == *'content_time_status='\''source_valid'\'''* ]] && { echo "${INJECT_LATE_V2_SEMANTIC:-0}"; return; }
    [[ "$*" == *'<'* ]] && { echo "${INJECT_AFTER_IN:-0}"; return; }
    echo 0
  }
  control_rates() { echo '1 0 0'; }
  pgstat_table_read() { echo '1|1|0|1|1'; }

  # 1. Truncated authorization preview fails closed without leaving authorization marker or convergence receipt
  migration_preview_one() {
    local out=$1
    printf '{"preview":{"scanned":10,"truncated":true,"counts":{"v2_valid_to_v3_valid":5,"v2_skew_to_v3_clamped":5,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":5}}}\n' >"$E/$out.json"
    echo "$E/$out.json"
  }
  set +e; ( cmd_migrate_apply forward-1 ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]
  [[ ! -e "$E/migrate-apply-authorized.txt" ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-post-convergence-*.txt' | wc -l | tr -d ' ') == 0 ]]

  # 2. Late v2 semantic rows at/after cutoff fails closed
  INJECT_LATE_V2_SEMANTIC=1
  migration_preview_one() {
    local out=$1
    printf '{"preview":{"scanned":8,"truncated":false,"counts":{"v2_valid_to_v3_valid":4,"v2_skew_to_v3_clamped":4,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":4}}}\n' >"$E/$out.json"
    echo "$E/$out.json"
  }
  set +e; ( cmd_migrate_apply forward-1 ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]
  [[ ! -e "$E/migrate-apply-authorized.txt" ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-post-convergence-*.txt' | wc -l | tr -d ' ') == 0 ]]
  INJECT_LATE_V2_SEMANTIC=0

  # 2b. Compatible (non-semantic) late v2 rows at/after floor fails closed
  INJECT_LATE_V2_ALL=1
  set +e; ( cmd_migrate_apply forward-1 ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]
  [[ ! -e "$E/migrate-apply-authorized.txt" ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-post-convergence-*.txt' | wc -l | tr -d ' ') == 0 ]]
  INJECT_LATE_V2_ALL=0

  # 3. Growing population fails closed
  migration_preview_one() {
    local out=$1
    printf '{"preview":{"scanned":11,"truncated":false,"counts":{"v2_valid_to_v3_valid":6,"v2_skew_to_v3_clamped":5,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":5}}}\n' >"$E/$out.json"
    echo "$E/$out.json"
  }
  set +e; ( cmd_migrate_apply forward-1 ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]
  [[ ! -e "$E/migrate-apply-authorized.txt" ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-post-convergence-*.txt' | wc -l | tr -d ' ') == 0 ]]

  # 4. Valid authorization and pre-apply preview converge 10 -> 8 rows.
  # The transaction updates seven; the producer legitimately converges the last
  # row before final readback.
  migration_preview_one() {
    local out=$1
    case "$out" in
      migrate-authorize-post-*)
        printf '{"preview":{"scanned":8,"truncated":false,"counts":{"v2_valid_to_v3_valid":4,"v2_skew_to_v3_clamped":4,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":4}}}\n' >"$E/$out.json";;
      migrate-post-preview-before-forward-1)
        printf '{"preview":{"scanned":8,"truncated":false,"counts":{"v2_valid_to_v3_valid":4,"v2_skew_to_v3_clamped":4,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":4}}}\n' >"$E/$out.json";;
      *) echo "unexpected preview $out" >&2; return 2;;
    esac
    echo "$E/$out.json"
  }
  migration_apply_one() {
    local target=$1 label=$2
    [[ $target == post ]]
    [[ $label == forward-1 ]]
    printf '{"table":"post","revalidation":{"updated":7,"skipped_cas":1,"complete":true,"counts":{"v2_valid_to_v3_valid":3,"v2_skew_to_v3_clamped":4,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":4}}}\n' >"$E/migrate-post-apply-$label.json"
  }
  set +e; ( cmd_migrate_apply forward-1 ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]
  [[ -f "$E/migrate-apply-authorized.txt" ]]
  grep -Fxq 'producer_converged_rows=2' "$E/migrate-post-convergence-authorize-3.txt"
  grep -Fxq 'v2_valid_to_v3_valid_producer_converged=1' "$E/migrate-post-convergence-authorize-3.txt"
  grep -Fxq 'v2_skew_to_v3_clamped_producer_converged=1' "$E/migrate-post-convergence-authorize-3.txt"
  grep -Fxq 'zero_to_5m_clamped_producer_converged=1' "$E/migrate-post-convergence-authorize-3.txt"
  grep -Fxq 'producer_converged_rows=0' "$E/migrate-post-convergence-forward-1.txt"
  [[ $(migration_remaining_rows post) == 1 ]]
  [[ $(migration_remaining_spec post) == 'v2_valid_to_v3_valid=1,v2_skew_to_v3_clamped=0,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=0' ]]

  # 6. Readback refusal tests:
  # 6a. Nonzero residual refuses readback marker
  migration_preview_one() {
    local out=$1
    printf '{"preview":{"scanned":1,"truncated":false,"counts":{"v2_valid_to_v3_valid":1,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/$out.json"
    echo "$E/$out.json"
  }
  set +e; ( set -e; cmd_migrate_readback ) >/dev/null 2>&1; readback_rc=$?; set -e
  [[ $readback_rc == 2 ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-readback-*.txt' | wc -l | tr -d ' ') == 0 ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-post-convergence-readback-*.txt' | wc -l | tr -d ' ') == 0 ]]

  # 6b. Compatible late v2 row refuses readback marker
  migration_preview_one() {
    local out=$1
    printf '{"preview":{"scanned":0,"truncated":false,"counts":{"v2_valid_to_v3_valid":0,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/$out.json"
    echo "$E/$out.json"
  }
  INJECT_LATE_V2_ALL=1
  set +e; ( set -e; cmd_migrate_readback ) >/dev/null 2>&1; readback_rc=$?; set -e
  [[ $readback_rc == 2 ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-readback-*.txt' | wc -l | tr -d ' ') == 0 ]]
  INJECT_LATE_V2_ALL=0

  # 6c. The immutable CAS-conflict receipt terminally refuses this evidence root.
  set +e; ( set -e; cmd_migrate_readback ) >/dev/null 2>&1; readback_rc=$?; set -e
  [[ $readback_rc == 2 ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-readback-*.txt' | wc -l | tr -d ' ') == 0 ]]
  [[ $(find "$E" -maxdepth 1 -name 'migrate-post-convergence-readback-*.txt' | wc -l | tr -d ' ') == 0 ]]

  [[ $(jsonq "$E/migrate-post-apply-forward-1.json" revalidation.skipped_cas) == 1 ]]
  set +e; ( cmd_migrate_apply forward-2 ) >/dev/null 2>&1; retry_rc=$?; set -e
  [[ $retry_rc == 2 && ! -e "$E/migrate-post-apply-forward-2.json" ]]
)

# 8. All-producer convergence with zero apply receipts
all_conv_e="$scratch/all-conv-e"; mkdir -p "$all_conv_e"
(
  E="$all_conv_e"
  preview_content='{"preview":{"scanned":5,"truncated":false,"counts":{"v2_valid_to_v3_valid":3,"v2_skew_to_v3_clamped":2,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":2}}}'
  printf '%s\n' "$preview_content" >"$E/migrate-freeze-post-preview-1.json"
  post_preview_sha=$(printf '%s\n' "$preview_content" | sha256sum | cut -d' ' -f1)

  ir_content='{"preview":{"scanned":0,"truncated":false,"counts":{"v2_valid_to_v3_valid":0,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}'
  printf '%s\n' "$ir_content" >"$E/migrate-freeze-ir-preview-1.json"
  ir_sha=$(migration_cells "$E/migrate-freeze-ir-preview-1.json" | sha256sum | cut -d' ' -f1)

  {
    echo "attempt=1"
    echo "drain_seconds=60"
    echo "max_preview_rows=123456"
    echo "post_cutoff=2026-08-21T00:00:00.000Z"
    echo "post_rows=5"
    echo "post_preview_sha256=$post_preview_sha"
    echo "ir_prereg_sha256=$ir_sha"
    echo "ir_semantic_changed=0"
    echo "ir_restored_valid=0"
    echo "ir_total_denominator=0"
  } >"$E/migrate-stable-population.txt"
  printf 'cutoff=2026-08-21T00:00:00.000Z\n' >"$E/migrate-freeze-post-cutoff-1.txt"
  printf 'from_in_horizon|5\nto_in_horizon|0\n' >"$E/migrate-freeze-post-scope-1.tsv"
  printf 'activation_floor=2026-08-20T23:59:59.000Z\n' >"$E/activation-floor.txt"

  assert_tree() { :; }
  assert_active_catalog_version() { [[ $1 == "$TO_VERSION" ]]; }
  sleep() { :; }
  psql_ro() {
    [[ "$*" == *'feedgen_ops.feed_catalog'* ]] && { echo "$EXPECTED_CONTRACT_ROWS|1|$TO_VERSION"; return; }
    echo 0
  }
  control_rates() { echo '1 0 0'; }
  pgstat_table_read() { echo '1|1|0|1|1'; }

  migration_preview_one() {
    local out=$1
    printf '{"preview":{"scanned":0,"truncated":false,"counts":{"v2_valid_to_v3_valid":0,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/$out.json"
    echo "$E/$out.json"
  }
  migration_apply_one() { echo "apply unexpectedly called" >&2; exit 1; }

  cmd_migrate_apply forward-1
  [[ -f "$E/migrate-apply-authorized.txt" ]]
  [[ ! -e "$E/migrate-post-apply-forward-1.json" ]]
  grep -Fxq 'producer_converged_rows=5' "$E/migrate-post-convergence-authorize-1.txt"
  grep -Fxq 'v2_valid_to_v3_valid_producer_converged=3' "$E/migrate-post-convergence-authorize-1.txt"
  grep -Fxq 'v2_skew_to_v3_clamped_producer_converged=2' "$E/migrate-post-convergence-authorize-1.txt"
  grep -Fxq 'zero_to_5m_clamped_producer_converged=2' "$E/migrate-post-convergence-authorize-1.txt"
  [[ $(migration_remaining_rows post) == 0 ]]

  cmd_migrate_readback
  [[ -s "$E/migrate-readback-1.txt" ]]
  grep -Fq 'post rows=5' "$E/migrate-readback-1.txt"
)

# 9. Strict field tests on preview and apply
strict_e="$scratch/strict-e"; mkdir -p "$strict_e"
(
  E="$strict_e"
  set +e; ( cmd_migrate_apply '../unsafe' ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 && ! -e "$scratch/migrate-post-apply-unsafe.json" ]]
  good_spec='v2_valid_to_v3_valid=1,v2_skew_to_v3_clamped=0,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=0'
  # Missing scanned
  printf '{"preview":{"truncated":false,"counts":{"v2_valid_to_v3_valid":1,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/missing-scanned.json"
  set +e; ( gate_migration_preview "$E/missing-scanned.json" "$good_spec" test ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]

  # Missing cell in counts
  printf '{"preview":{"scanned":1,"truncated":false,"counts":{"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/missing-cell.json"
  set +e; ( gate_migration_preview "$E/missing-cell.json" "$good_spec" test ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]

  # Malformed non-integer cell in counts
  printf '{"preview":{"scanned":1,"truncated":false,"counts":{"v2_valid_to_v3_valid":"one","v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/malformed-cell.json"
  set +e; ( gate_migration_preview "$E/malformed-cell.json" "$good_spec" test ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]

  # Malformed non-integer auxiliary count
  printf '{"preview":{"scanned":1,"truncated":false,"counts":{"v2_valid_to_v3_valid":1,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":"bad","zero_to_5m_clamped":0}}}\n' >"$E/malformed-aux.json"
  set +e; ( gate_migration_preview "$E/malformed-aux.json" "$good_spec" test ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]

  # migration_cells on missing count
  set +e; ( migration_cells "$E/missing-cell.json" ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]

  # migration_applied_value on malformed apply receipt
  printf '{"table":"post","revalidation":{"updated":"not_int","skipped_cas":0,"complete":true,"counts":{"v2_valid_to_v3_valid":1,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/migrate-post-apply-bad.json"
  set +e; ( migration_applied_value post revalidation.updated ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 ]]
)

# Final candidate accounting must follow cell deltas even when no rows remain.
prepare_forward_candidate_e() {
  local dst=$1
  mkdir -p "$dst"
  cp "$fwd_e"/migrate-stable-population.txt "$fwd_e"/migrate-freeze-{post,ir}-preview-1.json \
    "$fwd_e"/migrate-freeze-post-{cutoff-1.txt,scope-1.tsv} "$fwd_e"/activation-floor.txt "$dst/"
}
assert_active_catalog_version() { [[ $1 == "$TO_VERSION" ]]; }
psql_ro() {
  [[ "$*" == *'feedgen_ops.feed_catalog'* ]] && { echo "$EXPECTED_CONTRACT_ROWS|1|$TO_VERSION"; return; }
  echo 0
}
migration_preview_one() {
  local out=$1
  printf '{"preview":{"scanned":0,"truncated":false,"counts":{"v2_valid_to_v3_valid":0,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/$out.json"
  echo "$E/$out.json"
}
forward_bad_e="$scratch/forward-primary-short-e"; prepare_forward_candidate_e "$forward_bad_e"
(
  E=$forward_bad_e
  printf '{"revalidation":{"updated":10,"skipped_cas":0,"complete":true,"counts":{"v2_valid_to_v3_valid":4,"v2_skew_to_v3_clamped":5,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":5}}}\n' >"$E/migrate-post-apply-forward-1.json"
  set +e; ( set -e; cmd_migrate_readback ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 && ! -e "$E/migrate-readback-1.txt" ]]
  [[ $(find "$E" -name 'migrate-post-convergence-readback-*.txt' | wc -l | tr -d ' ') == 0 ]]
)
forward_aux_e="$scratch/forward-aux-only-e"; prepare_forward_candidate_e "$forward_aux_e"
(
  E=$forward_aux_e
  printf '{"revalidation":{"updated":10,"skipped_cas":0,"complete":true,"counts":{"v2_valid_to_v3_valid":5,"v2_skew_to_v3_clamped":5,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":4}}}\n' >"$E/migrate-post-apply-forward-1.json"
  cmd_migrate_readback
  grep -Fxq 'producer_converged_rows=0' "$E/migrate-post-convergence-readback-1.txt"
  grep -Fxq 'zero_to_5m_clamped_producer_converged=1' "$E/migrate-post-convergence-readback-1.txt"
  cmd_migrate_readback
  [[ $(find "$E" -name 'migrate-post-convergence-readback-*.txt' | wc -l | tr -d ' ') == 1 ]]
)

prepare_native_candidate_e() {
  local dst=$1 preview
  mkdir -p "$dst"
  printf 'activation_floor=2026-08-21T12:00:00.000Z\n' >"$dst/activation-floor.txt"
  printf 'Index Scan\n' >"$dst/native-tail-post-plan.txt"
  preview='{"preview":{"scanned":10,"truncated":false,"counts":{"v3_valid_to_v2_valid":5,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":5,"v3_to_v2_invalid":0,"gt_5m_invalidated":5,"zero_to_5m_unclamped":0}}}'
  printf '%s\n' "$preview" >"$dst/native-tail-post-preview.json"
  { echo 'activation_floor=2026-08-21T12:00:00.000Z'; echo "drain_seconds=$MIGRATION_DRAIN_SECONDS"; echo 'post_rows=10'; echo "post_preview_sha256=$(printf '%s\n' "$preview" | sha256sum | cut -d' ' -f1)"; } >"$dst/native-tail-stable.txt"
}
assert_active_catalog_version() { [[ $1 == "$FROM_VERSION" ]]; }
native_tail_preview() {
  local out=$1 scanned=10 valid=5 invalid=5 aux=5
  [[ $out == *-after-* || $out == *-before-aux-retry ]] && { scanned=0; valid=0; invalid=0; aux=0; }
  printf '{"preview":{"scanned":%s,"truncated":false,"counts":{"v3_valid_to_v2_valid":%s,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":%s,"v3_to_v2_invalid":0,"gt_5m_invalidated":%s,"zero_to_5m_unclamped":0}}}\n' "$scanned" "$valid" "$invalid" "$aux" >"$E/$out.json"
  echo "$E/$out.json"
}
migration_apply_one() {
  local target=$1 label=$2 prefix=$5 aux=5 valid=5
  [[ $target == post && $prefix == native ]]
  [[ $label == primary-short ]] && valid=4
  [[ $label == aux-only ]] && aux=4
  printf '{"revalidation":{"updated":10,"skipped_cas":0,"complete":true,"counts":{"v3_valid_to_v2_valid":%s,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":5,"v3_to_v2_invalid":0,"gt_5m_invalidated":%s,"zero_to_5m_unclamped":0}}}\n' "$valid" "$aux" >"$E/native-post-apply-$label.json"
}
native_bad_e="$scratch/native-primary-short-e"; prepare_native_candidate_e "$native_bad_e"
(
  E=$native_bad_e
  set +e; ( set -e; cmd_migrate_native_tail_rollback primary-short ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 && ! -e "$E/native-tail-readback.txt" && ! -e "$E/native-tail-post-convergence-primary-short-postapply.txt" ]]
)
native_aux_e="$scratch/native-aux-only-e"; prepare_native_candidate_e "$native_aux_e"
(
  E=$native_aux_e
  cmd_migrate_native_tail_rollback aux-only
  grep -Fxq 'producer_converged_rows=0' "$E/native-tail-post-convergence-aux-only-postapply.txt"
  grep -Fxq 'gt_5m_invalidated_producer_converged=1' "$E/native-tail-post-convergence-aux-only-postapply.txt"
  cmd_migrate_native_tail_rollback aux-retry
  [[ $(find "$E" -name 'native-tail-post-convergence-*-postapply.txt' | wc -l | tr -d ' ') == 1 ]]
)

echo 'content-time revalidation packet contract ok'
