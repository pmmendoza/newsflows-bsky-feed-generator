#!/usr/bin/env bash
set -euo pipefail

packet="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/content_time_revalidate_packet.sh"
bash -n "$packet"

must_have() { grep -Fq -- "$1" "$packet" || { echo "missing packet contract: $1" >&2; exit 1; }; }

must_have 'migrate-prereg) cmd_migrate_prereg'
must_have 'migrate-preflight) cmd_migrate_preflight'
must_have 'migrate-preview) cmd_migrate_preview'
must_have 'migrate-apply) cmd_migrate_apply'
must_have 'migrate-readback) cmd_migrate_readback'
must_have 'migrate-rollback) cmd_migrate_rollback'
must_have 'migrate-secret-scan) cmd_secret_scan'
must_have 'migrate-finalize) cmd_migrate_finalize'
must_have "migration_targets() { echo 'post engagement'; }"
must_have '--from-version "$from" --to-version "$to"'
must_have 'migration_apply_one "$target" "$label" "$TO_VERSION" "$FROM_VERSION" rollback'
must_have '$prefix-$target-checkpoint.json'
must_have 'migrate-$target-prestate.tsv'
must_have 'gt_5m_restored'
must_have 'zero_to_5m_clamped'
must_have 'PREREG_IR'
must_have 'PREREG_POST'
must_have 'PREREG_ENGAGEMENT'
must_have 'migration_transition=$FROM_VERSION->$TO_VERSION'

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export CONTENT_TIME_PACKET_SOURCE_ONLY=1 E="$scratch/evidence" TREE="$scratch/tree" EXPECTED_SHA=x \
  EXPECTED_DIST_SHA256=x EXPECTED_CT_SHA256=x EXPECTED_IMAGE_CT_SHA256=x \
  EXPECTED_TOOL_REFS=x PACKET_SHA=1111111111111111111111111111111111111111111111111111111111111111 \
  FROM_VERSION=newsflows-content-time/v2 TO_VERSION=newsflows-content-time/v3 \
  SINCE_MAIN=2026-08-18T00:00:00.000Z SINCE_BE=2026-08-11T00:00:00.000Z \
  SINCE_ENGAGEMENT=2026-08-11T00:00:00.000Z \
  PREREG_POST=x PREREG_ENGAGEMENT=x PREREG_IR=x
set -- migrate-preflight
# shellcheck source=content_time_revalidate_packet.sh
. "$packet"

[[ $(migration_targets) == 'post engagement' ]]
[[ $(migration_target_table post) == post && $(migration_target_table engagement) == engagement ]]
[[ $(migration_target_since post) == "$SINCE_ENGAGEMENT" ]]
[[ $(migration_snapshot_sql post "$FROM_VERSION") != *'author=ANY('* ]]
[[ $(migration_snapshot_sql engagement "$FROM_VERSION") != *'author=ANY('* ]]

good="$scratch/good.json"; bad="$scratch/bad.json"
cat >"$good" <<'JSON'
{"preview":{"scanned":10,"counts":{"v1_valid_to_v2_valid":0,"v1_invalid_to_v2_valid":0,"v1_to_v2_invalid":0,"v2_valid_to_v3_valid":5,"v2_skew_to_v3_clamped":2,"v2_invalid_to_v3_clamped":3,"v2_to_v3_invalid":0,"v3_valid_to_v2_valid":0,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_restored":3,"zero_to_5m_clamped":2,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0,"by_invalid_reason":{},"by_v2_invalid_reason":{}}}}
JSON
sed 's/"v1_to_v2_invalid":0/"v1_to_v2_invalid":1/' "$good" >"$bad"
spec='v2_valid_to_v3_valid=5,v2_skew_to_v3_clamped=2,v2_invalid_to_v3_clamped=3,v2_to_v3_invalid=0,gt_5m_restored=3,zero_to_5m_clamped=2'
gate_migration_preview "$good" "$spec" functional
if ( gate_migration_preview "$bad" "$spec" functional >/dev/null 2>&1 ); then echo 'unknown non-zero outcome was accepted' >&2; exit 1; fi

captured="$scratch/args"
run_tool() { printf '%s\n' "$*" >"$captured"; echo 0; }
psql_ro() { echo 'did:plc:a,did:plc:b'; }
migration_run_tool receipt post "$FROM_VERSION" "$TO_VERSION" --apply >/dev/null
grep -Fq -- "--table post --since $SINCE_ENGAGEMENT --actors did:plc:a,did:plc:b --from-version $FROM_VERSION --to-version $TO_VERSION" "$captured"
migration_run_tool receipt engagement "$TO_VERSION" "$FROM_VERSION" --apply >/dev/null
grep -Fq -- "--table engagement --since $SINCE_ENGAGEMENT --from-version $TO_VERSION --to-version $FROM_VERSION" "$captured"

assert_tree() { :; }; assert_active_catalog_version() { [[ $1 == "$TO_VERSION" ]]; }
migration_apply_one() { echo "$1"; }
[[ $(cmd_migrate_apply test) == $'post\nengagement' ]]

calls="$scratch/preview-calls"
migration_preview_one() { echo "$2" >>"$calls"; echo "$good"; }
PREREG_POST=$spec PREREG_ENGAGEMENT=$spec cmd_migrate_preview
[[ $(paste -sd, "$calls") == post,engagement ]]

mkdir -p "$E"
row_v2=$'at://row\tdid:plc:a\t2026-08-20T00:00:00.000Z\t2026-08-19T00:00:00.000Z\t2026-08-19T00:00:00.000Z\tsource_valid\t\\N\tnewsflows-content-time/v2\t616263'
row_v3=${row_v2/newsflows-content-time\/v2/newsflows-content-time\/v3}
printf '%s\n' "$row_v2" >"$E/migrate-post-prestate.tsv"
printf '%s\n' "$row_v2" >"$E/migrate-engagement-prestate.tsv"
printf 'from_outside_horizon|0\nfrom_total|1\n' >"$E/migrate-post-prestate-scope.tsv"
printf 'from_outside_horizon|0\nfrom_total|1\n' >"$E/migrate-engagement-prestate-scope.tsv"
counts='{"v2_valid_to_v3_valid":1,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}'
printf '{"revalidation":{"updated":1,"counts":%s}}\n' "$counts" >"$E/migrate-post-apply-test.json"
printf '{"revalidation":{"updated":1,"counts":%s}}\n' "$counts" >"$E/migrate-engagement-apply-test.json"
printf '{"preview":{"scanned":1,"counts":{"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' >"$E/migrate-ir-preview-preflight.json"
empty="$scratch/empty.json"; printf '{"preview":{"scanned":0,"counts":{}}}\n' >"$empty"
emit() { mkdir -p "$E"; dd of="$E/$1" status=none; }
migration_preview_one() { cp "$empty" "$E/$1.json"; echo "$E/$1.json"; }
psql_copy() { [[ $1 == *public.post* ]] && printf '%s\n' "$row_v3" || printf '%s\n' "$row_v3"; }
psql_ro() { echo 0; }
PREREG_POST='v2_valid_to_v3_valid=1,v2_skew_to_v3_clamped=0,v2_invalid_to_v3_clamped=0,v2_to_v3_invalid=0,gt_5m_restored=0,zero_to_5m_clamped=0'
PREREG_ENGAGEMENT=$PREREG_POST
cmd_migrate_readback
grep -q '^post rows=1$' "$E/migrate-readback-1.txt"
grep -q '^engagement rows=1$' "$E/migrate-readback-1.txt"

assert_active_catalog_version() { :; }
migration_apply_one() { :; }
psql_copy() { printf '%s\n' "$row_v2"; }
cmd_migrate_rollback apply test
grep -q 'restored_exact=1' "$E/rollback-post-diff-1.txt"
grep -q 'restored_exact=1' "$E/rollback-engagement-diff-1.txt"

printf 'hits=0\n' >"$E/secret-scan.txt"
printf 'migration_transition=v2->v3\n' >"$E/migrate-source-set.txt"
printf 'verdict=ok\n' >"$E/migrate-post-ceiling-test.txt"
seal_evidence() { printf 'sealed\n' >"$E/SHA256SUMS"; }
cmd_migrate_finalize start end
grep -q '^status=complete$' "$E/RESULT.txt"
grep -q '^sealed$' "$E/SHA256SUMS"

echo 'content-time revalidation packet contract ok'
