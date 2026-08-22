#!/usr/bin/env bash
set -euo pipefail
dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
packet=$dir/content_time_contract_upgrade_packet.sh
bash -n "$packet"
must() { grep -Fq -- "$1" "$packet" || { echo "missing: $1" >&2; exit 1; }; }
must 'RKEYS=newsflow-nl-2,newsflow-fr-2,newsflow-cz-2,newsflow-ir-2,newsflow-be-k,newsflow-be-m'
must "fields.length!==1||fields[0]!=='content_time_contract_version'"
must 'check(j.atomic_change_set.request_body,from,to);check(j.atomic_change_set.rollback_request_body,to,from)'
must "j.blockers[0].code==='feedgen-projection-blocked'"
must "x.code==='catalog-ranker-feed-time-contract-mismatch'"
must 'catalog_sync_apply "$SOURCE_ROOT"'
must '--source-root "$1/config/newsflows/catalogs"'
must '--bsr-effective-config-json "$BSR_EFFECTIVE_CONFIG_JSON"'
must 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply'
must 'revalidate_runner migrate-prepare'
must 'revalidate_runner migrate-freeze'
must 'forward_to_completion'
must '"$REVALIDATE" "$@"'
must 'EXPECTED_REVALIDATE_RUNNER_SHA'
must 'EXPECTED_SHA=$PACKET_SOURCE_SHA'
must 'x-feedgen-source: $FEEDGEN_SHA'
must 'assert_packet_tree'
must 'assert_runtime_provenance'
must 'sleep "${DRAIN_INTERVAL_SECONDS:-60}"'
must 'post_bulk "$E/feedgen-rollback.json" 01-feedgen-rollback-v3-to-v2'
must 'revalidate_runner migrate-rollback apply reverse'
must 'revalidate_runner migrate-secret-scan'
must 'revalidate_runner migrate-finalize'
! grep -Fq -- 'PREREG_POST_MAIN' "$packet"
! grep -Fq -- 'PREREG_POST_BE' "$packet"
[[ $(grep '^RKEYS=' "$packet" | cut -d= -f2 | tr ',' '\n' | sort -u | wc -l | tr -d ' ') == 6 ]]
line() { grep -nF -- "$1" "$packet" | tail -1 | cut -d: -f1; }
first_line() { grep -nF -- "$1" "$packet" | head -1 | cut -d: -f1; }
(( $(line 'catalog_sync_apply "$SOURCE_ROOT"') < $(line 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply') ))
(( $(line 'revalidate_runner migrate-prepare') < $(line 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply') ))
(( $(line 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply') < $(line 'revalidate_runner migrate-freeze') ))
(( $(first_line '  revalidate_runner migrate-freeze') < $(first_line '  forward_to_completion') ))
(( $(line 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply') < $(line 'forward_to_completion') ))
(( $(line 'post_bulk "$E/feedgen-rollback.json" 01-feedgen-rollback-v3-to-v2') < $(line 'revalidate_runner migrate-rollback apply reverse') ))

# Execute the delegated runner's boundary functions: exactly two global
# migration targets, both at the single widest storage horizon. IR remains a
# separately scoped preview inside that runner, never a migration target.
runner=$dir/content_time_revalidate_packet.sh
tmp=$(mktemp -d)
git -C "$tmp" init -q packet-tree
git -C "$tmp/packet-tree" -c user.name=test -c user.email=test@example.invalid commit --allow-empty -qm fixture
packet_tree_sha=$(git -C "$tmp/packet-tree" rev-parse HEAD)
mkdir "$tmp/tree-evidence"
FTFU1_TEST_MODE=1 E="$tmp/tree-evidence" TREE="$tmp/packet-tree" PACKET_SOURCE_SHA="$packet_tree_sha" bash "$packet" test-packet-tree-binding
set +e
FTFU1_TEST_MODE=1 E="$tmp/tree-evidence" TREE="$tmp/packet-tree" PACKET_SOURCE_SHA=0000000000000000000000000000000000000000 bash "$packet" test-packet-tree-binding >/dev/null 2>&1
tree_rc=$?
set -e
[[ $tree_rc == 2 ]]
mkdir "$tmp/runtime-mock"
cat >"$tmp/runtime-mock/sudo" <<'SH'
#!/usr/bin/env bash
[[ ${1:-} == -n ]] && shift
exec "$@"
SH
cat >"$tmp/runtime-mock/docker" <<'SH'
#!/usr/bin/env bash
[[ "$1 $2" == 'image inspect' ]] || exit 9
echo "$MOCK_IMAGE_REVISION"
SH
chmod +x "$tmp/runtime-mock/sudo" "$tmp/runtime-mock/docker"
runtime_sha=1111111111111111111111111111111111111111
PATH="$tmp/runtime-mock:$PATH" MOCK_IMAGE_REVISION="$runtime_sha" FTFU1_TEST_MODE=1 E="$tmp/tree-evidence" EXPECTED_IMAGE=fixture FEEDGEN_SHA="$runtime_sha" bash "$packet" test-runtime-provenance
set +e
PATH="$tmp/runtime-mock:$PATH" MOCK_IMAGE_REVISION=2222222222222222222222222222222222222222 FTFU1_TEST_MODE=1 E="$tmp/tree-evidence" EXPECTED_IMAGE=fixture FEEDGEN_SHA="$runtime_sha" bash "$packet" test-runtime-provenance >/dev/null 2>&1
runtime_rc=$?
set -e
[[ $runtime_rc == 2 ]]
mkdir -p "$tmp/resume-tree/scripts" "$tmp/resume-tree/dist/tools" "$tmp/resume-tree/dist/util" "$tmp/resume-evidence"
cat >"$tmp/resume-tree/scripts/content_time_revalidate_packet.sh" <<'SH'
#!/usr/bin/env bash
[[ "$1" == migrate-apply ]]
label=$2; echo "$label" >>"$E/coordinator-labels.txt"
for target in post engagement; do printf '{"revalidation":{"complete":true}}\n' >"$E/migrate-$target-apply-$label.json"; done
SH
touch "$tmp/resume-tree/dist/tools/backfill-publisher-posts.js" "$tmp/resume-tree/dist/util/content-time.js"
chmod +x "$tmp/resume-tree/scripts/content_time_revalidate_packet.sh"
git -C "$tmp/resume-tree" init -q
git -C "$tmp/resume-tree" add .
git -C "$tmp/resume-tree" -c user.name=test -c user.email=test@example.invalid commit -qm fixture
resume_tree_sha=$(git -C "$tmp/resume-tree" rev-parse HEAD)
printf '{"revalidation":{"complete":false}}\n' >"$tmp/resume-evidence/migrate-post-apply-forward-1.json"
printf '{}\n' >"$tmp/resume-evidence/migrate-post-preview-before-forward-1.json"
hex64=1111111111111111111111111111111111111111111111111111111111111111
PATH="$tmp/runtime-mock:$PATH" FTFU1_TEST_MODE=1 E="$tmp/resume-evidence" \
  SOURCE_ROOT="$tmp/packet-tree" SOURCE_SHA="$packet_tree_sha" SOURCE_CATALOG_SHA="$hex64" \
  ROLLBACK_SOURCE_ROOT="$tmp/packet-tree" ROLLBACK_SOURCE_SHA="$packet_tree_sha" ROLLBACK_CATALOG_SHA="$hex64" \
  TREE="$tmp/resume-tree" PACKET_SOURCE_SHA="$resume_tree_sha" FEEDGEN_SHA="$runtime_sha" \
  EXPECTED_DIST_SHA="$hex64" EXPECTED_CT_SHA="$hex64" EXPECTED_IMAGE_CT_SHA="$hex64" EXPECTED_IMAGE=fixture \
  EXPECTED_TOOL_REFS=x EXPECTED_RUNNER_SHA="$hex64" EXPECTED_REVALIDATE_RUNNER_SHA="$hex64" BSR_EFFECTIVE_CONFIG_JSON=x \
  PACKET_PATH="$packet" PACKET_SHA="$hex64" SINCE_MAIN=2026-08-18T00:00:00.000Z \
  SINCE_BE=2026-08-11T00:00:00.000Z SINCE_ENGAGEMENT=2026-08-11T00:00:00.000Z \
  bash "$packet" test-forward-resume
grep -Fxq 'forward-2' "$tmp/resume-evidence/coordinator-labels.txt"
[[ -s "$tmp/resume-evidence/migrate-post-apply-forward-2.json" && -s "$tmp/resume-evidence/migrate-engagement-apply-forward-2.json" ]]
(
  export CONTENT_TIME_PACKET_SOURCE_ONLY=1 E="$tmp/library-evidence" TREE="$tmp/library-tree" EXPECTED_SHA=x \
    EXPECTED_DIST_SHA256=x EXPECTED_CT_SHA256=x EXPECTED_IMAGE_CT_SHA256=x EXPECTED_TOOL_REFS=x \
    PACKET_SHA=1111111111111111111111111111111111111111111111111111111111111111 \
    FROM_VERSION=newsflows-content-time/v2 TO_VERSION=newsflows-content-time/v3 \
    SINCE_MAIN=2026-08-18T00:00:00.000Z SINCE_BE=2026-08-11T00:00:00.000Z \
    SINCE_ENGAGEMENT=2026-08-11T00:00:00.000Z PREREG_POST=x PREREG_ENGAGEMENT=x PREREG_IR=x
  set -- migrate-preflight
  # shellcheck source=content_time_revalidate_packet.sh
  . "$runner"
  [[ $(migration_targets) == 'post engagement' ]]
  [[ $(migration_target_since post) == "$SINCE_ENGAGEMENT" ]]
  [[ $(migration_target_since engagement) == "$SINCE_ENGAGEMENT" ]]
  [[ $(migration_cutoff_sql post) == *'(SELECT max("indexedAt")'* ]]
)

# Execute the post-switch population freeze with a fake DB. The single complete
# bounded preview becomes the denominator; truncation prevents publication.
(
  export CONTENT_TIME_PACKET_SOURCE_ONLY=1 E="$tmp/freeze-evidence" TREE="$tmp/library-tree" EXPECTED_SHA=x \
    EXPECTED_DIST_SHA256=x EXPECTED_CT_SHA256=x EXPECTED_IMAGE_CT_SHA256=x EXPECTED_TOOL_REFS=x \
    PACKET_SHA=1111111111111111111111111111111111111111111111111111111111111111 \
    FROM_VERSION=newsflows-content-time/v2 TO_VERSION=newsflows-content-time/v3 \
    SINCE_MAIN=2026-08-18T00:00:00.000Z SINCE_BE=2026-08-11T00:00:00.000Z \
    SINCE_ENGAGEMENT=2026-08-11T00:00:00.000Z PREREG_POST=x PREREG_ENGAGEMENT=x PREREG_IR=x \
    MIGRATION_DRAIN_SECONDS=7 FTFU1_TEST_MODE=1
  mkdir -p "$E"; : >"$E/pg-control-1.txt"; : >"$E/migrate-source-set.txt"
  set -- migrate-freeze
  # shellcheck source=content_time_revalidate_packet.sh
  . "$runner"
  FTFU1_TEST_MODE=0 MIGRATION_DRAIN_SECONDS=59
  set +e; ( validate_migration_inputs ) >/dev/null 2>&1; min_rc=$?; set -e
  [[ $min_rc == 2 ]]
  FTFU1_TEST_MODE=1 MIGRATION_DRAIN_SECONDS=7
  emit() { local name=$1; cat >"$E/$name"; }
  assert_tree() { :; }
  assert_active_catalog_version() { [[ $1 == newsflows-content-time/v3 || $1 == newsflows-content-time/v2 ]]; }
  latest_control() { echo "$E/pg-control-1.txt"; }
  sleep() { :; }
  psql_ro() {
    [[ "$*" == *'WITH bounds'* ]] && { printf 'max_from=2026-08-20T00:00:00.000Z|min_to=2026-08-21T00:00:00.000Z|cutoff=2026-08-21T00:00:00.000Z\n'; return; }
    [[ "$*" == *'created_at_source_raw IS NULL'* ]] && { echo 0; return; }
    [[ "$*" == *'SELECT count(*) FROM public.'* ]] && { echo 0; return; }
    printf 'from_in_horizon|1\nfrom_outside_horizon|0\nfrom_total|1\nto_in_horizon|0\n'
  }
  migration_preview_one() {
    local out=$1 target=$2 actors=${5:-} n=1 truncated=${INJECT_TRUNCATED:-false}
    [[ -z $actors ]] && n=2
    printf '{"preview":{"scanned":%s,"truncated":%s,"counts":{"v2_valid_to_v3_valid":%s,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' "$n" "$truncated" "$n" >"$E/$out.json"
    echo "$E/$out.json"
  }
  export INJECT_TRUNCATED=true
  set +e; ( cmd_migrate_freeze ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 && ! -e "$E/migrate-stable-population.txt" ]]
  export INJECT_TRUNCATED=false
  cmd_migrate_freeze >/dev/null
  [[ $(migration_stable_attempt) == 2 ]]
  grep -Fxq 'post_rows=2' "$E/migrate-stable-population.txt"
  grep -Fxq 'engagement_rows=2' "$E/migrate-stable-population.txt"
  grep -Fq 'v2_valid_to_v3_valid=2' <(migration_prereg_spec post)
  grep -Fq 'v2_valid_to_v3_valid=2' <(migration_prereg_spec engagement)
  grep -Fq 'v2_valid_to_v3_valid=1' <(migration_cells "$(migration_ir_preview_file)")
  grep -Fxq 'drain_seconds=7' "$E/migrate-stable-population.txt"

  # Rollback resumes against remaining cells/rows: post can already be complete
  # while engagement has only a partial receipt.
  migration_preview_one() {
    local out=$1 target=$2 from=$3 n
    if [[ $from == "$TO_VERSION" ]]; then
      if [[ -s "$E/rollback-$target-preview.json" ]]; then n=$(rollback_remaining_rows "$target"); else n=2; fi
      printf '{"preview":{"scanned":%s,"truncated":false,"counts":{"v3_valid_to_v2_valid":%s,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0}}}\n' "$n" "$n" >"$E/$out.json"
    else
      n=2
      printf '{"preview":{"scanned":%s,"truncated":false,"counts":{"v2_valid_to_v3_valid":%s,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' "$n" "$n" >"$E/$out.json"
    fi
    echo "$E/$out.json"
  }
  migration_apply_one() {
    local target=$1 label=$2 updated complete
    case "$label/$target" in
      reverse-1/post) updated=2; complete=true;;
      reverse-1/engagement) updated=1; complete=false;;
      reverse-2/engagement) updated=1; complete=true;;
      *) echo "unexpected rollback apply $label/$target" >&2; return 2;;
    esac
    printf '{"revalidation":{"updated":%s,"skipped_cas":0,"complete":%s,"counts":{"v3_valid_to_v2_valid":%s,"v3_clamped_to_v2_valid":0,"v3_clamped_to_v2_invalid":0,"v3_to_v2_invalid":0,"gt_5m_invalidated":0,"zero_to_5m_unclamped":0}}}\n' "$updated" "$complete" "$updated" >"$E/rollback-$target-apply-$label.json"
  }
  cmd_migrate_rollback dry-run
  set +e; cmd_migrate_rollback apply reverse-1; partial_rc=$?; set -e
  [[ $partial_rc == 3 ]]
  [[ $(rollback_remaining_rows post) == 0 && $(rollback_remaining_rows engagement) == 1 ]]
  [[ $(rollback_remaining_spec engagement) == *'v3_valid_to_v2_valid=1'* ]]
  cmd_migrate_rollback apply reverse-2
  [[ $(rollback_remaining_rows post) == 0 && $(rollback_remaining_rows engagement) == 0 ]]
  [[ ! -e "$E/rollback-post-apply-reverse-2.json" ]]
  grep -Fxq 'restored_rows=2' "$E/rollback-engagement-diff-1.txt"
)

mock=$tmp/mock; mkdir "$mock" "$tmp/evidence"
cat >"$mock/sudo" <<'SH'
#!/usr/bin/env bash
[[ ${1:-} == -n ]] && shift
echo "$*" >>"$MOCK_LOG"
"$@"
SH
cat >"$mock/systemctl" <<'SH'
#!/usr/bin/env bash
case "$1" in
  is-active) [[ $(grep -E "^(stop|start) $2$" "$MOCK_LOG" 2>/dev/null | tail -1) == "stop $2" ]] && echo inactive || echo active;;
  stop|start) echo "$1 $2" >>"$MOCK_LOG";;
esac
SH
chmod +x "$mock/sudo" "$mock/systemctl"
export MOCK_LOG=$tmp/systemctl.log
set +e
PATH="$mock:$PATH" FTFU1_TEST_MODE=1 E="$tmp/evidence" TIMER_UNITS=one.timer,two.timer SERVICE_UNITS=one.service,two.service bash "$packet" test-timer-restore >/dev/null 2>&1
rc=$?
set -e
[[ $rc != 0 ]]
grep -Fxq 'start one.timer' "$MOCK_LOG"
grep -Fxq 'start two.timer' "$MOCK_LOG"
rm -rf "$tmp"
echo 'content-time contract upgrade packet ok'
