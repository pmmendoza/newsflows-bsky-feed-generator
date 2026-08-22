#!/usr/bin/env bash
set -euo pipefail
dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
packet=$dir/content_time_contract_upgrade_packet.sh
bash -n "$packet"
must() { grep -Fq -- "$1" "$packet" || { echo "missing: $1" >&2; exit 1; }; }
must 'RKEYS=newsflow-nl-2,newsflow-fr-2,newsflow-cz-2,newsflow-ir-2,newsflow-be-k,newsflow-be-m'
must "fields.length!==1||fields[0]!=='content_time_contract_version'"
must 'check(j.atomic_change_set.request_body,from,to);check(j.atomic_change_set.rollback_request_body,to,from)'
must 'catalog_sync_apply "$SOURCE_ROOT"'
must '--source-root "$1/config/newsflows/catalogs"'
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
  EXPECTED_TOOL_REFS=x EXPECTED_RUNNER_SHA="$hex64" EXPECTED_REVALIDATE_RUNNER_SHA="$hex64" \
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
  [[ $(migration_snapshot_sql post "$FROM_VERSION") != *'author=ANY('* ]]
)

# Execute the post-switch population freeze with a fake DB. A v2 row arriving
# during the drain must stop before any canonical denominator is bound; a
# stable retry must bind the exact snapshot and generated cells.
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
  assert_active_catalog_version() { [[ $1 == newsflows-content-time/v3 ]]; }
  latest_control() { echo "$E/pg-control-1.txt"; }
  sleep() { :; }
  psql_ro() {
    [[ "$*" == *'created_at_source_raw IS NULL'* ]] && { [[ ${INJECT_NULL_AFTER_SCOPE:-0} == 1 && -e "$tmp/scopes-complete" ]] && echo 1 || echo 0; return; }
    if [[ ${FAIL_SCOPE_ONCE:-0} == 1 && "$*" == *'public.engagement'* && ! -e "$tmp/scope-failed" ]]; then : >"$tmp/scope-failed"; return 9; fi
    [[ "$*" == *'public.engagement'* ]] && : >"$tmp/scopes-complete"
    printf 'from_in_horizon|1\nfrom_outside_horizon|0\nfrom_total|1\nto_in_horizon|0\n'
  }
  psql_copy() {
    local sql=$1 target=engagement state n rows=1
    [[ "$sql" == *'public.post'* ]] && target=post
    state="$tmp/$target-calls"; n=$(cat "$state" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" >"$state"
    [[ $target == post && ${INJECT_V2:-0} == 1 && $n -ge 2 ]] && rows=2
    [[ $target == post && ${INJECT_V2:-0} == 0 ]] && rows=2
    [[ $target == post && ${INJECT_AFTER_SCOPE:-0} == 1 && -e "$tmp/scopes-complete" ]] && rows=3
    for i in $(seq 1 "$rows"); do printf 'uri-%s-%s\tauthor\t2026-08-20T00:00:00.000Z\t2026-08-20T00:00:00.000Z\t2026-08-20T00:00:00.000Z\tsource_valid\t\tnewsflows-content-time/v2\t31\n' "$target" "$i"; done
  }
  migration_preview_one() {
    local out=$1 target=$2 actors=${5:-} n=1
    [[ $target == post && -z $actors ]] && n=2
    printf '{"preview":{"scanned":%s,"counts":{"v2_valid_to_v3_valid":%s,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' "$n" "$n" >"$E/$out.json"
    echo "$E/$out.json"
  }
  export INJECT_V2=1
  set +e; ( cmd_migrate_freeze ) >/dev/null 2>&1; rc=$?; set -e
  [[ $rc == 2 && ! -e "$E/migrate-stable-population.txt" ]]
  rm -f "$tmp/post-calls" "$tmp/engagement-calls"; export INJECT_V2=0 FAIL_SCOPE_ONCE=1
  set +e; ( cmd_migrate_freeze ) >/dev/null 2>&1; publish_rc=$?; set -e
  [[ $publish_rc != 0 && -s "$E/migrate-freeze-post-scope-2.tsv" && ! -e "$E/migrate-stable-population.txt" ]]
  rm -f "$tmp/post-calls" "$tmp/engagement-calls" "$tmp/scopes-complete"; export FAIL_SCOPE_ONCE=0 INJECT_AFTER_SCOPE=1
  set +e; ( cmd_migrate_freeze ) >/dev/null 2>&1; late_rc=$?; set -e
  [[ $late_rc == 2 && ! -e "$E/migrate-stable-population.txt" ]]
  rm -f "$tmp/post-calls" "$tmp/engagement-calls" "$tmp/scopes-complete"; export INJECT_AFTER_SCOPE=0 INJECT_NULL_AFTER_SCOPE=1
  set +e; ( cmd_migrate_freeze ) >/dev/null 2>&1; null_rc=$?; set -e
  [[ $null_rc == 2 && ! -e "$E/migrate-stable-population.txt" ]]
  rm -f "$tmp/post-calls" "$tmp/engagement-calls" "$tmp/scopes-complete"; export INJECT_NULL_AFTER_SCOPE=0
  cmd_migrate_freeze >/dev/null
  [[ $(migration_stable_attempt) == 5 ]]
  (( $(wc -l <"$(migration_prestate_file post)") == 2 ))
  grep -Fq 'v2_valid_to_v3_valid=2' <(migration_prereg_spec post)
  grep -Fq 'v2_valid_to_v3_valid=1' <(migration_prereg_spec engagement)
  grep -Fq 'v2_valid_to_v3_valid=1' <(migration_cells "$(migration_ir_preview_file)")
  grep -Fxq 'drain_seconds=7' "$E/migrate-stable-population.txt"
  migration_preview_one() {
    local out=$1 target=$2 actors=${5:-} n=1
    [[ $target == post && -z $actors ]] && n=$(migration_remaining_rows post)
    [[ $target == engagement ]] && n=$(migration_remaining_rows engagement)
    printf '{"preview":{"scanned":%s,"counts":{"v2_valid_to_v3_valid":%s,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' "$n" "$n" >"$E/$out.json"
    echo "$E/$out.json"
  }
  migration_apply_one() {
    local target=$1 label=$2 updated=0 complete=true
    case "$label/$target" in forward-1/post) updated=1; complete=false;; forward-1/engagement) updated=1;; forward-2/post) updated=1;; esac
    printf '{"revalidation":{"updated":%s,"complete":%s,"counts":{"v2_valid_to_v3_valid":%s,"v2_skew_to_v3_clamped":0,"v2_invalid_to_v3_clamped":0,"v2_to_v3_invalid":0,"gt_5m_restored":0,"zero_to_5m_clamped":0}}}\n' "$updated" "$complete" "$updated" >"$E/migrate-$target-apply-$label.json"
  }
  cmd_migrate_apply forward-1
  [[ $(migration_remaining_rows post) == 1 && $(migration_remaining_rows engagement) == 0 ]]
  cmd_migrate_apply forward-2
  [[ $(migration_remaining_rows post) == 0 && $(migration_remaining_rows engagement) == 0 ]]
  grep -q '"complete":true' "$E/migrate-post-apply-forward-2.json"
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
