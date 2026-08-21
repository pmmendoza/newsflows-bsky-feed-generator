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
must 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply'
must 'forward_to_completion'
must '"$REVALIDATE" "$@"'
must 'EXPECTED_REVALIDATE_RUNNER_SHA'
must 'sleep "${DRAIN_INTERVAL_SECONDS:-60}"'
must 'post_bulk "$E/feedgen-rollback.json" 01-feedgen-rollback-v3-to-v2'
must 'revalidate_runner migrate-rollback apply reverse'
[[ $(grep '^RKEYS=' "$packet" | cut -d= -f2 | tr ',' '\n' | sort -u | wc -l | tr -d ' ') == 6 ]]
line() { grep -nF -- "$1" "$packet" | tail -1 | cut -d: -f1; }
(( $(line 'catalog_sync_apply "$SOURCE_ROOT"') < $(line 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply') ))
(( $(line 'post_bulk "$E/feedgen-forward.json" 03-feedgen-forward-apply') < $(line 'forward_to_completion') ))
(( $(line 'post_bulk "$E/feedgen-rollback.json" 01-feedgen-rollback-v3-to-v2') < $(line 'revalidate_runner migrate-rollback apply reverse') ))

tmp=$(mktemp -d); mock=$tmp/mock; mkdir "$mock" "$tmp/evidence"
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
