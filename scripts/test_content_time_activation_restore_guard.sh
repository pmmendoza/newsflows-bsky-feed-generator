#!/usr/bin/env bash
set -euo pipefail

evidence=$(mktemp -d)
trap 'rm -rf "$evidence"' EXIT
touch "$evidence/rollback-rowstate-gate-123456.txt"

set +e
output=$(env \
  E="$evidence" PACKET_PATH=/missing EXPECTED_PACKET_SHA=x TREE_CATALOG=/missing \
  CATALOG_SOURCE_SHA=0000000000000000000000000000000000000000 \
  EXPECTED_UPDATE_RKEYS=newsflow-nl-2 EXPECTED_EXPIRY=null EXPECTED_FLOOR=0.80 \
  EXPECTED_TOOL_REFS=x PREDECESSOR_SOURCE_SHA=1111111111111111111111111111111111111111 \
  EXPECTED_CATALOG_SHA=x EXPECTED_PREDECESSOR_CATALOG_SHA=x EXPECTED_RUNNER_SHA=x \
  EXPECTED_SHADOW_SQL_SHA=x EXPECTED_LATENCY_SH_SHA=x EXPECTED_REQUESTER_SHA=x \
  EXPECTED_FEEDGEN_IMAGE=x \
  bash "$(dirname "$0")/content_time_activation_packet.sh" apply 2>&1)
status=$?
set -e

[[ $status -eq 4 ]]
[[ "$output" == *"verified restore evidence already exists"* ]]
[[ ! -e "$evidence/approval-B2.txt" ]]
