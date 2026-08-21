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
must_have "migration_targets() { echo 'post-main post-be engagement'; }"
must_have '--from-version "$from" --to-version "$to"'
must_have 'migration_apply_one "$target" "$label" "$TO_VERSION" "$FROM_VERSION" rollback'
must_have '$prefix-$target-checkpoint.json'
must_have 'migrate-$target-prestate.tsv'
must_have 'gt_5m_restored'
must_have 'zero_to_5m_clamped'
must_have 'PREREG_IR'
must_have 'PREREG_POST_MAIN'
must_have 'PREREG_POST_BE'
must_have 'PREREG_ENGAGEMENT'
must_have 'migration_transition=$FROM_VERSION->$TO_VERSION'

echo 'content-time revalidation packet contract ok'
