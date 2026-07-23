#!/usr/bin/env bash
# compute-feed-code-hash.sh [commit]  (default: HEAD)
#
# Behaviour-scoped hash of the feed-CONTROLLING code (FEEDGEN-CATHIST-002 §3b).
# Emits a sha256 over the git blob shas of the serving-behaviour path-set, so the
# hash changes only when a file that affects served feeds changes — not on
# unrelated commits, tests, or docs. Set the result as FEEDGEN_FEED_CODE_HASH at
# deploy time; /api/config and feed_catalog_history then capture it.
#
# Usage at deploy:
#   FEEDGEN_FEED_CODE_HASH=$(bash scripts/compute-feed-code-hash.sh "$FEEDGEN_BUILD_SHA")
set -euo pipefail

commit="${1:-HEAD}"

# Explicit path-set: the files whose logic decides what a feed serves.
paths=(
  src/methods/feed-generation.ts
  src/algos/catalog-dispatch.ts
  src/algos/make-handler.ts
  src/algos/feed-builder.ts
  src/algos/politician-filter.ts
  src/algos/ranker-priority-helper.ts
  src/util/access-policy.ts
  src/util/score-source-cache.ts
  src/util/publisher-dids.ts
  src/util/ingestion-scope.ts
  src/subscription.ts
  src/util/queries.ts
  src/util/link-fields.ts
  src/util/scheduled-updater.ts
)

# Plus every per-feed algo + policy file, so a new variant is picked up
# automatically without editing this list.
while IFS= read -r p; do
  [ -n "$p" ] && paths+=("$p")
done < <(git ls-tree -r --name-only "$commit" -- src/algos/ | grep -E '\.ts$' || true)

{
  for p in "${paths[@]}"; do
    # third field of ls-tree is the blob sha; empty if the path is absent.
    blob=$(git ls-tree "$commit" -- "$p" | awk '{print $3}')
    if [ -z "$blob" ]; then
      echo "compute-feed-code-hash: path absent at $commit: $p" >&2
      exit 1
    fi
    printf '%s:%s\n' "$p" "$blob"
  done
} | sort -u | shasum -a 256 | awk '{print $1}'
