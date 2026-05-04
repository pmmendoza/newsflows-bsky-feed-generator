# Drafts — preserved-but-inactive feed handlers

This directory holds feed-handler files that are **not registered**
in `algos/index.ts` and are **not compiled** (excluded in
`tsconfig.json`). They are preserved here verbatim so future
work can revive or reference them without a git-history dive.

## Why kept

Operator decision 2026-05-04: variant-4 (hybrid) and the UK family
are not part of any active study, but the implementations are
non-trivial — particularly the variant-4 hybrid ordering
(engagement + priority composite) — and may be wanted again.
Keeping them in `_drafts/` is cheaper than reconstructing them
from `git log`.

## What lives here

| File | Why parked |
|---|---|
| `feed-nl-4.ts` | Variant 4 (hybrid engagement+priority). Retired from main study. |
| `feed-fr-4.ts` | Same. |
| `feed-cz-4.ts` | Same. |
| `feed-uk-4.ts` | Same. |
| `feed-uk-1.ts` | UK chronological. Code complete; UK not in study. |
| `feed-uk-2.ts` | UK ranker-priority. Code complete; UK not in study. |
| `feed-uk-3.ts` | UK engagement-sorted. Code complete; UK not in study. |

`feed-uk-{1,2,3}.ts` here are the post-Sprint-11 shim form
(`makeHandler({...})`). `feed-{nl,fr,cz,uk}-4.ts` here are the
original full-handler forms — the variant-4 ordering uses an
engagement + `coalesce(priority, 0)` composite that doesn't
match any of the 3 active policies and so wasn't collapsed.

## How to revive

1. Move the file back to `src/algos/`.
2. Re-add the `import * as feedXX from './feed-xx-N'` line plus
   the registry entry in `algos/index.ts`.
3. Insert / re-enable the corresponding `feedgen_ops.feed_catalog`
   row (if a row exists with `enabled=false`, set it back to
   `true`; otherwise insert a new row via `POST /api/admin/feed_catalog`
   per the runbook).
4. If the variant-4 ordering is wanted, define a 4th policy in
   `src/algos/policies/` and update the shim `policy:` accordingly.

## Cross-references

- Sprint 11 collapse plan:
  `BSKY/dev_feeds/blueskyranker_v2/dev/storage/plan_storage_refactor/plan_feed_catalog_and_registry.md`
  ("Sprint 11 / Task 5").
- Post-Sprint-11 followup:
  `BSKY/dev_feeds/blueskyranker_v2/dev/storage/plan_storage_refactor/SPRINT_11.md`.
