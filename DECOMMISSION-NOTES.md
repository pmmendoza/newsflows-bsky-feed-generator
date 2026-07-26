# newsflow-be-1/2/3 decommission (feedgen side)

Scope: PRD.md decision D2 (`dev/be-vlg-elif/PRD.md`, R2) + the 2026-07-24
HANDOFF — `newsflow-be-1/2/3` are disabled+retired since 2026-07-18 and are
now being fully decommissioned. This lane covers only the feedgen repo build
work; catalog/AppView/deploy steps below are operator-owned and NOT executed
here.

## Files changed

- `src/algos/politician-filter.ts` — `BE_FILTER_RKEY` narrowed from
  `/^newsflow-be-(k|m|[123])$/` to `/^newsflow-be-(k|m)$/`. Updated the
  routing-comment and the two log/summary strings that listed
  `newsflow-be-{k,m,1,2,3}`. This was the only code path in this repo that
  special-cased the `[123]` variants — feed dispatch itself is already fully
  dynamic (`src/algos/catalog-dispatch.ts` reads `feedgen_ops.feed_catalog`
  at request time; there is no static `feed-be-{1,2,3}.ts` handler
  registration left to remove — the static registry was retired in Sprint 15
  and `src/algos/index.ts` exports an empty map). Once the catalog rows for
  be-1/2/3 are deleted (operator step below), those rkeys resolve through the
  same "unknown feed" path as any other unregistered rkey — no feedgen
  redeploy is required for that step alone.
- `scripts/test_politician_filter.ts` — flipped the routing-table assertions
  so `newsflow-be-1/2/3` assert `isPoliticianFilterRouted(...) === false`
  (previously asserted `true`), moved alongside `be-4` and non-BE rkeys in
  the "not routed" group. Also repointed the kill-switch assertion from
  `newsflow-be-1` (now unrouted, which would make the assertion vacuous) to
  `newsflow-be-k` so it still meaningfully exercises the kill-switch on a
  live BE feed.
- `README.md` — the `/api/subscribe` example payload used `"feed":
  "newsflow-be-1"`; updated to `"newsflow-be-k"` since be-1 is retired and no
  longer a valid subscribe target.

## Explicitly NOT touched (by design)

- `newsflow-be-k` / `newsflow-be-m` routing and `FEEDGEN_BE_POLITICIAN_FILTER`
  kill-switch semantics — unchanged, per task scope (the keep-all flip is a
  separate, not-yet-authorized step).
- `scripts/test_access_policy.ts` (`feed-be-1` / `newsflow-be-1`) and
  `scripts/test_describe_generator_catalog.ts` (`newsflow-be-2`) — these use
  BE-ish rkeys only as arbitrary example strings against a `fakeDb`/static
  fixture to test generic catalog-row and access-policy logic; they don't
  assert anything about be-1/2/3 as real, servable feeds and aren't part of
  the politician-filter routing surface. Left as-is to keep the diff scoped
  to the actual decommission surface.

## Tests

- `yarn test:politician-filter` — 42 passed, 0 failed (was 42/42 before the
  change too; the assertions moved, the count didn't, since equal numbers of
  cases moved between the "routed" and "not routed" groups).
- `npx tsc --noEmit` — clean.
- `yarn test:execute` (full chain) — fails at `test_retired_priority_endpoints.ts`
  with `TypeError: Cannot read properties of undefined (reading 'prototype')`
  in `node_modules/buffer-equal-constant-time` (via `jsonwebtoken`/`jwa`).
  **Confirmed pre-existing** via `git stash` + rerun on unmodified
  `origin/main`: identical failure, same file, same line. Root cause is a
  Node v26 vs. old `jsonwebtoken` transitive dependency incompatibility
  (`Buffer.SlowBuffer` removed), unrelated to this change and outside this
  task's scope.
- Every DB-backed script in the chain (`test_politician_filter_execute.ts`,
  `test_catalog_dispatch_execute.ts`, `test_policies_execute.ts`,
  `test_make_handler_dispatcher_execute.ts`, `test_buildfeed_smoke.ts`,
  firehose/db-restore rehearsals, etc.) SKIPs cleanly in this environment —
  no `FEEDGEN_TEST_DSN` / Postgres available. Ran each standalone to confirm
  clean skip, not silent failure.

## Remaining operator steps (NOT done here — production mutation, owner-gated)

1. **Catalog rows**: remove/retire the `newsflow-be-1/2/3` rows from
   `config/newsflows/catalogs/publishers.yml` (root BSKY repo), then run the
   catalog-sync/apply flow so `feedgen_ops.feed_catalog` reflects it.
2. **AppView records**: for each of the 3 rkeys, run feedgen's
   `yarn unpublishFeed` (or the non-interactive variant) against production to
   delete the `app.bsky.feed.generator` record from the AppView. Preserve
   `request_log`/`request_posts` history — this only removes the published
   feed record, not exposure/analysis data (PRD R2).
3. **Deploy this feedgen change**: build + deploy per
   `docs/runbooks/feedgen_production_deploy_runbook.md` (server checkout of
   `origin/main`, `/etc/newsflows/secrets/feedgen.env`, `docker-compose
   --env-file ... up -d --no-deps feedgen`, app-only verification). This step
   is optional in isolation (the `[123]` regex removal has no observable
   effect until the catalog rows are gone too, since a disabled catalog row
   already returns `null`/unknown-feed regardless of the filter regex) but
   should land before or alongside step 1 so the two sides don't drift.
4. Confirm post-deploy: `GET` the 3 retired rkeys' skeleton endpoint returns
   standard unknown-feed behavior, and `newsflow-be-k`/`newsflow-be-m` are
   unaffected (filter still routes/applies to them exactly as before).

## Rollback

- Code: `git revert <this commit>` on `origin/main`, rebuild, redeploy per the
  runbook. Restores the `[123]` branch of `BE_FILTER_RKEY` (a no-op filter
  match against feeds that no longer have catalog rows or AppView records —
  inert, not a functional restore of be-1/2/3 serving).
- Catalog/AppView: re-adding `newsflow-be-1/2/3` rows to
  `publishers.yml` + catalog-sync, and republishing via `yarn publishFeed`,
  is a separate, full reconstruction — not part of this code rollback. This
  build change does not delete any historical `request_log`/`request_posts`
  rows, so no data-loss recovery is needed on the feedgen-repo side.
