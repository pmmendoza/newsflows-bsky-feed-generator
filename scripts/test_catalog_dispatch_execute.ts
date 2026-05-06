/**
 * Sprint 14 / T2 Phase 1 — executing test for catalog-dispatch.
 *
 * Two assertions per active feed:
 *   1. `resolveDynamicHandler(db, rkey)` returns a non-null handler.
 *   2. The returned handler builds a query that Postgres accepts
 *      (`.execute()` does not throw). We invoke the handler with a
 *      synthetic `ctx + params + requesterDid` mocked just enough to
 *      reach `buildFeed()` query execution.
 *
 * The intent mirrors `test_policies_execute.ts` (Sprint 13 / T5):
 * we are NOT verifying business logic; we are catching SQL/grant
 * bugs that DummyDriver-only tests miss. Business logic stays in
 * compile-only tests.
 *
 * Bootstrap (one-off):
 *   docker run -d --rm --name fg-test-pg -e POSTGRES_USER=feedgen \
 *     -e POSTGRES_PASSWORD=feedgen -e POSTGRES_DB=feedgen-db-staging \
 *     -p 5436:5432 postgres:17
 *   # apply migrations + seed feed_catalog rows
 *
 * Run:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *     npx ts-node scripts/test_catalog_dispatch_execute.ts
 *
 * Skips silently if FEEDGEN_TEST_DSN is unset.
 */

import { Kysely, PostgresDialect } from 'kysely'
import { Pool } from 'pg'
import {
  resolveDynamicHandler,
  invalidateDispatchCache,
} from '../src/algos/catalog-dispatch'
import type { AppContext } from '../src/config'

const FAKE_REQUESTER = 'did:plc:does-not-exist-test'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn) {
    console.log('SKIP: set FEEDGEN_TEST_DSN to a reachable Postgres to enable')
    return
  }

  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
  })

  // Enumerate every enabled feed in the catalog. This is the same
  // set T2 Phase 2 will start dispatching dynamically.
  let feedRows: Array<{ rkey: string; algo_policy_id: string }> = []
  try {
    feedRows = await db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['rkey', 'algo_policy_id'])
      .where('enabled', '=', true)
      .execute()
  } catch (err: any) {
    console.error(`failed to enumerate feed_catalog: ${err.message ?? err}`)
    await db.destroy()
    process.exit(2)
  }

  if (feedRows.length === 0) {
    console.log('SKIP: feed_catalog has zero enabled rows')
    await db.destroy()
    return
  }

  // Mock just enough of AppContext to reach buildFeed query execution.
  const ctx = { db } as unknown as AppContext

  let failed = 0
  for (const { rkey, algo_policy_id } of feedRows) {
    invalidateDispatchCache(rkey) // force a fresh resolve
    const handler = await resolveDynamicHandler(db, rkey)
    if (!handler) {
      failed++
      console.log(`  ✗ ${rkey} (${algo_policy_id}): resolveDynamicHandler returned null`)
      continue
    }

    // Required env var for ranker-priority handlers — set true so we
    // exercise the ranker_prod cross-schema JOIN (the cast-bug class).
    let prevPriorityFlag: string | undefined
    let priorityFlagKey = ''
    if (algo_policy_id === 'ranker-priority') {
      priorityFlagKey = `FEEDGEN_PRIORITY_FROM_RANKER_PROD_${rkey
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '_')}`
      prevPriorityFlag = process.env[priorityFlagKey]
      process.env[priorityFlagKey] = 'true'
    }

    try {
      // Use a tiny limit; we only care that SQL executes cleanly.
      // limit=3 → publisherLimit=1, followsLimit=2 (per Sprint 13 / T1).
      const result = await handler(ctx, { feed: '', limit: 3, cursor: undefined } as any, FAKE_REQUESTER)
      // result.feed exists on success; we don't care about contents
      console.log(`  ✓ ${rkey} (${algo_policy_id}): handler executed; feed.length=${result?.feed?.length ?? 0}`)
    } catch (err: any) {
      failed++
      console.log(`  ✗ ${rkey} (${algo_policy_id}) handler FAILED:`)
      console.log(`    ${err.message ?? err}`)
    } finally {
      if (priorityFlagKey) {
        if (prevPriorityFlag === undefined) delete process.env[priorityFlagKey]
        else process.env[priorityFlagKey] = prevPriorityFlag
      }
    }
  }

  await db.destroy()

  console.log()
  console.log(`Summary: ${feedRows.length - failed} passed, ${failed} failed (across ${feedRows.length} enabled rkeys)`)
  if (failed > 0) {
    console.log()
    console.log('Common causes:')
    console.log('  - Missing publisher_did in feed_catalog row → dispatch returns null.')
    console.log('  - SQL error in policy module (the cast-bug class).')
    console.log('  - permission denied → run preflight_db_perms.sh for scoped diagnostics.')
    process.exit(1)
  }
}

main().catch((err) => {
  console.error('test harness error:', err)
  process.exit(2)
})
