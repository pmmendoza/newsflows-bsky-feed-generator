/**
 * Sprint 12 incident regression test (2026-05-04).
 *
 * The DummyDriver-based unit tests in test_ranker_priority_helper.ts
 * only **compile** SQL; they don't execute it. This let an invalid-SQL
 * regression (`eb.fn('cast', [...])` → `cast(arg1, arg2)`, which
 * Postgres rejects with `syntax error at or near ","`) ship to prod
 * with green tests. Every variant-2 / IR-4 feed request 500'd silently.
 *
 * This test connects to a real Postgres (env var FEEDGEN_TEST_DSN, e.g.
 * a staging database) and EXECUTES the helper against an empty / cheap
 * post window. Skips silently if DSN env is unset.
 *
 * Run:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *     npx ts-node scripts/test_ranker_priority_helper_execute.ts
 */

import { Kysely, PostgresDialect } from 'kysely'
import { Pool } from 'pg'
import { applyPriorityOrderForFeed } from '../src/algos/ranker-priority-helper'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn) {
    console.log('SKIP: set FEEDGEN_TEST_DSN to a reachable Postgres to enable')
    return
  }

  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
  })

  const cases = [
    { rkey: 'newsflow-ir-4', envOn: true },
    { rkey: 'newsflow-nl-2', envOn: true },
    { rkey: 'newsflow-fr-2', envOn: false }, // legacy fallback
  ]

  let failed = 0
  for (const { rkey, envOn } of cases) {
    const envKey = `FEEDGEN_PRIORITY_FROM_RANKER_PROD_${rkey
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, '_')}`
    const prev = process.env[envKey]
    if (envOn) process.env[envKey] = 'true'
    else delete process.env[envKey]

    const base = db
      .selectFrom('post')
      .selectAll('post')
      .where('post.author', '=', 'did:plc:does-not-exist-test')
      .where('post.indexedAt', '>=', '2099-01-01T00:00:00Z')
    const q = applyPriorityOrderForFeed(base, rkey).limit(1)

    try {
      const rows = await q.execute()
      console.log(
        `  ✓ ${rkey} (envOn=${envOn}) executed cleanly; rows=${rows.length}`,
      )
    } catch (err: any) {
      failed++
      console.log(`  ✗ ${rkey} (envOn=${envOn}) execution FAILED:`)
      console.log(`    ${err.message ?? err}`)
    }

    if (prev === undefined) delete process.env[envKey]
    else process.env[envKey] = prev
  }

  await db.destroy()

  console.log()
  console.log(`Summary: ${cases.length - failed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
}

main().catch((err) => {
  console.error('test harness error:', err)
  process.exit(2)
})
