/**
 * Sprint 13 / T5 — executing integration test pattern across all
 * three policy modules.
 *
 * Why this exists: the 2026-05-04 cast-bug incident shipped to prod
 * with green DummyDriver compile-only tests. The
 * `eb.fn('cast', [...])` Kysely helper produced
 * `CAST(arg1, arg2)` SQL, which is invalid Postgres. The unit tests
 * for `ranker-priority-helper.ts` only **compiled** the SQL via the
 * compile-time stub driver — they never `.execute()`'d it against a
 * real database. Result: every variant-2 / IR-4 feed request 500'd
 * silently for 14 hours.
 *
 * `test_ranker_priority_helper_execute.ts` (Sprint 12 hotfix) was
 * the first executing integration test. T5 generalises the pattern
 * to cover the other two policy modules (chronological,
 * engagement-sorted) and the dispatcher (`pickPolicy`).
 *
 * Coverage matrix:
 *
 *   | policy             | publisher query | follows query |
 *   | chronological      |     ✓           |     ✓         |
 *   | engagement-sorted  |     ✓           |     ✓         |
 *   | ranker-priority    |  (in test_ranker_priority_helper_execute.ts)
 *
 * Each query is executed with a publisher DID guaranteed not to
 * have any matching rows (so `rows.length === 0` is the expected
 * result). The test only asserts that the SQL was accepted by
 * Postgres — we are catching SYNTAX / TYPE / GRANT errors, not
 * verifying business logic. Business-logic tests live in the
 * compile-only `test_*.ts` files and remain unchanged.
 *
 * Bootstrap (one-off, on the developer machine or CI runner):
 *   docker run -d --rm --name fg-test-pg -e POSTGRES_USER=feedgen \
 *     -e POSTGRES_PASSWORD=feedgen -e POSTGRES_DB=feedgen-db-staging \
 *     -p 5436:5432 postgres:17
 *   # then apply the migrations from src/db/migrations.ts via Kysely
 *
 * Run:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *     npx ts-node scripts/test_policies_execute.ts
 *
 * Skips silently if FEEDGEN_TEST_DSN is unset, so CI without a
 * Postgres bootstrap stays green and developers can opt-in.
 */

import { Kysely, PostgresDialect } from 'kysely'
import { Pool } from 'pg'
import {
  publisherQueryChronological,
  followsQueryChronological,
} from '../src/algos/policies/chronological'
import {
  publisherQueryEngagement,
  followsQueryEngagement,
} from '../src/algos/policies/engagement-sorted'
import {
  publisherQueryRankerPriority,
  followsQueryRankerPriority,
} from '../src/algos/policies/ranker-priority'

const FAKE_PUBLISHER = 'did:plc:does-not-exist-test'
const FAKE_FOLLOWS = ['did:plc:also-does-not-exist']
const TIME_LIMIT_FUTURE = '2099-01-01T00:00:00.000Z'

type Probe = {
  label: string
  build: (db: Kysely<any>) => any
  /** Set to true if the SQL JOINs ranker_prod (rare grant gap class). */
  touchesRankerProd?: boolean
}

const probes: Probe[] = [
  {
    label: 'chronological / publisher',
    build: (db) => publisherQueryChronological(db, TIME_LIMIT_FUTURE, [], 0, 1, FAKE_PUBLISHER),
  },
  {
    label: 'chronological / follows',
    build: (db) =>
      followsQueryChronological(db, TIME_LIMIT_FUTURE, FAKE_FOLLOWS, 0, 2, FAKE_PUBLISHER),
  },
  {
    label: 'engagement-sorted / publisher',
    build: (db) => publisherQueryEngagement(db, TIME_LIMIT_FUTURE, [], 0, 1, FAKE_PUBLISHER),
  },
  {
    label: 'engagement-sorted / follows',
    build: (db) =>
      followsQueryEngagement(db, TIME_LIMIT_FUTURE, FAKE_FOLLOWS, 0, 2, FAKE_PUBLISHER),
  },
  {
    label: 'ranker-priority / publisher (rkey=newsflow-nl-2)',
    touchesRankerProd: true,
    build: (db) =>
      publisherQueryRankerPriority(
        db,
        TIME_LIMIT_FUTURE,
        [],
        0,
        1,
        FAKE_PUBLISHER,
        'newsflow-nl-2',
      ),
  },
  {
    label: 'ranker-priority / follows (rkey=newsflow-nl-2)',
    touchesRankerProd: true,
    build: (db) =>
      followsQueryRankerPriority(
        db,
        TIME_LIMIT_FUTURE,
        FAKE_FOLLOWS,
        0,
        2,
        FAKE_PUBLISHER,
        'newsflow-nl-2',
      ),
  },
]

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn) {
    console.log('SKIP: set FEEDGEN_TEST_DSN to a reachable Postgres to enable')
    console.log('  example: FEEDGEN_TEST_DSN=postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging')
    return
  }

  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
  })

  // Required for ranker-priority probes; falls back to legacy path
  // when unset, but we want to exercise the new code path here.
  const prevPriorityFlag = process.env.FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2
  process.env.FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2 = 'true'

  let failed = 0
  for (const probe of probes) {
    const q = probe.build(db)
    try {
      const rows = await q.execute()
      const detail = probe.touchesRankerProd ? ' (touches ranker_prod)' : ''
      console.log(`  ✓ ${probe.label}${detail} executed cleanly; rows=${rows.length}`)
    } catch (err: any) {
      failed++
      console.log(`  ✗ ${probe.label} FAILED:`)
      console.log(`    ${err.message ?? err}`)
    }
  }

  if (prevPriorityFlag === undefined) {
    delete process.env.FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2
  } else {
    process.env.FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2 = prevPriorityFlag
  }

  await db.destroy()

  console.log()
  console.log(`Summary: ${probes.length - failed} passed, ${failed} failed`)
  if (failed > 0) {
    console.log()
    console.log('Common causes:')
    console.log('  - SQL syntax error: a Kysely helper produced invalid Postgres')
    console.log('    (the cast-bug class). Look for the exact error above.')
    console.log('  - Type error: an expression yielded the wrong column type')
    console.log('    (e.g. text vs. timestamptz). Add an explicit cast.')
    console.log('  - permission denied: a GRANT is missing. Run')
    console.log('    `bash dev/deploy/preflight_db_perms.sh` for a focused report.')
    process.exit(1)
  }
}

main().catch((err) => {
  console.error('test harness error:', err)
  process.exit(2)
})
