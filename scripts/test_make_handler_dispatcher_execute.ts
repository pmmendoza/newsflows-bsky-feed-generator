/**
 * Sprint 15 / T5 polish — executing test for `pickPolicy`.
 *
 * Asserts:
 *   1. `pickPolicy(policy, publisherDid, rkey)` returns
 *      non-undefined `buildPublisher` + `buildFollows` builders for
 *      every value of the canonical Policy enum.
 *   2. Each pair of builders produces SQL that Postgres accepts when
 *      executed (with a fake publisher_did so zero rows match).
 *   3. The post-Sprint 15 catalog ↔ code naming convention holds:
 *      every `feed_catalog.algo_policy_id` value enumerated from
 *      staging Postgres is a known Policy. Catches a regression of
 *      the Sprint 14 unexpected finding (where the catalog enum
 *      diverged from the TS Policy type).
 *
 * Companion to `test_catalog_dispatch_execute.ts` (Sprint 14): that
 * test goes through `resolveDynamicHandler`; this one goes directly
 * through `pickPolicy`. Together they pin the static handler factory
 * + the dynamic dispatcher to the same SQL surface.
 *
 * Run:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *     npx ts-node scripts/test_make_handler_dispatcher_execute.ts
 *
 * Skips silently if FEEDGEN_TEST_DSN is unset.
 */

import { Kysely, PostgresDialect } from 'kysely'
import { Pool } from 'pg'
import { Policy, pickPolicy } from '../src/algos/make-handler'

const FAKE_PUBLISHER = 'did:plc:does-not-exist-test'
const FAKE_FOLLOWS = ['did:plc:also-does-not-exist']
const FUTURE_TIMESTAMP = '2099-01-01T00:00:00.000Z'

const KNOWN_POLICIES: Policy[] = [
  'chronological',
  'ranker-priority',
  'engagement-sorted',
]

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn) {
    console.log('SKIP: set FEEDGEN_TEST_DSN to a reachable Postgres to enable')
    return
  }

  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
  })

  let failed = 0

  // Part 1: pickPolicy returns valid builders for every Policy value.
  for (const policy of KNOWN_POLICIES) {
    const rkey = `test-${policy}`
    let pair
    try {
      pair = pickPolicy(policy, FAKE_PUBLISHER, rkey)
    } catch (err: any) {
      failed++
      console.log(`  ✗ pickPolicy('${policy}') threw: ${err.message ?? err}`)
      continue
    }
    if (!pair?.buildPublisher || !pair?.buildFollows) {
      failed++
      console.log(`  ✗ pickPolicy('${policy}') returned incomplete pair`)
      continue
    }
    // Required env for ranker-priority builders.
    let prevFlag: string | undefined
    let flagKey = ''
    if (policy === 'ranker-priority') {
      flagKey = `FEEDGEN_PRIORITY_FROM_RANKER_PROD_${rkey
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '_')}`
      prevFlag = process.env[flagKey]
      process.env[flagKey] = 'true'
    }
    try {
      const pubQ = pair.buildPublisher(db, FUTURE_TIMESTAMP, FAKE_FOLLOWS, 0, 1)
      const folQ = pair.buildFollows(db, FUTURE_TIMESTAMP, FAKE_FOLLOWS, 0, 2)
      const [pubRows, folRows] = await Promise.all([pubQ.execute(), folQ.execute()])
      console.log(`  ✓ pickPolicy('${policy}') executed cleanly; pub=${pubRows.length} fol=${folRows.length}`)
    } catch (err: any) {
      failed++
      console.log(`  ✗ pickPolicy('${policy}') execute FAILED: ${err.message ?? err}`)
    } finally {
      if (flagKey) {
        if (prevFlag === undefined) delete process.env[flagKey]
        else process.env[flagKey] = prevFlag
      }
    }
  }

  // Part 2: every catalog row's algo_policy_id is in KNOWN_POLICIES.
  // This guards against the Sprint 14 unexpected finding regressing
  // (where the catalog had 'ranker-driven' but the code used
  // 'ranker-priority'). After migration 021 the names match;
  // this assertion locks that down.
  let catalogRows: Array<{ rkey: string; algo_policy_id: string }>
  try {
    catalogRows = await db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['rkey', 'algo_policy_id'])
      .where('enabled', '=', true)
      .execute()
  } catch (err: any) {
    failed++
    console.log(`  ✗ catalog enum audit FAILED: ${err.message ?? err}`)
    catalogRows = []
  }
  const knownSet = new Set<string>(KNOWN_POLICIES)
  const stranger = catalogRows.find((r) => !knownSet.has(r.algo_policy_id))
  if (stranger) {
    failed++
    console.log(`  ✗ catalog row ${stranger.rkey} has algo_policy_id='${stranger.algo_policy_id}' which is not in the Policy enum`)
  } else if (catalogRows.length > 0) {
    console.log(`  ✓ all ${catalogRows.length} enabled catalog rows use canonical Policy names`)
  }

  await db.destroy()

  console.log()
  if (failed > 0) {
    console.log(`FAILED: ${failed} probe(s) failed`)
    process.exit(1)
  }
  console.log(`OK: pickPolicy + catalog enum agree on the canonical Policy enum`)
}

main().catch((err) => {
  console.error('test harness error:', err)
  process.exit(2)
})
