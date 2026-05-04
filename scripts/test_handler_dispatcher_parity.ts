/**
 * Sprint 11 / Task 5 unit test — handler-dispatcher SQL parity.
 *
 * Verifies that the collapsed shim handlers (via `makeHandler`)
 * produce SQL byte-for-byte equivalent to the per-feed handlers
 * they replaced. We compile both shapes through Kysely's
 * `DummyDriver` and assert string equality of the emitted SQL.
 *
 * Run: `npx ts-node scripts/test_handler_dispatcher_parity.ts`
 * Exits non-zero on any mismatch.
 *
 * Cross-references:
 *   - Shim factory: src/algos/make-handler.ts
 *   - Policies: src/algos/policies/{chronological,ranker-priority,engagement-sorted}.ts
 *   - Plan: dev/storage/plan_storage_refactor/plan_feed_catalog_and_registry.md
 */

import {
  Kysely,
  DummyDriver,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
  sql,
} from 'kysely'

import {
  publisherQueryChronological,
  followsQueryChronological,
} from '../src/algos/policies/chronological'
import {
  publisherQueryRankerPriority,
  followsQueryRankerPriority,
} from '../src/algos/policies/ranker-priority'
import {
  publisherQueryEngagement,
  followsQueryEngagement,
} from '../src/algos/policies/engagement-sorted'

const db = new Kysely<any>({
  dialect: {
    createAdapter: () => new PostgresAdapter(),
    createDriver: () => new DummyDriver(),
    createIntrospector: (d) => new PostgresIntrospector(d),
    createQueryCompiler: () => new PostgresQueryCompiler(),
  },
})

let failed = 0
let passed = 0

function assert(cond: boolean, label: string, detail?: string) {
  if (cond) {
    passed++
    console.log(`  ✓ ${label}`)
  } else {
    failed++
    console.log(`  ✗ ${label}${detail ? ` — ${detail}` : ''}`)
  }
}

// --- Reference query builders mirroring the OLD per-feed handlers ---

function refChronologicalPublisher(timeLimit: string, publisherDid: string) {
  return db
    .selectFrom('post')
    .selectAll()
    .where('author', '=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(0)
    .limit(10)
}
function refChronologicalFollows(
  timeLimit: string,
  publisherDid: string,
  follows: string[],
) {
  return db
    .selectFrom('post')
    .selectAll()
    .where('author', '!=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .where((eb: any) => eb('author', 'in', follows))
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(0)
    .limit(10)
}

function refEngagementPublisher(timeLimit: string, publisherDid: string) {
  return db
    .selectFrom('post')
    .selectAll()
    .where('author', '=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .orderBy(
      sql`
    -- Base engagement score (likes + reposts + comments + quotes)
    COALESCE(
      (COALESCE(likes_count, 0) +
       COALESCE(repost_count, 0) +
       COALESCE(comments_count, 0) +
       COALESCE(quote_count, 0)),
      0
    )
    *
    -- Time decay factor (newer posts get higher multiplier)
    (1 - POWER(
      -- Age since timeLimit / Total time window
      (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM "indexedAt"::timestamp)) /
      (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM ${timeLimit}::timestamp)),
      2
    ))
  `,
      'desc',
    )
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(0)
    .limit(10)
}

const TL = '2026-05-01T00:00:00.000Z'
const PUB = 'did:plc:newsbot'
const FOL = ['did:plc:a', 'did:plc:b']

console.log('chronological policy SQL parity')
{
  const newC = publisherQueryChronological(db, TL, FOL, 0, 10, PUB).compile()
  const refC = refChronologicalPublisher(TL, PUB).compile()
  assert(newC.sql === refC.sql, 'publisher SQL identical', `\n  new: ${newC.sql}\n  ref: ${refC.sql}`)

  const newCf = followsQueryChronological(db, TL, FOL, 0, 10, PUB).compile()
  const refCf = refChronologicalFollows(TL, PUB, FOL).compile()
  assert(newCf.sql === refCf.sql, 'follows SQL identical', `\n  new: ${newCf.sql}\n  ref: ${refCf.sql}`)
}

console.log('engagement-sorted policy SQL parity')
{
  const newE = publisherQueryEngagement(db, TL, FOL, 0, 10, PUB).compile()
  const refE = refEngagementPublisher(TL, PUB).compile()
  // Whitespace inside template literals is significant — assert exact
  // equality. If this ever drifts, check policies/engagement-sorted.ts
  // and the ref builder above are character-for-character identical.
  assert(newE.sql === refE.sql, 'publisher SQL identical', `\n  new: ${JSON.stringify(newE.sql)}\n  ref: ${JSON.stringify(refE.sql)}`)
}

console.log('ranker-priority policy SQL shape')
{
  // Ranker-priority delegates to applyPriorityOrderForFeed, which is
  // already covered by test_ranker_priority_helper.ts. Here we just
  // sanity-check that the policy adds the right WHERE clauses.
  const c = publisherQueryRankerPriority(db, TL, FOL, 0, 10, PUB, 'newsflow-nl-2').compile()
  assert(c.sql.includes('"author" ='), 'publisher filters by author=publisher')
  assert(c.sql.includes('"post"."indexedAt" >='), 'publisher filters by indexedAt')
  assert(c.parameters.includes(PUB), 'publisher DID bound')

  const cf = followsQueryRankerPriority(db, TL, FOL, 0, 10, PUB, 'newsflow-nl-2').compile()
  assert(cf.sql.includes('"author" !='), 'follows filters by author!=publisher')
  assert(cf.sql.includes(' in '), 'follows uses IN over requesterFollows')
}

console.log()
console.log(`Summary: ${passed} passed, ${failed} failed`)
if (failed > 0) process.exit(1)
