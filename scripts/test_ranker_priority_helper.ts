/**
 * Sprint 5 Lane C unit test for `applyPriorityOrderForFeed` and
 * surrounding helpers in `src/algos/ranker-priority-helper.ts`.
 *
 * No DB required. Compiles the resulting Kysely query through the
 * Postgres SQL compiler with a `DummyDriver` and asserts on the
 * emitted SQL string for both the legacy path (env unset) and the
 * canary path (per-feed env set).
 *
 * Run: `npx ts-node scripts/test_ranker_priority_helper.ts`
 * Exits non-zero on any assertion failure.
 *
 * Cross-references:
 *   - Plan: dev/storage/plan_storage_refactor/plan_multi_ranker_priority.md
 *   - Schema migration: dev/storage/migrations/011_ranker_prod_schema.sql
 */

import {
  Kysely,
  DummyDriver,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
} from 'kysely'
import {
  applyRankerPriorityOrder,
  applyPriorityOrderForFeed,
  rkeyToEnvSuffix,
  useRankerPriority,
} from '../src/algos/ranker-priority-helper'
import { refreshScoreSourceCache } from '../src/util/score-source-cache'

// Minimal fake Database exposing the selectFrom(...).select(...).execute()
// chain refreshScoreSourceCache uses. Lets us seed the score-source map
// without a real Postgres.
const fakeDbReturning = (rows: any[]): any => ({
  selectFrom: () => ({ select: () => ({ execute: async () => rows }) }),
})

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

function basePostQuery() {
  return db
    .selectFrom('post')
    .selectAll('post')
    .where('post.author', '=', 'did:plc:newsbot')
    .where('post.indexedAt', '>=', '2026-05-03T00:00:00.000Z')
}

function withEnv<T>(env: Record<string, string | undefined>, fn: () => T): T {
  const saved: Record<string, string | undefined> = {}
  for (const [k, v] of Object.entries(env)) {
    saved[k] = process.env[k]
    if (v === undefined) delete process.env[k]
    else process.env[k] = v
  }
  try {
    return fn()
  } finally {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k]
      else process.env[k] = v
    }
  }
}

console.log('rkeyToEnvSuffix')
{
  assert(rkeyToEnvSuffix('newsflow-nl-2') === 'NEWSFLOW_NL_2', 'newsflow-nl-2 → NEWSFLOW_NL_2')
  assert(rkeyToEnvSuffix('newsflow-uk-2') === 'NEWSFLOW_UK_2', 'newsflow-uk-2 → NEWSFLOW_UK_2')
  assert(rkeyToEnvSuffix('a.b_c-d') === 'A_B_C_D', 'punctuation collapses to single underscores')
}

console.log('useRankerPriority — score path is unconditional')
{
  withEnv(
    {
      FEEDGEN_PRIORITY_FROM_RANKER_PROD: undefined,
      FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2: undefined,
    },
    () => {
      assert(useRankerPriority('newsflow-nl-2') === true, 'unset → true')
    },
  )
  withEnv({ FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2: 'true' }, () => {
    assert(useRankerPriority('newsflow-nl-2') === true, 'per-feed flag true → true')
    assert(useRankerPriority('newsflow-fr-2') === true, 'other feed still true')
  })
  withEnv(
    { FEEDGEN_PRIORITY_FROM_RANKER_PROD: 'true' },
    () => {
      assert(useRankerPriority('newsflow-nl-2') === true, 'master flag true → true')
      assert(useRankerPriority('newsflow-fr-2') === true, 'master flag covers every feed')
    },
  )
  withEnv(
    {
      FEEDGEN_PRIORITY_FROM_RANKER_PROD: 'true',
      FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2: 'false',
    },
    () => {
      assert(
        useRankerPriority('newsflow-nl-2') === true,
        'per-feed false no longer disables ranker-prod score path',
      )
      assert(
        useRankerPriority('newsflow-fr-2') === true,
        'other feed true',
      )
    },
  )
}

console.log('Score path — applyRankerPriorityOrder')
{
  const c = applyRankerPriorityOrder(basePostQuery(), 'newsflow-nl-2').compile()
  assert(
    /left join "ranker_prod"\."feed_current_priority"/i.test(c.sql),
    'LEFT JOIN ranker_prod.feed_current_priority',
    c.sql,
  )
  assert(
    /"fcp"\."post_uri"\s*=\s*"post"\."uri"/i.test(c.sql),
    'JOIN condition fcp.post_uri = post.uri',
  )
  assert(
    c.parameters.includes('newsflow-nl-2'),
    'feed_id parameter bound',
    JSON.stringify(c.parameters),
  )
  // Migration 024: score-only ordering. Do not keep any fcp.priority fallback.
  assert(
    /coalesce\("fcp"\."score",\s*\$\d+\)\s+desc/i.test(c.sql),
    'orders by coalesce(fcp.score, -1.0) DESC',
    c.sql,
  )
  assert(
    !/"fcp"\."priority"/i.test(c.sql),
    'must NOT read fcp.priority',
    c.sql,
  )
  // Demote-elimination via recency filter (added on
  // feature/eliminate-demote-recency-filter):
  assert(
    /"fcp"\."updated_at"\s*>=/i.test(c.sql),
    'JOIN includes recency filter on fcp.updated_at',
    c.sql,
  )
  // The cutoff should be a recent ISO timestamp (within last day).
  const isoParam = c.parameters.find(
    (p) =>
      typeof p === 'string' &&
      /^\d{4}-\d{2}-\d{2}T/.test(p as string),
  )
  assert(
    Boolean(isoParam),
    'recency cutoff parameter is bound as an ISO timestamp',
    JSON.stringify(c.parameters),
  )
}

console.log('Recency filter respects FEEDGEN_RANKER_PROD_FRESHNESS_HOURS env')
{
  withEnv({ FEEDGEN_RANKER_PROD_FRESHNESS_HOURS: '6' }, () => {
    const c = applyRankerPriorityOrder(basePostQuery(), 'newsflow-nl-2').compile()
    const isoParam = c.parameters.find(
      (p) =>
        typeof p === 'string' &&
        /^\d{4}-\d{2}-\d{2}T/.test(p as string),
    ) as string | undefined
    if (!isoParam) {
      assert(false, 'expected ISO cutoff parameter', JSON.stringify(c.parameters))
    } else {
      const cutoffMs = new Date(isoParam).getTime()
      const sixHoursAgo = Date.now() - 6 * 60 * 60 * 1000
      // Allow ±2 minutes of slack for clock + execution time.
      const slackMs = 2 * 60 * 1000
      assert(
        Math.abs(cutoffMs - sixHoursAgo) < slackMs,
        '6h env honoured',
        `cutoff=${isoParam} expected~${new Date(sixHoursAgo).toISOString()}`,
      )
    }
  })
}

console.log('Wrapper — applyPriorityOrderForFeed routes via env')
{
  withEnv(
    {
      FEEDGEN_PRIORITY_FROM_RANKER_PROD: undefined,
      FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2: undefined,
    },
    () => {
      const c = applyPriorityOrderForFeed(basePostQuery(), 'newsflow-nl-2').compile()
      assert(
        /feed_current_priority/i.test(c.sql),
        'env unset → score-backed ranker_prod JOIN',
        c.sql,
      )
    },
  )
  withEnv(
    { FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2: 'true' },
    () => {
      const c = applyPriorityOrderForFeed(basePostQuery(), 'newsflow-nl-2').compile()
      assert(
        /feed_current_priority/i.test(c.sql),
        'per-feed env=true → JOIN to ranker_prod',
        c.sql,
      )
    },
  )
}

// D1.4 read-path cutover: join on fcp.profile_id resolved from the
// score-source cache, falling back to the rkey when the source is NULL.
async function scoreSourceCutoverTests() {
  console.log('D1.4 — join column is fcp.profile_id')
  {
    const c = applyRankerPriorityOrder(basePostQuery(), 'newsflow-nl-2').compile()
    assert(
      /"fcp"\."profile_id"\s*=/i.test(c.sql),
      'JOIN filters on fcp.profile_id (not fcp.feed_id)',
      c.sql,
    )
    assert(
      !/"fcp"\."feed_id"\s*=/i.test(c.sql),
      'JOIN must NOT filter on fcp.feed_id anymore',
      c.sql,
    )
  }

  console.log('D1.4 — NULL score source ⇒ profile_id falls back to rkey (identical to pre-cutover)')
  {
    // Empty / unloaded cache returns null for every feed.
    await refreshScoreSourceCache(fakeDbReturning([]))
    const c = applyRankerPriorityOrder(basePostQuery(), 'newsflow-nl-2').compile()
    assert(
      c.parameters.includes('newsflow-nl-2'),
      'profile_id bound to the rkey when ranker_score_source is NULL',
      JSON.stringify(c.parameters),
    )
  }

  console.log('D1.4 — non-null score source ⇒ profile_id is the configured source')
  {
    await refreshScoreSourceCache(
      fakeDbReturning([
        { rkey: 'newsflow-nl-2', feed_id: 'newsflow-nl-2', ranker_score_source: 'science-nl-a' },
      ]),
    )
    const c = applyRankerPriorityOrder(basePostQuery(), 'newsflow-nl-2').compile()
    assert(
      c.parameters.includes('science-nl-a'),
      'profile_id bound to the configured ranker_score_source',
      JSON.stringify(c.parameters),
    )
    assert(
      !c.parameters.includes('newsflow-nl-2'),
      'rkey no longer bound once a source is configured',
      JSON.stringify(c.parameters),
    )
    // A feed with no cache entry still falls back to its own rkey.
    const other = applyRankerPriorityOrder(basePostQuery(), 'newsflow-fr-2').compile()
    assert(
      other.parameters.includes('newsflow-fr-2'),
      'unconfigured feed falls back to its rkey',
      JSON.stringify(other.parameters),
    )
  }

  // Reset the shared map so nothing leaks to other test files/runs.
  await refreshScoreSourceCache(fakeDbReturning([]))
}

scoreSourceCutoverTests()
  .then(() => {
    console.log()
    console.log(`Summary: ${passed} passed, ${failed} failed`)
    if (failed > 0) process.exit(1)
  })
  .catch((err) => {
    console.error('test harness error:', err)
    process.exit(2)
  })
