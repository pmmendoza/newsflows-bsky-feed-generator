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
  applyLegacyPriorityOrder,
  applyRankerPriorityOrder,
  applyPriorityOrderForFeed,
  rkeyToEnvSuffix,
  useRankerPriority,
} from '../src/algos/ranker-priority-helper'

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

console.log('useRankerPriority — env flag resolution')
{
  withEnv(
    {
      FEEDGEN_PRIORITY_FROM_RANKER_PROD: undefined,
      FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2: undefined,
    },
    () => {
      assert(useRankerPriority('newsflow-nl-2') === false, 'unset → false (default)')
    },
  )
  withEnv({ FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2: 'true' }, () => {
    assert(useRankerPriority('newsflow-nl-2') === true, 'per-feed flag true → true')
    assert(useRankerPriority('newsflow-fr-2') === false, 'other feed unaffected')
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
        useRankerPriority('newsflow-nl-2') === false,
        'per-feed false overrides master true',
      )
      assert(
        useRankerPriority('newsflow-fr-2') === true,
        'master true still applies to feeds without override',
      )
    },
  )
}

console.log('Legacy path — applyLegacyPriorityOrder')
{
  const c = applyLegacyPriorityOrder(basePostQuery()).compile()
  assert(
    /coalesce\("priority",\s*\$\d+\)\s+desc/i.test(c.sql),
    'orders by coalesce(priority,0) DESC',
    c.sql,
  )
  assert(/order by/i.test(c.sql), 'has ORDER BY')
  assert(!c.sql.includes('feed_current_priority'), 'no JOIN to ranker_prod', c.sql)
}

console.log('Canary path — applyRankerPriorityOrder')
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
  assert(
    /coalesce\("fcp"\."priority",\s*\$\d+\)\s+desc/i.test(c.sql),
    'orders by coalesce(fcp.priority, -1) DESC',
    c.sql,
  )
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
        !c.sql.includes('feed_current_priority'),
        'env unset → legacy ordering, no JOIN',
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

console.log()
console.log(`Summary: ${passed} passed, ${failed} failed`)
if (failed > 0) process.exit(1)
