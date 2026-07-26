import {
  DummyDriver,
  Kysely,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
} from 'kysely'
import {
  applyRankerPriorityOrder,
  getRankerPriorityServingStatus,
  recordRankerPriorityResult,
} from '../src/algos/ranker-priority-helper'
import {
  getScoreSource,
  refreshScoreSourceCache,
} from '../src/util/score-source-cache'

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

function check(condition: boolean, message: string): void {
  if (!condition) throw new Error(message)
  console.log(`  ✓ ${message}`)
}

function basePostQuery() {
  return db.selectFrom('post').selectAll('post')
}

async function main(): Promise<void> {
  console.log('unpopulated cache fails closed before traffic')
  check(
    (() => {
      try {
        getScoreSource('newsflow-nl-2')
        return false
      } catch (error) {
        return error instanceof Error &&
          error.message === 'score_source_cache_unready'
      }
    })(),
    'unpopulated lookup reports score_source_cache_unready',
  )

  const startupError = new Error('catalog unavailable')
  let rejected = false
  try {
    await refreshScoreSourceCache({
      selectFrom: () => ({
        select: () => ({ execute: async () => { throw startupError } }),
      }),
    } as any)
  } catch (error) {
    rejected = error === startupError
  }
  check(rejected, 'failed initial refresh rejects startup priming')
  check(
    (() => {
      try {
        applyRankerPriorityOrder(basePostQuery(), 'newsflow-nl-2')
        return false
      } catch (error) {
        return error instanceof Error &&
          error.message === 'score_source_cache_unready'
      }
    })(),
    'ranker traffic cannot build a query while the cache is unready',
  )

  console.log('NULL score source keeps the legitimate rkey default')
  await refreshScoreSourceCache(fakeDbReturning([
    {
      rkey: 'newsflow-nl-2',
      feed_id: 'newsflow-nl-2',
      ranker_score_source: null,
    },
  ]))
  check(getScoreSource('newsflow-nl-2') === null, 'catalog NULL resolves to serve-self')

  const compiled = applyRankerPriorityOrder(
    basePostQuery(),
    'newsflow-nl-2',
  ).compile()
  check(
    compiled.parameters.includes('newsflow-nl-2'),
    'serve-self binds profile_id to the rkey',
  )

  console.log('fresh scores preserve score-first ordering')
  check(
    /coalesce\("fcp"\."score",\s*\$\d+\)\s+desc[\s\S]*"post"\."indexedAt"\s+desc[\s\S]*"post"\."cid"\s+desc/i.test(compiled.sql),
    'healthy ordering remains score DESC, indexedAt DESC, cid DESC',
  )
  recordRankerPriorityResult('newsflow-nl-2', [{
    __ranker_score: 0.75,
    __ranker_profile_id: 'newsflow-nl-2',
  }])
  check(
    getRankerPriorityServingStatus()[0]?.serving_ordering === 'ranked',
    'healthy serving state is observable as ranked',
  )

  console.log('zero fresh scores serve recency with a loud signal')
  check(
    /select[\s\S]*fcp\.score as "__ranker_score"/i.test(compiled.sql) &&
      !/ranker_priority_no_fresh_scores|clock_timestamp\(\)::text\)::boolean/i.test(compiled.sql),
    'query returns joined-score metadata without filtering or throwing',
  )

  const warnings: string[] = []
  const originalWarn = console.warn
  console.warn = (message?: any) => warnings.push(String(message))
  try {
    recordRankerPriorityResult('newsflow-nl-2', [{
      __ranker_score: null,
      __ranker_profile_id: 'science-nl-a',
    }])
  } finally {
    console.warn = originalWarn
  }
  const warning = JSON.parse(warnings[0] ?? '{}')
  check(
    warnings.length === 1 &&
      warning.event === 'ranker_priority_unranked_recency' &&
      warning.feed_rkey === 'newsflow-nl-2' &&
      warning.profile_id === 'science-nl-a' &&
      warning.served_ordering === 'unranked_recency',
    'degraded request emits one structured unranked-recency warning',
  )
  check(
    getRankerPriorityServingStatus()[0]?.serving_ordering === 'unranked_recency',
    'degraded serving state is available to /api/config',
  )
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
