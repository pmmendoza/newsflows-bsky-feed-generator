import {
  DummyDriver,
  Kysely,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
} from 'kysely'
import {
  applyPoliticianFilterIfEnabled,
  isPoliticianFilterEnabled,
  politicianFilterFreshnessCutoffIso,
} from '../src/algos/politician-filter'

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

console.log('BE politician filter env routing')
withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: 'true' }, () => {
  assert(isPoliticianFilterEnabled('newsflow-be-1') === true, 'BE variant 1 enabled')
  assert(isPoliticianFilterEnabled('newsflow-be-2') === true, 'BE variant 2 enabled')
  assert(isPoliticianFilterEnabled('newsflow-be-3') === true, 'BE variant 3 enabled')
  assert(isPoliticianFilterEnabled('newsflow-nl-1') === false, 'NL unchanged')
  assert(isPoliticianFilterEnabled('newsflow-fr-2') === false, 'FR unchanged')
})

withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: undefined }, () => {
  assert(isPoliticianFilterEnabled('newsflow-be-1') === false, 'filter defaults off')
})

console.log('freshness cutoff')
withEnv({ FEEDGEN_BE_POLITICIAN_FILTER_MAX_AGE_HOURS: '12' }, () => {
  const now = new Date('2026-06-26T12:00:00.000Z')
  assert(
    politicianFilterFreshnessCutoffIso(now) === '2026-06-26T00:00:00.000Z',
    '12h freshness env honoured',
    politicianFilterFreshnessCutoffIso(now),
  )
})

console.log('query shape')
const db = new Kysely<any>({
  dialect: {
    createAdapter: () => new PostgresAdapter(),
    createDriver: () => new DummyDriver(),
    createIntrospector: (d) => new PostgresIntrospector(d),
    createQueryCompiler: () => new PostgresQueryCompiler(),
  },
})

function basePostQuery() {
  return db
    .selectFrom('post')
    .selectAll('post')
    .where('post.indexedAt', '>=', '2026-06-26T00:00:00.000Z')
}

withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: 'true' }, () => {
  const beSql = applyPoliticianFilterIfEnabled(basePostQuery(), 'newsflow-be-1').compile().sql
  const nlSql = applyPoliticianFilterIfEnabled(basePostQuery(), 'newsflow-nl-1').compile().sql
  assert(/left join "ranker_prod"\."post_politician"/i.test(beSql), 'BE query joins post_politician')
  assert(/"pp"\."post_uri" is null/i.test(beSql), 'BE query keeps missing/stale rows fail-open')
  assert(!/post_politician/i.test(nlSql), 'NL query has no politician join')
})

console.log(`Summary: ${passed} passed, ${failed} failed`)
if (failed > 0) process.exit(1)
