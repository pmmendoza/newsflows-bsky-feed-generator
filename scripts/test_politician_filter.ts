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
  isPoliticianFilterRouted,
  politicianFilterStartupSummary,
} from '../src/algos/politician-filter'
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

// --- routing (regex): be-(k|m|[123]) routed; be-4 and non-BE untouched --------
console.log('BE politician-or-party filter routing')
for (const rkey of ['newsflow-be-k', 'newsflow-be-m', 'newsflow-be-1', 'newsflow-be-2', 'newsflow-be-3']) {
  assert(isPoliticianFilterRouted(rkey) === true, `${rkey} routed`)
}
for (const rkey of ['newsflow-be-4', 'newsflow-nl-1', 'newsflow-fr-2', 'newsflow-cz-3', 'newsflow-ir-5', 'newsflow-be-t2', '']) {
  assert(isPoliticianFilterRouted(rkey) === false, `${rkey || '(empty)'} not routed`)
}

// --- kill-switch: default on; explicit falsy disables entirely ----------------
console.log('kill-switch (FEEDGEN_BE_POLITICIAN_FILTER)')
withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: undefined }, () => {
  assert(isPoliticianFilterEnabled('newsflow-be-k') === true, 'default (unset) enabled for BE-K')
  assert(isPoliticianFilterEnabled('newsflow-nl-1') === false, 'never enabled for non-BE even by default')
})
withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: 'true' }, () => {
  assert(isPoliticianFilterEnabled('newsflow-be-m') === true, '=true enabled for BE-M')
})
for (const off of ['false', '0', 'no', 'off', 'FALSE', ' Off ']) {
  withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: off }, () => {
    assert(isPoliticianFilterEnabled('newsflow-be-1') === false, `kill-switch '${off}' disables filter`)
  })
}

// --- startup summary reflects state -------------------------------------------
console.log('startup summary')
withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: undefined }, () =>
  assert(/ENABLED/.test(politicianFilterStartupSummary()), 'summary says ENABLED when active'),
)
withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: 'false' }, () =>
  assert(/DISABLED/.test(politicianFilterStartupSummary()), 'summary says DISABLED under kill-switch'),
)

// --- query shape: OR semantics + fail-open, on the new BSR surface ------------
console.log('query shape (OR semantics + fail-open)')
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
  const beSql = applyPoliticianFilterIfEnabled(basePostQuery(), 'newsflow-be-k').compile().sql
  // Joins the new eligibility surface (consumes uri + eligible only).
  assert(
    /left join "ranker_prod"\."post_political_eligibility"/i.test(beSql),
    'BE query joins post_political_eligibility',
  )
  assert(!/post_politician/i.test(beSql), 'BE query no longer joins the old post_politician table')
  // A recognized politician-only post and a recognized party-only post both
  // arrive as eligible=true from BSR, so the single OR clause admits both.
  assert(/"pe"\."eligible" = /i.test(beSql), 'OR semantics: eligible=true rows are kept')
  // no-row post (surface missing/behind) is kept — fail-open.
  assert(/"pe"\."uri" is null/i.test(beSql), 'fail-open: rows with no eligibility row are kept')
  // "neither" (recognized nothing) arrives as eligible=false and is the only
  // present-row case the WHERE excludes; feedgen reads no party/politician cols.
  assert(!/party_ids|has_party|has_politician/i.test(beSql), 'feedgen reads only uri + eligible')

  const nlSql = applyPoliticianFilterIfEnabled(basePostQuery(), 'newsflow-nl-1').compile().sql
  assert(!/post_political_eligibility/i.test(nlSql), 'non-BE query has no eligibility join')
})

withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: 'false' }, () => {
  const killed = applyPoliticianFilterIfEnabled(basePostQuery(), 'newsflow-be-k').compile().sql
  assert(!/post_political_eligibility/i.test(killed), 'kill-switch: BE query served UNFILTERED (no join)')
})

// --- projection guard: every filtered policy must project post.* only --------
// The eligibility LEFT JOIN adds a `pe.uri` column. A bare `select *` projects
// both and node-postgres keeps the LAST, so pe.uri (NULL on fail-open rows)
// clobbers post.uri and feed-builder serves broken URIs. All BE-routed policy
// legs must compile to `select "post".*`, never a bare `select *`.
console.log('projection guard (no pe.uri clobber)')
withEnv({ FEEDGEN_BE_POLITICIAN_FILTER: 'true' }, () => {
  const legs: Array<[string, any]> = [
    ['chronological/publisher', publisherQueryChronological(db, '2026-06-26T00:00:00.000Z', ['did:x'], 0, 10, 'did:pub', 'newsflow-be-k')],
    ['chronological/follows', followsQueryChronological(db, '2026-06-26T00:00:00.000Z', ['did:x'], 0, 10, 'did:pub', 'newsflow-be-k')],
    ['engagement/publisher', publisherQueryEngagement(db, '2026-06-26T00:00:00.000Z', ['did:x'], 0, 10, 'did:pub', 'newsflow-be-k')],
    ['engagement/follows', followsQueryEngagement(db, '2026-06-26T00:00:00.000Z', ['did:x'], 0, 10, 'did:pub', 'newsflow-be-k')],
    ['ranker-priority/publisher', publisherQueryRankerPriority(db, '2026-06-26T00:00:00.000Z', ['did:x'], 0, 10, 'did:pub', 'newsflow-be-k')],
    ['ranker-priority/follows', followsQueryRankerPriority(db, '2026-06-26T00:00:00.000Z', ['did:x'], 0, 10, 'did:pub', 'newsflow-be-k')],
  ]
  for (const [label, q] of legs) {
    const s = q.compile().sql
    assert(/select "post"\.\*/i.test(s), `${label} projects post.*`)
    assert(!/select \*/i.test(s), `${label} has no bare select *`)
  }
})

console.log(`Summary: ${passed} passed, ${failed} failed`)
if (failed > 0) process.exit(1)
