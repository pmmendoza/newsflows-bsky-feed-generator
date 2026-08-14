/**
 * Disposable-Postgres proof for the real buildFeed pagination path.
 *
 * FEEDGEN_TEST_DSN=postgresql://... FEEDGEN_PAGINATION_TEST_CONFIRM=disposable \
 *   yarn test:pagination-db
 *
 * The fixture and any bootstrap tables live in one rolled-back transaction.
 */
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import { invalidateDispatchCache, resolveDynamicHandler } from '../src/algos/catalog-dispatch'
import { Policy } from '../src/algos/make-handler'
import { refreshScoreSourceCache } from '../src/util/score-source-cache'

const REQUESTER = 'did:plc:pagination-requester'
const PUBLISHER = 'did:plc:pagination-publisher'
const FOLLOWED = 'did:plc:pagination-followed'

const policyCases: Array<{ rkey: string; policy: Policy }> = [
  ...['nl', 'fr', 'cz', 'ir'].flatMap((country) => [
    { rkey: `newsflow-${country}-1`, policy: 'chronological' as const },
    { rkey: `newsflow-${country}-2`, policy: 'ranker-priority' as const },
    { rkey: `newsflow-${country}-3`, policy: 'engagement-sorted' as const },
  ]),
  { rkey: 'newsflow-be-k', policy: 'ranker-priority' },
  { rkey: 'newsflow-be-m', policy: 'ranker-priority' },
]

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

async function serve(db: any, rkey: string, cursor?: string, publisherDid?: string) {
  if (publisherDid) {
    await db.updateTable('feedgen_ops.feed_catalog' as any).set({ publisher_did: publisherDid }).where('rkey', '=', rkey).execute()
  }
  invalidateDispatchCache(rkey)
  const handler = await resolveDynamicHandler(db, rkey)
  check(handler, `${rkey}: active catalog handler must resolve`)
  return handler({ db } as any, { feed: '', limit: 3, cursor } as any, REQUESTER)
}

async function bootstrap(db: any, prefix: string) {
  await sql`CREATE SCHEMA IF NOT EXISTS feedgen_ops`.execute(db)
  await sql`CREATE SCHEMA IF NOT EXISTS ranker_prod`.execute(db)
  await sql`
    CREATE TABLE IF NOT EXISTS post (
      uri text PRIMARY KEY, cid text NOT NULL, "indexedAt" text NOT NULL, "createdAt" text NOT NULL,
      author text NOT NULL, likes_count integer DEFAULT 0, repost_count integer DEFAULT 0,
      comments_count integer DEFAULT 0, quote_count integer DEFAULT 0
    )
  `.execute(db)
  await sql`CREATE TABLE IF NOT EXISTS follows (subject text NOT NULL, follows text NOT NULL)`.execute(db)
  await sql`
    CREATE TABLE IF NOT EXISTS feedgen_ops.feed_catalog (
      rkey text PRIMARY KEY, feed_id text NOT NULL, publisher_did text NOT NULL,
      algo_policy_id text NOT NULL, enabled boolean NOT NULL,
      publisher_post_max_age_days integer, publisher_post_max_age_source text,
      publisher_time_clock text, ranker_score_max_age_hours integer,
      ranker_score_max_age_source text, ranker_min_score_backed_share double precision,
      ranker_min_score_backed_source text, catalog_revision integer,
      ranker_score_source text
    )
  `.execute(db)
  await sql`
    CREATE TABLE IF NOT EXISTS ranker_prod.feed_current_priority (
      profile_id text NOT NULL, post_uri text NOT NULL, score double precision, updated_at text NOT NULL
    )
  `.execute(db)
  await sql`
    CREATE TABLE IF NOT EXISTS ranker_prod.post_political_eligibility (
      uri text PRIMARY KEY, eligible boolean NOT NULL, party_ids jsonb, updated_at text NOT NULL
    )
  `.execute(db)

  const now = new Date(Date.now() - 60 * 60 * 1000).toISOString()
  const publisherUris = [0, 1, 2].map((id) => `${prefix}/publisher-${id}`)
  const followedUris = [0, 1, 2, 3, 4, 5].map((id) => `${prefix}/followed-${id}`)
  const rows = [
    ...publisherUris.map((uri, id) => ({ uri, cid: `p-${String(id).padStart(2, '0')}`, author: PUBLISHER })),
    ...followedUris.map((uri, id) => ({ uri, cid: `f-${String(id).padStart(2, '0')}`, author: FOLLOWED })),
  ]
  await db.insertInto('follows').values({ subject: REQUESTER, follows: FOLLOWED }).execute()
  await db.insertInto('feedgen_ops.feed_catalog' as any)
    .values(policyCases.map(({ rkey, policy }) => ({
      rkey, feed_id: rkey, publisher_did: PUBLISHER, algo_policy_id: policy, enabled: true,
      publisher_post_max_age_days: 1, publisher_post_max_age_source: 'feed_override',
      publisher_time_clock: 'receipt_time', ranker_score_max_age_hours: policy === 'ranker-priority' ? 24 : null,
      ranker_score_max_age_source: policy === 'ranker-priority' ? 'feed_override' : null,
      ranker_min_score_backed_share: policy === 'ranker-priority' ? 0.8 : null,
      ranker_min_score_backed_source: policy === 'ranker-priority' ? 'feed_override' : null,
      catalog_revision: 1, ranker_score_source: null,
    }))).execute()
  await refreshScoreSourceCache(db)
  await db.insertInto('post').values(rows.map((row) => ({ ...row, indexedAt: now, createdAt: now }))).execute()
  await db.insertInto('ranker_prod.post_political_eligibility' as any)
    .values(rows.map((row) => ({ uri: row.uri, eligible: true, party_ids: null, updated_at: now }))).execute()
  await db.insertInto('ranker_prod.feed_current_priority' as any)
    .values(policyCases.flatMap(({ rkey }) => rows.map((row) => ({ profile_id: rkey, post_uri: row.uri, score: 1, updated_at: now })))).execute()
  return { publisherUris, followedUris }
}

async function assertThreePages(db: any, rkey: string, publisherUris: string[], followedUris: string[]) {
  let cursor: string | undefined
  const served: string[] = []
  for (let page = 0; page < 3; page += 1) {
    const result = await serve(db, rkey, cursor)
    const posts = result.feed.map((item) => item.post)
    const expected = [publisherUris[2 - page], followedUris[5 - page * 2], followedUris[4 - page * 2]]
    check(posts.join(',') === expected.join(','), `${rkey}: equal timestamps must order page ${page + 1} by deterministic tie-breakers`)
    check(result.cursor === String((page + 1) * 2), `${rkey}: page ${page + 1} must retain legacy integer cursor progression`)
    served.push(...posts)
    cursor = result.cursor
  }
  check(new Set(served).size === served.length, `${rkey}: three pages must not duplicate URIs`)
  check(served.length === 9, `${rkey}: three pages must not skip URIs`)
  const legacy = await serve(db, rkey, '2')
  check(legacy.feed.map((item) => item.post).join(',') === served.slice(3, 6).join(','), `${rkey}: legacy integer cursor must select page two`)
}

async function assertExhaustion(db: any, rkey: string) {
  const publisher = `${PUBLISHER}-${rkey}`
  const followed = `${FOLLOWED}-${rkey}`
  const now = new Date(Date.now() - 60 * 60 * 1000).toISOString()
  const publisherRows = [0, 1, 2].map((id) => ({ uri: `exhaustion-${rkey}-publisher-${id}`, cid: `p-${id}`, indexedAt: now, createdAt: now, author: publisher }))
  const followedRows = [0, 1, 2, 3, 4, 5].map((id) => ({ uri: `exhaustion-${rkey}-followed-${id}`, cid: `f-${id}`, indexedAt: now, createdAt: now, author: followed }))
  await db.deleteFrom('follows').where('subject', '=', REQUESTER).execute()
  await db.insertInto('follows').values({ subject: REQUESTER, follows: followed }).execute()
  await db.insertInto('post').values([...publisherRows, ...followedRows]).execute()
  await db.insertInto('ranker_prod.post_political_eligibility' as any)
    .values([...publisherRows, ...followedRows].map((row) => ({ uri: row.uri, eligible: true, party_ids: null, updated_at: now }))).execute()
  await db.insertInto('ranker_prod.feed_current_priority' as any)
    .values([...publisherRows, ...followedRows].map((row) => ({ profile_id: rkey, post_uri: row.uri, score: 1, updated_at: now }))).execute()

  await db.deleteFrom('post').where('author', '=', followed).execute()
  let cursor: string | undefined
  for (let page = 0; page < 3; page += 1) {
    const result = await serve(db, rkey, cursor, publisher)
    check(result.feed.length === 1, `publisher-only leg must serve page ${page + 1}`)
    cursor = result.cursor
  }
  const publisherTerminal = await serve(db, rkey, cursor, publisher)
  check(publisherTerminal.feed.length === 0 && publisherTerminal.cursor === undefined, 'publisher-only exhaustion must terminate without a cursor')

  await db.deleteFrom('post').where('author', '=', publisher).execute()
  await db.insertInto('post').values(followedRows).execute()
  cursor = undefined
  for (let page = 0; page < 3; page += 1) {
    const result = await serve(db, rkey, cursor, publisher)
    check(result.feed.length === 2, `followed-only leg must serve page ${page + 1}`)
    cursor = result.cursor
  }
  const terminal = await serve(db, rkey, cursor, publisher)
  check(terminal.feed.length === 0 && terminal.cursor === undefined, 'followed-only exhaustion must terminate without a cursor')
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_PAGINATION_TEST_CONFIRM !== 'disposable') {
    console.log('SKIP: requires FEEDGEN_TEST_DSN and FEEDGEN_PAGINATION_TEST_CONFIRM=disposable (transaction-scoped disposable Postgres proof)')
    return
  }
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }) })
  const originalSetTimeout = global.setTimeout
  ;(global as any).setTimeout = () => 0 // Request logging is intentionally outside the serving proof.
  try {
    await db.transaction().execute(async (trx) => {
      const fixture = await bootstrap(trx, `at://pagination-${process.pid}-${Date.now()}`)
      for (const testCase of policyCases) await assertThreePages(trx, testCase.rkey, fixture.publisherUris, fixture.followedUris)
      for (const testCase of policyCases) await assertExhaustion(trx, testCase.rkey)
      throw new Error('__ROLLBACK_PAGINATION_FIXTURE__')
    })
  } catch (error) {
    if (!(error instanceof Error) || error.message !== '__ROLLBACK_PAGINATION_FIXTURE__') throw error
  } finally {
    global.setTimeout = originalSetTimeout
    await db.destroy()
  }
  console.log(`feed pagination disposable Postgres proof passed (${policyCases.length} active policy cases)`)
}

main().catch((error) => { console.error(error); process.exit(1) })
