import assert from 'node:assert/strict'
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import {
  deriveEngagementRefreshPlan,
  selectEngagementRefreshPosts,
  selectRecentFollowedPosts,
  selectRecentPublisherPosts,
  updateEngagement,
} from '../src/util/engagement-updater'
import { cutoffFromHours } from '../src/algos/publisher-serving-window'
import { resolveEngagementTimeHours } from '../src/algos/feed-builder'

const dsn = process.env.FEEDGEN_ENGAGEMENT_REFRESH_TEST_DSN
if (!dsn) throw new Error('FEEDGEN_ENGAGEMENT_REFRESH_TEST_DSN must name a disposable Postgres database')

const referenceMs = Date.parse('2026-08-14T12:00:00Z')
const atHoursAgo = (hours: number) => new Date(referenceMs - hours * 3_600_000).toISOString()
const atDaysAgo = (days: number) => atHoursAgo(days * 24)

async function main() {
  const queries: Array<{ sql: string, parameters: readonly unknown[] }> = []
  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
    log(event) {
      if (event.level === 'query') queries.push(event.query)
    },
  })
  const priorEngagementTimeHours = process.env.ENGAGEMENT_TIME_HOURS
  process.env.ENGAGEMENT_TIME_HOURS = '48'
  try {
    await db.transaction().execute(async (trx) => {
      // This intentionally fails before writing if the target is not an empty disposable database.
      await sql`CREATE SCHEMA feedgen_ops`.execute(trx)
      await sql`CREATE TABLE follows (subject text NOT NULL, follows text NOT NULL)`.execute(trx)
      await sql`CREATE TABLE subscriber (did text PRIMARY KEY)`.execute(trx)
      await sql`
        CREATE TABLE post (
          uri text PRIMARY KEY, cid text NOT NULL, "indexedAt" text NOT NULL, "createdAt" text NOT NULL,
          author text NOT NULL, text text NOT NULL, "rootUri" text NOT NULL, "rootCid" text NOT NULL,
          "linkUrl" text NOT NULL, "linkTitle" text NOT NULL, "linkDescription" text NOT NULL,
          content_time_utc text, content_time_status text,
          likes_count integer NOT NULL DEFAULT 0, repost_count integer NOT NULL DEFAULT 0,
          comments_count integer NOT NULL DEFAULT 0, quote_count integer NOT NULL DEFAULT 0
        )
      `.execute(trx)
      await sql`
        CREATE TABLE engagement (
          uri text PRIMARY KEY, cid text NOT NULL, "subjectUri" text NOT NULL, "subjectCid" text NOT NULL,
          type integer NOT NULL, "indexedAt" text NOT NULL, "createdAt" text NOT NULL, author text NOT NULL
        )
      `.execute(trx)
      await sql`
        CREATE TABLE feedgen_ops.feed_catalog (
          rkey text PRIMARY KEY, publisher_did text, publisher_post_max_age_days integer,
          publisher_time_clock text, enabled boolean NOT NULL, algo_policy_id text NOT NULL
        )
      `.execute(trx)
      await sql`CREATE INDEX idx_post_indexedat ON post ("indexedAt")`.execute(trx)
      await sql`CREATE INDEX post_author_index ON post (author)`.execute(trx)
      await sql`INSERT INTO subscriber VALUES ('did:subscriber')`.execute(trx)
      await sql`INSERT INTO follows SELECT 'did:sub', 'did:follow-' || n FROM generate_series(1, 1999) AS n`.execute(trx)
      await sql`INSERT INTO follows VALUES ('did:sub', 'did:publisher-receipt')`.execute(trx)
      await sql`INSERT INTO follows VALUES ('did:sub', 'did:publisher-all-active')`.execute(trx)
      await sql`INSERT INTO follows VALUES ('did:sub', 'did:follow-1')`.execute(trx)
      await sql`
        INSERT INTO feedgen_ops.feed_catalog VALUES
          ('engagement-receipt', 'did:publisher-receipt', 3, 'receipt_time', true, 'engagement-sorted'),
          ('engagement-content', 'did:publisher-content', 10, 'content_time_v1', true, 'engagement-sorted'),
          ('ranker-only', 'did:ranker', 2, 'receipt_time', true, 'ranker-priority'),
          ('all-active-publisher', 'did:publisher-all-active', 5, 'receipt_time', true, 'chronological')
      `.execute(trx)

      const insertPost = (uri: string, author: string, indexedAt: string, contentTime: string | null = null, contentStatus: string | null = null, rootUri = '') => sql`
        INSERT INTO post(uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid", "linkUrl", "linkTitle", "linkDescription", content_time_utc, content_time_status)
        VALUES (${uri}, 'cid', ${indexedAt}, ${indexedAt}, ${author}, '', ${rootUri}, '', '', '', '', ${contentTime}, ${contentStatus})
      `.execute(trx)
      await insertPost('at://follow/recent', 'did:follow-1', atHoursAgo(47))
      await insertPost('at://follow/boundary', 'did:follow-3', atHoursAgo(48))
      await insertPost('at://publisher/receipt', 'did:publisher-receipt', atDaysAgo(2))
      await insertPost('at://publisher/content', 'did:publisher-content', atDaysAgo(12), atDaysAgo(9), 'source_valid')
      await insertPost('at://follow/expired', 'did:follow-2', atHoursAgo(49))
      await insertPost('at://publisher/receipt-expired', 'did:publisher-receipt', atDaysAgo(4))
      await insertPost('at://publisher/content-invalid', 'did:publisher-content', atDaysAgo(1), atDaysAgo(1), 'source_invalid')
      await insertPost('at://ranker/eligible', 'did:ranker', atHoursAgo(1))
      await insertPost('at://publisher/all-active', 'did:publisher-all-active', atHoursAgo(47))
      await insertPost('at://comment/one', 'did:commenter', atHoursAgo(1), null, null, 'at://follow/recent')
      await sql`
        INSERT INTO post(uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid", "linkUrl", "linkTitle", "linkDescription")
        SELECT 'at://bulk/other-' || n, 'cid', ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:follow-' || n, '', '', '', '', '', ''
        FROM generate_series(1, 501) AS n
      `.execute(trx)
      await sql`
        INSERT INTO post(uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid", "linkUrl", "linkTitle", "linkDescription")
        SELECT 'at://bulk/publisher-' || n, 'cid', ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:publisher-receipt', '', '', '', '', '', ''
        FROM generate_series(1, 501) AS n
      `.execute(trx)
      await insertPost('at://comment/publisher-subscriber', 'did:subscriber', atHoursAgo(1), null, null, 'at://bulk/publisher-1')
      await insertPost('at://comment/publisher-outsider', 'did:outsider', atHoursAgo(1), null, null, 'at://bulk/publisher-1')
      await sql`
        INSERT INTO engagement VALUES
          ('at://like/one', 'cid', 'at://follow/recent', 'cid', 2, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:liker'),
          ('at://repost/one', 'cid', 'at://follow/recent', 'cid', 1, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:reposter'),
          ('at://quote/one', 'cid', 'at://follow/recent', 'cid', 3, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:quoter'),
          ('at://like/all-active', 'cid', 'at://publisher/all-active', 'cid', 2, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:outsider'),
          ('at://publisher/repost-subscriber', 'cid', 'at://bulk/publisher-1', 'cid', 1, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:subscriber'),
          ('at://publisher/like-subscriber', 'cid', 'at://bulk/publisher-1', 'cid', 2, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:subscriber'),
          ('at://publisher/quote-subscriber', 'cid', 'at://bulk/publisher-1', 'cid', 3, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:subscriber'),
          ('at://publisher/repost-outsider', 'cid', 'at://bulk/publisher-1', 'cid', 1, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:outsider'),
          ('at://publisher/like-outsider', 'cid', 'at://bulk/publisher-1', 'cid', 2, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:outsider'),
          ('at://publisher/quote-outsider', 'cid', 'at://bulk/publisher-1', 'cid', 3, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:outsider')
      `.execute(trx)

      const plan = deriveEngagementRefreshPlan(await trx.selectFrom('feedgen_ops.feed_catalog')
        .select(['rkey', 'publisher_did', 'publisher_post_max_age_days', 'publisher_time_clock'])
        .where('enabled', '=', true)
        .where('algo_policy_id', 'in', ['engagement-sorted', 'ranker-priority']).execute(), referenceMs)
      assert.equal(plan.receiptDays, 3, 'chronological-only publisher must not widen the cached publisher plan')
      const followedCutoff = cutoffFromHours(referenceMs, resolveEngagementTimeHours())
      assert.equal(followedCutoff, atHoursAgo(48))
      queries.length = 0
      const followed = await selectRecentFollowedPosts(trx as any, followedCutoff)
      const followedAuthorQueries = queries.filter((query) => /select distinct "follows" from "follows"/i.test(query.sql))
      const followedPostQueries = queries.filter((query) => /select "post"\."uri", "post"\."author" from "post"/i.test(query.sql))
      assert.equal(followedAuthorQueries.length, 1, 'followed authors must be selected once')
      assert.equal(followedPostQueries.length, 3, '2,001 distinct authors must use three sequential post selects')
      for (const query of followedPostQueries) {
        assert.doesNotMatch(query.sql, /exists\s*\(|\bany\s*\(/i)
        assert.match(query.sql, /"post"\."author" in \(/i)
        assert.equal(query.parameters[0], followedCutoff)
        assert.ok(query.parameters.length - 1 <= 1000, 'each post select must bind at most 1,000 authors')
      }
      const receipt = await selectRecentPublisherPosts(trx as any, ['did:publisher-receipt'], 'receipt_time', plan.receiptCutoff)!.execute()
      const selected = await selectEngagementRefreshPosts(trx as any, plan, referenceMs)
      assert.equal(followed.length, 1006)
      assert.deepEqual(followed.map((row) => row.uri).filter((uri) => !uri.startsWith('at://bulk/')).sort(), ['at://follow/boundary', 'at://follow/recent', 'at://publisher/all-active', 'at://publisher/receipt'])
      assert.equal(receipt.length, 502)
      assert.deepEqual(receipt.map((row) => row.uri).filter((uri) => !uri.startsWith('at://bulk/')), ['at://publisher/receipt'])
      assert.equal(selected.length, 1008)
      assert.deepEqual(selected.map((row) => row.uri).filter((uri) => !uri.startsWith('at://bulk/')).sort(), [
        'at://follow/boundary', 'at://follow/recent', 'at://publisher/all-active', 'at://publisher/content', 'at://publisher/receipt', 'at://ranker/eligible',
      ])

      queries.length = 0
      await Promise.all([
        updateEngagement(trx as any, referenceMs),
        updateEngagement(trx as any, referenceMs),
      ])
      const reactionQueries = queries.filter((query) => /from "engagement"/i.test(query.sql) && /group by .*"engagement"\."type"/i.test(query.sql))
      assert.equal(reactionQueries.length, 4, 'publisher and other reactions must each use two combined queries')
      for (const query of reactionQueries) {
        assert.match(query.sql, /"engagement"\."type" in \(/i)
        assert.ok(query.parameters.filter((value) => typeof value === 'string' && value.startsWith('at://')).length <= 500, 'reaction queries must bind at most 500 URIs')
        assert.deepEqual(query.parameters.filter((value) => [1, 2, 3].includes(Number(value))).map(Number).sort(), [1, 2, 3])
      }
      assert.equal(reactionQueries.filter((query) => /"engagement"\."author" in \(/i.test(query.sql)).length, 2, 'publisher reaction queries must retain subscriber filtering')
      const commentQueries = queries.filter((query) => /from "post" as "comments"/i.test(query.sql))
      assert.equal(commentQueries.length, 4, 'publisher and other comments must each use two queries')
      assert.ok(commentQueries.every((query) => query.parameters.filter((value) => typeof value === 'string' && value.startsWith('at://')).length <= 500), 'comment queries must bind at most 500 URIs')
      const updateQueries = queries.filter((query) => /^\s*update post\s+set/im.test(query.sql))
      assert.equal(updateQueries.length, 3, 'one coalesced refresh must update 1,008 posts in three batches')
      assert.ok(updateQueries.every((query) => new Set(query.parameters.filter((value) => typeof value === 'string' && value.startsWith('at://'))).size <= 500), 'update queries must target at most 500 distinct URIs')
      const refreshed = await trx.selectFrom('post').select(['likes_count', 'repost_count', 'quote_count', 'comments_count'])
        .where('uri', '=', 'at://follow/recent').executeTakeFirstOrThrow()
      assert.deepEqual(refreshed, { likes_count: 1, repost_count: 1, quote_count: 1, comments_count: 1 })
      const publisherRefreshed = await trx.selectFrom('post').select(['likes_count', 'repost_count', 'quote_count', 'comments_count'])
        .where('uri', '=', 'at://bulk/publisher-1').executeTakeFirstOrThrow()
      assert.deepEqual(publisherRefreshed, { likes_count: 1, repost_count: 1, quote_count: 1, comments_count: 1 })
      const allActivePublisher = await trx.selectFrom('post').select('likes_count')
        .where('uri', '=', 'at://publisher/all-active').executeTakeFirstOrThrow()
      assert.equal(allActivePublisher.likes_count, 0, 'followed chronological publisher must retain subscriber-only classification')
      await sql`DELETE FROM feedgen_ops.feed_catalog WHERE algo_policy_id = 'engagement-sorted'`.execute(trx)
      await updateEngagement(trx as any, referenceMs)
      console.log('Summary: 1 passed, 0 skipped')
      throw new Error('__ROLLBACK_DISPOSABLE_REFRESH_TEST__')
    })
  } catch (error) {
    if (!(error instanceof Error) || error.message !== '__ROLLBACK_DISPOSABLE_REFRESH_TEST__') throw error
  } finally {
    if (priorEngagementTimeHours === undefined) delete process.env.ENGAGEMENT_TIME_HOURS
    else process.env.ENGAGEMENT_TIME_HOURS = priorEngagementTimeHours
    await db.destroy()
  }
}

main().catch((error) => {
  console.error('engagement refresh execution test failed:', error)
  process.exit(1)
})
