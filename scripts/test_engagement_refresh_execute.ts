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
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }) })
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
      await sql`INSERT INTO follows SELECT 'did:sub', 'did:follow-' || n FROM generate_series(1, 50000) AS n`.execute(trx)
      await sql`INSERT INTO follows VALUES ('did:sub', 'did:publisher-receipt')`.execute(trx)
      await sql`INSERT INTO follows VALUES ('did:sub', 'did:publisher-all-active')`.execute(trx)
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
      await insertPost('at://publisher/receipt', 'did:publisher-receipt', atDaysAgo(2))
      await insertPost('at://publisher/content', 'did:publisher-content', atDaysAgo(12), atDaysAgo(9), 'source_valid')
      await insertPost('at://follow/expired', 'did:follow-2', atHoursAgo(49))
      await insertPost('at://publisher/receipt-expired', 'did:publisher-receipt', atDaysAgo(4))
      await insertPost('at://publisher/content-invalid', 'did:publisher-content', atDaysAgo(1), atDaysAgo(1), 'source_invalid')
      await insertPost('at://ranker/eligible', 'did:ranker', atHoursAgo(1))
      await insertPost('at://publisher/all-active', 'did:publisher-all-active', atHoursAgo(47))
      await insertPost('at://comment/one', 'did:commenter', atHoursAgo(1), null, null, 'at://follow/recent')
      await sql`
        INSERT INTO engagement VALUES
          ('at://like/one', 'cid', 'at://follow/recent', 'cid', 2, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:liker'),
          ('at://repost/one', 'cid', 'at://follow/recent', 'cid', 1, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:reposter'),
          ('at://quote/one', 'cid', 'at://follow/recent', 'cid', 3, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:quoter'),
          ('at://like/all-active', 'cid', 'at://publisher/all-active', 'cid', 2, ${atHoursAgo(1)}, ${atHoursAgo(1)}, 'did:outsider')
      `.execute(trx)

      const plan = deriveEngagementRefreshPlan(await trx.selectFrom('feedgen_ops.feed_catalog')
        .select(['rkey', 'publisher_did', 'publisher_post_max_age_days', 'publisher_time_clock'])
        .where('enabled', '=', true)
        .where('algo_policy_id', 'in', ['engagement-sorted', 'ranker-priority']).execute(), referenceMs)
      assert.equal(plan.receiptDays, 3, 'chronological-only publisher must not widen the cached publisher plan')
      const followedCutoff = cutoffFromHours(referenceMs, resolveEngagementTimeHours())
      assert.equal(followedCutoff, atHoursAgo(48))
      const followedSql = selectRecentFollowedPosts(trx as any, followedCutoff).compile().sql.toLowerCase()
      assert.match(followedSql, /post\.author\s*=\s*any\s*\(\s*array\s*\(\s*select\s+f\.follows\s+from\s+follows\s+as\s+f\s*\)\s*\)/)
      assert.doesNotMatch(followedSql, /exists\s*\(/)
      assert.deepEqual(selectRecentFollowedPosts(trx as any, followedCutoff).compile().parameters, [followedCutoff])
      const followed = await selectRecentFollowedPosts(trx as any, followedCutoff).execute()
      const receipt = await selectRecentPublisherPosts(trx as any, ['did:publisher-receipt'], 'receipt_time', plan.receiptCutoff)!.execute()
      const selected = await selectEngagementRefreshPosts(trx as any, plan, referenceMs)
      assert.deepEqual(followed.map((row) => row.uri).sort(), ['at://follow/recent', 'at://publisher/all-active', 'at://publisher/receipt'])
      assert.deepEqual(receipt.map((row) => row.uri), ['at://publisher/receipt'])
      assert.deepEqual(selected.map((row) => row.uri).sort(), [
        'at://follow/recent', 'at://publisher/all-active', 'at://publisher/content', 'at://publisher/receipt', 'at://ranker/eligible',
      ])

      await updateEngagement(trx as any, referenceMs)
      const refreshed = await trx.selectFrom('post').select(['likes_count', 'repost_count', 'quote_count', 'comments_count'])
        .where('uri', '=', 'at://follow/recent').executeTakeFirstOrThrow()
      assert.deepEqual(refreshed, { likes_count: 1, repost_count: 1, quote_count: 1, comments_count: 1 })
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
