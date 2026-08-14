import { Database } from '../db';
import { sql } from 'kysely';
import { cutoffFromHours } from '../algos/publisher-serving-window'
import { resolveEngagementTimeHours } from '../algos/feed-builder'
import { resolvePublisherDids } from './publisher-dids'

const ENGAGEMENT_TYPE_REPOST = 1
const ENGAGEMENT_TYPE_LIKE = 2
const ENGAGEMENT_TYPE_QUOTE = 3
const FOLLOWED_AUTHOR_CHUNK_SIZE = 1000

type RefreshCatalogRow = {
  rkey: string
  publisher_did?: string | null
  publisher_post_max_age_days?: number | null
  publisher_time_clock?: string | null
}

export function deriveEngagementRefreshPlan(rows: RefreshCatalogRow[], referenceMs: number) {
  if (rows.length === 0) throw new Error('engagement refresh requires active catalog consumers')
  const invalid = rows.find((row) =>
    !row.publisher_did
    || !Number.isSafeInteger(row.publisher_post_max_age_days)
    || Number(row.publisher_post_max_age_days) <= 0
    || !['receipt_time', 'content_time_v1'].includes(String(row.publisher_time_clock)))
  if (invalid) throw new Error(`engagement refresh cannot resolve feed window for ${invalid.rkey}`)

  const maxDays = (candidates: RefreshCatalogRow[]) => candidates.reduce(
    (maximum, row) => Math.max(maximum, Number(row.publisher_post_max_age_days)),
    0,
  )
  const receiptPublisherRows = rows.filter((row) => row.publisher_time_clock === 'receipt_time')
  const contentPublisherRows = rows.filter((row) => row.publisher_time_clock === 'content_time_v1')
  const receiptDays = maxDays(receiptPublisherRows)
  const contentDays = maxDays(contentPublisherRows)
  return {
    receiptPublisherRows,
    contentPublisherRows,
    receiptDays,
    contentDays,
    receiptCutoff: new Date(referenceMs - receiptDays * 86_400_000).toISOString(),
    contentCutoff: contentDays === 0 ? null : new Date(referenceMs - contentDays * 86_400_000).toISOString(),
  }
}

export async function selectRecentFollowedPosts(db: Database, cutoff: string) {
  const authors = (await db.selectFrom('follows').select('follows').distinct().execute()).map((row) => row.follows)
  const posts: Array<{ uri: string, author: string }> = []
  for (let index = 0; index < authors.length; index += FOLLOWED_AUTHOR_CHUNK_SIZE) {
    posts.push(...await db
      .selectFrom('post')
      .select(['post.uri', 'post.author'])
      .where('post.indexedAt', '>=', cutoff)
      .where('post.author', 'in', authors.slice(index, index + FOLLOWED_AUTHOR_CHUNK_SIZE))
      .execute())
  }
  return posts
}

export function selectRecentPublisherPosts(
  db: Database,
  publishers: string[],
  clock: 'receipt_time' | 'content_time_v1',
  cutoff: string | null,
) {
  if (publishers.length === 0 || cutoff === null) return null
  let query = db.selectFrom('post').where('post.author', 'in', publishers)
  query = clock === 'content_time_v1'
    ? query.where('post.content_time_status', '=', 'source_valid').where('post.content_time_utc', '>=', cutoff)
    : query.where('post.indexedAt', '>=', cutoff)
  return query.select(['post.uri', 'post.author'])
}

export async function selectEngagementRefreshPosts(
  db: Database,
  refreshPlan: ReturnType<typeof deriveEngagementRefreshPlan>,
  referenceMs: number,
) {
  const followedCutoff = cutoffFromHours(referenceMs, resolveEngagementTimeHours())
  const receiptPublisherQuery = selectRecentPublisherPosts(
    db,
    refreshPlan.receiptPublisherRows.map((row) => String(row.publisher_did)),
    'receipt_time',
    refreshPlan.receiptCutoff,
  )
  const contentPublisherQuery = selectRecentPublisherPosts(
    db,
    refreshPlan.contentPublisherRows.map((row) => String(row.publisher_did)),
    'content_time_v1',
    refreshPlan.contentCutoff,
  )
  const [followedPosts, receiptPosts, contentPosts] = await Promise.all([
    selectRecentFollowedPosts(db, followedCutoff),
    receiptPublisherQuery?.execute() ?? [],
    contentPublisherQuery?.execute() ?? [],
  ])
  return [...new Map(
    [...followedPosts, ...receiptPosts, ...contentPosts].map((post) => [post.uri, post]),
  ).values()]
}

/**
 * Updates engagement counts (likes, reposts, comments) for recent posts
 * For publisher posts (from newsbots), only counts engagement from subscribers
 * For other posts, counts all engagement
 */
export async function updateEngagement(db: Database, referenceMs = Date.now()): Promise<void> {
  try {
    // Postgres supports at most 65535 bind params. Kysely binds one param per array element for `IN (...)`.
    // Chunk large URI lists to avoid protocol errors at scale.
    const IN_CLAUSE_CHUNK_SIZE = 5000;
    const execInChunks = async <T>(items: string[], fn: (chunk: string[]) => Promise<T[]>): Promise<T[]> => {
      const results: T[] = [];
      for (let i = 0; i < items.length; i += IN_CLAUSE_CHUNK_SIZE) {
        const chunk = items.slice(i, i + IN_CLAUSE_CHUNK_SIZE);
        results.push(...(await fn(chunk)));
      }
      return results;
    };

    const catalogRows = await db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['rkey', 'publisher_did', 'publisher_post_max_age_days', 'publisher_time_clock', 'algo_policy_id'])
      .where('enabled', '=', true)
      .execute()
    const cachedConsumerRows = catalogRows.filter((row) =>
      ['engagement-sorted', 'ranker-priority'].includes(row.algo_policy_id),
    )
    if (cachedConsumerRows.length === 0) throw new Error('engagement refresh requires active catalog consumers')
    const refreshPlan = deriveEngagementRefreshPlan(
      cachedConsumerRows.filter((row) => row.publisher_did),
      referenceMs,
    )
    const publisherDids = new Set(await resolvePublisherDids(db))

    console.log(`[${new Date(referenceMs).toISOString()}] - Starting clock-matched engagement refresh...`);

    // Get all subscribers from the database
    const subscribers = await db
      .selectFrom('subscriber')
      .select('did')
      .execute();
    const subscriberDids = subscribers.map(s => s.did);

    const recentPosts = await selectEngagementRefreshPosts(db, refreshPlan, referenceMs)

    const postUris = recentPosts.map(post => post.uri);

    // Separate publisher posts from other posts
    const publisherPostUris: string[] = [];
    const otherPostUris: string[] = [];
    
    recentPosts.forEach(post => {
      if (publisherDids.has(post.author)) {
        publisherPostUris.push(post.uri);
      } else {
        otherPostUris.push(post.uri);
      }
    });

    if (postUris.length === 0) {
      console.log(`[${new Date().toISOString()}] - No recent posts to update.`);
      return;
    }

    console.log(`[${new Date().toISOString()}] - Found ${postUris.length} posts to update engagement stats for (${publisherPostUris.length} from publishers, ${otherPostUris.length} from others).`);

    // Count likes for each post
    // For other posts: count all engagement
    const otherLikesResult = otherPostUris.length > 0
      ? await execInChunks(otherPostUris, async (chunk) => {
        return db
          .selectFrom('engagement')
          .where('engagement.subjectUri', 'in', chunk)
          .where('engagement.type', '=', ENGAGEMENT_TYPE_LIKE)
          .select([
            'engagement.subjectUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('engagement.subjectUri')
          .execute();
      })
      : [];

    // For publisher posts: only count engagement from subscribers
    const publisherLikesResult = (publisherPostUris.length > 0 && subscriberDids.length > 0)
      ? await execInChunks(publisherPostUris, async (chunk) => {
        return db
          .selectFrom('engagement')
          .where('engagement.subjectUri', 'in', chunk)
          .where('engagement.author', 'in', subscriberDids)
          .where('engagement.type', '=', ENGAGEMENT_TYPE_LIKE)
          .select([
            'engagement.subjectUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('engagement.subjectUri')
          .execute();
      })
      : [];

    const likeCountsResult = [...otherLikesResult, ...publisherLikesResult];

    // Count reposts for each post
    // For other posts: count all engagement
    const otherRepostsResult = otherPostUris.length > 0
      ? await execInChunks(otherPostUris, async (chunk) => {
        return db
          .selectFrom('engagement')
          .where('engagement.subjectUri', 'in', chunk)
          .where('engagement.type', '=', ENGAGEMENT_TYPE_REPOST)
          .select([
            'engagement.subjectUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('engagement.subjectUri')
          .execute();
      })
      : [];

    // For publisher posts: only count engagement from subscribers
    const publisherRepostsResult = (publisherPostUris.length > 0 && subscriberDids.length > 0)
      ? await execInChunks(publisherPostUris, async (chunk) => {
        return db
          .selectFrom('engagement')
          .where('engagement.subjectUri', 'in', chunk)
          .where('engagement.author', 'in', subscriberDids)
          .where('engagement.type', '=', ENGAGEMENT_TYPE_REPOST)
          .select([
            'engagement.subjectUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('engagement.subjectUri')
          .execute();
      })
      : [];

    const repostCountsResult = [...otherRepostsResult, ...publisherRepostsResult];

    // Count quotes for each post
    // For other posts: count all engagement
    const otherQuotesResult = otherPostUris.length > 0
      ? await execInChunks(otherPostUris, async (chunk) => {
        return db
          .selectFrom('engagement')
          .where('engagement.subjectUri', 'in', chunk)
          .where('engagement.type', '=', ENGAGEMENT_TYPE_QUOTE)
          .select([
            'engagement.subjectUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('engagement.subjectUri')
          .execute();
      })
      : [];

    // For publisher posts: only count engagement from subscribers
    const publisherQuotesResult = (publisherPostUris.length > 0 && subscriberDids.length > 0)
      ? await execInChunks(publisherPostUris, async (chunk) => {
        return db
          .selectFrom('engagement')
          .where('engagement.subjectUri', 'in', chunk)
          .where('engagement.author', 'in', subscriberDids)
          .where('engagement.type', '=', ENGAGEMENT_TYPE_QUOTE)
          .select([
            'engagement.subjectUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('engagement.subjectUri')
          .execute();
      })
      : [];

    const quoteCountsResult = [...otherQuotesResult, ...publisherQuotesResult];

    // Count comments for each post (comments are posts with rootUri pointing to the original post)
    // For other posts: count all comments
    const otherCommentsResult = otherPostUris.length > 0
      ? await execInChunks(otherPostUris, async (chunk) => {
        return db
          .selectFrom('post as comments')
          .where('comments.rootUri', 'in', chunk)
          .where('comments.rootUri', '!=', '') // Ensure it's a real comment
          .select([
            'comments.rootUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('comments.rootUri')
          .execute();
      })
      : [];

    // For publisher posts: only count comments from subscribers
    const publisherCommentsResult = (publisherPostUris.length > 0 && subscriberDids.length > 0)
      ? await execInChunks(publisherPostUris, async (chunk) => {
        return db
          .selectFrom('post as comments')
          .where('comments.rootUri', 'in', chunk)
          .where('comments.author', 'in', subscriberDids)
          .where('comments.rootUri', '!=', '') // Ensure it's a real comment
          .select([
            'comments.rootUri as uri',
            db.fn.count<number>('uri').as('count')
          ])
          .groupBy('comments.rootUri')
          .execute();
      })
      : [];

    const commentCountsResult = [...otherCommentsResult, ...publisherCommentsResult];

    // Create maps for quick lookups
    const likesMap = new Map(
      likeCountsResult.map(result => [result.uri, Number(result.count)])
    );

    const repostsMap = new Map(
      repostCountsResult.map(result => [result.uri, Number(result.count)])
    );

    const commentsMap = new Map(
      commentCountsResult.map(result => [result.uri, Number(result.count)])
    );

    const quotesMap = new Map(
      quoteCountsResult.map(result => [result.uri, Number(result.count)])
    );

    // Update posts with counts
    const batchSize = 5000;
    for (let i = 0; i < postUris.length; i += batchSize) {
      const batchUris = postUris.slice(i, i + batchSize);

      // Build CASE statements for bulk update using sql template
      const likesCases = sql.join(
        batchUris.map(uri => sql`WHEN uri = ${uri} THEN ${likesMap.get(uri) || 0}`),
        sql` `
      );

      const repostsCases = sql.join(
        batchUris.map(uri => sql`WHEN uri = ${uri} THEN ${repostsMap.get(uri) || 0}`),
        sql` `
      );

      const commentsCases = sql.join(
        batchUris.map(uri => sql`WHEN uri = ${uri} THEN ${commentsMap.get(uri) || 0}`),
        sql` `
      );

      const quotesCases = sql.join(
        batchUris.map(uri => sql`WHEN uri = ${uri} THEN ${quotesMap.get(uri) || 0}`),
        sql` `
      );

      // Execute single UPDATE with CASE for the entire batch
      await sql`
        UPDATE post
        SET
          likes_count = CASE ${likesCases} ELSE likes_count END,
          repost_count = CASE ${repostsCases} ELSE repost_count END,
          comments_count = CASE ${commentsCases} ELSE comments_count END,
          quote_count = CASE ${quotesCases} ELSE quote_count END
        WHERE uri IN (${sql.join(batchUris.map(uri => sql`${uri}`), sql`, `)})
      `.execute(db);

      console.log(`[${new Date().toISOString()}] - Updated batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(postUris.length / batchSize)} (${batchUris.length} posts)`);
    }

    console.log(JSON.stringify({
      event: 'engagement_refresh_complete',
      reference_time: new Date(referenceMs).toISOString(),
      receipt_clock_days: refreshPlan.receiptDays,
      receipt_clock_feeds: refreshPlan.receiptPublisherRows.map((row) => row.rkey).sort(),
      content_clock_days: refreshPlan.contentDays,
      content_clock_feeds: refreshPlan.contentPublisherRows.map((row) => row.rkey).sort(),
      updated_posts: postUris.length,
    }));
  } catch (error) {
    console.error('Error in scheduled engagement update:', error);
    throw error; // Re-throw to allow caller to handle the error
  }
}
