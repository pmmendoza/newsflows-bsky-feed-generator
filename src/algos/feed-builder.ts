import { Kysely } from 'kysely'
import { QueryParams, OutputSchema as AlgoOutput } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { DatabaseSchema, Post } from '../db/schema'
import { AppContext } from '../config'
import { SkeletonFeedPost } from '../lexicon/types/app/bsky/feed/defs'

// Type definition for FeedGenerator handler
export type FeedGenerator = (ctx: AppContext, params: QueryParams, requesterDid: string) => Promise<AlgoOutput>

// Type for the query builder functions
export type QueryBuilder = (
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  requesterFollows: string[],
  cursorOffset: number,
  limit: number
) => any

// Interface for the feed generator options
export interface FeedGeneratorOptions {
  shortname: string
  ctx: AppContext
  params: QueryParams
  requesterDid: string
  buildPublisherQuery: QueryBuilder
  buildFollowsQuery: QueryBuilder
}

// Main function to build a feed
export async function buildFeed({
  shortname,
  ctx,
  params,
  requesterDid,
  buildPublisherQuery,
  buildFollowsQuery
}: FeedGeneratorOptions) {
  console.log(`[${new Date().toISOString()}] - Feed ${shortname} requested by ${requesterDid}`);
  const limit = Math.floor(params.limit / 3); // 1/3 from news + 2/3 other
  const requesterFollows = await getFollows(requesterDid, ctx.db)
  
  // don't consider posts older than time limit hours
  const engagementTimeHours = process.env.ENGAGEMENT_TIME_HOURS ?
    parseInt(process.env.ENGAGEMENT_TIME_HOURS, 10) : 72;
  const timeLimit = new Date(Date.now() - engagementTimeHours * 60 * 60 * 1000).toISOString();

  // Parse cursor if provided
  let cursorOffset = 0;
  if (params.cursor) {
    cursorOffset = parseInt(params.cursor, 10);
  }

  // Build the queries using the provided builder functions
  const publisherPostsQuery = buildPublisherQuery(
    ctx.db,
    timeLimit,
    requesterFollows,
    cursorOffset,
    limit
  );

  const otherPostsQuery = buildFollowsQuery(
    ctx.db,
    timeLimit,
    requesterFollows,
    cursorOffset,
    limit * 2
  );

  // Execute both queries in parallel
  const [publisherPosts, otherPosts] = await Promise.all([
    publisherPostsQuery.execute(),
    otherPostsQuery.execute()
  ]);

  console.log(`[${new Date().toISOString()}] - Feed ${shortname} retrieved ${publisherPosts.length} publisher posts and ${otherPosts.length} other posts`);

  // Merge both post lists in a 1:2 pattern (1 publisher post, 2 other posts)
  const feed: SkeletonFeedPost[] = [];
  const servedPosts: Post[] = [];
  let publisherIndex = 0;
  let otherIndex = 0;

  while (publisherIndex < publisherPosts.length || otherIndex < otherPosts.length) {
    // Add 1 publisher post
    if (publisherIndex < publisherPosts.length) {
      const post = publisherPosts[publisherIndex] as Post;
      feed.push({ post: post.uri });
      servedPosts.push(post);
      publisherIndex++;
    }

    // Add 2 other posts
    for (let i = 0; i < 2 && otherIndex < otherPosts.length; i++) {
      const post = otherPosts[otherIndex] as Post;
      feed.push({ post: post.uri });
      servedPosts.push(post);
      otherIndex++;
    }
  }

  // Calculate cursor based on the offset for the next page
  let cursor: string | undefined;
  const totalPostsReturned = publisherPosts.length + otherPosts.length;
  if (totalPostsReturned > 0) {
    // Set the next offset to current offset + number of posts returned
    cursor = (cursorOffset + limit * 2).toString();
  }

  const archiveOutboxEnabled = process.env.FEEDGEN_ARCHIVE_OUTBOX_ENABLED === 'true';
  const requestLogInput = {
    shortname,
    requesterDid,
    params,
    cursor,
    publisherCount: publisherPosts.length,
    followsCount: otherPosts.length,
    servedPosts,
  };

  if (archiveOutboxEnabled) {
    await logRequest(ctx, requestLogInput, true);
  } else {
    // Preserve the current non-blocking behavior until the archive cut-over flag is enabled.
    setTimeout(async () => {
      try {
        await logRequest(ctx, requestLogInput, false);
      } catch (error) {
        console.error('Error logging request:', error);
      }
    }, 0);
  }

  return {
    cursor,
    feed,
  };
}

type RequestLogInput = {
  shortname: string
  requesterDid: string
  params: QueryParams
  cursor?: string
  publisherCount: number
  followsCount: number
  servedPosts: Post[]
}

async function logRequest(
  ctx: AppContext,
  input: RequestLogInput,
  includeArchiveOutbox: boolean,
) {
  const timestamp = new Date().toISOString();
  const requestedLimit =
    typeof input.params.limit === 'number' && Number.isFinite(input.params.limit)
      ? input.params.limit
      : null;

  await ctx.db.transaction().execute(async (trx) => {
    const requestInsertResult = await trx
      .insertInto('request_log')
      .values({
        algo: input.shortname,
        requester_did: input.requesterDid,
        timestamp,
        cursor_in: input.params.cursor ?? null,
        cursor_out: input.cursor ?? null,
        requested_limit: requestedLimit,
        publisher_count: input.publisherCount,
        follows_count: input.followsCount,
        result_count: input.servedPosts.length,
      })
      .returning('id')
      .executeTakeFirstOrThrow();

    const requestId = requestInsertResult.id as number;

    if (input.servedPosts.length === 0) {
      return;
    }

    await trx
      .insertInto('request_posts')
      .values(input.servedPosts.map((post, index) => ({
        position: index + 1,
        request_id: requestId,
        post_uri: post.uri,
      })))
      .execute();

    if (!includeArchiveOutbox) {
      return;
    }

    await trx
      .insertInto('feedgen_ops.archive_outbox')
      .values(input.servedPosts.map((post, index) => ({
        request_id: requestId,
        position: index + 1,
        feed_id: input.shortname,
        study_id: null,
        requester_did: input.requesterDid,
        requested_at: timestamp,
        post_uri: post.uri,
        post_cid: post.cid,
        payload_json: buildArchivePayload({
          requestId,
          position: index + 1,
          timestamp,
          requestedLimit,
          input,
          post,
        }),
      })))
      .execute();
  });
}

function buildArchivePayload({
  requestId,
  position,
  timestamp,
  requestedLimit,
  input,
  post,
}: {
  requestId: number
  position: number
  timestamp: string
  requestedLimit: number | null
  input: RequestLogInput
  post: Post
}) {
  return {
    schema_version: 1,
    captured_from: 'served',
    request: {
      request_id: requestId,
      position,
      feed_id: input.shortname,
      study_id: null,
      requester_did: input.requesterDid,
      requested_at: timestamp,
      cursor_in: input.params.cursor ?? null,
      cursor_out: input.cursor ?? null,
      requested_limit: requestedLimit,
      result_count: input.servedPosts.length,
      feedgen_build_sha: process.env.FEEDGEN_BUILD_SHA || null,
      algo_policy_id: input.shortname,
    },
    post: {
      uri: post.uri,
      cid: post.cid,
      author: post.author,
      createdAt: post.createdAt,
      indexedAt: post.indexedAt,
      text: post.text,
      rootUri: post.rootUri,
      rootCid: post.rootCid,
      linkUrl: post.linkUrl,
      linkTitle: post.linkTitle,
      linkDescription: post.linkDescription,
      priority: post.priority ?? null,
      likes_count: post.likes_count ?? null,
      repost_count: post.repost_count ?? null,
      comments_count: post.comments_count ?? null,
      quote_count: post.quote_count ?? null,
    },
  };
}

// Helper function to get follows
async function getFollows(actor: string, db: any): Promise<string[]> {
  // Import the function dynamically to avoid circular imports
  const { getFollows } = await import('../util/queries');
  return getFollows(actor, db);
}
