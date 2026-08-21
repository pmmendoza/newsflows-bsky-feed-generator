import { Kysely } from 'kysely'
import { QueryParams, OutputSchema as AlgoOutput } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { DatabaseSchema, Post, PublisherPostMaxAgeSource, PublisherTimeClock } from '../db/schema'
import { AppContext } from '../config'
import { SkeletonFeedPost } from '../lexicon/types/app/bsky/feed/defs'
import { dualWriteLinkFields } from '../util/link-fields'
import { recordRankerPriorityResult } from './ranker-priority-helper'
import {
  cutoffFromHours,
  parseStrictPositiveHours,
  resolvePublisherServingWindow,
} from './publisher-serving-window'

// Type definition for FeedGenerator handler
export type FeedGenerator = (ctx: AppContext, params: QueryParams, requesterDid: string) => Promise<AlgoOutput>

// Type for the query builder functions
export type QueryBuilder = (
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  referenceTimeIso: string,
) => any

// Followed-account compatibility horizon. Publisher serving uses the
// materialized catalog value when present. The manifest calls this same strict
// resolver so readback and serving cannot disagree on invalid legacy input.
export function resolveEngagementTimeHours(): number {
  return parseStrictPositiveHours(process.env.ENGAGEMENT_TIME_HOURS) ?? 72
}

export function archiveOutboxEnabled(): boolean {
  return process.env.FEEDGEN_ARCHIVE_OUTBOX_ENABLED === 'true'
}

// Interface for the feed generator options
export interface FeedGeneratorOptions {
  shortname: string
  ctx: AppContext
  params: QueryParams
  requesterDid: string
  buildPublisherQuery: QueryBuilder
  buildFollowsQuery: QueryBuilder
  publisherPostMaxAgeDays?: number | null
  publisherPostMaxAgeSource?: PublisherPostMaxAgeSource | null
  publisherTimeClock?: PublisherTimeClock
  feedCatalogRevision?: number | string | null
  rankerScoreMaxAgeHours?: number | null
  rankerScoreCompatibilityFallbackActive?: boolean
  rankerMinScoreBackedShare?: number | null
}

export function feedPageOffsets(limit: number, cursor?: string) {
  const followsOffset = cursor ? parseInt(cursor, 10) : 0
  const publisherLimit = Math.max(1, Math.floor(limit / 3))
  const followsLimit = publisherLimit * 2
  return {
    publisherLimit,
    followsLimit,
    publisherOffset: Math.floor(followsOffset / 2),
    followsOffset,
  }
}

// Main function to build a feed
export async function buildFeed({
  shortname,
  ctx,
  params,
  requesterDid,
  buildPublisherQuery,
  buildFollowsQuery,
  publisherPostMaxAgeDays,
  publisherPostMaxAgeSource,
  publisherTimeClock = 'receipt_time',
  feedCatalogRevision = null,
  rankerScoreMaxAgeHours = null,
  rankerScoreCompatibilityFallbackActive = false,
  rankerMinScoreBackedShare = null,
}: FeedGeneratorOptions) {
  const referenceTimeMs = Date.now()
  const referenceTimeIso = new Date(referenceTimeMs).toISOString()
  console.log(`[${referenceTimeIso}] - Feed ${shortname} requested by ${requesterDid}`);
  // Sprint 13 / T1: AppView probes with limit=1 and Math.floor(1/3)=0
  // makes the publisher query ask for 0 rows. Result: 200 + empty feed,
  // which AppView's online-probe heuristic can interpret as "feed broken".
  // Clamp to >=1 publisher and >=2 follows so probes always return content.
  // For limit>=3 this is a no-op (the floor split is unchanged).
  const paging = feedPageOffsets(params.limit, params.cursor)
  const { publisherLimit, followsLimit } = paging
  const limit = publisherLimit; // 1/3 from news + 2/3 other (legacy alias for cursor math)
  const requesterFollows = await getFollows(requesterDid, ctx.db)
  
  // Publisher and followed slices keep distinct cutoffs. The publisher slice
  // consumes its materialized per-feed value; followed accounts retain the
  // legacy receipt-time horizon. Both derive from one request reference time.
  const engagementTimeHours = resolveEngagementTimeHours();
  const servingWindow = resolvePublisherServingWindow(
    publisherPostMaxAgeDays,
    publisherPostMaxAgeSource,
  )
  const followsTimeLimit = cutoffFromHours(referenceTimeMs, engagementTimeHours)
  const publisherTimeLimit = cutoffFromHours(referenceTimeMs, servingWindow.effectiveHours)

  // Parse cursor if provided
  const cursorOffset = paging.followsOffset

  // Build the queries using the provided builder functions
  const publisherPostsQuery = buildPublisherQuery(
    ctx.db,
    publisherTimeLimit,
    requesterFollows,
    paging.publisherOffset,
    limit,
    referenceTimeIso,
  );

  const otherPostsQuery = buildFollowsQuery(
    ctx.db,
    followsTimeLimit,
    requesterFollows,
    cursorOffset,
    followsLimit,
    referenceTimeIso,
  );

  // Execute both queries in parallel
  const [publisherPosts, otherPosts] = await Promise.all([
    publisherPostsQuery.execute(),
    otherPostsQuery.execute()
  ]);

  // Ranker queries carry one request-level health marker on every result row.
  // Chronological/engagement rows do not, so this is a no-op for those feeds.
  recordRankerPriorityResult(shortname, publisherPosts, rankerMinScoreBackedShare)
  const rankerFreshScoredPublisherSlots = publisherPosts.filter((post: any) => post.__ranker_score != null).length

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
    // Set the next offset to current offset + size of follows window
    // (matches how the original code paged with `limit * 2`; we now use
    // `followsLimit` directly so the cursor stays consistent with the
    // post-T1 floor-division clamp).
    cursor = (cursorOffset + followsLimit).toString();
  }

  const archiveOutboxIsEnabled = archiveOutboxEnabled();
  const requestLogInput = {
    shortname,
    requesterDid,
    params,
    cursor,
    publisherCount: publisherPosts.length,
    followsCount: otherPosts.length,
    servedPosts,
    requestReferenceTime: referenceTimeIso,
    publisherPostMaxAgeDays: publisherPostMaxAgeDays ?? null,
    publisherPostMaxAgeSource: publisherPostMaxAgeSource ?? null,
    publisherTimeClock,
    publisherServingWindow: servingWindow,
    feedCatalogRevision,
    rankerScoreMaxAgeHours,
    rankerScoreCompatibilityFallbackActive,
    rankerMinScoreBackedShare,
    rankerFreshScoredPublisherSlots,
  };

  if (archiveOutboxIsEnabled) {
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
  requestReferenceTime: string
  publisherPostMaxAgeDays: number | null
  publisherPostMaxAgeSource: PublisherPostMaxAgeSource | null
  publisherTimeClock: PublisherTimeClock
  publisherServingWindow: ReturnType<typeof resolvePublisherServingWindow>
  feedCatalogRevision: number | string | null
  rankerScoreMaxAgeHours: number | null
  rankerScoreCompatibilityFallbackActive: boolean
  rankerMinScoreBackedShare: number | null
  rankerFreshScoredPublisherSlots: number
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
        request_reference_time: input.requestReferenceTime,
        publisher_post_max_age_days: input.publisherPostMaxAgeDays,
        publisher_post_max_age_source: input.publisherPostMaxAgeSource,
        publisher_time_clock: input.publisherTimeClock,
        publisher_serving_window_source: input.publisherServingWindow.source,
        publisher_compatibility_fallback_active: input.publisherServingWindow.compatibilityFallbackActive,
        feed_catalog_revision: input.feedCatalogRevision,
        ranker_score_max_age_hours: input.rankerScoreMaxAgeHours,
        ranker_score_compatibility_fallback_active: input.rankerScoreCompatibilityFallbackActive,
        ranker_fresh_scored_publisher_slots: input.rankerFreshScoredPublisherSlots,
        ranker_publisher_slots: input.publisherCount,
        ranker_min_score_backed_share: input.rankerMinScoreBackedShare,
        ranker_observed_score_backed_share: input.publisherCount > 0
          ? input.rankerFreshScoredPublisherSlots / input.publisherCount
          : null,
      })
      .returning('id')
      .executeTakeFirstOrThrow();

    const requestId = requestInsertResult.id as number;

    if (input.servedPosts.length === 0) {
      // Empty-result request. Still write a single archive_outbox row
      // with null post_uri / post_cid so the archive worker can produce
      // a research_archive.request_event row for this request. The
      // post-level archive surfaces stay untouched. Nothing else
      // changes for the public.request_log / public.request_posts
      // serving contract.
      if (includeArchiveOutbox) {
        await trx
          .insertInto('feedgen_ops.archive_outbox')
          .values({
            request_id: requestId,
            position: 0,
            feed_id: input.shortname,
            study_id: null,
            requester_did: input.requesterDid,
            requested_at: timestamp,
            post_uri: null,
            post_cid: null,
            payload_json: buildEmptyRequestPayload({
              requestId,
              timestamp,
              requestedLimit,
              input,
            }),
          })
          .execute();
      }
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
      ...requestTimeProvenance(input),
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
      ...dualWriteLinkFields({
        link_uri: post.link_uri,
        link_title: post.link_title,
        link_description: post.link_description,
        linkUrl: post.linkUrl,
        linkTitle: post.linkTitle,
        linkDescription: post.linkDescription,
      }),
      likes_count: post.likes_count ?? null,
      repost_count: post.repost_count ?? null,
      comments_count: post.comments_count ?? null,
      quote_count: post.quote_count ?? null,
      content_time_utc: post.content_time_utc ?? null,
      content_time_status: post.content_time_status ?? 'legacy_unknown',
      content_time_clamp_reason: post.content_time_clamp_reason ?? null,
      content_time_validator_version: post.content_time_validator_version ?? null,
    },
  };
}

function buildEmptyRequestPayload({
  requestId,
  timestamp,
  requestedLimit,
  input,
}: {
  requestId: number
  timestamp: string
  requestedLimit: number | null
  input: RequestLogInput
}) {
  return {
    schema_version: 1,
    captured_from: 'served',
    request: {
      request_id: requestId,
      position: 0,
      feed_id: input.shortname,
      study_id: null,
      requester_did: input.requesterDid,
      requested_at: timestamp,
      cursor_in: input.params.cursor ?? null,
      cursor_out: input.cursor ?? null,
      requested_limit: requestedLimit,
      result_count: 0,
      feedgen_build_sha: process.env.FEEDGEN_BUILD_SHA || null,
      algo_policy_id: input.shortname,
      ...requestTimeProvenance(input),
    },
    post: null,
  };
}

function requestTimeProvenance(input: RequestLogInput) {
  return {
    request_reference_time: input.requestReferenceTime,
    publisher_post_max_age_days: input.publisherPostMaxAgeDays,
    publisher_post_max_age_source: input.publisherPostMaxAgeSource,
    publisher_time_clock: input.publisherTimeClock,
    publisher_serving_window: input.publisherServingWindow,
    feed_catalog_revision: input.feedCatalogRevision,
    ranker_score_max_age_hours: input.rankerScoreMaxAgeHours,
    ranker_score_compatibility_fallback_active: input.rankerScoreCompatibilityFallbackActive,
    ranker_fresh_scored_publisher_slots: input.rankerFreshScoredPublisherSlots,
    ranker_publisher_slots: input.publisherCount,
    ranker_observed_score_backed_share: input.publisherCount > 0
      ? input.rankerFreshScoredPublisherSlots / input.publisherCount
      : null,
    ranker_min_score_backed_share: input.rankerMinScoreBackedShare,
  }
}

// Helper function to get follows
async function getFollows(actor: string, db: any): Promise<string[]> {
  // Import the function dynamically to avoid circular imports
  const { getFollows } = await import('../util/queries');
  return getFollows(actor, db);
}
