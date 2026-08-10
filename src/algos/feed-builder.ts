import { Kysely } from 'kysely'
import { QueryParams, OutputSchema as AlgoOutput } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { DatabaseSchema, Post } from '../db/schema'
import { AppContext } from '../config'
import { SkeletonFeedPost } from '../lexicon/types/app/bsky/feed/defs'
import { dualWriteLinkFields } from '../util/link-fields'
import { recordRankerPriorityResult, rkeyToEnvSuffix } from './ranker-priority-helper'

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

// Shared resolvers — the config-activation manifest (src/util/config-manifest.ts)
// calls these SAME functions so it records exactly what serving uses, not a
// separate re-parse. Deliberately a raw parseInt (NaN on invalid input,
// e.g. `ENGAGEMENT_TIME_HOURS=abc`), matching the pre-existing behavior here
// byte-for-byte — do NOT normalize invalid input to 72 (that's a different,
// display-only resolver: methods/monitor.ts's getEngagementTimeHours(), used
// only for the /api/config `engagement.time_hours` presentational field).
export function resolveEngagementTimeHours(): number {
  return process.env.ENGAGEMENT_TIME_HOURS
    ? parseInt(process.env.ENGAGEMENT_TIME_HOURS, 10)
    : 72
}

/**
 * Per-feed serving-age window (hours) for a feed's PUBLISHER (ranked) posts.
 *
 * Motivation (BE K/M study feeds): the ranker ranks political clusters over a
 * ~10-day push window, but high-engagement political stories are already several
 * days old by the time they rank. The global 72h serving filter
 * (resolveEngagementTimeHours) drops them before they reach the feed, so only a
 * couple of political clusters survive and the ideology-diversity treatment has
 * almost nothing to act on. Setting
 *   FEEDGEN_SERVING_TIME_HOURS_<RKEY>=168   (e.g. FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K)
 * lets that ONE feed serve its ranked publisher posts over a longer window.
 *
 * Scope — this changes ONLY the serving-time post-age filter for that feed's
 * publisher query. It deliberately does NOT touch:
 *   - the follows (2/3 of the feed) window — still resolveEngagementTimeHours();
 *   - the engagement-RECOUNT window (util/engagement-updater.ts) — still the
 *     global value, so ranker engagement inputs are unchanged; and
 *   - any other feed — an unset/invalid override falls back to
 *     resolveEngagementTimeHours(), i.e. byte-for-byte the prior behavior.
 *
 * Raw parseInt mirrors resolveEngagementTimeHours's contract; a non-finite or
 * non-positive value falls back to the global default rather than serving junk.
 */
export function resolveServingTimeHours(shortname: string): number {
  const raw = process.env[`FEEDGEN_SERVING_TIME_HOURS_${rkeyToEnvSuffix(shortname)}`]
  if (raw !== undefined && raw !== '') {
    const parsed = parseInt(raw, 10)
    if (Number.isFinite(parsed) && parsed > 0) return parsed
  }
  return resolveEngagementTimeHours()
}

/**
 * All active per-feed serving-window overrides as { <RKEY_ENV_SUFFIX>: hours },
 * for the config-activation manifest (util/config-manifest.ts). Uses the same
 * parse rule as resolveServingTimeHours so the manifest records exactly what
 * serving uses (the manifest's load-bearing "no separate re-parse" rule).
 */
export function servingTimeHourOverrides(): Record<string, number> {
  const out: Record<string, number> = {}
  for (const key of Object.keys(process.env)) {
    const match = /^FEEDGEN_SERVING_TIME_HOURS_([A-Z0-9_]+)$/.exec(key)
    if (!match) continue
    const value = process.env[key]
    if (!value) continue
    const parsed = parseInt(value, 10)
    if (Number.isFinite(parsed) && parsed > 0) out[match[1]] = parsed
  }
  return out
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
  // Sprint 13 / T1: AppView probes with limit=1 and Math.floor(1/3)=0
  // makes the publisher query ask for 0 rows. Result: 200 + empty feed,
  // which AppView's online-probe heuristic can interpret as "feed broken".
  // Clamp to >=1 publisher and >=2 follows so probes always return content.
  // For limit>=3 this is a no-op (the floor split is unchanged).
  const publisherLimit = Math.max(1, Math.floor(params.limit / 3));
  const followsLimit = Math.max(2, Math.floor(params.limit * 2 / 3));
  const limit = publisherLimit; // 1/3 from news + 2/3 other (legacy alias for cursor math)
  const requesterFollows = await getFollows(requesterDid, ctx.db)
  
  // don't consider posts older than the serving window. The FOLLOWS window is
  // always the global engagement window. The PUBLISHER (ranked) window is the
  // SAME unless this feed sets FEEDGEN_SERVING_TIME_HOURS_<RKEY> (BE K/M study
  // feeds only) — when it doesn't, resolveServingTimeHours() returns the global
  // value and publisherTimeLimit reuses the identical string, so any feed
  // without an override is byte-for-byte unchanged (one Date.now, one timeLimit).
  const engagementTimeHours = resolveEngagementTimeHours();
  const publisherTimeHours = resolveServingTimeHours(shortname);
  const nowMs = Date.now();
  const followsTimeLimit = new Date(nowMs - engagementTimeHours * 60 * 60 * 1000).toISOString();
  const publisherTimeLimit =
    publisherTimeHours === engagementTimeHours
      ? followsTimeLimit
      : new Date(nowMs - publisherTimeHours * 60 * 60 * 1000).toISOString();

  // Parse cursor if provided
  let cursorOffset = 0;
  if (params.cursor) {
    cursorOffset = parseInt(params.cursor, 10);
  }

  // Build the queries using the provided builder functions
  const publisherPostsQuery = buildPublisherQuery(
    ctx.db,
    publisherTimeLimit,
    requesterFollows,
    cursorOffset,
    limit
  );

  const otherPostsQuery = buildFollowsQuery(
    ctx.db,
    followsTimeLimit,
    requesterFollows,
    cursorOffset,
    followsLimit
  );

  // Execute both queries in parallel
  const [publisherPosts, otherPosts] = await Promise.all([
    publisherPostsQuery.execute(),
    otherPostsQuery.execute()
  ]);

  // Ranker queries carry one request-level health marker on every result row.
  // Chronological/engagement rows do not, so this is a no-op for those feeds.
  recordRankerPriorityResult(shortname, [...publisherPosts, ...otherPosts])

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
    },
    post: null,
  };
}

// Helper function to get follows
async function getFollows(actor: string, db: any): Promise<string[]> {
  // Import the function dynamically to avoid circular imports
  const { getFollows } = await import('../util/queries');
  return getFollows(actor, db);
}
