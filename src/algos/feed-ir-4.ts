import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { buildFeed, FeedGenerator } from './feed-builder'
import { Kysely } from 'kysely'
import { DatabaseSchema } from '../db/schema'
import { applyPriorityOrderForFeed } from './ranker-priority-helper'

// max 15 chars
export const shortname = 'newsflow-ir-4'

// Sprint 5 fourth-feed for Ireland (operator decision 2026-05-03).
//
// Feed handler for `newsflow-ir-4`. Sources priority from
// `ranker_prod.feed_current_priority` written by the ranker's
// secondary-feed dual-write under ranker_id `actor-diversity`.
//
// Per-feed canary control: env
// `FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_IR_4=true` (or master
// flag `FEEDGEN_PRIORITY_FROM_RANKER_PROD=true`).
//
// When the per-feed canary is OFF, the legacy ordering falls back to
// `coalesce(post.priority, 0) DESC`. **Note**: `post.priority` is
// owned by the primary `news-cluster-engagement` ranker; the
// actor-diversity ranker never writes to that column. So the OFF
// path serves the *primary* ranker's ordering, NOT the
// actor-diversity ordering. Activation of this feed therefore
// requires the per-feed canary flag to be ON; turning it OFF
// effectively reverts to variant-2-style priority.
//
// Plan: dev/storage/sprint5_ir4_actor_diversity_plan_2026-05-03.md
export const handler: FeedGenerator = async (ctx: AppContext, params: QueryParams, requesterDid: string) => {
  return buildFeed({
    shortname,
    ctx,
    params,
    requesterDid,
    buildPublisherQuery: buildPublisherPostsQuery,
    buildFollowsQuery: buildFollowsPostsQuery
  });
};

function buildPublisherPostsQuery(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  requesterFollows: string[],
  cursorOffset: number,
  limit: number
) {
  const publisherDid = process.env.NEWSBOT_IR_DID || '';
  const base = db
    .selectFrom('post')
    .selectAll('post')
    .where('author', '=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit);
  return applyPriorityOrderForFeed(base, shortname)
    .offset(cursorOffset)
    .limit(limit);
}

function buildFollowsPostsQuery(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  requesterFollows: string[],
  cursorOffset: number,
  limit: number
) {
  const publisherDid = process.env.NEWSBOT_IR_DID || '';
  const base = db
    .selectFrom('post')
    .selectAll('post')
    .where('author', '!=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .where((eb) => eb('author', 'in', requesterFollows));
  return applyPriorityOrderForFeed(base, shortname)
    .offset(cursorOffset)
    .limit(limit);
}
