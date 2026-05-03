import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { buildFeed, FeedGenerator } from './feed-builder'
import { Kysely } from 'kysely'
import { DatabaseSchema } from '../db/schema'
import { applyPriorityOrderForFeed } from './ranker-priority-helper'

// max 15 chars
export const shortname = 'newsflow-nl-2'

// Feed with priority ordering. Sprint 5 Lane C (2026-05-03) added
// per-feed canary support: when env
// `FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2=true` (or the
// master flag `FEEDGEN_PRIORITY_FROM_RANKER_PROD=true`), the handler
// joins `ranker_prod.feed_current_priority` and orders by that
// table's `priority` instead of legacy `post.priority`. Default
// behaviour is unchanged.
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
  const publisherDid = process.env.NEWSBOT_NL_DID || '';
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
  const publisherDid = process.env.NEWSBOT_NL_DID || '';
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
