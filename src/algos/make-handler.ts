/**
 * Sprint 11 / Task 5 — handler factory.
 *
 * Reduces every per-feed handler to a 3-line shim. Each
 * `feed-{cc}-{n}.ts` becomes:
 *
 *   export const shortname = 'newsflow-...'
 *   export const handler = makeHandler({ shortname, policy, publisherEnv })
 *
 * The factory composes a `FeedGenerator` from a chosen policy and a
 * publisher-DID env-var name. Shortname stays a per-file constant so
 * `algos/index.ts` (the static registry) and the catalog parity check
 * keep working unchanged.
 *
 * Future iteration (Sprint 12): once the catalog admin endpoint is
 * deployed, swap `publisherEnv` for `publisherDid` read from
 * `feed_catalog.publisher_did` so adding a feed becomes catalog-only.
 */

import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { buildFeed, FeedGenerator, QueryBuilder } from './feed-builder'
import {
  publisherQueryChronological,
  followsQueryChronological,
} from './policies/chronological'
import {
  publisherQueryRankerPriority,
  followsQueryRankerPriority,
} from './policies/ranker-priority'
import {
  publisherQueryEngagement,
  followsQueryEngagement,
} from './policies/engagement-sorted'

export type Policy = 'chronological' | 'ranker-priority' | 'engagement-sorted'

export type MakeHandlerOptions = {
  shortname: string
  policy: Policy
  publisherEnv: string
}

export function makeHandler(opts: MakeHandlerOptions): FeedGenerator {
  const { shortname, policy, publisherEnv } = opts
  return async (ctx: AppContext, params: QueryParams, requesterDid: string) => {
    const publisherDid = process.env[publisherEnv] || ''
    const { buildPublisher, buildFollows } = pickPolicy(policy, publisherDid, shortname)
    return buildFeed({
      shortname,
      ctx,
      params,
      requesterDid,
      buildPublisherQuery: buildPublisher,
      buildFollowsQuery: buildFollows,
    })
  }
}

// Sprint 14 / T2 Phase 1: exported so `algos/catalog-dispatch.ts`
// (the new dynamic-dispatch path) can reuse the same policy mapping.
// Static `makeHandler()` above also calls this; the two callers must
// stay aligned so static and dynamic paths produce identical handlers.
export function pickPolicy(
  policy: Policy,
  publisherDid: string,
  shortname: string,
): { buildPublisher: QueryBuilder; buildFollows: QueryBuilder } {
  switch (policy) {
    case 'chronological':
      return {
        buildPublisher: (db, t, f, c, l) =>
          publisherQueryChronological(db, t, f, c, l, publisherDid, shortname),
        buildFollows: (db, t, f, c, l) =>
          followsQueryChronological(db, t, f, c, l, publisherDid, shortname),
      }
    case 'ranker-priority':
      return {
        buildPublisher: (db, t, f, c, l) =>
          publisherQueryRankerPriority(db, t, f, c, l, publisherDid, shortname),
        buildFollows: (db, t, f, c, l) =>
          followsQueryRankerPriority(db, t, f, c, l, publisherDid, shortname),
      }
    case 'engagement-sorted':
      return {
        buildPublisher: (db, t, f, c, l) =>
          publisherQueryEngagement(db, t, f, c, l, publisherDid, shortname),
        buildFollows: (db, t, f, c, l) =>
          followsQueryEngagement(db, t, f, c, l, publisherDid, shortname),
      }
  }
}
