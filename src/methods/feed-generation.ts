// Sprint 6 Lane D / TASK-039.03 — per-feed access-policy dispatcher.
//
// Replaces the previous single-mode subscriber-only check with a
// catalog-driven dispatcher that reads
// `feedgen_ops.feed_catalog.access_policy_id` and routes to the
// appropriate access predicate. The 3-value enum
// (subscriber-default / study-only / disabled) was operator-confirmed
// 2026-05-04 and CHECK-constrained on the catalog by migration 013.
//
// Sprint 10 Lane I retirement: the legacy
// `FEEDGEN_SUBSCRIBER_ONLY=false` env-only escape hatch is gone with
// this change. Auth is always required; access decisions always go
// through the catalog policy. Production is already on
// FEEDGEN_SUBSCRIBER_ONLY=true so the behaviour is unchanged at
// activation; the env can now be removed from operator config.
//
// Plan: dev/storage/sprint6_lane_d_access_policy_design_2026-05-04.md
import { InvalidRequestError } from '@atproto/xrpc-server'
import { Server } from '../lexicon'
import { AppContext } from '../config'
import algos from '../algos'
import { extractDidFromAuth } from '../auth'
import { AtUri } from '@atproto/syntax'
import { evaluateAccessPolicy } from '../util/access-policy'

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.getFeedSkeleton(async ({ params, req }) => {
    const feedUri = new AtUri(params.feed)
    const algo = algos[feedUri.rkey]
    if (
      // turned off publisherDid validation
      // feedUri.hostname !== ctx.cfg.publisherDid ||
      feedUri.collection !== 'app.bsky.feed.generator' ||
      !algo
    ) {
      throw new InvalidRequestError(
        'Unsupported algorithm',
        'UnsupportedAlgorithm',
      )
    }

    // Always require authentication; anonymous requests get empty feed.
    let requesterDid: string
    try {
      requesterDid = await extractDidFromAuth(req)
    } catch (e) {
      console.log(
        `[${new Date().toISOString()}] - request denied (no auth) rkey=${feedUri.rkey}`,
      )
      return {
        encoding: 'application/json',
        body: { feed: [] },
      }
    }

    // Catalog-driven access policy dispatch.
    const verdict = await evaluateAccessPolicy(
      ctx.db,
      feedUri.rkey,
      requesterDid,
    )

    if (!verdict.allowed) {
      console.log(
        `[${new Date().toISOString()}] - request denied (${requesterDid}) rkey=${feedUri.rkey} reason=${verdict.reason}`,
      )
      return {
        encoding: 'application/json',
        body: { feed: [] },
      }
    }

    const body = await algo(ctx, params, requesterDid)
    return {
      encoding: 'application/json',
      body: body,
    }
  })
}
