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
import { resolveDynamicHandler } from '../algos/catalog-dispatch'

// Sprint 15 / T2 Phase 2 — flipped 2026-05-06 after Phase 1 shadow
// soak with zero `catalog-dispatch` warning lines. Dynamic resolver
// now serves; static shims remain as fallback only. Phase 3 deletes
// them country-by-country once Phase 2 has soaked clean.
// Plan: dev/storage/plan_storage_refactor/T2_dynamic_dispatch_plan.md
const DYNAMIC_DISPATCH_WINS = true

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.getFeedSkeleton(async ({ params, req }) => {
    const feedUri = new AtUri(params.feed)
    if (
      // turned off publisherDid validation
      // feedUri.hostname !== ctx.cfg.publisherDid ||
      feedUri.collection !== 'app.bsky.feed.generator'
    ) {
      throw new InvalidRequestError(
        'Unsupported algorithm',
        'UnsupportedAlgorithm',
      )
    }

    // Resolve handler via static map AND the dynamic catalog path. In
    // Phase 1, static wins; we still call the dynamic resolver so its
    // cache stays warm, errors surface in logs, and `tsc --noEmit`
    // verifies the new code is in the build path.
    const staticAlgo = algos[feedUri.rkey]
    const dynamicAlgo = await resolveDynamicHandler(ctx.db, feedUri.rkey)
    const algo = DYNAMIC_DISPATCH_WINS
      ? dynamicAlgo ?? staticAlgo
      : staticAlgo ?? dynamicAlgo

    if (!algo) {
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
