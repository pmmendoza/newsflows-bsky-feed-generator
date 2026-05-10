/**
 * Sprint 14 / T2 Phase 1 — dynamic feed-handler dispatch.
 *
 * Resolves `rkey → FeedGenerator` at request time by reading
 * `feedgen_ops.feed_catalog` rather than the static `algos[rkey]`
 * map. Mirrors the access-policy cache pattern in
 * `src/util/access-policy.ts`:
 *   - per-process LRU cache, 5-min TTL
 *   - LISTEN/NOTIFY invalidation via `invalidateDispatchCache(rkey)`
 *   - read failures are NOT cached (so the next request retries)
 *
 * Phase 1 contract: `feed-generation.ts` keeps the static `algos[]`
 * lookup as the primary path. The dynamic resolver runs alongside
 * it, gated by `DYNAMIC_DISPATCH_WINS` (false in Phase 1). Behaviour
 * is identical to today; the dynamic path warms its cache and
 * exercises the SQL/policy mapping silently. Phase 2 (Sprint 15)
 * flips the flag.
 *
 * Why two caches (this + `policyCache` in access-policy.ts) rather
 * than merging: the `FeedCatalogPolicyRow` type used by the access
 * dispatcher would need to grow `publisher_did + algo_policy_id`,
 * touching every caller. Two small caches with one NOTIFY trigger
 * invalidating both is lower blast radius. Both share the same
 * 5-min TTL constant.
 *
 * Plan: dev/storage/plan_storage_refactor/T2_dynamic_dispatch_plan.md
 * (Section 5.1).
 */

import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'
import { Database } from '../db'
import { buildFeed, FeedGenerator } from './feed-builder'
import { Policy, pickPolicy } from './make-handler'

const CACHE_TTL_MS = 5 * 60 * 1000

// Sprint 15 — catalog enum + TS Policy type now share the same set of
// values. Migration 021 renamed `feed_catalog.algo_policy_id` from
// 'ranker-driven' to 'ranker-priority' so the two sides match end-to-
// end and the previous CATALOG_TO_POLICY bridge map is gone.
export const KNOWN_POLICIES: ReadonlySet<Policy> = new Set<Policy>([
  'chronological',
  'ranker-priority',
  'engagement-sorted',
])

type DispatchCacheEntry = {
  /** The compiled handler, or null when the catalog row is unknown / disabled / unsupported. */
  handler: FeedGenerator | null
  expires_at_ms: number
}

const dispatchCache = new Map<string, DispatchCacheEntry>()

/**
 * Drop a cached dispatch entry for a feed. Called from
 * `catalog-listener.ts` whenever a NOTIFY arrives, alongside
 * `invalidatePolicyCache(rkey)`.
 */
export function invalidateDispatchCache(rkey?: string): void {
  if (rkey === undefined) {
    dispatchCache.clear()
  } else {
    dispatchCache.delete(rkey)
  }
}

/**
 * Resolve a feed's handler from `feed_catalog`. Returns `null` when:
 *   - no row exists for this rkey (unknown feed)
 *   - the row exists but `enabled=false` (retired feed)
 *   - the row's `algo_policy_id` is not one of the three known policies
 *     (the variant-4 hybrid stub lives in `_drafts/`; if a catalog row
 *      with `algo_policy_id='hybrid'` ever appears we fail closed)
 *
 * Read failures (transport / Postgres unreachable / GRANT gap) do NOT
 * cache; the next request retries. This mirrors `readPolicyRow` in
 * access-policy.ts so the two caches behave identically under outages.
 */
export async function resolveDynamicHandler(
  db: Database,
  rkey: string,
): Promise<FeedGenerator | null> {
  const cached = dispatchCache.get(rkey)
  if (cached && cached.expires_at_ms > Date.now()) {
    return cached.handler
  }

  let handler: FeedGenerator | null = null
  try {
    const row = await db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['publisher_did', 'algo_policy_id', 'enabled'])
      .where('rkey', '=', rkey)
      .executeTakeFirst()

    if (!row) {
      // Unknown rkey — cache null so we don't hammer Postgres on
      // every request for a 404'd feed.
    } else if (!row.enabled) {
      // Retired / disabled — `evaluateAccessPolicy` already short-
      // circuits these requests with a denial, but we cache null
      // here so dispatch stays consistent with access-policy.
    } else if (!KNOWN_POLICIES.has(String(row.algo_policy_id) as Policy)) {
      console.warn(
        `[${new Date().toISOString()}] - catalog-dispatch: unknown algo_policy_id='${row.algo_policy_id}' for rkey=${rkey}; returning null (would 400)`,
      )
    } else {
      const policy = String(row.algo_policy_id) as Policy
      const publisherDid = String(row.publisher_did ?? '')
      if (!publisherDid) {
        // Empty `publisher_did` would silently produce zero-publisher
        // posts (the IR-4-class bug). Surface this as a warning; the
        // handler still builds (legacy callers used env-var lookups
        // that could also be empty), but we want a log line.
        console.warn(
          `[${new Date().toISOString()}] - catalog-dispatch: publisher_did empty for rkey=${rkey}; will return zero publisher posts`,
        )
      }
      const { buildPublisher, buildFollows } = pickPolicy(policy, publisherDid, rkey)
      handler = async (
        ctx: AppContext,
        params: QueryParams,
        requesterDid: string,
      ) => {
        return buildFeed({
          shortname: rkey,
          ctx,
          params,
          requesterDid,
          buildPublisherQuery: buildPublisher,
          buildFollowsQuery: buildFollows,
        })
      }
    }
  } catch (err) {
    // DB read failed — DON'T cache; retry on next request.
    console.warn(
      `[${new Date().toISOString()}] - catalog-dispatch: feed_catalog read failed for rkey=${rkey}; error=${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    return null
  }

  dispatchCache.set(rkey, { handler, expires_at_ms: Date.now() + CACHE_TTL_MS })
  return handler
}
