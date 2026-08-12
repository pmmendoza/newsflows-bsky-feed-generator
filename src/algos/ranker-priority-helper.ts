/**
 * Sprint 5 Lane C — feedgen serving rewrite for ranker-driven feeds.
 *
 * Variant-2 feed handlers now read only
 * `ranker_prod.feed_current_priority(feed_id, post_uri).score`. The legacy
 * `public.post.priority` ordering path has been retired from active code so the
 * integer column can be dropped after production soak and zero-reader proof.
 *
 * Design notes
 * ------------
 *   - The publisher and follows queries LEFT JOIN
 *     `ranker_prod.feed_current_priority` filtered to that feed_id and a
 *     freshness window, then ORDER BY `fcp.score DESC`.
 *   - Rollback before DDL is a code/image rollback. The env-flag rollback path
 *     has intentionally been removed from active serving code.
 */

import { Kysely, SqlBool, sql } from 'kysely'
import { DatabaseSchema, PublisherTimeClock } from '../db/schema'
import { getScoreSource } from '../util/score-source-cache'
import { applyPublisherRecencyOrder } from './publisher-time'

/**
 * Normalise a feed rkey to the env-name suffix.
 * `newsflow-nl-2` → `NEWSFLOW_NL_2`
 */
export function rkeyToEnvSuffix(rkey: string): string {
  return rkey.replace(/[^a-zA-Z0-9]+/g, '_').toUpperCase()
}

/**
 * Migration 024: unconditional. The `ranker_prod.feed_current_priority`
 * table's `score` column is the only ranking source for variant-2 feeds.
 * The per-feed + master env flags (`FEEDGEN_PRIORITY_FROM_RANKER_PROD[_*]`)
 * no longer have an effect; every variant-2 query goes through
 * `applyRankerPriorityOrder`.
 *
 * Returns `true` always. Kept as a function (not inlined) so the
 * existing call sites in the policy modules remain valid imports.
 */
export function useRankerPriority(_rkey: string): boolean {
  return true
}

/**
 * Type guard for the Kysely query-builder shape we receive in the
 * variant-2 feed query callbacks. We deliberately keep this loose
 * (`any`) because the chain is stage-dependent (the feed handlers'
 * query-builder helpers return `SelectQueryBuilder<...>` whose exact
 * generic type is not exported uniformly across Kysely versions in
 * this repo).
 */
type AnySelect = any

export type RankerPriorityServingStatus = {
  feed_rkey: string
  profile_id: string
  serving_ordering: 'ranked' | 'unranked_recency'
  observed_at: string
  fresh_scored_publisher_slots: number
  publisher_slots: number
  score_backed_share: number | null
  minimum_score_backed_share: number | null
  floor_met: boolean | null
}

const servingStatusByFeed = new Map<string, RankerPriorityServingStatus>()

export function recordRankerPriorityResult(feedRkey: string, rows: any[], minimumShare: number | null = null): void {
  const row = rows[0]
  if (
    !row ||
    typeof row.__ranker_profile_id !== 'string'
  ) return

  const serving_ordering = rows.some(
    (candidate) => candidate.__ranker_score !== null &&
      candidate.__ranker_score !== undefined,
  )
    ? 'ranked'
    : 'unranked_recency'
  const status: RankerPriorityServingStatus = {
    feed_rkey: feedRkey,
    profile_id: row.__ranker_profile_id,
    serving_ordering,
    observed_at: new Date().toISOString(),
    fresh_scored_publisher_slots: rows.filter((candidate) => candidate.__ranker_score != null).length,
    publisher_slots: rows.length,
    score_backed_share: rows.length > 0 ? rows.filter((candidate) => candidate.__ranker_score != null).length / rows.length : null,
    minimum_score_backed_share: minimumShare,
    floor_met: rows.length > 0 && minimumShare != null
      ? rows.filter((candidate) => candidate.__ranker_score != null).length / rows.length >= minimumShare
      : null,
  }
  servingStatusByFeed.set(status.feed_rkey, status)

  if (serving_ordering === 'unranked_recency') {
    console.warn(JSON.stringify({
      event: 'ranker_priority_unranked_recency',
      feed_rkey: status.feed_rkey,
      profile_id: status.profile_id,
      served_ordering: serving_ordering,
      reason: 'zero_fresh_scores',
    }))
  }
}

export function getRankerPriorityServingStatus(): RankerPriorityServingStatus[] {
  return [...servingStatusByFeed.values()].sort((a, b) =>
    a.feed_rkey.localeCompare(b.feed_rkey),
  )
}

/**
 * Apply the ranker-prod score ORDER BY to a base query that
 * already has `selectFrom('post')` and the time-window predicates
 * applied. Returns the augmented query with a LEFT JOIN to
 * `ranker_prod.feed_current_priority` (filtered to the feed_id) and
 * score-first ordering.
 *
 * The caller adds `.offset()` and `.limit()` after this returns.
 *
 * Type note: we deliberately accept and return `any` because the
 * Kysely chain at this stage carries a generic type that varies
 * across versions / partial selections; coupling the helper to a
 * specific generic shape would force every caller to import the
 * exact same type, which is brittle. The runtime contract is
 * stable: input is a query post-`selectFrom('post')` with filters,
 * output is the same with extra JOIN + ORDER BY.
 */
/**
 * Recency window for score freshness. A post whose ranker score was last
 * updated more than `RANKER_PROD_FRESHNESS_HOURS` hours ago is treated as
 * having no score (LEFT JOIN miss -> -1 sentinel -> bottom of feed).
 *
 * Default: 24 h (one ranker push window). Override per environment via
 * `FEEDGEN_RANKER_PROD_FRESHNESS_HOURS` env var.
 */
export function freshnessHours(): number {
  const raw = process.env.FEEDGEN_RANKER_PROD_FRESHNESS_HOURS
  const parsed = raw ? Number(raw) : 24
  if (!Number.isFinite(parsed) || parsed <= 0) return 24
  return parsed
}

export function applyRankerPriorityOrder(
  query: AnySelect,
  rkey: string,
  publisherTimeClock: PublisherTimeClock = 'receipt_time',
  referenceTimeIso = new Date().toISOString(),
  rankerScoreMaxAgeHours?: number,
): AnySelect {
  const maxAgeHours = rankerScoreMaxAgeHours ?? freshnessHours()
  const cutoffIso = new Date(
    Date.parse(referenceTimeIso) - maxAgeHours * 60 * 60 * 1000,
  ).toISOString()
  // D1.4 read-path cutover: serve scores by the feed's configured profile.
  // ranker_score_source is NULL for every feed today, so profileId === rkey
  // and the join is identical to the pre-cutover `fcp.feed_id = rkey`.
  const profileId = getScoreSource(rkey) ?? rkey
  const ranked = (query as any)
    .leftJoin(
      'ranker_prod.feed_current_priority as fcp',
      (join: any) =>
        join
          .onRef('fcp.post_uri', '=', 'post.uri')
          .on('fcp.profile_id', '=', profileId)
          .on('fcp.updated_at', '>=', cutoffIso),
    )
    // D-3: keep serving recency when joined scores are all null, but carry
    // request-level metadata in the existing result rows so the fallback is
    // logged and exposed through /api/config rather than remaining silent.
    .select([
      sql<number | null>`fcp.score`.as('__ranker_score'),
      sql<string>`${profileId}`.as('__ranker_profile_id'),
    ])
    // Migration 024: `score` is the sole canonical numeric ranking column.
    // Do not fall back to `fcp.priority`; keeping such a fallback would keep
    // the integer priority column alive.
    .orderBy(
      (eb: any) =>
        eb.fn('coalesce', [
          eb.ref('fcp.score'),
          eb.val(-1.0),
        ]),
      'desc',
    )
  return applyPublisherRecencyOrder(ranked, publisherTimeClock)
}

/**
 * Convenience wrapper used by every variant-2 feed handler:
 *   - takes the partially-built post query (selectAll + filters)
 *   - applies score-backed ranker-prod ordering
 *   - returns the query ready for offset/limit
 */
export function applyPriorityOrderForFeed(
  query: AnySelect,
  rkey: string,
  publisherTimeClock: PublisherTimeClock = 'receipt_time',
  referenceTimeIso = new Date().toISOString(),
  rankerScoreMaxAgeHours?: number,
): AnySelect {
  return applyRankerPriorityOrder(query, rkey, publisherTimeClock, referenceTimeIso, rankerScoreMaxAgeHours)
}

// Re-export for the type system
export type { Kysely, SqlBool, DatabaseSchema }
