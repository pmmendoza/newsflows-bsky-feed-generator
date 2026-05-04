/**
 * Sprint 5 Lane C — feedgen serving rewrite for ranker-driven feeds.
 *
 * When the ranker's Lane B dual-write is active and
 * `ranker_prod.feed_current_priority` is being maintained, variant-2
 * feed handlers (the priority-driven feeds) can switch from reading
 * `post.priority` (the legacy mutable cell shared across rankers) to
 * joining `ranker_prod.feed_current_priority(feed_id, post_uri)`,
 * which is keyed by feed and survives multiple coexisting rankers.
 *
 * The cut-over is **per-feed canary** so a regression on one country
 * does not break the others. Each feed handler reads its own env
 * flag; when unset, the handler keeps the legacy ordering.
 *
 * Env flag conventions
 * --------------------
 * Per-feed flag (preferred):
 *   `FEEDGEN_PRIORITY_FROM_RANKER_PROD_<NORMALISED_RKEY>=true`
 * where `<NORMALISED_RKEY>` is the feed's rkey with all
 * non-alphanumerics replaced by `_` and uppercased. Examples:
 *   rkey `newsflow-nl-2` → env `FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_NL_2`
 *   rkey `newsflow-fr-2` → env `FEEDGEN_PRIORITY_FROM_RANKER_PROD_NEWSFLOW_FR_2`
 *
 * Master flag (fallback, applies to every variant-2 feed):
 *   `FEEDGEN_PRIORITY_FROM_RANKER_PROD=true`
 *
 * The per-feed flag wins when both are set.
 *
 * Design notes
 * ------------
 *   - Default: every flag is unset, behaviour matches today exactly.
 *   - When activated for a feed, the publisher and follows queries
 *     LEFT JOIN `ranker_prod.feed_current_priority` filtered to that
 *     feed_id, then ORDER BY `fcp.priority DESC NULLS LAST` (matches
 *     today's `coalesce(priority, 0) DESC` because LEFT JOIN +
 *     NULLS LAST sends "no row found" to the bottom).
 *   - Rollback: set the per-feed flag to false (or unset) and recreate
 *     the container. No DDL involved.
 */

import { Kysely, SqlBool, sql } from 'kysely'
import { DatabaseSchema } from '../db/schema'

/**
 * Normalise a feed rkey to the env-name suffix.
 * `newsflow-nl-2` → `NEWSFLOW_NL_2`
 */
export function rkeyToEnvSuffix(rkey: string): string {
  return rkey.replace(/[^a-zA-Z0-9]+/g, '_').toUpperCase()
}

/**
 * Decide whether the given feed should source priority from the new
 * `ranker_prod.feed_current_priority` table instead of legacy
 * `post.priority`.
 */
export function useRankerPriority(rkey: string): boolean {
  const perFeed = process.env[`FEEDGEN_PRIORITY_FROM_RANKER_PROD_${rkeyToEnvSuffix(rkey)}`]
  if (typeof perFeed === 'string' && perFeed.length > 0) {
    return perFeed.toLowerCase() === 'true'
  }
  return String(process.env.FEEDGEN_PRIORITY_FROM_RANKER_PROD ?? '').toLowerCase() === 'true'
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

/**
 * Apply the ranker-prod priority ORDER BY to a base query that
 * already has `selectFrom('post')` and the time-window predicates
 * applied. Returns the augmented query with a LEFT JOIN to
 * `ranker_prod.feed_current_priority` (filtered to the feed_id) and
 * priority-first ordering.
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
 * Recency window for priority freshness. A post that was last given a
 * priority by the ranker more than `RANKER_PROD_FRESHNESS_HOURS`
 * hours ago is treated as having no priority (LEFT JOIN miss → -1
 * sentinel → bottom of feed). This eliminates the need for explicit
 * priority=0 demote rows in the ranker output: any post not refreshed
 * in the active window naturally falls out.
 *
 * Default: 24 h (one ranker push window). Override per environment via
 * `FEEDGEN_RANKER_PROD_FRESHNESS_HOURS` env var.
 */
function freshnessHours(): number {
  const raw = process.env.FEEDGEN_RANKER_PROD_FRESHNESS_HOURS
  const parsed = raw ? Number(raw) : 24
  if (!Number.isFinite(parsed) || parsed <= 0) return 24
  return parsed
}

export function applyRankerPriorityOrder(
  query: AnySelect,
  rkey: string,
): AnySelect {
  const cutoffIso = new Date(
    Date.now() - freshnessHours() * 60 * 60 * 1000,
  ).toISOString()
  return (query as any)
    .leftJoin(
      'ranker_prod.feed_current_priority as fcp',
      (join: any) =>
        join
          .onRef('fcp.post_uri', '=', 'post.uri')
          .on('fcp.feed_id', '=', rkey)
          .on('fcp.updated_at', '>=', cutoffIso),
    )
    // Score-precedence (Sprint 5 follow-on, plan_priority_to_score_migration.md
    // Stage 1): order by `score` first when present, fall back to integer
    // `priority` when `score IS NULL`. Today's integer-native ranker leaves
    // `score` NULL → behaviour identical to legacy ordering. After Stage 2
    // backfill or Stage 3 ranker change, `score` carries values and the
    // float-native rankers (e.g. Belgian) get meaningful sub-integer
    // tiebreakers.
    //
    // Kysely's text orderBy doesn't expose `nulls last` directly across
    // versions; CASE coalesce puts nulls below 0 (equivalent semantically:
    // missing rows sort as if score were a sentinel below all real values).
    .orderBy(
      (eb: any) =>
        eb.fn('coalesce', [
          eb.ref('fcp.score'),
          eb.fn('cast', [eb.ref('fcp.priority'), sql`double precision`]),
          eb.val(-1.0),
        ]),
      'desc',
    )
    .orderBy('post.indexedAt', 'desc')
    .orderBy('post.cid', 'desc')
}

/**
 * Apply the legacy `post.priority` ORDER BY (today's behaviour).
 * Returns the augmented query.
 */
export function applyLegacyPriorityOrder(query: AnySelect): AnySelect {
  return (query as any)
    .orderBy(
      (eb: any) =>
        eb.fn('coalesce', [eb.ref('priority'), eb.val(0)]),
      'desc',
    )
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
}

/**
 * Convenience wrapper used by every variant-2 feed handler:
 *   - takes the partially-built post query (selectAll + filters)
 *   - applies the right ordering based on the per-feed flag
 *   - returns the query ready for offset/limit
 */
export function applyPriorityOrderForFeed(
  query: AnySelect,
  rkey: string,
): AnySelect {
  return useRankerPriority(rkey)
    ? applyRankerPriorityOrder(query, rkey)
    : applyLegacyPriorityOrder(query)
}

// Re-export for the type system
export type { Kysely, SqlBool, DatabaseSchema }
