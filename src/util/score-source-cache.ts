/**
 * D1.4 — read-path cutover for the ranker score-storage decoupling
 * (TARGET_STATE DEC-MOD-051 / ontology T-D).
 *
 * Maps each feed identifier (rkey and feed_id) to its
 * `feedgen_ops.feed_catalog.ranker_score_source` — the profile whose scores
 * that feed currently serves. NULL ⇒ the feed serves its own rkey.
 *
 * The per-request lookup (`getScoreSource`) is SYNCHRONOUS because the
 * variant-2 query builders (`src/algos/policies/ranker-priority.ts` →
 * `applyRankerPriorityOrder`) are synchronous: they return a Kysely query
 * that `buildFeed` executes later, and `db` is not in scope there. A
 * background refresh (started at boot, like the scheduled updaters) keeps the
 * in-memory map warm, so the hot path is a plain `Map.get`.
 *
 * Fail-safe: before the first successful load, and on any refresh error, the
 * map yields `null`, so callers fall back to the rkey — byte-for-byte the
 * pre-cutover behavior. Since every catalog row currently has
 * `ranker_score_source = NULL`, the served ordering is unchanged until an
 * operator sets a feed's source.
 */
import { Database } from '../db'

let scoreSourceByFeed: Map<string, string | null> = new Map()
let refreshInFlight: Promise<void> | null = null

export const scoreSourceRefreshMs = (): number => {
  const raw = process.env.FEEDGEN_SCORE_SOURCE_REFRESH_MS
  if (!raw) return 60_000
  const n = parseInt(raw, 10)
  return Number.isFinite(n) && n > 0 ? n : 60_000
}

/**
 * Synchronous per-request lookup. Returns the configured score source for a
 * feed (by rkey or feed_id), or `null` to mean "serve self" (fall back to the
 * rkey). Never throws.
 */
export function getScoreSource(feedKey: string): string | null {
  return scoreSourceByFeed.get(feedKey) ?? null
}

/**
 * Rebuild the map from `feedgen_ops.feed_catalog`. Keyed by both rkey and
 * feed_id so the lookup resolves whichever identifier the feed handlers pass.
 * On error the previous map is retained (fail-safe: stale ⇒ null ⇒ rkey).
 */
export async function refreshScoreSourceCache(db: Database): Promise<void> {
  if (refreshInFlight) return refreshInFlight

  refreshInFlight = (async () => {
    try {
      const rows = await db
        .selectFrom('feedgen_ops.feed_catalog')
        .select(['rkey', 'feed_id', 'ranker_score_source'])
        .execute()

      const next = new Map<string, string | null>()
      for (const row of rows) {
        const source = row.ranker_score_source ?? null
        if (row.rkey) next.set(row.rkey, source)
        if (row.feed_id) next.set(row.feed_id, source)
      }
      scoreSourceByFeed = next
    } catch (error) {
      console.error(
        `[${new Date().toISOString()}] - score-source-cache refresh failed; keeping previous map. error=${
          error instanceof Error ? error.message : String(error)
        }`,
      )
    }
  })()

  try {
    await refreshInFlight
  } finally {
    refreshInFlight = null
  }
}
