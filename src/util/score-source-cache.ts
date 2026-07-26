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
 * The cache fails closed until it has loaded successfully. A catalog row with
 * `ranker_score_source = NULL` still explicitly means "serve self"; a missing
 * row or failed refresh must not silently turn into that legitimate default.
 */
import { Database } from '../db'

let scoreSourceByFeed: Map<string, string | null> | null = null
let cacheHealthy = false
let refreshInFlight: Promise<void> | null = null

export const scoreSourceRefreshMs = (): number => {
  const raw = process.env.FEEDGEN_SCORE_SOURCE_REFRESH_MS
  if (!raw) return 60_000
  const n = parseInt(raw, 10)
  return Number.isFinite(n) && n > 0 ? n : 60_000
}

/**
 * Synchronous per-request lookup. A stored `null` means "serve self"; absence
 * means the cache cannot prove that no named source was declared, so it fails
 * closed instead of silently using the rkey.
 */
export function getScoreSource(feedKey: string): string | null {
  if (!scoreSourceByFeed || !cacheHealthy) {
    throw new Error('score_source_cache_unready')
  }
  if (!scoreSourceByFeed.has(feedKey)) {
    throw new Error(`score_source_binding_missing feed=${feedKey}`)
  }
  return scoreSourceByFeed.get(feedKey) ?? null
}

/**
 * Rebuild the map from `feedgen_ops.feed_catalog`. Keyed by both rkey and
 * feed_id so the lookup resolves whichever identifier the feed handlers pass.
 * On error the previous map is retained but marked unhealthy, preventing it
 * from silently serving a stale null binding as the rkey.
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
      cacheHealthy = true
    } catch (error) {
      cacheHealthy = false
      console.error(
        `[${new Date().toISOString()}] - score-source-cache refresh failed; keeping previous map. error=${
          error instanceof Error ? error.message : String(error)
        }`,
      )
      throw error
    }
  })()

  try {
    await refreshInFlight
  } finally {
    refreshInFlight = null
  }
}
