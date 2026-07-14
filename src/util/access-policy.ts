/**
 * Sprint 6 Lane D / TASK-039.03 — per-feed access policy dispatcher.
 *
 * Resolves a feed's `access_policy_id` from `feedgen_ops.feed_catalog`
 * and decides whether a given DID may pull the feed. Each feed handler
 * passes its own `rkey` and the requester DID; the dispatcher returns
 * `{ allowed, reason }` and the handler emits an empty feed when
 * `allowed=false`.
 *
 * Operator-confirmed enum (2026-05-04):
 *   - subscriber-default: subscriber has omni scope or an active exact assignment
 *   - study-only: exact-feed access plus active lifecycle membership
 *   - disabled: always empty
 *
 * Catalog reads use a 5-minute LRU cache (per process) to avoid one
 * query per request. Operator catalog updates (via `bsr study` CLI
 * etc.) take effect within the cache TTL — explicit invalidation is
 * possible by restarting the container.
 *
 * Failure mode: an unknown `access_policy_id` (would only happen if a
 * future DDL relaxed the CHECK constraint) is treated fail-closed
 * (`allowed=false`) with a warning log.
 *
 * Plan: dev/storage/sprint6_lane_d_access_policy_design_2026-05-04.md
 * Migration: dev/storage/migrations/013_access_policy_check_constraint.sql
 */

import { Database } from '../db'

export type AccessPolicyId = 'subscriber-default' | 'study-only' | 'disabled'

export type FeedCatalogPolicyRow = {
  feed_id: string
  access_policy_id: string
  study_id: string | null
  enabled: boolean
  retired_at: string | Date | null
}

export type AccessVerdict = {
  allowed: boolean
  reason: string
}

const CACHE_TTL_MS = 5 * 60 * 1000

type CacheEntry = {
  row: FeedCatalogPolicyRow | null
  expires_at_ms: number
}

const policyCache = new Map<string, CacheEntry>()

/**
 * Drop a cached policy entry for a feed. Use this from any operator
 * tool that mutates `feedgen_ops.feed_catalog` so the next request
 * re-reads.
 */
export function invalidatePolicyCache(rkey?: string): void {
  if (rkey === undefined) {
    policyCache.clear()
  } else {
    policyCache.delete(rkey)
  }
}

async function readPolicyRow(
  db: Database,
  rkey: string,
): Promise<FeedCatalogPolicyRow | null> {
  const cached = policyCache.get(rkey)
  if (cached && cached.expires_at_ms > Date.now()) {
    return cached.row
  }
  let row: FeedCatalogPolicyRow | null = null
  try {
    const result = await db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['feed_id', 'access_policy_id', 'study_id', 'enabled', 'retired_at'])
      .where('rkey', '=', rkey)
      .executeTakeFirst()
    if (result) {
      row = {
        feed_id: String(result.feed_id),
        access_policy_id: String(result.access_policy_id),
        study_id: result.study_id ?? null,
        enabled: Boolean(result.enabled),
        retired_at: result.retired_at ?? null,
      }
    }
  } catch (err) {
    // Catalog read failed — DON'T cache the failure; the next request
    // retries. Log + return null so the caller denies access.
    console.warn(
      `[${new Date().toISOString()}] - access-policy: feed_catalog read failed for rkey=${rkey}; error=${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    return null
  }
  policyCache.set(rkey, {
    row,
    expires_at_ms: Date.now() + CACHE_TTL_MS,
  })
  return row
}

async function readSubscriberScope(
  db: Database,
  did: string,
): Promise<'omni' | 'assigned' | 'none' | null> {
  if (!did) return null
  const result = await db
    .selectFrom('subscriber')
    .select('access_scope')
    .where('did', '=', did)
    .executeTakeFirst()
  return result?.access_scope ?? null
}

async function hasActiveFeedAssignment(
  db: Database,
  did: string,
  feedId: string,
): Promise<boolean> {
  const result = await db
    .selectFrom('feedgen_ops.subscriber_feed_assignment')
    .select('did')
    .where('did', '=', did)
    .where('feed_id', '=', feedId)
    .where('active_until', 'is', null)
    .executeTakeFirst()
  return Boolean(result)
}

async function hasFeedAccess(
  db: Database,
  did: string,
  feedId: string,
): Promise<boolean> {
  const scope = await readSubscriberScope(db, did)
  if (scope === 'omni') return true
  if (scope !== 'assigned') return false
  return hasActiveFeedAssignment(db, did, feedId)
}

async function isInActiveStudyRegistry(
  db: Database,
  did: string,
  studyId: string,
): Promise<boolean> {
  if (!did || !studyId) return false
  const result = await db
    .selectFrom('feedgen_ops.study_registry')
    .select('did')
    .where('did', '=', did)
    .where('study_id', '=', studyId)
    .where((eb) => eb.not(eb('status', 'like', '%:stop_tracking')))
    .where('active_from', '<=', new Date())
    .where((eb) =>
      eb.or([
        eb('active_until', 'is', null),
        eb('active_until', '>', new Date()),
      ]),
    )
    .executeTakeFirst()
  return Boolean(result)
}

/**
 * Decide whether `requesterDid` may pull the feed identified by `rkey`.
 *
 * Returns `{ allowed: true }` to serve the feed normally, or
 * `{ allowed: false, reason }` to return an empty skeleton. Reasons
 * are short tokens suitable for log emission (e.g. 'no-catalog-row',
 * 'subscriber-default:not-subscriber', 'study-only:not-active',
 * 'disabled', 'unknown-policy', 'catalog-read-failed').
 */
export async function evaluateAccessPolicy(
  db: Database,
  rkey: string,
  requesterDid: string,
): Promise<AccessVerdict> {
  const row = await readPolicyRow(db, rkey)
  if (!row) {
    return { allowed: false, reason: 'no-catalog-row' }
  }
  if (!row.enabled || row.retired_at) {
    return { allowed: false, reason: 'feed-disabled' }
  }
  try {
    switch (row.access_policy_id) {
      case 'disabled':
        return { allowed: false, reason: 'disabled' }
      case 'subscriber-default': {
        const ok = await hasFeedAccess(db, requesterDid, row.feed_id)
        return ok
          ? { allowed: true, reason: 'subscriber-default' }
          : { allowed: false, reason: 'subscriber-default:not-assigned' }
      }
      case 'study-only': {
        if (!row.study_id) {
          console.warn(
            `[${new Date().toISOString()}] - access-policy: rkey=${rkey} has access_policy_id='study-only' but no study_id; fail-closed`,
          )
          return { allowed: false, reason: 'study-only:misconfigured' }
        }
        const [feedAccess, activeStudy] = await Promise.all([
          hasFeedAccess(db, requesterDid, row.feed_id),
          isInActiveStudyRegistry(db, requesterDid, row.study_id),
        ])
        return feedAccess && activeStudy
          ? { allowed: true, reason: `study-only:${row.study_id}` }
          : { allowed: false, reason: `study-only:${row.study_id}:not-active-or-assigned` }
      }
      default: {
        console.warn(
          `[${new Date().toISOString()}] - access-policy: rkey=${rkey} has unknown access_policy_id=${row.access_policy_id}; fail-closed`,
        )
        return { allowed: false, reason: 'unknown-policy' }
      }
    }
  } catch (err) {
    console.warn(
      `[${new Date().toISOString()}] - access-policy: access-state read failed for rkey=${rkey}; error=${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    return { allowed: false, reason: 'access-state-read-failed' }
  }
}
