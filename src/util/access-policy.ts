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

import { sql } from 'kysely'
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

export type FeedAccessState = {
  scope: 'omni' | 'assigned' | 'none' | null
  hasActiveAssignment: boolean
  activeStudy: boolean
}

export function accessVerdictForState(
  row: FeedCatalogPolicyRow | null,
  state: FeedAccessState,
): AccessVerdict {
  if (!row) return { allowed: false, reason: 'no-catalog-row' }
  if (!row.enabled || row.retired_at) return { allowed: false, reason: 'feed-disabled' }
  const hasFeedAccess = state.scope === 'omni' || (
    state.scope === 'assigned' && state.hasActiveAssignment
  )
  switch (row.access_policy_id) {
    case 'disabled':
      return { allowed: false, reason: 'disabled' }
    case 'subscriber-default':
      return hasFeedAccess
        ? { allowed: true, reason: 'subscriber-default' }
        : { allowed: false, reason: 'subscriber-default:not-assigned' }
    case 'study-only':
      if (!row.study_id) return { allowed: false, reason: 'study-only:misconfigured' }
      return hasFeedAccess && state.activeStudy
        ? { allowed: true, reason: `study-only:${row.study_id}` }
        : { allowed: false, reason: `study-only:${row.study_id}:not-active-or-assigned` }
    default:
      return { allowed: false, reason: 'unknown-policy' }
  }
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

export async function readFeedAccessStates(
  db: Database,
  row: FeedCatalogPolicyRow,
  dids: string[],
): Promise<Map<string, FeedAccessState>> {
  if (dids.length === 0) return new Map()
  const now = new Date()
  const activeStudy = row.access_policy_id === 'study-only' && row.study_id
    ? sql<boolean>`EXISTS (
        SELECT 1 FROM feedgen_ops.study_registry registry
        WHERE registry.did = subscriber.did
          AND registry.study_id = ${row.study_id}
          AND registry.status NOT LIKE ${'%:stop_tracking'}
          AND registry.active_from <= ${now}
          AND (registry.active_until IS NULL OR registry.active_until > ${now})
      )`
    : sql<boolean>`false`
  const states = await db
    .selectFrom('subscriber as subscriber')
    .select([
      'subscriber.did',
      'subscriber.access_scope',
      sql<boolean>`EXISTS (
        SELECT 1 FROM feedgen_ops.subscriber_feed_assignment assignment
        WHERE assignment.did = subscriber.did
          AND assignment.feed_id = ${row.feed_id}
          AND assignment.active_until IS NULL
      )`.as('has_active_assignment'),
      activeStudy.as('active_study'),
    ])
    .where('subscriber.did', 'in', dids)
    .execute()
  return new Map(states.map((state) => [state.did, {
    scope: state.access_scope ?? 'omni',
    hasActiveAssignment: state.has_active_assignment,
    activeStudy: state.active_study,
  }]))
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
  if (!row || !row.enabled || row.retired_at || row.access_policy_id === 'disabled') {
    return accessVerdictForState(row, {
      scope: null,
      hasActiveAssignment: false,
      activeStudy: false,
    })
  }
  try {
    const states = await readFeedAccessStates(db, row, [requesterDid])
    const verdict = accessVerdictForState(row, states.get(requesterDid) ?? {
      scope: null,
      hasActiveAssignment: false,
      activeStudy: false,
    })
    if (verdict.reason === 'study-only:misconfigured' || verdict.reason === 'unknown-policy') {
      console.warn(
        `[${new Date().toISOString()}] - access-policy: rkey=${rkey} failed closed; reason=${verdict.reason}`,
      )
    }
    return verdict
  } catch (err) {
    console.warn(
      `[${new Date().toISOString()}] - access-policy: access-state read failed for rkey=${rkey}; error=${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    return { allowed: false, reason: 'access-state-read-failed' }
  }
}
