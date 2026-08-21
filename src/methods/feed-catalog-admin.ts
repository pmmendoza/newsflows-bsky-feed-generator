/**
 * Sprint 11 / Task 4 — `feed_catalog` admin endpoints.
 *
 * Replaces ad-hoc psql edits for common operator actions and provides
 * read/dry-run surfaces for the future `bskyops` operator CLI:
 *   - GET all catalog rows
 *   - GET one catalog row
 *   - DRY-RUN an UPDATE without writing
 *   - INSERT a new row (e.g. when a Belgian feed is provisioned)
 *   - UPDATE a single row's enabled/access_policy_id/study_id/retired_at
 *
 * Pairs with the LISTEN/NOTIFY trigger (migration 015): every mutation
 * fires NOTIFY → feedgen serving replicas drop their per-rkey cache
 * within 1 s. Removes the only remaining "must SSH and run psql" path
 * for routine catalog edits.
 *
 * Auth: GET accepts FEEDGEN_READ_API_KEY or FEEDGEN_ADMIN_API_KEY;
 * dry-run and mutation require FEEDGEN_ADMIN_API_KEY.
 *
 * Out of scope:
 *   - Schema-evolving edits (column adds, etc.) — still require a
 *     migration.
 *   - Bulk imports — operator can call this endpoint in a loop or use
 *     `bsr feed new` (future tooling).
 *   - Live apply transaction orchestration — `bskyops` owns the
 *     higher-level workflow; this endpoint owns feedgen validation and
 *     catalog mutation.
 *
 * Plan: dev/storage/plan_storage_refactor/plan_feed_catalog_listen_notify.md
 */

import { Server } from '../lexicon'
import { AppContext } from '../config'
import { FeedCatalog, FeedCatalogHistory, DatabaseSchema } from '../db/schema'
import type { Transaction } from 'kysely'
import { sql } from 'kysely'
import {
  ApiKeyAuthConfig,
  isApiKeyAuthorized,
  logUnauthorized,
} from '../util/api-auth'
import { isSubscribableFeed } from '../util/subscribable-feed'
import {
  isPublisherPostMaxAgeDays,
  PUBLISHER_POST_MAX_AGE_DAYS_MAX,
  resolvePublisherServingWindow,
} from '../algos/publisher-serving-window'
import { invalidateActiveContentTimeContractCache } from '../util/content-time'

const adminWriteAuth: ApiKeyAuthConfig = {
  primaryEnv: ['FEEDGEN_ADMIN_API_KEY'],
}
const readAuth: ApiKeyAuthConfig = {
  primaryEnv: ['FEEDGEN_READ_API_KEY', 'FEEDGEN_ADMIN_API_KEY'],
}

const HISTORY_DEFAULT_LIMIT = 50
const HISTORY_MAX_LIMIT = 200

export const ALLOWED_ACCESS_POLICIES = new Set([
  'subscriber-default',
  'study-only',
  'disabled',
])

export const ALLOWED_ALGO_POLICIES = new Set([
  'chronological',
  'ranker-priority',
  'engagement-sorted',
])

const UPDATE_FIELDS = [
  'display_name',
  'publisher_did',
  'algo_policy_id',
  'ranker_policy_id',
  'ranker_score_source',
  'ranker_score_max_age_hours',
  'ranker_score_max_age_source',
  'ranker_min_score_backed_share',
  'ranker_min_score_backed_source',
  'publisher_post_max_age_days',
  'publisher_post_max_age_source',
  'publisher_time_clock',
  'publisher_time_transition_expires_at',
  'content_time_cutover_min_valid_share',
  'content_time_contract_version',
  'enabled',
  'access_policy_id',
  'study_id',
  'retired_at',
] as const

type UpdateField = typeof UPDATE_FIELDS[number]

type CatalogInsertBody = {
  op: 'insert'
  feed_id: string
  rkey: string
  display_name: string
  algo_policy_id: string
  access_policy_id: string
  country?: string | null
  study_id?: string | null
  publisher_did?: string | null
  ranker_policy_id?: string | null
  ranker_score_max_age_hours?: number | null
  ranker_score_max_age_source?: 'study_default' | 'feed_override' | null
  ranker_min_score_backed_share?: number | null
  ranker_min_score_backed_source?: 'study_default' | 'feed_override' | null
  publisher_post_max_age_days?: number | null
  publisher_post_max_age_source?: 'study_default' | 'feed_override' | null
  publisher_time_clock?: 'receipt_time' | 'content_time_v1'
  publisher_time_transition_expires_at?: string | null
  content_time_cutover_min_valid_share?: number | null
  content_time_contract_version?: string | null
  enabled?: boolean
}

type CatalogUpdateBody = {
  op: 'update'
  rkey: string
  display_name?: string
  publisher_did?: string | null
  algo_policy_id?: string
  ranker_policy_id?: string | null
  ranker_score_source?: string | null
  ranker_score_max_age_hours?: number | null
  ranker_score_max_age_source?: 'study_default' | 'feed_override' | null
  ranker_min_score_backed_share?: number | null
  ranker_min_score_backed_source?: 'study_default' | 'feed_override' | null
  enabled?: boolean
  access_policy_id?: string
  study_id?: string | null
  retired_at?: string | null
  publisher_post_max_age_days?: number | null
  publisher_post_max_age_source?: 'study_default' | 'feed_override' | null
  publisher_time_clock?: 'receipt_time' | 'content_time_v1'
  publisher_time_transition_expires_at?: string | null
  content_time_cutover_min_valid_share?: number | null
  content_time_contract_version?: string | null
  if_current?: Partial<Record<UpdateField, boolean | number | string | null>>
}

type CatalogBody = CatalogInsertBody | CatalogUpdateBody

type CatalogDryRunBody = Omit<CatalogUpdateBody, 'op'> & {
  op?: 'update'
}

type CatalogBulkBody = {
  updates: CatalogUpdateBody[]
}

type CatalogUpdatePatch = Partial<Pick<
  FeedCatalog,
  | 'display_name'
  | 'publisher_did'
  | 'algo_policy_id'
  | 'ranker_policy_id'
  | 'ranker_score_source'
  | 'ranker_score_max_age_hours'
  | 'ranker_score_max_age_source'
  | 'ranker_min_score_backed_share'
  | 'ranker_min_score_backed_source'
  | 'publisher_post_max_age_days'
  | 'publisher_post_max_age_source'
  | 'publisher_time_clock'
  | 'publisher_time_transition_expires_at'
  | 'content_time_cutover_min_valid_share'
  | 'content_time_contract_version'
  | 'enabled'
  | 'access_policy_id'
  | 'study_id'
  | 'retired_at'
>>

type ValidatedCatalogUpdate = {
  op: 'update'
  rkey: string
  patch: CatalogUpdatePatch
  ifCurrent?: Partial<Record<UpdateField, boolean | number | string | null>>
}

type FeedCatalogDryRunMessage = {
  code: string
  message: string
  [key: string]: unknown
}

type FeedCatalogHistoryIdentity = {
  actor: string
  source: string
}

function isString(v: unknown): v is string {
  return typeof v === 'string' && v.length > 0
}

function nullableString(v: unknown): v is string | null {
  return v === null || typeof v === 'string'
}

function selfReportedHeader(value: unknown, fallback: string): string {
  const first = Array.isArray(value) ? value[0] : value
  return typeof first === 'string' && first.trim() ? first.trim() : fallback
}

function historyIdentity(headers: Record<string, unknown>): FeedCatalogHistoryIdentity {
  // These labels are caller-supplied audit context, not cryptographically
  // verified identities. The API-key gate remains the authorization boundary.
  return {
    actor: selfReportedHeader(headers['x-feedgen-actor'], 'api-key'),
    source: selfReportedHeader(headers['x-feedgen-source'], 'direct-api'),
  }
}

function parseIntegerQuery(
  value: unknown,
  name: string,
  defaultValue: number,
  minimum: number,
): number {
  if (value === undefined) return defaultValue
  if (typeof value !== 'string' || !/^\d+$/.test(value)) {
    throw new Error(`${name} must be a non-negative integer`)
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    throw new Error(`${name} must be ${minimum === 0 ? 'a non-negative' : 'a positive'} integer`)
  }
  return parsed
}

function parseHistoryPagination(limit: unknown, offset: unknown) {
  return {
    limit: Math.min(
      parseIntegerQuery(limit, 'limit', HISTORY_DEFAULT_LIMIT, 1),
      HISTORY_MAX_LIMIT,
    ),
    offset: parseIntegerQuery(offset, 'offset', 0, 0),
  }
}

function isUpdateField(field: string): field is UpdateField {
  return (UPDATE_FIELDS as readonly string[]).includes(field)
}

function fieldValue(row: Pick<FeedCatalog, UpdateField>, field: UpdateField) {
  const value = row[field]
  if (value instanceof Date) return value.toISOString()
  return value ?? null
}

function currentFieldValues(row: Pick<FeedCatalog, UpdateField>) {
  return Object.fromEntries(
    UPDATE_FIELDS.map((field) => [field, fieldValue(row, field)]),
  ) as Record<UpdateField, boolean | number | string | null>
}

function proposedFieldValues(row: Pick<FeedCatalog, UpdateField>, patch: CatalogUpdatePatch) {
  const proposed = { ...row, ...patch }
  return currentFieldValues(proposed)
}

function feedCatalogChanges(
  current: Record<UpdateField, boolean | number | string | null>,
  proposed: Record<UpdateField, boolean | number | string | null>,
  fields: readonly UpdateField[],
) {
  return fields
    .filter((field) => current[field] !== proposed[field])
    .map((field) => ({
      field,
      current: current[field],
      proposed: proposed[field],
    }))
}

function validateCurrentValues(
  value: unknown,
): { ok: true; current?: Partial<Record<UpdateField, boolean | number | string | null>> } | { ok: false; error: string } {
  if (value === undefined) return { ok: true }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return { ok: false, error: 'if_current must be an object when provided' }
  }
  const current: Partial<Record<UpdateField, boolean | number | string | null>> = {}
  for (const [field, fieldValue] of Object.entries(value)) {
    if (!isUpdateField(field)) {
      return { ok: false, error: `if_current contains unsupported field: ${field}` }
    }
    if (field === 'enabled' && typeof fieldValue !== 'boolean') {
      return { ok: false, error: 'if_current.enabled must be boolean' }
    }
    if (field === 'display_name' && !isString(fieldValue)) {
      return { ok: false, error: 'if_current.display_name must be a non-empty string' }
    }
    if (field === 'publisher_did' && !nullableString(fieldValue)) {
      return { ok: false, error: 'if_current.publisher_did must be string or null' }
    }
    if (
      field === 'algo_policy_id' &&
      (typeof fieldValue !== 'string' || !ALLOWED_ALGO_POLICIES.has(fieldValue))
    ) {
      return { ok: false, error: `if_current.algo_policy_id must be one of ${[...ALLOWED_ALGO_POLICIES].join(', ')}` }
    }
    if (field === 'ranker_policy_id' && !nullableString(fieldValue)) {
      return { ok: false, error: 'if_current.ranker_policy_id must be string or null' }
    }
    if (field === 'ranker_score_source' && !nullableString(fieldValue)) {
      return { ok: false, error: 'if_current.ranker_score_source must be string or null' }
    }
    if (field === 'ranker_score_max_age_hours' && fieldValue !== null && (!Number.isInteger(fieldValue) || Number(fieldValue) < 1 || Number(fieldValue) > 8760)) {
      return { ok: false, error: 'if_current.ranker_score_max_age_hours must be an integer from 1 to 8760, or null' }
    }
    if (field === 'ranker_min_score_backed_share' && fieldValue !== null && (typeof fieldValue !== 'number' || !Number.isFinite(fieldValue) || fieldValue <= 0 || fieldValue > 1)) {
      return { ok: false, error: 'if_current.ranker_min_score_backed_share must be > 0 and <= 1, or null' }
    }
    if ((field === 'ranker_score_max_age_source' || field === 'ranker_min_score_backed_source') && fieldValue !== null && fieldValue !== 'study_default' && fieldValue !== 'feed_override') {
      return { ok: false, error: `if_current.${field} must be study_default, feed_override, or null` }
    }
    if (
      field === 'access_policy_id' &&
      (typeof fieldValue !== 'string' || !ALLOWED_ACCESS_POLICIES.has(fieldValue))
    ) {
      return { ok: false, error: `if_current.access_policy_id must be one of ${[...ALLOWED_ACCESS_POLICIES].join(', ')}` }
    }
    if ((field === 'study_id' || field === 'retired_at') && !nullableString(fieldValue)) {
      return { ok: false, error: `if_current.${field} must be string or null` }
    }
    if ((field === 'publisher_time_transition_expires_at' || field === 'content_time_contract_version') && !nullableString(fieldValue)) {
      return { ok: false, error: `if_current.${field} must be string or null` }
    }
    if (field === 'content_time_cutover_min_valid_share' && fieldValue !== null && (typeof fieldValue !== 'number' || !Number.isFinite(fieldValue) || fieldValue <= 0 || fieldValue > 1)) {
      return { ok: false, error: 'if_current.content_time_cutover_min_valid_share must be > 0 and <= 1, or null' }
    }
    if (field === 'publisher_post_max_age_days' && fieldValue !== null && !isPublisherPostMaxAgeDays(fieldValue)) {
      return { ok: false, error: `if_current.publisher_post_max_age_days must be an integer from 1 to ${PUBLISHER_POST_MAX_AGE_DAYS_MAX}, or null` }
    }
    if (field === 'publisher_post_max_age_source' && fieldValue !== null && fieldValue !== 'study_default' && fieldValue !== 'feed_override') {
      return { ok: false, error: 'if_current.publisher_post_max_age_source must be study_default, feed_override, or null' }
    }
    if (field === 'publisher_time_clock' && fieldValue !== 'receipt_time' && fieldValue !== 'content_time_v1') {
      return { ok: false, error: 'if_current.publisher_time_clock must be receipt_time or content_time_v1' }
    }
    current[field] = fieldValue as boolean | number | string | null
  }
  return { ok: true, current }
}

function publisherAgeErrors(row: Pick<FeedCatalog, 'enabled' | 'publisher_post_max_age_days' | 'publisher_post_max_age_source' | 'publisher_time_clock' | 'publisher_time_transition_expires_at' | 'content_time_cutover_min_valid_share' | 'content_time_contract_version'>): string[] {
  const errors: string[] = []
  const days = row.publisher_post_max_age_days
  const source = row.publisher_post_max_age_source
  if (days != null && !isPublisherPostMaxAgeDays(days)) {
    errors.push(`publisher_post_max_age_days must be an integer from 1 to ${PUBLISHER_POST_MAX_AGE_DAYS_MAX}`)
  }
  if (source != null && source !== 'study_default' && source !== 'feed_override') {
    errors.push('publisher_post_max_age_source must be study_default or feed_override')
  }
  if ((days == null) !== (source == null)) {
    errors.push('publisher_post_max_age_days and publisher_post_max_age_source must be set together')
  }
  if (row.enabled && (days == null || source == null)) {
    errors.push('enabled feeds require a materialized publisher_post_max_age_days and provenance')
  }
  if (row.publisher_time_clock !== undefined && row.publisher_time_clock !== 'receipt_time' && row.publisher_time_clock !== 'content_time_v1') {
    errors.push('publisher_time_clock must be receipt_time or content_time_v1')
  }
  if (row.content_time_cutover_min_valid_share != null && (!Number.isFinite(row.content_time_cutover_min_valid_share) || row.content_time_cutover_min_valid_share <= 0 || row.content_time_cutover_min_valid_share > 1)) {
    errors.push('content_time_cutover_min_valid_share must be > 0 and <= 1')
  }
  if (row.publisher_time_transition_expires_at != null && !Number.isFinite(Date.parse(row.publisher_time_transition_expires_at))) {
    errors.push('publisher_time_transition_expires_at must be a valid ISO timestamp')
  }
  if (row.publisher_time_clock === 'content_time_v1') {
    if (row.content_time_cutover_min_valid_share == null) errors.push('content_time_v1 requires content_time_cutover_min_valid_share')
    if (!row.content_time_contract_version) {
      errors.push('content_time_v1 requires content_time_contract_version')
    } else if (
      row.content_time_contract_version !== 'newsflows-content-time/v2' &&
      row.content_time_contract_version !== 'newsflows-content-time/v3'
    ) {
      errors.push('content_time_contract_version must be newsflows-content-time/v2 or newsflows-content-time/v3')
    }
    // No expiry means the transition is permanent (FT-FU-6), which is the target
    // state; a malformed value is still rejected above. The floor and contract
    // version remain required -- they are what keep the clock auditable.
  }
  return errors
}

function rankerControlErrors(row: Pick<FeedCatalog, 'algo_policy_id' | 'ranker_score_max_age_hours' | 'ranker_score_max_age_source' | 'ranker_min_score_backed_share' | 'ranker_min_score_backed_source'>): string[] {
  const values = [row.ranker_score_max_age_hours, row.ranker_score_max_age_source, row.ranker_min_score_backed_share, row.ranker_min_score_backed_source]
  if (row.algo_policy_id !== 'ranker-priority') {
    return values.some((value) => value != null) ? ['ranker serving controls must be null for non-ranker feeds'] : []
  }
  const errors: string[] = []
  if (!Number.isInteger(row.ranker_score_max_age_hours) || Number(row.ranker_score_max_age_hours) < 1 || Number(row.ranker_score_max_age_hours) > 8760) errors.push('ranker-priority feeds require ranker_score_max_age_hours from 1 to 8760')
  if (typeof row.ranker_min_score_backed_share !== 'number' || !Number.isFinite(row.ranker_min_score_backed_share) || row.ranker_min_score_backed_share <= 0 || row.ranker_min_score_backed_share > 1) errors.push('ranker-priority feeds require ranker_min_score_backed_share > 0 and <= 1')
  if (row.ranker_score_max_age_source !== 'study_default' && row.ranker_score_max_age_source !== 'feed_override') errors.push('ranker_score_max_age_source must declare provenance')
  if (row.ranker_min_score_backed_source !== 'study_default' && row.ranker_min_score_backed_source !== 'feed_override') errors.push('ranker_min_score_backed_source must declare provenance')
  return errors
}

function validatePolicyPair(
  algoPolicyId: string | undefined,
  rankerPolicyId: string | null | undefined,
): { ok: true } | { ok: false; error: string } {
  if (algoPolicyId === undefined) return { ok: true }
  if (!ALLOWED_ALGO_POLICIES.has(algoPolicyId)) {
    return { ok: false, error: `algo_policy_id must be one of ${[...ALLOWED_ALGO_POLICIES].join(', ')}` }
  }
  if (algoPolicyId === 'ranker-priority') {
    if (!isString(rankerPolicyId)) {
      return {
        ok: false,
        error: 'ranker_policy_id required when algo_policy_id=ranker-priority',
      }
    }
    return { ok: true }
  }
  if (rankerPolicyId !== undefined && rankerPolicyId !== null) {
    return {
      ok: false,
      error: 'ranker_policy_id must be null when algo_policy_id is chronological or engagement-sorted',
    }
  }
  return { ok: true }
}

export function operatorStatus(row: Pick<FeedCatalog, 'enabled' | 'access_policy_id' | 'retired_at'>): string {
  if (row.retired_at) return 'retired'
  if (row.enabled === false) return 'disabled'
  if (row.access_policy_id === 'disabled') return 'paused'
  return 'active'
}

export function feedCatalogItemPayload(row: FeedCatalog) {
  const servingWindow = resolvePublisherServingWindow(
    row.rkey,
    row.publisher_post_max_age_days,
    row.publisher_post_max_age_source,
  )
  return {
    feed_id: row.feed_id,
    rkey: row.rkey,
    display_name: row.display_name,
    country: row.country ?? null,
    publisher_did: row.publisher_did ?? null,
    study_id: row.study_id ?? null,
    algo_policy_id: row.algo_policy_id,
    ranker_policy_id: row.ranker_policy_id ?? null,
    ranker_score_source: row.ranker_score_source ?? null,
    ranker_score_max_age_hours: row.ranker_score_max_age_hours ?? null,
    ranker_score_max_age_source: row.ranker_score_max_age_source ?? null,
    ranker_min_score_backed_share: row.ranker_min_score_backed_share ?? null,
    ranker_min_score_backed_source: row.ranker_min_score_backed_source ?? null,
    ranker_score_freshness: {
      effective_hours: row.ranker_score_max_age_hours ?? 24,
      effective_source: row.ranker_score_max_age_hours == null ? 'compatibility_default_24h' : row.ranker_score_max_age_source,
      compatibility_fallback_active: row.algo_policy_id === 'ranker-priority' && row.ranker_score_max_age_hours == null,
    },
    publisher_post_max_age_days: row.publisher_post_max_age_days ?? null,
    publisher_post_max_age_source: row.publisher_post_max_age_source ?? null,
    publisher_time_clock: row.publisher_time_clock ?? 'receipt_time',
    publisher_time_transition_expires_at: row.publisher_time_transition_expires_at ?? null,
    content_time_cutover_min_valid_share: row.content_time_cutover_min_valid_share ?? null,
    content_time_contract_version: row.content_time_contract_version ?? null,
    catalog_revision: Number(row.catalog_revision ?? 0),
    publisher_serving_window: {
      effective_hours: servingWindow.effectiveHours,
      effective_days: servingWindow.effectiveDays,
      effective_source: servingWindow.source,
      compatibility_fallback_active: servingWindow.compatibilityFallbackActive,
      compatibility_env_key: servingWindow.compatibilityEnvKey,
    },
    access_policy_id: row.access_policy_id,
    enabled: row.enabled,
    created_at: row.created_at ?? null,
    retired_at: row.retired_at ?? null,
    operator_status: operatorStatus(row),
    published: {
      status: 'unknown',
      uri: null,
    },
    health: {
      status: 'unknown',
      checked_at: null,
    },
    raw_values_in_output: false,
  }
}

export function feedCatalogListPayload(rows: FeedCatalog[], subscribableOnly = false) {
  const feeds = subscribableOnly ? rows.filter(isSubscribableFeed) : rows
  return {
    schema_version: 1,
    feed_count: feeds.length,
    feeds: feeds.map(feedCatalogItemPayload),
    subscribable_only: subscribableOnly,
    raw_values_in_output: false,
  }
}

export function parseSubscribableFilter(value: unknown): boolean {
  if (value === undefined || value === 'false') return false
  if (value === 'true') return true
  throw new Error('subscribable must be true or false')
}

export function feedCatalogShowPayload(row: FeedCatalog) {
  return {
    schema_version: 1,
    ...feedCatalogItemPayload(row),
  }
}

export function feedCatalogNotFoundPayload(rkey: string) {
  return { error: `rkey=${rkey} not found` }
}

export function validateInsert(body: any): { ok: true; row: CatalogInsertBody } | { ok: false; error: string } {
  if (!isString(body?.feed_id)) return { ok: false, error: 'feed_id required' }
  if (!isString(body?.rkey)) return { ok: false, error: 'rkey required' }
  if (body.rkey.length > 15) return { ok: false, error: 'rkey must be ≤15 chars (ATProto record-key constraint)' }
  if (!isString(body?.display_name)) return { ok: false, error: 'display_name required (NOT NULL in feed_catalog)' }
  if (!isString(body?.algo_policy_id)) return { ok: false, error: 'algo_policy_id required' }
  const policy = validatePolicyPair(body.algo_policy_id, body.ranker_policy_id ?? null)
  if (!policy.ok) return policy
  if (!isString(body?.access_policy_id)) return { ok: false, error: 'access_policy_id required' }
  if (!ALLOWED_ACCESS_POLICIES.has(body.access_policy_id)) {
    return { ok: false, error: `access_policy_id must be one of ${[...ALLOWED_ACCESS_POLICIES].join(', ')}` }
  }
  if (body.access_policy_id === 'study-only' && !isString(body?.study_id)) {
    return { ok: false, error: 'study_id required when access_policy_id=study-only' }
  }
  const enabled = typeof body.enabled === 'boolean' ? body.enabled : true
  const ageErrors = publisherAgeErrors({
    enabled,
    publisher_post_max_age_days: body.publisher_post_max_age_days,
    publisher_post_max_age_source: body.publisher_post_max_age_source,
    publisher_time_clock: body.publisher_time_clock ?? 'receipt_time',
    publisher_time_transition_expires_at: body.publisher_time_transition_expires_at ?? null,
    content_time_cutover_min_valid_share: body.content_time_cutover_min_valid_share ?? null,
    content_time_contract_version: body.content_time_contract_version ?? null,
  })
  if (ageErrors.length > 0) return { ok: false, error: ageErrors.join('; ') }
  const rankerErrors = rankerControlErrors({
    algo_policy_id: body.algo_policy_id,
    ranker_score_max_age_hours: body.ranker_score_max_age_hours ?? null,
    ranker_score_max_age_source: body.ranker_score_max_age_source ?? null,
    ranker_min_score_backed_share: body.ranker_min_score_backed_share ?? null,
    ranker_min_score_backed_source: body.ranker_min_score_backed_source ?? null,
  })
  if (rankerErrors.length > 0) return { ok: false, error: rankerErrors.join('; ') }
  return {
    ok: true,
    row: {
      op: 'insert',
      feed_id: body.feed_id,
      rkey: body.rkey,
      display_name: body.display_name,
      algo_policy_id: body.algo_policy_id,
      access_policy_id: body.access_policy_id,
      country: body.country ?? null,
      study_id: body.study_id ?? null,
      publisher_did: body.publisher_did ?? null,
      ranker_policy_id: body.ranker_policy_id ?? null,
      ranker_score_max_age_hours: body.ranker_score_max_age_hours ?? null,
      ranker_score_max_age_source: body.ranker_score_max_age_source ?? null,
      ranker_min_score_backed_share: body.ranker_min_score_backed_share ?? null,
      ranker_min_score_backed_source: body.ranker_min_score_backed_source ?? null,
      publisher_post_max_age_days: body.publisher_post_max_age_days ?? null,
      publisher_post_max_age_source: body.publisher_post_max_age_source ?? null,
      publisher_time_clock: body.publisher_time_clock ?? 'receipt_time',
      enabled,
    },
  }
}

export function validateUpdate(body: any): { ok: true; row: ValidatedCatalogUpdate } | { ok: false; error: string } {
  if (body?.op !== undefined && body.op !== 'update') {
    return { ok: false, error: "op must be 'update' when provided" }
  }
  if (!isString(body?.rkey)) return { ok: false, error: 'rkey required' }
  if (body.display_name !== undefined && !isString(body.display_name)) {
    return { ok: false, error: 'display_name must be a non-empty string' }
  }
  if (body.publisher_did !== undefined && !nullableString(body.publisher_did)) {
    return { ok: false, error: 'publisher_did must be string or null' }
  }
  if (body.algo_policy_id !== undefined && !isString(body.algo_policy_id)) {
    return { ok: false, error: 'algo_policy_id must be a non-empty string' }
  }
  if (body.ranker_policy_id !== undefined && !nullableString(body.ranker_policy_id)) {
    return { ok: false, error: 'ranker_policy_id must be string or null' }
  }
  if (body.ranker_score_source !== undefined && !nullableString(body.ranker_score_source)) {
    return { ok: false, error: 'ranker_score_source must be string or null' }
  }
  if (body.ranker_score_max_age_hours !== undefined && body.ranker_score_max_age_hours !== null && (!Number.isInteger(body.ranker_score_max_age_hours) || body.ranker_score_max_age_hours < 1 || body.ranker_score_max_age_hours > 8760)) {
    return { ok: false, error: 'ranker_score_max_age_hours must be an integer from 1 to 8760, or null' }
  }
  if (body.ranker_min_score_backed_share !== undefined && body.ranker_min_score_backed_share !== null && (typeof body.ranker_min_score_backed_share !== 'number' || !Number.isFinite(body.ranker_min_score_backed_share) || body.ranker_min_score_backed_share <= 0 || body.ranker_min_score_backed_share > 1)) {
    return { ok: false, error: 'ranker_min_score_backed_share must be > 0 and <= 1, or null' }
  }
  for (const field of ['ranker_score_max_age_source', 'ranker_min_score_backed_source'] as const) {
    if (body[field] !== undefined && body[field] !== null && body[field] !== 'study_default' && body[field] !== 'feed_override') {
      return { ok: false, error: `${field} must be study_default, feed_override, or null` }
    }
  }
  const policy = validatePolicyPair(body.algo_policy_id, body.ranker_policy_id)
  if (!policy.ok) return policy
  if (body.enabled !== undefined && typeof body.enabled !== 'boolean') {
    return { ok: false, error: 'enabled must be boolean' }
  }
  if (
    body.access_policy_id !== undefined &&
    !ALLOWED_ACCESS_POLICIES.has(body.access_policy_id)
  ) {
    return { ok: false, error: `access_policy_id must be one of ${[...ALLOWED_ACCESS_POLICIES].join(', ')}` }
  }
  if (body.study_id !== undefined && !nullableString(body.study_id)) {
    return { ok: false, error: 'study_id must be string or null' }
  }
  if (body.retired_at !== undefined && !nullableString(body.retired_at)) {
    return { ok: false, error: 'retired_at must be string or null' }
  }
  if (body.publisher_post_max_age_days !== undefined && body.publisher_post_max_age_days !== null && !isPublisherPostMaxAgeDays(body.publisher_post_max_age_days)) {
    return { ok: false, error: `publisher_post_max_age_days must be an integer from 1 to ${PUBLISHER_POST_MAX_AGE_DAYS_MAX}, or null` }
  }
  if (body.publisher_post_max_age_source !== undefined && body.publisher_post_max_age_source !== null && body.publisher_post_max_age_source !== 'study_default' && body.publisher_post_max_age_source !== 'feed_override') {
    return { ok: false, error: 'publisher_post_max_age_source must be study_default, feed_override, or null' }
  }
  if (body.publisher_time_clock !== undefined && body.publisher_time_clock !== 'receipt_time' && body.publisher_time_clock !== 'content_time_v1') {
    return { ok: false, error: 'publisher_time_clock must be receipt_time or content_time_v1' }
  }
  if (body.publisher_time_transition_expires_at !== undefined && !nullableString(body.publisher_time_transition_expires_at)) {
    return { ok: false, error: 'publisher_time_transition_expires_at must be string or null' }
  }
  if (body.content_time_cutover_min_valid_share !== undefined && body.content_time_cutover_min_valid_share !== null && (typeof body.content_time_cutover_min_valid_share !== 'number' || !Number.isFinite(body.content_time_cutover_min_valid_share) || body.content_time_cutover_min_valid_share <= 0 || body.content_time_cutover_min_valid_share > 1)) {
    return { ok: false, error: 'content_time_cutover_min_valid_share must be > 0 and <= 1, or null' }
  }
  if (body.content_time_contract_version !== undefined && body.content_time_contract_version !== null && body.content_time_contract_version !== 'newsflows-content-time/v2' && body.content_time_contract_version !== 'newsflows-content-time/v3') {
    return { ok: false, error: 'content_time_contract_version must be newsflows-content-time/v2 or newsflows-content-time/v3, or null' }
  }
  const updates = {
    display_name: body.display_name,
    publisher_did: body.publisher_did,
    algo_policy_id: body.algo_policy_id,
    ranker_policy_id: body.ranker_policy_id,
    ranker_score_source: body.ranker_score_source,
    ranker_score_max_age_hours: body.ranker_score_max_age_hours,
    ranker_score_max_age_source: body.ranker_score_max_age_source,
    ranker_min_score_backed_share: body.ranker_min_score_backed_share,
    ranker_min_score_backed_source: body.ranker_min_score_backed_source,
    enabled: body.enabled,
    access_policy_id: body.access_policy_id,
    study_id: body.study_id,
    retired_at: body.retired_at,
    publisher_post_max_age_days: body.publisher_post_max_age_days,
    publisher_post_max_age_source: body.publisher_post_max_age_source,
    publisher_time_clock: body.publisher_time_clock,
    publisher_time_transition_expires_at: body.publisher_time_transition_expires_at,
    content_time_cutover_min_valid_share: body.content_time_cutover_min_valid_share,
    content_time_contract_version: body.content_time_contract_version,
  }
  if (Object.values(updates).every((v) => v === undefined)) {
    return { ok: false, error: 'at least one supported field is required' }
  }
  const current = validateCurrentValues(body.if_current)
  if (!current.ok) return current
  const patch: CatalogUpdatePatch = {}
  for (const field of UPDATE_FIELDS) {
    if (updates[field] !== undefined) {
      ;(patch as any)[field] = updates[field]
    }
  }
  return { ok: true, row: { op: 'update', rkey: body.rkey, patch, ifCurrent: current.current } }
}

export function buildFeedCatalogDryRun(
  current: FeedCatalog,
  update: ValidatedCatalogUpdate,
  opts: { studyExists?: boolean } = {},
) {
  const currentValues = currentFieldValues(current)
  const proposedValues = proposedFieldValues(current, update.patch)
  const proposed = {
    ...current,
    ...update.patch,
  }
  const changes = feedCatalogChanges(
    currentValues,
    proposedValues,
    UPDATE_FIELDS.filter((field) =>
      Object.prototype.hasOwnProperty.call(update.patch, field)),
  )
  const blockers: FeedCatalogDryRunMessage[] = []
  const warnings: FeedCatalogDryRunMessage[] = []
  if (proposed.access_policy_id === 'study-only' && !proposed.study_id) {
    blockers.push({
      code: 'study-id-required',
      message: 'study_id is required when access_policy_id=study-only',
    })
  }
  const policy = validatePolicyPair(
    proposed.algo_policy_id,
    proposed.ranker_policy_id ?? null,
  )
  if (!policy.ok) {
    blockers.push({
      code: 'invalid-policy-pair',
      message: policy.error,
    })
  }
  if (proposed.study_id && opts.studyExists === false) {
    blockers.push({
      code: 'study-id-not-found',
      message: `study_id does not exist in study_catalog: ${proposed.study_id}`,
    })
  }
  for (const message of publisherAgeErrors(proposed)) {
    blockers.push({ code: 'invalid-publisher-age', message })
  }
  for (const message of rankerControlErrors(proposed)) {
    blockers.push({ code: 'invalid-ranker-controls', message })
  }
  if (proposed.access_policy_id === 'disabled' && proposed.enabled === true) {
    warnings.push({
      code: 'access-disabled-feed-enabled',
      message: 'feed remains enabled but access policy disables serving',
    })
  }
  if (Object.prototype.hasOwnProperty.call(update.patch, 'retired_at')) {
    warnings.push({
      code: 'retirement-semantics-review',
      message: 'retire/unretire semantics must be reviewed before live apply',
    })
  }
  return {
    schema_version: 1,
    mode: 'dry-run',
    operation: 'feed.update',
    target: `feed:${current.rkey}`,
    source: 'feedgen',
    status: blockers.length > 0 ? 'blocked' : changes.length === 0 ? 'no-op' : 'dry-run',
    dry_run: true,
    would_write: false,
    current: currentValues,
    proposed: proposedValues,
    current_status: operatorStatus(current),
    proposed_status: operatorStatus(proposed),
    changes,
    change_count: changes.length,
    blockers,
    warnings,
    rollback: {
      strategy: 'restore-current-values',
      fields: Object.fromEntries(changes.map((change) => [change.field, change.current])),
    },
    raw_values_in_output: false,
  }
}

export function currentValueMismatches(
  current: FeedCatalog,
  expected: Partial<Record<UpdateField, boolean | number | string | null>> | undefined,
) {
  if (!expected) return []
  const actual = currentFieldValues(current)
  return UPDATE_FIELDS
    .filter((field) => Object.prototype.hasOwnProperty.call(expected, field))
    .filter((field) => actual[field] !== expected[field])
    .map((field) => ({
      field,
      expected: expected[field] ?? null,
      actual: actual[field],
    }))
}

export function buildFeedCatalogApplyBlocked(
  dryRun: ReturnType<typeof buildFeedCatalogDryRun>,
  blocker: FeedCatalogDryRunMessage,
  status = 'blocked',
) {
  return {
    ...dryRun,
    mode: 'apply',
    status,
    dry_run: false,
    would_write: false,
    applied: false,
    blockers: [...dryRun.blockers, blocker],
    raw_values_in_output: false,
  }
}

export function buildFeedCatalogApplyConflict(
  dryRun: ReturnType<typeof buildFeedCatalogDryRun>,
  mismatches: ReturnType<typeof currentValueMismatches>,
) {
  return buildFeedCatalogApplyBlocked(
    dryRun,
    {
      code: 'stale-current-values',
      message: 'feed_catalog row changed since dry-run/current-state capture',
      mismatches,
    } as FeedCatalogDryRunMessage & { mismatches: ReturnType<typeof currentValueMismatches> },
    'conflict',
  )
}

export function buildFeedCatalogApplyResult(
  before: FeedCatalog,
  after: FeedCatalog,
  dryRun: ReturnType<typeof buildFeedCatalogDryRun>,
  applied: boolean,
) {
  const afterValues = currentFieldValues(after)
  return {
    schema_version: 1,
    mode: 'apply',
    operation: 'feed.update',
    target: `feed:${before.rkey}`,
    source: 'feedgen',
    status: applied ? 'applied' : 'no-op',
    dry_run: false,
    would_write: false,
    applied,
    wrote: applied,
    before: dryRun.current,
    after: afterValues,
    current: dryRun.current,
    proposed: dryRun.proposed,
    changes: dryRun.changes,
    change_count: dryRun.change_count,
    blockers: [],
    warnings: dryRun.warnings,
    before_status: dryRun.current_status,
    after_status: operatorStatus(after),
    rollback: dryRun.rollback,
    readback: feedCatalogShowPayload(after),
    raw_values_in_output: false,
  }
}

async function readCatalogRows(ctx: AppContext): Promise<FeedCatalog[]> {
  return (await ctx.db
    .selectFrom('feedgen_ops.feed_catalog')
    .select([
      'feed_id',
      'rkey',
      'display_name',
      'country',
      'publisher_did',
      'study_id',
      'algo_policy_id',
      'ranker_policy_id',
      'ranker_score_source',
      'ranker_score_max_age_hours',
      'ranker_score_max_age_source',
      'ranker_min_score_backed_share',
      'ranker_min_score_backed_source',
      'publisher_post_max_age_days',
      'publisher_post_max_age_source',
      'publisher_time_clock',
      'publisher_time_transition_expires_at',
      'content_time_cutover_min_valid_share',
      'content_time_contract_version',
      'catalog_revision',
      'access_policy_id',
      'enabled',
      'created_at',
      'retired_at',
    ])
    .orderBy('rkey', 'asc')
    .execute()) as FeedCatalog[]
}

export async function readCatalogRowFromDb(
  db: any,
  rkey: string,
  forUpdate = false,
): Promise<FeedCatalog | undefined> {
  let query = db
    .selectFrom('feedgen_ops.feed_catalog')
    .select([
      'feed_id',
      'rkey',
      'display_name',
      'country',
      'publisher_did',
      'study_id',
      'algo_policy_id',
      'ranker_policy_id',
      'ranker_score_source',
      'ranker_score_max_age_hours',
      'ranker_score_max_age_source',
      'ranker_min_score_backed_share',
      'ranker_min_score_backed_source',
      'publisher_post_max_age_days',
      'publisher_post_max_age_source',
      'publisher_time_clock',
      'publisher_time_transition_expires_at',
      'content_time_cutover_min_valid_share',
      'content_time_contract_version',
      'catalog_revision',
      'access_policy_id',
      'enabled',
      'created_at',
      'retired_at',
    ])
    .where('rkey', '=', rkey)
  // forUpdate is passed ONLY by the update transaction: it locks the row so a
  // concurrent update blocks until we commit, then re-reads the now-current
  // value and its if_current CAS check correctly rejects (409). Two switches
  // with the same if_current can't both pass the check and clobber each other.
  // GET readback and dry-run reads leave forUpdate=false (unlocked, unchanged).
  if (forUpdate) query = query.forUpdate()
  return (await query.executeTakeFirst()) as FeedCatalog | undefined
}

async function appendFeedCatalogHistory(
  db: Transaction<DatabaseSchema>,
  before: FeedCatalog | null,
  after: FeedCatalog,
  changedFields: FeedCatalogHistory['changed_fields'],
  identity: FeedCatalogHistoryIdentity,
): Promise<number> {
  // The feed_catalog row lock in the update transaction serializes writers for
  // this feed, so the latest revision remains current until this insert.
  const previous = await db
    .selectFrom('feedgen_ops.feed_catalog_history')
    .select(['revision', 'feed_code_hash_after', 'ranker_code_hash_after'])
    .where('feed_id', '=', after.feed_id)
    .orderBy('revision', 'desc')
    .limit(1)
    .executeTakeFirst()
  const revision = Number(previous?.revision ?? 0) + 1
  const feedCodeHashAfter = process.env.FEEDGEN_FEED_CODE_HASH || null
  const rankerCodeHashAfter = process.env.FEEDGEN_RANKER_CODE_HASH || null

  await db
    .insertInto('feedgen_ops.feed_catalog_history')
    .values({
      feed_id: after.feed_id,
      rkey: after.rkey,
      revision,
      actor: identity.actor,
      source: identity.source,
      // jsonb columns need an explicit JSON serialization + ::jsonb cast: node-postgres
      // renders a JS array (changed_fields) as a Postgres array literal, not JSON, which
      // fails as 'invalid input syntax for type json' and would roll back the apply.
      before_row: before == null ? null : sql`${JSON.stringify(before)}::jsonb`,
      after_row: sql`${JSON.stringify(after)}::jsonb`,
      changed_fields: sql`${JSON.stringify(changedFields)}::jsonb`,
      feed_code_hash_before: previous
        ? previous.feed_code_hash_after ?? null
        : feedCodeHashAfter,
      feed_code_hash_after: feedCodeHashAfter,
      ranker_code_hash_before: previous
        ? previous.ranker_code_hash_after ?? null
        : rankerCodeHashAfter,
      ranker_code_hash_after: rankerCodeHashAfter,
    })
    .execute()

  return revision
}

async function readCatalogRow(ctx: AppContext, rkey: string): Promise<FeedCatalog | undefined> {
  return readCatalogRowFromDb(ctx.db, rkey)
}

async function studyExistsFromDb(db: any, studyId: string | null | undefined): Promise<boolean | undefined> {
  if (!studyId) return undefined
  const row = await db
    .selectFrom('feedgen_ops.study_catalog')
    .select('study_id')
    .where('study_id', '=', studyId)
    .executeTakeFirst()
  return Boolean(row)
}

async function studyExists(ctx: AppContext, studyId: string | null | undefined): Promise<boolean | undefined> {
  return studyExistsFromDb(ctx.db, studyId)
}

async function checkCatalogContractCoherence(
  trx: Transaction<DatabaseSchema>,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const rows = await trx
    .selectFrom('feedgen_ops.feed_catalog')
    .select(['rkey', 'enabled', 'publisher_time_clock', 'content_time_contract_version'])
    .where('enabled', '=', true)
    .where('publisher_time_clock', '=', 'content_time_v1')
    .execute()

  if (rows.length === 0) return { ok: true }

  const versions = new Set<string>()
  for (const row of rows) {
    const version = String(row.content_time_contract_version ?? '').trim()
    if (!version) {
      return {
        ok: false,
        error: `enabled content_time_v1 feed ${row.rkey} is missing content_time_contract_version`,
      }
    }
    versions.add(version)
  }

  if (versions.size > 1) {
    return {
      ok: false,
      error: `mutation would leave enabled content_time_v1 feeds with mixed contract versions: ${[...versions].join(', ')}`,
    }
  }

  const [version] = versions
  if (version !== 'newsflows-content-time/v2' && version !== 'newsflows-content-time/v3') {
    return {
      ok: false,
      error: `unsupported content-time contract version in catalog: ${version}`,
    }
  }

  return { ok: true }
}

export class CatalogApplyAbort extends Error {
  constructor(readonly httpStatus: number, readonly payload: unknown) {
    super('catalog apply aborted')
  }
}

async function applyCatalogUpdate(
  trx: Transaction<DatabaseSchema>,
  update: ValidatedCatalogUpdate,
  identity: FeedCatalogHistoryIdentity,
  opts: { skipCoherenceCheck?: boolean } = {},
) {
  const current = await readCatalogRowFromDb(trx, update.rkey, true)
  if (!current) throw new CatalogApplyAbort(404, feedCatalogNotFoundPayload(update.rkey))
  const proposedStudyId = update.patch.study_id !== undefined
    ? update.patch.study_id
    : current.study_id
  const dryRun = buildFeedCatalogDryRun(current, update, {
    studyExists: await studyExistsFromDb(trx, proposedStudyId),
  })
  if (dryRun.blockers.length > 0) {
    throw new CatalogApplyAbort(
      409,
      buildFeedCatalogApplyBlocked(dryRun, {
        code: 'dry-run-blocked',
        message: 'apply refused because feedgen dry-run has blockers',
      }),
    )
  }
  const mismatches = currentValueMismatches(current, update.ifCurrent)
  if (mismatches.length > 0) {
    throw new CatalogApplyAbort(409, buildFeedCatalogApplyConflict(dryRun, mismatches))
  }
  if (dryRun.change_count === 0) {
    return {
      httpStatus: 200,
      payload: buildFeedCatalogApplyResult(current, current, dryRun, false),
      applied: false,
    }
  }
  const result = await trx
    .updateTable('feedgen_ops.feed_catalog')
    .set({ ...update.patch, catalog_revision: Number(current.catalog_revision ?? 0) + 1 } as any)
    .where('rkey', '=', update.rkey)
    .executeTakeFirst()
  if (Number(result.numUpdatedRows ?? 0) === 0) {
    throw new CatalogApplyAbort(404, feedCatalogNotFoundPayload(update.rkey))
  }
  const after = await readCatalogRowFromDb(trx, update.rkey)
  if (!after) throw new CatalogApplyAbort(500, { error: 'updated row could not be read back' })
  if (!opts.skipCoherenceCheck) {
    const coherence = await checkCatalogContractCoherence(trx)
    if (!coherence.ok) {
      throw new CatalogApplyAbort(
        409,
        buildFeedCatalogApplyBlocked(dryRun, {
          code: 'incoherent-catalog-contract-version',
          message: coherence.error,
        }),
      )
    }
  }
  await appendFeedCatalogHistory(trx, current, after, dryRun.changes, identity)
  return { httpStatus: 200, payload: buildFeedCatalogApplyResult(current, after, dryRun, true), applied: true }
}

export default function registerFeedCatalogAdminEndpoint(
  server: Server,
  ctx: AppContext,
) {
  server.xrpc.router.get('/api/admin/feed_catalog', async (req, res) => {
    if (!isApiKeyAuthorized(req, readAuth)) {
      logUnauthorized('/api/admin/feed_catalog')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    let subscribableOnly: boolean
    try {
      subscribableOnly = parseSubscribableFilter(req.query?.subscribable)
    } catch (err) {
      return res.status(400).json({ error: err instanceof Error ? err.message : 'invalid subscribable filter' })
    }
    try {
      const rows = await readCatalogRows(ctx)
      return res.json(feedCatalogListPayload(rows, subscribableOnly))
    } catch (err) {
      console.error(
        `[${new Date().toISOString()}] - feed_catalog-admin: read error. ${err instanceof Error ? err.message : String(err)}`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })

  server.xrpc.router.get('/api/admin/feed_catalog/:rkey/history', async (req, res) => {
    if (!isApiKeyAuthorized(req, readAuth)) {
      logUnauthorized('/api/admin/feed_catalog/:rkey/history')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    let pagination: ReturnType<typeof parseHistoryPagination>
    try {
      pagination = parseHistoryPagination(req.query?.limit, req.query?.offset)
    } catch (err) {
      return res.status(400).json({
        error: err instanceof Error ? err.message : 'invalid pagination',
      })
    }

    const rkey = String(req.params.rkey || '')
    try {
      const feed = await readCatalogRow(ctx, rkey)
      if (!feed) return res.status(404).json(feedCatalogNotFoundPayload(rkey))
      const history = await ctx.db
        .selectFrom('feedgen_ops.feed_catalog_history')
        .selectAll()
        .where('feed_id', '=', feed.feed_id)
        .orderBy('changed_at', 'desc')
        .orderBy('revision', 'desc')
        .limit(pagination.limit)
        .offset(pagination.offset)
        .execute()
      return res.json({
        schema_version: 1,
        feed_id: feed.feed_id,
        rkey,
        returned_count: history.length,
        limit: pagination.limit,
        offset: pagination.offset,
        history,
        raw_values_in_output: false,
      })
    } catch (err) {
      console.error(
        `[${new Date().toISOString()}] - feed_catalog-admin: history read error. ${err instanceof Error ? err.message : String(err)}`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })

  server.xrpc.router.get('/api/admin/feed_catalog/:rkey', async (req, res) => {
    if (!isApiKeyAuthorized(req, readAuth)) {
      logUnauthorized('/api/admin/feed_catalog/:rkey')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    try {
      const row = await readCatalogRow(ctx, String(req.params.rkey || ''))
      if (!row) return res.status(404).json(feedCatalogNotFoundPayload(String(req.params.rkey || '')))
      return res.json(feedCatalogShowPayload(row))
    } catch (err) {
      console.error(
        `[${new Date().toISOString()}] - feed_catalog-admin: read error. ${err instanceof Error ? err.message : String(err)}`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })

  server.xrpc.router.post('/api/admin/feed_catalog/dry-run', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      logUnauthorized('/api/admin/feed_catalog/dry-run')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    const body = req.body as CatalogDryRunBody | undefined
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'JSON body required' })
    }
    const v = validateUpdate(body)
    if (!v.ok) return res.status(400).json({ error: v.error })

    try {
      const current = await readCatalogRow(ctx, v.row.rkey)
      if (!current) return res.status(404).json(feedCatalogNotFoundPayload(v.row.rkey))
      const proposedStudyId =
        v.row.patch.study_id !== undefined
          ? v.row.patch.study_id
          : current.study_id
      const result = buildFeedCatalogDryRun(current, v.row, {
        studyExists: await studyExists(ctx, proposedStudyId),
      })
      return res.json(result)
    } catch (err) {
      console.error(
        `[${new Date().toISOString()}] - feed_catalog-admin: dry-run error. ${err instanceof Error ? err.message : String(err)}`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })

  server.xrpc.router.post('/api/admin/feed_catalog/bulk', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      logUnauthorized('/api/admin/feed_catalog/bulk')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }
    const body = req.body as CatalogBulkBody | undefined
    if (!body || !Array.isArray(body.updates) || body.updates.length < 1 || body.updates.length > 100) {
      return res.status(400).json({ error: 'updates must contain 1 to 100 update rows' })
    }
    const validated: ValidatedCatalogUpdate[] = []
    for (const item of body.updates) {
      const result = validateUpdate(item)
      if (!result.ok) return res.status(400).json({ error: result.error })
      if (!result.row.ifCurrent || Object.keys(result.row.ifCurrent).length === 0) {
        return res.status(400).json({
          error: 'bulk updates require non-empty if_current CAS values for every row',
        })
      }
      validated.push(result.row)
    }
    const rkeys = validated.map((item) => item.rkey)
    if (new Set(rkeys).size !== rkeys.length) {
      return res.status(400).json({ error: 'bulk updates must contain unique rkeys' })
    }
    const identity = historyIdentity(req.headers)
    try {
      const results = await ctx.db.transaction().execute(async (trx) => {
        await sql`SELECT pg_advisory_xact_lock(hashtext('feedgen_ops.feed_catalog'))`.execute(trx)
        const rows: unknown[] = []
        // Stable lock order prevents two overlapping bulk applies deadlocking.
        for (const update of [...validated].sort((a, b) => a.rkey.localeCompare(b.rkey))) {
          const result = await applyCatalogUpdate(trx, update, identity, { skipCoherenceCheck: true })
          rows.push(result.payload)
        }
        const coherence = await checkCatalogContractCoherence(trx)
        if (!coherence.ok) {
          throw new CatalogApplyAbort(409, {
            code: 'incoherent-catalog-contract-version',
            message: coherence.error,
          })
        }
        return rows
      })
      invalidateActiveContentTimeContractCache()
      return res.json({
        schema_version: 1,
        operation: 'feed.bulk-update',
        status: 'applied',
        applied: true,
        result_count: results.length,
        results,
        raw_values_in_output: false,
      })
    } catch (err) {
      if (err instanceof CatalogApplyAbort) {
        return res.status(err.httpStatus).json({
          schema_version: 1,
          operation: 'feed.bulk-update',
          status: 'aborted',
          applied: false,
          blocker: err.payload,
          raw_values_in_output: false,
        })
      }
      console.error(
        `[${new Date().toISOString()}] - feed_catalog-admin: bulk error. ${err instanceof Error ? err.message : String(err)}`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })

  server.xrpc.router.post('/api/admin/feed_catalog', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      logUnauthorized('/api/admin/feed_catalog')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    const body = req.body as CatalogBody | undefined
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'JSON body required' })
    }
    const identity = historyIdentity(req.headers)

    try {
      if (body.op === 'insert') {
        const v = validateInsert(body)
        if (!v.ok) return res.status(400).json({ error: v.error })
        await ctx.db.transaction().execute(async (trx) => {
          await sql`SELECT pg_advisory_xact_lock(hashtext('feedgen_ops.feed_catalog'))`.execute(trx)
          await trx
            .insertInto('feedgen_ops.feed_catalog')
            .values({
              feed_id: v.row.feed_id,
              rkey: v.row.rkey,
              display_name: v.row.display_name,
              algo_policy_id: v.row.algo_policy_id,
              access_policy_id: v.row.access_policy_id,
              country: v.row.country,
              study_id: v.row.study_id,
              publisher_did: v.row.publisher_did,
              ranker_policy_id: v.row.ranker_policy_id,
              ranker_score_max_age_hours: v.row.ranker_score_max_age_hours,
              ranker_score_max_age_source: v.row.ranker_score_max_age_source,
              ranker_min_score_backed_share: v.row.ranker_min_score_backed_share,
              ranker_min_score_backed_source: v.row.ranker_min_score_backed_source,
              publisher_post_max_age_days: v.row.publisher_post_max_age_days,
              publisher_post_max_age_source: v.row.publisher_post_max_age_source,
              publisher_time_clock: v.row.publisher_time_clock,
              publisher_time_transition_expires_at: v.row.publisher_time_transition_expires_at,
              content_time_cutover_min_valid_share: v.row.content_time_cutover_min_valid_share,
              content_time_contract_version: v.row.content_time_contract_version,
              catalog_revision: 1,
              enabled: v.row.enabled,
            } as any)
            .execute()
          const after = await readCatalogRowFromDb(trx, v.row.rkey)
          if (!after) throw new CatalogApplyAbort(500, { error: 'inserted row could not be read back' })
          const coherence = await checkCatalogContractCoherence(trx)
          if (!coherence.ok) {
            throw new CatalogApplyAbort(409, {
              code: 'incoherent-catalog-contract-version',
              message: coherence.error,
            })
          }
          const emptyValues = Object.fromEntries(
            UPDATE_FIELDS.map((field) => [field, null]),
          ) as Record<UpdateField, null>
          await appendFeedCatalogHistory(
            trx,
            null,
            after,
            feedCatalogChanges(emptyValues, currentFieldValues(after), UPDATE_FIELDS),
            identity,
          )
        })
        invalidateActiveContentTimeContractCache()
        console.log(
          `[${new Date().toISOString()}] - feed_catalog-admin: INSERT rkey=${v.row.rkey} feed_id=${v.row.feed_id} algo=${v.row.algo_policy_id} access=${v.row.access_policy_id}`,
        )
        return res.json({ ok: true, op: 'insert', rkey: v.row.rkey })
      }
      if (body.op === 'update') {
        const v = validateUpdate(body)
        if (!v.ok) return res.status(400).json({ error: v.error })
        const apply = await ctx.db.transaction().execute(async (trx) => {
          await sql`SELECT pg_advisory_xact_lock(hashtext('feedgen_ops.feed_catalog'))`.execute(trx)
          return await applyCatalogUpdate(trx, v.row, identity)
        })
        if (apply.applied) {
          invalidateActiveContentTimeContractCache()
          console.log(
            `[${new Date().toISOString()}] - feed_catalog-admin: UPDATE rkey=${v.row.rkey} ${JSON.stringify(v.row.patch)}`,
          )
        }
        return res.status(apply.httpStatus).json(apply.payload)
      }
      return res.status(400).json({ error: "op must be 'insert' or 'update'" })
    } catch (err) {
      if (err instanceof CatalogApplyAbort) {
        return res.status(err.httpStatus).json(err.payload)
      }
      console.error(
        `[${new Date().toISOString()}] - feed_catalog-admin: error. ${err instanceof Error ? err.message : String(err)}`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })
}
