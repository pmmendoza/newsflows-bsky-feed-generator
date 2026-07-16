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
 * Auth: FEEDGEN_ADMIN_API_KEY only (same as /api/update-engagement).
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
import { FeedCatalog } from '../db/schema'
import {
  ApiKeyAuthConfig,
  isApiKeyAuthorized,
  logUnauthorized,
} from '../util/api-auth'
import { isSubscribableFeed } from '../util/subscribable-feed'

const adminWriteAuth: ApiKeyAuthConfig = {
  primaryEnv: ['FEEDGEN_ADMIN_API_KEY'],
}

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
  enabled?: boolean
}

type CatalogUpdateBody = {
  op: 'update'
  rkey: string
  display_name?: string
  publisher_did?: string | null
  algo_policy_id?: string
  ranker_policy_id?: string | null
  enabled?: boolean
  access_policy_id?: string
  study_id?: string | null
  retired_at?: string | null
  if_current?: Partial<Record<UpdateField, boolean | string | null>>
}

type CatalogBody = CatalogInsertBody | CatalogUpdateBody

type CatalogDryRunBody = Omit<CatalogUpdateBody, 'op'> & {
  op?: 'update'
}

type CatalogUpdatePatch = Partial<Pick<
  FeedCatalog,
  | 'display_name'
  | 'publisher_did'
  | 'algo_policy_id'
  | 'ranker_policy_id'
  | 'enabled'
  | 'access_policy_id'
  | 'study_id'
  | 'retired_at'
>>

type ValidatedCatalogUpdate = {
  op: 'update'
  rkey: string
  patch: CatalogUpdatePatch
  ifCurrent?: Partial<Record<UpdateField, boolean | string | null>>
}

type FeedCatalogDryRunMessage = {
  code: string
  message: string
  [key: string]: unknown
}

function isString(v: unknown): v is string {
  return typeof v === 'string' && v.length > 0
}

function nullableString(v: unknown): v is string | null {
  return v === null || typeof v === 'string'
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
  ) as Record<UpdateField, boolean | string | null>
}

function proposedFieldValues(row: Pick<FeedCatalog, UpdateField>, patch: CatalogUpdatePatch) {
  const proposed = { ...row, ...patch }
  return currentFieldValues(proposed)
}

function validateCurrentValues(
  value: unknown,
): { ok: true; current?: Partial<Record<UpdateField, boolean | string | null>> } | { ok: false; error: string } {
  if (value === undefined) return { ok: true }
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return { ok: false, error: 'if_current must be an object when provided' }
  }
  const current: Partial<Record<UpdateField, boolean | string | null>> = {}
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
    if (
      field === 'access_policy_id' &&
      (typeof fieldValue !== 'string' || !ALLOWED_ACCESS_POLICIES.has(fieldValue))
    ) {
      return { ok: false, error: `if_current.access_policy_id must be one of ${[...ALLOWED_ACCESS_POLICIES].join(', ')}` }
    }
    if ((field === 'study_id' || field === 'retired_at') && !nullableString(fieldValue)) {
      return { ok: false, error: `if_current.${field} must be string or null` }
    }
    current[field] = fieldValue as boolean | string | null
  }
  return { ok: true, current }
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
  return {
    feed_id: row.feed_id,
    rkey: row.rkey,
    display_name: row.display_name,
    country: row.country ?? null,
    publisher_did: row.publisher_did ?? null,
    study_id: row.study_id ?? null,
    algo_policy_id: row.algo_policy_id,
    ranker_policy_id: row.ranker_policy_id ?? null,
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
      enabled: typeof body.enabled === 'boolean' ? body.enabled : true,
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
  const updates = {
    display_name: body.display_name,
    publisher_did: body.publisher_did,
    algo_policy_id: body.algo_policy_id,
    ranker_policy_id: body.ranker_policy_id,
    enabled: body.enabled,
    access_policy_id: body.access_policy_id,
    study_id: body.study_id,
    retired_at: body.retired_at,
  }
  if (Object.values(updates).every((v) => v === undefined)) {
    return { ok: false, error: 'at least one of enabled/access_policy_id/study_id/retired_at required' }
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
  const changes = UPDATE_FIELDS
    .filter((field) => Object.prototype.hasOwnProperty.call(update.patch, field))
    .filter((field) => currentValues[field] !== proposedValues[field])
    .map((field) => ({
      field,
      current: currentValues[field],
      proposed: proposedValues[field],
    }))
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
  expected: Partial<Record<UpdateField, boolean | string | null>> | undefined,
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
      'access_policy_id',
      'enabled',
      'created_at',
      'retired_at',
    ])
    .orderBy('rkey', 'asc')
    .execute()) as FeedCatalog[]
}

async function readCatalogRowFromDb(db: any, rkey: string): Promise<FeedCatalog | undefined> {
  return (await db
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
      'access_policy_id',
      'enabled',
      'created_at',
      'retired_at',
    ])
    .where('rkey', '=', rkey)
    .executeTakeFirst()) as FeedCatalog | undefined
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

export default function registerFeedCatalogAdminEndpoint(
  server: Server,
  ctx: AppContext,
) {
  server.xrpc.router.get('/api/admin/feed_catalog', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
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

  server.xrpc.router.get('/api/admin/feed_catalog/:rkey', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
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

  server.xrpc.router.post('/api/admin/feed_catalog', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      logUnauthorized('/api/admin/feed_catalog')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    const body = req.body as CatalogBody | undefined
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'JSON body required' })
    }

    try {
      if (body.op === 'insert') {
        const v = validateInsert(body)
        if (!v.ok) return res.status(400).json({ error: v.error })
        await ctx.db
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
            enabled: v.row.enabled,
          } as any)
          .execute()
        console.log(
          `[${new Date().toISOString()}] - feed_catalog-admin: INSERT rkey=${v.row.rkey} feed_id=${v.row.feed_id} algo=${v.row.algo_policy_id} access=${v.row.access_policy_id}`,
        )
        return res.json({ ok: true, op: 'insert', rkey: v.row.rkey })
      }
      if (body.op === 'update') {
        const v = validateUpdate(body)
        if (!v.ok) return res.status(400).json({ error: v.error })
        const apply = await ctx.db.transaction().execute(async (trx) => {
          const current = await readCatalogRowFromDb(trx, v.row.rkey)
          if (!current) {
            return {
              httpStatus: 404,
              payload: feedCatalogNotFoundPayload(v.row.rkey),
              applied: false,
            }
          }
          const proposedStudyId =
            v.row.patch.study_id !== undefined
              ? v.row.patch.study_id
              : current.study_id
          const dryRun = buildFeedCatalogDryRun(current, v.row, {
            studyExists: await studyExistsFromDb(trx, proposedStudyId),
          })
          if (dryRun.blockers.length > 0) {
            return {
              httpStatus: 409,
              payload: buildFeedCatalogApplyBlocked(
                dryRun,
                {
                  code: 'dry-run-blocked',
                  message: 'apply refused because feedgen dry-run has blockers',
                },
              ),
              applied: false,
            }
          }
          const mismatches = currentValueMismatches(current, v.row.ifCurrent)
          if (mismatches.length > 0) {
            return {
              httpStatus: 409,
              payload: buildFeedCatalogApplyConflict(dryRun, mismatches),
              applied: false,
            }
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
            .set(v.row.patch as any)
            .where('rkey', '=', v.row.rkey)
            .executeTakeFirst()
          const numUpdated = Number(result.numUpdatedRows ?? 0)
          if (numUpdated === 0) {
            return {
              httpStatus: 404,
              payload: feedCatalogNotFoundPayload(v.row.rkey),
              applied: false,
            }
          }
          const after = await readCatalogRowFromDb(trx, v.row.rkey)
          if (!after) {
            return {
              httpStatus: 500,
              payload: { error: 'updated row could not be read back' },
              applied: false,
            }
          }
          return {
            httpStatus: 200,
            payload: buildFeedCatalogApplyResult(current, after, dryRun, true),
            applied: true,
          }
        })
        if (apply.applied) {
          console.log(
            `[${new Date().toISOString()}] - feed_catalog-admin: UPDATE rkey=${v.row.rkey} ${JSON.stringify(v.row.patch)}`,
          )
        }
        return res.status(apply.httpStatus).json(apply.payload)
      }
      return res.status(400).json({ error: "op must be 'insert' or 'update'" })
    } catch (err) {
      console.error(
        `[${new Date().toISOString()}] - feed_catalog-admin: error. ${err instanceof Error ? err.message : String(err)}`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })
}
