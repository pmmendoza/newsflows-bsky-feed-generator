/**
 * Sprint 11 / Task 4 — minimal `feed_catalog` admin write endpoint.
 *
 * Replaces ad-hoc psql edits for the common operator actions:
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
 *   - Read endpoints — describe-generator already serves the catalog
 *     listing.
 *
 * Plan: dev/storage/plan_storage_refactor/plan_feed_catalog_listen_notify.md
 */

import { Server } from '../lexicon'
import { AppContext } from '../config'
import {
  ApiKeyAuthConfig,
  isApiKeyAuthorized,
  logUnauthorized,
} from '../util/api-auth'

const adminWriteAuth: ApiKeyAuthConfig = {
  primaryEnv: ['FEEDGEN_ADMIN_API_KEY'],
}

const ALLOWED_ACCESS_POLICIES = new Set([
  'subscriber-default',
  'study-only',
  'disabled',
])

type CatalogInsertBody = {
  op: 'insert'
  feed_id: string
  rkey: string
  algo_policy_id: string
  access_policy_id: string
  study_id?: string | null
  publisher_did?: string | null
  enabled?: boolean
}

type CatalogUpdateBody = {
  op: 'update'
  rkey: string
  enabled?: boolean
  access_policy_id?: string
  study_id?: string | null
  retired_at?: string | null
}

type CatalogBody = CatalogInsertBody | CatalogUpdateBody

function isString(v: unknown): v is string {
  return typeof v === 'string' && v.length > 0
}

function validateInsert(body: any): { ok: true; row: CatalogInsertBody } | { ok: false; error: string } {
  if (!isString(body?.feed_id)) return { ok: false, error: 'feed_id required' }
  if (!isString(body?.rkey)) return { ok: false, error: 'rkey required' }
  if (body.rkey.length > 15) return { ok: false, error: 'rkey must be ≤15 chars (ATProto record-key constraint)' }
  if (!isString(body?.algo_policy_id)) return { ok: false, error: 'algo_policy_id required' }
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
      algo_policy_id: body.algo_policy_id,
      access_policy_id: body.access_policy_id,
      study_id: body.study_id ?? null,
      publisher_did: body.publisher_did ?? null,
      enabled: typeof body.enabled === 'boolean' ? body.enabled : true,
    },
  }
}

function validateUpdate(body: any): { ok: true; row: CatalogUpdateBody } | { ok: false; error: string } {
  if (!isString(body?.rkey)) return { ok: false, error: 'rkey required' }
  if (
    body.access_policy_id !== undefined &&
    !ALLOWED_ACCESS_POLICIES.has(body.access_policy_id)
  ) {
    return { ok: false, error: `access_policy_id must be one of ${[...ALLOWED_ACCESS_POLICIES].join(', ')}` }
  }
  const updates = {
    enabled: body.enabled,
    access_policy_id: body.access_policy_id,
    study_id: body.study_id,
    retired_at: body.retired_at,
  }
  if (Object.values(updates).every((v) => v === undefined)) {
    return { ok: false, error: 'at least one of enabled/access_policy_id/study_id/retired_at required' }
  }
  return { ok: true, row: { op: 'update', rkey: body.rkey, ...updates } }
}

export default function registerFeedCatalogAdminEndpoint(
  server: Server,
  ctx: AppContext,
) {
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
            algo_policy_id: v.row.algo_policy_id,
            access_policy_id: v.row.access_policy_id,
            study_id: v.row.study_id,
            publisher_did: v.row.publisher_did,
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
        const patch: any = {}
        if (v.row.enabled !== undefined) patch.enabled = v.row.enabled
        if (v.row.access_policy_id !== undefined)
          patch.access_policy_id = v.row.access_policy_id
        if (v.row.study_id !== undefined) patch.study_id = v.row.study_id
        if (v.row.retired_at !== undefined) patch.retired_at = v.row.retired_at
        const result = await ctx.db
          .updateTable('feedgen_ops.feed_catalog')
          .set(patch)
          .where('rkey', '=', v.row.rkey)
          .executeTakeFirst()
        const numUpdated = Number(result.numUpdatedRows ?? 0)
        if (numUpdated === 0) {
          return res.status(404).json({ error: `rkey=${v.row.rkey} not found` })
        }
        console.log(
          `[${new Date().toISOString()}] - feed_catalog-admin: UPDATE rkey=${v.row.rkey} ${JSON.stringify(patch)}`,
        )
        return res.json({ ok: true, op: 'update', rkey: v.row.rkey, fields: Object.keys(patch) })
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
