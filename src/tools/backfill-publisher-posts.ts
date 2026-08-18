import crypto from 'crypto'
import fs from 'fs'
import path from 'path'
import { sql } from 'kysely'
import { createDb } from '../db'
import type { Database } from '../db'
import { dualWriteLinkFields } from '../util/link-fields'
import {
  CONTENT_TIME_VALIDATOR_VERSION,
  CONTENT_TIME_VALIDATOR_VERSION_V1,
  validateContentTime,
} from '../util/content-time'
import type { ContentTimeClampReason } from '../db/schema'
import { resolvePublisherDids } from '../util/publisher-dids'

type AppViewPost = {
  uri?: string
  cid?: string
  author?: { did?: string }
  indexedAt?: string
  record?: {
    text?: string
    createdAt?: string
    reply?: {
      root?: { uri?: string; cid?: string }
    }
    embed?: any
  }
}

export type AuthorFeedPage = {
  posts: AppViewPost[]
  cursor?: string
}

export type BackfillPostRow = {
  uri: string
  cid: string
  indexedAt: string
  createdAt: string
  created_at_source_raw: Buffer
  content_time_utc: string | null
  content_time_status: 'source_valid' | 'source_invalid'
  content_time_clamp_reason: 'missing' | 'unparseable' | 'future_skew' | 'past_bound' | null
  content_time_validator_version: string
  author: string
  text: string
  rootUri: string
  rootCid: string
  link_uri: string
  link_title: string
  link_description: string
  linkUrl: string
  linkTitle: string
  linkDescription: string
}

export type BackfillPlan = {
  posts: BackfillPostRow[]
  scanned: number
  skipped_out_of_window: number
  skipped_wrong_author: number
  by_actor: Record<string, {
    scanned: number
    candidate_posts: number
    skipped_out_of_window: number
    skipped_wrong_author: number
  }>
}

type FetchPage = (actor: string, cursor?: string) => Promise<AuthorFeedPage>

type CollectOptions = {
  actors: string[]
  since: Date
  until: Date
  fetchPage: FetchPage
  maxPagesPerActor?: number
  deadlineMs?: number
  // Which post timestamp bounds the [since, until) window: the record's own
  // createdAt (default, historical behaviour) or the AppView indexedAt (matches
  // a public.post population bounded by "indexedAt").
  windowField?: 'createdAt' | 'indexedAt'
  // Stop paging an actor once an entire page lies before `since` by indexedAt
  // (the author feed is ordered by indexedAt, so nothing newer can follow).
  stopWhenPageBeforeSince?: boolean
}

type CliOptions = {
  actors: string[]
  since: Date
  until: Date
  apply: boolean
  json: boolean
  apiBase: string
  maxPagesPerActor: number
  dbUrl?: string
  checkpointFile?: string
  packetSha256?: string
  maxBatches?: number
  pauseBaselineBytesPerSecond?: number
  noInsert: boolean
  windowField: 'createdAt' | 'indexedAt'
}

export type PublisherPostRecoveryProgress = {
  batch: number
  scanned: number
  inserted: number
  recovered: number
  already_current: number
  skipped_missing: number
  cursor_sha256: string
  elapsed_ms: number
  wal_bytes: number
  relation_bytes_before: number
  relation_bytes_after: number
}

// Per-batch receipt for the recovery apply path (mirrors the revalidate
// receipts so the same packet runner ceilings -- per-batch WAL, relation
// growth, adaptive pause paid -- apply to both modes).
export type PublisherPostRecoveryBatch = {
  batch: number
  candidates: number
  inserted: number
  recovered: number
  already_current: number
  skipped_missing: number
  cursor_uri: string
  elapsed_ms: number
  wal_bytes: number
  relation_bytes_before: number
  relation_bytes_after: number
  pause_ms: number
  pause_required_ms: number
}

export type PublisherPostRecoveryResult = PublisherPostRecoveryProgress & {
  complete: boolean
  batches: PublisherPostRecoveryBatch[]
}

export type PublisherPostRecoveryOptions = {
  posts: BackfillPostRow[]
  packetSha256: string
  batchSize: number
  afterUri?: string
  planSha256?: string
  maxBatches?: number
  maxDurationMs: number
  pauseMs: number
  // Adaptive pause (D4-b): when set, each batch pauses max(pauseMs, wal_bytes / rate).
  pauseBaselineBytesPerSecond?: number
  // When true, posts present in the AppView but missing from public.post are
  // NOT inserted (counted as skipped_missing) -- classification-only packets.
  noInsert?: boolean
  lockTimeoutMs: number
  statementTimeoutMs: number
  onProgress?: (progress: PublisherPostRecoveryProgress) => void
  onCheckpoint?: (
    checkpoint: PublisherPostRecoveryProgress & {
      packet_sha256: string
      plan_sha256: string
      cursor_uri: string
    },
  ) => void
}

export const RECOVERY_LIMITS = {
  batchSize: 500,
  maxDurationMs: 30 * 60 * 1000,
  pauseMs: 1000,
  lockTimeoutMs: 5000,
  statementTimeoutMs: 30_000,
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const sha256 = (value: string) => crypto.createHash('sha256').update(value).digest('hex')

function sanitizeForPostgres(text: string | null | undefined): string {
  if (text === null || text === undefined) return ''
  return text.replace(/\0/g, '')
}

function externalEmbed(embed: any): { uri?: string; title?: string; description?: string } | null {
  if (embed?.external && typeof embed.external.uri === 'string') return embed.external
  return null
}

function parseDate(value: string | undefined): Date | null {
  if (!value) return null
  const parsed = new Date(value)
  return isNaN(parsed.getTime()) ? null : parsed
}

export function normalizeAppViewPost(
  post: AppViewPost,
  expectedAuthorDid: string,
): BackfillPostRow | null {
  if (!post.uri || !post.cid) return null
  if (post.author?.did !== expectedAuthorDid) return null

  const indexedAt = post.indexedAt || new Date().toISOString()
  const record = post.record || {}
  const embed = externalEmbed(record.embed)
  const contentTime = validateContentTime(record.createdAt, indexedAt)

  return {
    uri: post.uri,
    cid: post.cid,
    indexedAt,
    createdAt: contentTime.legacy_created_at,
    created_at_source_raw: contentTime.created_at_source_raw,
    content_time_utc: contentTime.content_time_utc,
    content_time_status: contentTime.content_time_status,
    content_time_clamp_reason: contentTime.content_time_clamp_reason,
    content_time_validator_version: contentTime.content_time_validator_version,
    author: expectedAuthorDid,
    text: sanitizeForPostgres(record.text),
    rootUri: record.reply?.root?.uri || '',
    rootCid: record.reply?.root?.cid || '',
    ...dualWriteLinkFields({
      link_uri: embed?.uri || '',
      link_title: sanitizeForPostgres(embed?.title),
      link_description: sanitizeForPostgres(embed?.description),
    }),
  }
}

export async function collectPublisherPosts(options: CollectOptions): Promise<BackfillPlan> {
  const maxPagesPerActor = options.maxPagesPerActor ?? 50
  const posts = new Map<string, BackfillPostRow>()
  let scanned = 0
  let skippedOutOfWindow = 0
  let skippedWrongAuthor = 0
  const byActor: BackfillPlan['by_actor'] = {}

  for (const actor of options.actors) {
    if (options.deadlineMs !== undefined && Date.now() >= options.deadlineMs) {
      throw new Error('publisher recovery collection exceeded its deadline')
    }
    byActor[actor] = {
      scanned: 0,
      candidate_posts: 0,
      skipped_out_of_window: 0,
      skipped_wrong_author: 0,
    }
    let cursor: string | undefined
    for (let page = 0; page < maxPagesPerActor; page += 1) {
      if (options.deadlineMs !== undefined && Date.now() >= options.deadlineMs) {
        throw new Error('publisher recovery collection exceeded its deadline')
      }
      const authorPage = await options.fetchPage(actor, cursor)
      if (!authorPage.posts.length) break

      let pageAllBeforeSince = true
      for (const post of authorPage.posts) {
        const indexed = parseDate(post.indexedAt)
        if (!indexed || indexed >= options.since) pageAllBeforeSince = false
        scanned += 1
        byActor[actor].scanned += 1
        const normalized = normalizeAppViewPost(post, actor)
        if (!normalized) {
          skippedWrongAuthor += 1
          byActor[actor].skipped_wrong_author += 1
          continue
        }
        const created = parseDate(normalized.createdAt)
        const windowTime = options.windowField === 'indexedAt' ? parseDate(post.indexedAt) : created
        if (!created || !windowTime || windowTime < options.since || windowTime >= options.until) {
          skippedOutOfWindow += 1
          byActor[actor].skipped_out_of_window += 1
          continue
        }
        posts.set(normalized.uri, normalized)
        byActor[actor].candidate_posts += 1
      }

      if (!authorPage.cursor) break
      if (options.stopWhenPageBeforeSince && pageAllBeforeSince) break
      cursor = authorPage.cursor
    }
  }

  return {
    posts: [...posts.values()],
    scanned,
    skipped_out_of_window: skippedOutOfWindow,
    skipped_wrong_author: skippedWrongAuthor,
    by_actor: byActor,
  }
}

type ExistingContentTime = {
  uri: string
  cid: string
  author: string
  indexedAt: string
  created_at_source_raw: Buffer | null
  content_time_utc: string | null
  content_time_status: string | null
  content_time_clamp_reason: string | null
  content_time_validator_version: string | null
}

function recoveryPlanSha256(posts: BackfillPostRow[]): string {
  return sha256(JSON.stringify(posts.map(({ created_at_source_raw, ...row }) => ({
    ...row,
    created_at_source_raw_hex: created_at_source_raw.toString('hex'),
  }))))
}

function revalidateForExisting(
  current: ExistingContentTime,
  expected: BackfillPostRow,
): BackfillPostRow {
  if (current.cid !== expected.cid || current.author !== expected.author) {
    throw new Error(`content-time recovery revision conflict uri_sha256=${sha256(expected.uri)}`)
  }
  const validated = validateContentTime(expected.created_at_source_raw.toString('utf8'), current.indexedAt)
  return {
    ...expected,
    indexedAt: current.indexedAt,
    created_at_source_raw: validated.created_at_source_raw,
    content_time_utc: validated.content_time_utc,
    content_time_status: validated.content_time_status,
    content_time_clamp_reason: validated.content_time_clamp_reason,
    content_time_validator_version: validated.content_time_validator_version,
  }
}

function matchesRecovery(row: ExistingContentTime, expected: BackfillPostRow): boolean {
  return (
    row.created_at_source_raw?.equals(expected.created_at_source_raw) === true &&
    row.content_time_utc === expected.content_time_utc &&
    row.content_time_status === expected.content_time_status &&
    row.content_time_clamp_reason === expected.content_time_clamp_reason &&
    row.content_time_validator_version === expected.content_time_validator_version
  )
}

function isUnclassified(row: ExistingContentTime): boolean {
  return (
    row.created_at_source_raw === null &&
    row.content_time_utc === null &&
    (row.content_time_status === null || row.content_time_status === 'legacy_unknown') &&
    row.content_time_clamp_reason === null &&
    row.content_time_validator_version === null
  )
}

async function applyRecoveryBatch(
  db: Database,
  batch: BackfillPostRow[],
  lockTimeoutMs: number,
  statementTimeoutMs: number,
  deadlineMs: number,
  noInsert = false,
) {
  return db.transaction().execute(async (trx) => {
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0) throw new Error('content-time recovery deadline reached')
    await sql`SELECT set_config('transaction_timeout', ${`${remainingMs}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('lock_timeout', ${`${Math.min(lockTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('statement_timeout', ${`${Math.min(statementTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)
    // WAL attribution for this batch's transaction only (same measurement as
    // the revalidate path): cluster-wide insert LSN before/after.
    const before = (await sql<{ lsn: string; relation_bytes: string }>`
      SELECT pg_current_wal_insert_lsn()::text AS lsn,
             pg_total_relation_size('public.post')::text AS relation_bytes
    `.execute(trx)).rows[0]

    const uris = batch.map((row) => row.uri)
    const existingRows = (await sql<ExistingContentTime>`
      SELECT uri, cid, author, "indexedAt", created_at_source_raw, content_time_utc, content_time_status,
             content_time_clamp_reason, content_time_validator_version
      FROM public.post
      WHERE uri = ANY(${uris}::text[])
      FOR UPDATE
    `.execute(trx)).rows
    const existing = new Map(existingRows.map((row) => [row.uri, row]))
    const inserts: BackfillPostRow[] = []
    const recoveries: BackfillPostRow[] = []
    let alreadyCurrent = 0

    let skippedMissing = 0
    for (const row of batch) {
      const current = existing.get(row.uri)
      if (!current) {
        if (noInsert) skippedMissing += 1
        else inserts.push(row)
      } else {
        const expected = revalidateForExisting(current, row)
        if (matchesRecovery(current, expected)) alreadyCurrent += 1
        else if (isUnclassified(current)) recoveries.push(expected)
        else throw new Error(`content-time recovery CAS conflict uri_sha256=${sha256(row.uri)}`)
      }
    }

    if (inserts.length) {
      const inserted = await trx
        .insertInto('post')
        .values(inserts)
        .onConflict((oc) => oc.column('uri').doNothing())
        .returning('uri')
        .execute()
      if (inserted.length !== inserts.length) {
        throw new Error('content-time recovery insert CAS conflict')
      }
    }

    if (recoveries.length) {
      const payload = recoveries.map((row) => ({
        uri: row.uri,
        raw_hex: row.created_at_source_raw.toString('hex'),
        content_time_utc: row.content_time_utc,
        content_time_status: row.content_time_status,
        content_time_clamp_reason: row.content_time_clamp_reason,
        content_time_validator_version: row.content_time_validator_version,
      }))
      const recovered = await sql<{ uri: string }>`
        UPDATE public.post AS target
        SET created_at_source_raw = decode(batch.raw_hex, 'hex'),
            content_time_utc = batch.content_time_utc,
            content_time_status = batch.content_time_status,
            content_time_clamp_reason = batch.content_time_clamp_reason,
            content_time_validator_version = batch.content_time_validator_version
        FROM jsonb_to_recordset(${JSON.stringify(payload)}::jsonb) AS batch(
          uri text,
          raw_hex text,
          content_time_utc text,
          content_time_status text,
          content_time_clamp_reason text,
          content_time_validator_version text
        )
        WHERE target.uri = batch.uri
          AND target.created_at_source_raw IS NULL
          AND target.content_time_utc IS NULL
          AND (target.content_time_status IS NULL OR target.content_time_status = 'legacy_unknown')
          AND target.content_time_clamp_reason IS NULL
          AND target.content_time_validator_version IS NULL
        RETURNING target.uri
      `.execute(trx)
      if (recovered.rows.length !== recoveries.length) {
        throw new Error('content-time recovery update CAS conflict')
      }
    }

    const after = (await sql<{ wal_bytes: string; relation_bytes: string }>`
      SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), ${before.lsn}::pg_lsn)::text AS wal_bytes,
             pg_total_relation_size('public.post')::text AS relation_bytes
    `.execute(trx)).rows[0]

    return {
      inserted: inserts.length,
      recovered: recoveries.length,
      already_current: alreadyCurrent,
      skipped_missing: skippedMissing,
      walBytes: Number(after.wal_bytes),
      relationBytesBefore: Number(before.relation_bytes),
      relationBytesAfter: Number(after.relation_bytes),
    }
  })
}

// Read-only preview of what a recovery apply would do to public.post for the
// collected plan (no AppView state is changed; nothing is written): per-URI
// classification against the current rows plus the DB-side residual -- legacy
// rows of the actors inside [since, until) by "indexedAt" that the plan does
// not cover (they would stay unclassified after the apply).
export type PublisherPostRecoveryPreview = {
  candidates: number
  would_insert: number
  would_recover: number
  already_current: number
  conflict: number
  recover_by_status: { source_valid: number; source_invalid: number }
  recover_by_reason: Partial<Record<ContentTimeClampReason, number>>
  db_legacy_in_window: number
  db_legacy_not_in_plan: number
}

export async function previewPublisherPostRecovery(
  db: Database,
  posts: BackfillPostRow[],
  actors: string[],
  since: Date,
  until: Date,
): Promise<PublisherPostRecoveryPreview> {
  const preview: PublisherPostRecoveryPreview = {
    candidates: posts.length,
    would_insert: 0,
    would_recover: 0,
    already_current: 0,
    conflict: 0,
    recover_by_status: { source_valid: 0, source_invalid: 0 },
    recover_by_reason: {},
    db_legacy_in_window: 0,
    db_legacy_not_in_plan: 0,
  }
  const uris = posts.map((row) => row.uri)
  for (let offset = 0; offset < uris.length; offset += 1000) {
    const slice = uris.slice(offset, offset + 1000)
    const rows = (await sql<ExistingContentTime>`
      SELECT uri, cid, author, "indexedAt", created_at_source_raw, content_time_utc, content_time_status,
             content_time_clamp_reason, content_time_validator_version
      FROM public.post WHERE uri = ANY(${slice}::text[])
    `.execute(db)).rows
    const existing = new Map(rows.map((row) => [row.uri, row]))
    for (const row of posts.slice(offset, offset + 1000)) {
      const current = existing.get(row.uri)
      if (!current) { preview.would_insert += 1; continue }
      let expected: BackfillPostRow
      try { expected = revalidateForExisting(current, row) } catch { preview.conflict += 1; continue }
      if (matchesRecovery(current, expected)) preview.already_current += 1
      else if (isUnclassified(current)) {
        preview.would_recover += 1
        preview.recover_by_status[expected.content_time_status] += 1
        if (expected.content_time_clamp_reason) {
          preview.recover_by_reason[expected.content_time_clamp_reason] =
            (preview.recover_by_reason[expected.content_time_clamp_reason] ?? 0) + 1
        }
      } else preview.conflict += 1
    }
  }
  const residual = (await sql<{ in_window: string; not_in_plan: string }>`
    SELECT count(*)::text AS in_window,
           count(*) FILTER (WHERE NOT (uri = ANY(${uris}::text[])))::text AS not_in_plan
    FROM public.post
    WHERE author = ANY(${actors}::text[])
      AND "indexedAt" >= ${since.toISOString()} AND "indexedAt" < ${until.toISOString()}
      AND created_at_source_raw IS NULL
      AND (content_time_status IS NULL OR content_time_status = 'legacy_unknown')
  `.execute(db)).rows[0]
  preview.db_legacy_in_window = Number(residual.in_window)
  preview.db_legacy_not_in_plan = Number(residual.not_in_plan)
  return preview
}

export async function runPublisherPostRecovery(
  db: Database,
  options: PublisherPostRecoveryOptions,
): Promise<PublisherPostRecoveryResult> {
  if (!/^[0-9a-f]{64}$/.test(options.packetSha256)) {
    throw new Error('packetSha256 must be a lowercase SHA-256')
  }
  if (!Number.isInteger(options.batchSize) || options.batchSize < 1 || options.batchSize > 500) {
    throw new Error('batchSize must be an integer from 1 to 500')
  }
  for (const [name, value] of [
    ['maxDurationMs', options.maxDurationMs],
    ['pauseMs', options.pauseMs],
    ['lockTimeoutMs', options.lockTimeoutMs],
    ['statementTimeoutMs', options.statementTimeoutMs],
  ] as const) {
    if (!Number.isInteger(value) || value < (name === 'pauseMs' ? 0 : 1)) {
      throw new Error(`${name} must be a ${name === 'pauseMs' ? 'non-negative' : 'positive'} integer`)
    }
  }

  const plan = [...options.posts].sort((left, right) => left.uri.localeCompare(right.uri))
  if (new Set(plan.map((row) => row.uri)).size !== plan.length) {
    throw new Error('recovery input contains duplicate URIs')
  }
  const planSha256 = recoveryPlanSha256(plan)
  if (options.afterUri) {
    if (options.planSha256 !== planSha256 || !plan.some((row) => row.uri === options.afterUri)) {
      throw new Error('checkpoint does not match the immutable recovery plan')
    }
  }
  const ordered = plan.filter((row) => !options.afterUri || row.uri > options.afterUri)

  const startedAt = Date.now()
  const deadlineMs = startedAt + options.maxDurationMs
  let batch = 0
  let scanned = 0
  let inserted = 0
  let recovered = 0
  let alreadyCurrent = 0
  let skippedMissing = 0
  let cursorUri = options.afterUri ?? ''
  let complete = true
  const batches: PublisherPostRecoveryBatch[] = []
  let lastWalBytes = 0
  let lastRelationBefore = 0
  let lastRelationAfter = 0

  for (let offset = 0; offset < ordered.length; offset += options.batchSize) {
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0 || (options.maxBatches !== undefined && batch >= options.maxBatches)) {
      complete = false
      break
    }
    const rows = ordered.slice(offset, offset + options.batchSize)
    const batchStartedAt = Date.now()
    const result = await applyRecoveryBatch(
      db,
      rows,
      options.lockTimeoutMs,
      Math.min(options.statementTimeoutMs, remainingMs),
      deadlineMs,
      options.noInsert === true,
    )
    const batchElapsedMs = Date.now() - batchStartedAt
    batch += 1
    scanned += rows.length
    inserted += result.inserted
    recovered += result.recovered
    alreadyCurrent += result.already_current
    skippedMissing += result.skipped_missing
    cursorUri = rows[rows.length - 1].uri
    lastWalBytes = result.walBytes
    lastRelationBefore = result.relationBytesBefore
    lastRelationAfter = result.relationBytesAfter
    // Adaptive pause (D4-b): pay this batch's WAL back at the baseline rate,
    // after every batch (including the last), clipped by the hard deadline.
    const pauseRequiredMs = options.pauseBaselineBytesPerSecond && options.pauseBaselineBytesPerSecond > 0
      ? Math.max(options.pauseMs, Math.ceil((result.walBytes * 1000) / options.pauseBaselineBytesPerSecond))
      : options.pauseMs
    const pauseSleptMs = Math.min(pauseRequiredMs, Math.max(0, deadlineMs - Date.now()))
    if (pauseSleptMs > 0) await sleep(pauseSleptMs)
    batches.push({
      batch,
      candidates: rows.length,
      inserted: result.inserted,
      recovered: result.recovered,
      already_current: result.already_current,
      skipped_missing: result.skipped_missing,
      cursor_uri: cursorUri,
      elapsed_ms: batchElapsedMs,
      wal_bytes: result.walBytes,
      relation_bytes_before: result.relationBytesBefore,
      relation_bytes_after: result.relationBytesAfter,
      pause_ms: pauseSleptMs,
      pause_required_ms: pauseRequiredMs,
    })
    const progress: PublisherPostRecoveryProgress = {
      batch,
      scanned,
      inserted,
      recovered,
      already_current: alreadyCurrent,
      skipped_missing: skippedMissing,
      cursor_sha256: sha256(cursorUri),
      elapsed_ms: Date.now() - startedAt,
      wal_bytes: result.walBytes,
      relation_bytes_before: result.relationBytesBefore,
      relation_bytes_after: result.relationBytesAfter,
    }
    options.onProgress?.(progress)
    options.onCheckpoint?.({
      ...progress,
      packet_sha256: options.packetSha256,
      plan_sha256: planSha256,
      cursor_uri: cursorUri,
    })
  }

  return {
    batch,
    scanned,
    inserted,
    recovered,
    already_current: alreadyCurrent,
    skipped_missing: skippedMissing,
    cursor_sha256: cursorUri ? sha256(cursorUri) : '',
    elapsed_ms: Date.now() - startedAt,
    wal_bytes: lastWalBytes,
    relation_bytes_before: lastRelationBefore,
    relation_bytes_after: lastRelationAfter,
    complete,
    batches,
  }
}

// --- Content-time revalidation mode -------------------------------------
//
// Re-validates existing public.post rows that were classified by validator
// v1 (`newsflows-content-time/v1`) against the current v2 policy
// (`CONTENT_TIME_POLICY_V2` in ../util/content-time). Unlike the AppView
// recovery mode above, this mode never touches the network: it reads
// created_at_source_raw + indexedAt already stored in Postgres and
// recomputes content_time_utc/status/clamp_reason/validator_version purely
// in-process. It reuses the same bounded-batch contract (batch size,
// lock/statement timeouts, inter-batch pause, 30-minute hard stop, durable
// resumable checkpoint) established by runPublisherPostRecovery, but the
// batch loop and CAS predicate are specific to a DB-only "recompute in
// place" operation, so they are implemented as sibling functions rather than
// forced through the AppView-shaped `posts: BackfillPostRow[]` API.
//
// engagement rows are out of scope for this mode; only public.post is
// touched. legacy_unknown rows (created_at_source_raw IS NULL) and rows
// already on CONTENT_TIME_VALIDATOR_VERSION (v2) are never selected.

export const REVALIDATION_LIMITS = {
  batchSize: 500,
  maxDurationMs: 30 * 60 * 1000,
  pauseMs: 1000,
  lockTimeoutMs: 5000,
  statementTimeoutMs: 30_000,
}

export type ContentTimeRevalidationOutcome =
  | 'v1_valid_to_v2_valid'
  | 'v1_invalid_to_v2_valid'
  | 'v1_to_v2_invalid'

export type ContentTimeRevalidationCandidate = {
  uri: string
  author: string
  indexedAt: string
  created_at_source_raw: Buffer
  content_time_status: string
}

export type RevalidatedContentTime = {
  createdAt: string
  content_time_utc: string | null
  content_time_status: 'source_valid' | 'source_invalid'
  content_time_clamp_reason: ContentTimeClampReason | null
  outcome: ContentTimeRevalidationOutcome
}

// Pure transform: given a v1-classified row's stored raw source time and
// receipt time (indexedAt), recompute against the *current* content-time
// policy (imported validateContentTime always uses CONTENT_TIME_POLICY_V2
// unless a caller overrides the policy argument, which this tool never
// does). No DB or network access — fully unit-testable.
export function revalidateContentTimeCandidate(
  candidate: Pick<ContentTimeRevalidationCandidate, 'indexedAt' | 'created_at_source_raw' | 'content_time_status'>,
): RevalidatedContentTime {
  const validated = validateContentTime(
    candidate.created_at_source_raw.toString('utf8'),
    candidate.indexedAt,
  )
  const outcome: ContentTimeRevalidationOutcome =
    validated.content_time_status === 'source_invalid'
      ? 'v1_to_v2_invalid'
      : candidate.content_time_status === 'source_valid'
        ? 'v1_valid_to_v2_valid'
        : 'v1_invalid_to_v2_valid'
  return {
    // Mirrors src/subscription.ts exactly: the live firehose ingestion path
    // always sets createdAt = validateContentTime(...).legacy_created_at for
    // both post and engagement rows (checked 2026-08-17). Revalidation must
    // therefore also rewrite the legacy createdAt column so a v1 row looks
    // identical to a row ingestion would produce today. This is a
    // deliberate divergence from applyRecoveryBatch()'s legacy_unknown
    // recovery path above, which intentionally leaves createdAt untouched
    // for old-app compatibility: those rows' createdAt predates the
    // content-time columns entirely and was never derived from
    // legacy_created_at, so preserving it is the correct old-app-compat
    // choice there. v1 rows' createdAt *was* already derived from
    // legacy_created_at (v1 policy), so recomputing it under v2 is a
    // like-for-like refresh, not a compatibility break.
    createdAt: validated.legacy_created_at,
    content_time_utc: validated.content_time_utc,
    content_time_status: validated.content_time_status,
    content_time_clamp_reason: validated.content_time_clamp_reason,
    outcome,
  }
}

export type ContentTimeRevalidationCounts = {
  v1_valid_to_v2_valid: number
  v1_invalid_to_v2_valid: number
  v1_to_v2_invalid: number
  by_v2_invalid_reason: Partial<Record<ContentTimeClampReason, number>>
}

function emptyRevalidationCounts(): ContentTimeRevalidationCounts {
  return {
    v1_valid_to_v2_valid: 0,
    v1_invalid_to_v2_valid: 0,
    v1_to_v2_invalid: 0,
    by_v2_invalid_reason: {},
  }
}

function mergeRevalidationCounts(
  a: ContentTimeRevalidationCounts,
  b: ContentTimeRevalidationCounts,
): ContentTimeRevalidationCounts {
  const reasons: Partial<Record<ContentTimeClampReason, number>> = { ...a.by_v2_invalid_reason }
  for (const [reason, count] of Object.entries(b.by_v2_invalid_reason)) {
    const key = reason as ContentTimeClampReason
    reasons[key] = (reasons[key] ?? 0) + (count ?? 0)
  }
  return {
    v1_valid_to_v2_valid: a.v1_valid_to_v2_valid + b.v1_valid_to_v2_valid,
    v1_invalid_to_v2_valid: a.v1_invalid_to_v2_valid + b.v1_invalid_to_v2_valid,
    v1_to_v2_invalid: a.v1_to_v2_invalid + b.v1_to_v2_invalid,
    by_v2_invalid_reason: reasons,
  }
}

export type ContentTimeRevalidationProgress = {
  batch: number
  scanned: number
  updated: number
  skipped_cas: number
  counts: ContentTimeRevalidationCounts
  cursor_author_sha256: string
  cursor_uri_sha256: string
  elapsed_ms: number
  packet_sha256: string
  // This batch's own WAL/relation-size deltas, measured inside the batch
  // transaction (pg_current_wal_insert_lsn() + pg_total_relation_size('public.post')
  // right after the SET_CONFIGs, again right after the UPDATE and before
  // commit) -- not cumulative across the run, unlike the fields above. This
  // is what makes a per-batch ceiling check attributable to that batch alone
  // rather than confounded by earlier batches or concurrent writers.
  wal_bytes: number
  relation_bytes_before: number
  relation_bytes_after: number
}

// One entry per committed batch transaction, in order. cursor_author/
// cursor_uri are the raw publisher DID / post URI the batch advanced to --
// deliberately not hashed here (unlike the cumulative cursor_*_sha256
// fields above): an operator comparing this array against pg_stat_wal /
// relation-size / dead-tuple deltas per batch needs the actual cursor to
// know where to look, and publisher URIs are not participant data.
export type ContentTimeRevalidationBatchSummary = {
  batch: number
  candidates: number
  updated: number
  skipped_cas: number
  counts: ContentTimeRevalidationCounts
  cursor_author: string
  cursor_uri: string
  elapsed_ms: number
  wal_bytes: number
  relation_bytes_before: number
  relation_bytes_after: number
  // Adaptive pause (D4-b, 2026-08-18): after this batch the tool slept
  // pause_ms = max(pauseMs, wal_bytes / pauseBaselineBytesPerSecond) so the
  // backfill never averages faster than the estate's own write rate.
  pause_ms: number
  pause_required_ms: number
}

export type ContentTimeRevalidationResult = ContentTimeRevalidationProgress & {
  complete: boolean
  batches: ContentTimeRevalidationBatchSummary[]
}

export type ContentTimeRevalidationCheckpoint = ContentTimeRevalidationProgress & {
  config_sha256: string
  cursor_author: string
  cursor_uri: string
}

export type ContentTimeRevalidationOptions = {
  actors: string[]
  since: Date
  batchSize: number
  packetSha256: string
  afterAuthor?: string
  afterUri?: string
  configSha256?: string
  maxBatches?: number
  maxDurationMs: number
  pauseMs: number
  // When set (> 0), the inter-batch pause becomes adaptive: at least
  // wal_bytes_of_the_batch / pauseBaselineBytesPerSecond seconds (floored at
  // pauseMs) -- the batch "pays back" its WAL at the estate's baseline rate.
  pauseBaselineBytesPerSecond?: number
  lockTimeoutMs: number
  statementTimeoutMs: number
  onProgress?: (progress: ContentTimeRevalidationProgress) => void
  onCheckpoint?: (checkpoint: ContentTimeRevalidationCheckpoint) => void
}

export function contentTimeRevalidationConfigSha256(actors: string[], sinceIso: string): string {
  const sortedActors = [...new Set(actors)].sort()
  return sha256(JSON.stringify({
    actors: sortedActors,
    since: sinceIso,
    from_validator_version: CONTENT_TIME_VALIDATOR_VERSION_V1,
    to_validator_version: CONTENT_TIME_VALIDATOR_VERSION,
  }))
}

type RevalidationSelectRow = {
  uri: string
  author: string
  indexedAt: string
  created_at_source_raw: Buffer
  content_time_status: string
}

// Selects up to `limit` v1-validator rows for the given publisher DIDs whose
// indexedAt is at or after `since`, ordered by (author, uri) starting
// strictly after the (afterAuthor, afterUri) cursor. author is indexed
// (post_author_index); indexedAt and content_time_validator_version are not,
// so this is an index scan over author = ANY(actors) with indexedAt/
// validator_version applied as a residual filter, bounded to publisher-
// authored rows only (never a scan of the full public.post table, which
// also holds every followed-account post). See AGENTS-facing notes in the
// deploy runbook / final report for the cost estimate and the smallest
// available mitigation if this ever needs a dedicated index.
async function selectRevalidationBatch(
  db: Database,
  actors: string[],
  sinceIso: string,
  afterAuthor: string,
  afterUri: string,
  limit: number,
  forUpdate: boolean,
): Promise<RevalidationSelectRow[]> {
  const rows = forUpdate
    ? (await sql<RevalidationSelectRow>`
        SELECT uri, author, "indexedAt", created_at_source_raw, content_time_status
        FROM public.post
        WHERE author = ANY(${actors}::text[])
          AND "indexedAt" >= ${sinceIso}
          AND content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION_V1}
          AND (author, uri) > (${afterAuthor}, ${afterUri})
        ORDER BY author, uri
        LIMIT ${limit}
        FOR UPDATE
      `.execute(db)).rows
    : (await sql<RevalidationSelectRow>`
        SELECT uri, author, "indexedAt", created_at_source_raw, content_time_status
        FROM public.post
        WHERE author = ANY(${actors}::text[])
          AND "indexedAt" >= ${sinceIso}
          AND content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION_V1}
          AND (author, uri) > (${afterAuthor}, ${afterUri})
        ORDER BY author, uri
        LIMIT ${limit}
      `.execute(db)).rows
  return rows
}

type RevalidationBatchResult = {
  candidates: number
  updated: number
  skipped_cas: number
  counts: ContentTimeRevalidationCounts
  cursorAuthor: string
  cursorUri: string
  walBytes: number
  relationBytesBefore: number
  relationBytesAfter: number
}

async function applyRevalidationBatch(
  db: Database,
  actors: string[],
  sinceIso: string,
  afterAuthor: string,
  afterUri: string,
  batchSize: number,
  lockTimeoutMs: number,
  statementTimeoutMs: number,
  deadlineMs: number,
): Promise<RevalidationBatchResult> {
  return db.transaction().execute(async (trx) => {
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0) throw new Error('content-time revalidation deadline reached')
    await sql`SELECT set_config('transaction_timeout', ${`${remainingMs}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('lock_timeout', ${`${Math.min(lockTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('statement_timeout', ${`${Math.min(statementTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)

    // Attributable-ceiling measurement, point one: taken right after the
    // SET_CONFIGs, before the SELECT ... FOR UPDATE, so it reflects this
    // batch's transaction only -- not whatever an earlier batch or a
    // concurrent writer already did to public.post.
    const before = (await sql<{ lsn: string; relation_bytes: string }>`
      SELECT pg_current_wal_insert_lsn()::text AS lsn,
             pg_total_relation_size('public.post')::text AS relation_bytes
    `.execute(trx)).rows[0]

    const rows = await selectRevalidationBatch(trx, actors, sinceIso, afterAuthor, afterUri, batchSize, true)
    if (rows.length === 0) {
      return {
        candidates: 0,
        updated: 0,
        skipped_cas: 0,
        counts: emptyRevalidationCounts(),
        cursorAuthor: afterAuthor,
        cursorUri: afterUri,
        walBytes: 0,
        relationBytesBefore: Number(before.relation_bytes),
        relationBytesAfter: Number(before.relation_bytes),
      }
    }

    const payload = rows.map((row) => {
      const revalidated = revalidateContentTimeCandidate(row)
      return {
        uri: row.uri,
        raw_hex: row.created_at_source_raw.toString('hex'),
        created_at: revalidated.createdAt,
        content_time_utc: revalidated.content_time_utc,
        content_time_status: revalidated.content_time_status,
        content_time_clamp_reason: revalidated.content_time_clamp_reason,
        outcome: revalidated.outcome,
      }
    })

    // Single atomic UPDATE ... WHERE is the compare-and-swap: it only
    // touches a row if content_time_validator_version is still v1 AND
    // created_at_source_raw still equals the bytes we just read inside this
    // same locked transaction. Any row a concurrent writer changed between
    // our SELECT and here (impossible under the FOR UPDATE lock from another
    // transaction touching the same row, but defended anyway) simply will
    // not appear in RETURNING and is counted as skipped_cas below, never
    // clobbered.
    const updateResult = await sql<{
      uri: string
      outcome: string
      content_time_clamp_reason: string | null
    }>`
      UPDATE public.post AS target
      SET "createdAt" = batch.created_at,
          content_time_utc = batch.content_time_utc,
          content_time_status = batch.content_time_status,
          content_time_clamp_reason = batch.content_time_clamp_reason,
          content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION}
      FROM jsonb_to_recordset(${JSON.stringify(payload)}::jsonb) AS batch(
        uri text,
        raw_hex text,
        created_at text,
        content_time_utc text,
        content_time_status text,
        content_time_clamp_reason text,
        outcome text
      )
      WHERE target.uri = batch.uri
        AND target.content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION_V1}
        AND target.created_at_source_raw = decode(batch.raw_hex, 'hex')
      RETURNING target.uri, batch.outcome AS outcome, batch.content_time_clamp_reason AS content_time_clamp_reason
    `.execute(trx)

    const counts = emptyRevalidationCounts()
    for (const row of updateResult.rows) {
      const outcome = row.outcome as ContentTimeRevalidationOutcome
      counts[outcome] += 1
      if (outcome === 'v1_to_v2_invalid' && row.content_time_clamp_reason) {
        const reason = row.content_time_clamp_reason as ContentTimeClampReason
        counts.by_v2_invalid_reason[reason] = (counts.by_v2_invalid_reason[reason] ?? 0) + 1
      }
    }

    // Attributable-ceiling measurement, point two: taken right after the
    // UPDATE and before this transaction commits (db.transaction().execute
    // commits when this callback resolves), so pg_wal_lsn_diff isolates
    // exactly this batch's WAL, and relation_bytes_after captures the size
    // this same UPDATE just produced.
    const after = (await sql<{ wal_bytes: string; relation_bytes: string }>`
      SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), ${before.lsn}::pg_lsn)::text AS wal_bytes,
             pg_total_relation_size('public.post')::text AS relation_bytes
    `.execute(trx)).rows[0]

    const last = rows[rows.length - 1]
    return {
      candidates: rows.length,
      updated: updateResult.rows.length,
      skipped_cas: rows.length - updateResult.rows.length,
      counts,
      cursorAuthor: last.author,
      cursorUri: last.uri,
      walBytes: Number(after.wal_bytes),
      relationBytesBefore: Number(before.relation_bytes),
      relationBytesAfter: Number(after.relation_bytes),
    }
  })
}

export async function runContentTimeRevalidation(
  db: Database,
  options: ContentTimeRevalidationOptions,
): Promise<ContentTimeRevalidationResult> {
  if (!/^[0-9a-f]{64}$/.test(options.packetSha256)) {
    throw new Error('packetSha256 must be a lowercase SHA-256')
  }
  const actors = [...new Set(options.actors)].sort()
  if (actors.length === 0) {
    throw new Error('content-time revalidation requires at least one publisher DID')
  }
  if (!Number.isInteger(options.batchSize) || options.batchSize < 1 || options.batchSize > REVALIDATION_LIMITS.batchSize) {
    throw new Error(`batchSize must be an integer from 1 to ${REVALIDATION_LIMITS.batchSize}`)
  }
  for (const [name, value] of [
    ['maxDurationMs', options.maxDurationMs],
    ['pauseMs', options.pauseMs],
    ['lockTimeoutMs', options.lockTimeoutMs],
    ['statementTimeoutMs', options.statementTimeoutMs],
  ] as const) {
    if (!Number.isInteger(value) || value < (name === 'pauseMs' ? 0 : 1)) {
      throw new Error(`${name} must be a ${name === 'pauseMs' ? 'non-negative' : 'positive'} integer`)
    }
  }
  if (options.maxBatches !== undefined && (!Number.isInteger(options.maxBatches) || options.maxBatches < 1)) {
    throw new Error('maxBatches must be a positive integer')
  }

  const sinceIso = options.since.toISOString()
  const configSha256 = contentTimeRevalidationConfigSha256(actors, sinceIso)
  if (options.afterAuthor || options.afterUri) {
    if (options.configSha256 !== configSha256) {
      throw new Error('checkpoint does not match the immutable revalidation config (actors, since, or validator versions changed)')
    }
  }

  const startedAt = Date.now()
  const deadlineMs = startedAt + options.maxDurationMs
  let batch = 0
  let scanned = 0
  let updated = 0
  let skippedCas = 0
  let counts = emptyRevalidationCounts()
  let cursorAuthor = options.afterAuthor ?? ''
  let cursorUri = options.afterUri ?? ''
  let complete = false
  let lastWalBytes = 0
  let lastRelationBytesBefore = 0
  let lastRelationBytesAfter = 0
  const batches: ContentTimeRevalidationBatchSummary[] = []

  while (true) {
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0) {
      complete = false
      break
    }
    if (options.maxBatches !== undefined && batch >= options.maxBatches) {
      complete = false
      break
    }
    const batchStartedAt = Date.now()
    const result = await applyRevalidationBatch(
      db,
      actors,
      sinceIso,
      cursorAuthor,
      cursorUri,
      options.batchSize,
      options.lockTimeoutMs,
      Math.min(options.statementTimeoutMs, remainingMs),
      deadlineMs,
    )
    if (result.candidates === 0) {
      complete = true
      break
    }
    batch += 1
    scanned += result.candidates
    updated += result.updated
    skippedCas += result.skipped_cas
    counts = mergeRevalidationCounts(counts, result.counts)
    cursorAuthor = result.cursorAuthor
    cursorUri = result.cursorUri
    lastWalBytes = result.walBytes
    lastRelationBytesBefore = result.relationBytesBefore
    lastRelationBytesAfter = result.relationBytesAfter
    const batchElapsedMs = Date.now() - batchStartedAt
    // Adaptive pause: pay this batch's WAL back at the baseline rate before
    // anything else happens (also after the final batch of an invocation, so
    // every batch is paid). Clipped by the hard deadline; the receipt records
    // both the required and the actually slept duration.
    const pauseRequiredMs = result.candidates === 0
      ? 0 // an empty (terminal) batch wrote nothing and owes nothing
      : options.pauseBaselineBytesPerSecond && options.pauseBaselineBytesPerSecond > 0
        ? Math.max(options.pauseMs, Math.ceil((result.walBytes * 1000) / options.pauseBaselineBytesPerSecond))
        : options.pauseMs
    const pauseSleptMs = Math.min(pauseRequiredMs, Math.max(0, deadlineMs - Date.now()))
    if (pauseSleptMs > 0) await sleep(pauseSleptMs)
    batches.push({
      batch,
      candidates: result.candidates,
      updated: result.updated,
      skipped_cas: result.skipped_cas,
      counts: result.counts,
      cursor_author: cursorAuthor,
      cursor_uri: cursorUri,
      elapsed_ms: batchElapsedMs,
      wal_bytes: result.walBytes,
      relation_bytes_before: result.relationBytesBefore,
      relation_bytes_after: result.relationBytesAfter,
      pause_ms: pauseSleptMs,
      pause_required_ms: pauseRequiredMs,
    })

    const progress: ContentTimeRevalidationProgress = {
      batch,
      scanned,
      updated,
      skipped_cas: skippedCas,
      counts,
      cursor_author_sha256: sha256(cursorAuthor),
      cursor_uri_sha256: sha256(cursorUri),
      elapsed_ms: Date.now() - startedAt,
      packet_sha256: options.packetSha256,
      wal_bytes: result.walBytes,
      relation_bytes_before: result.relationBytesBefore,
      relation_bytes_after: result.relationBytesAfter,
    }
    options.onProgress?.(progress)
    options.onCheckpoint?.({
      ...progress,
      config_sha256: configSha256,
      cursor_author: cursorAuthor,
      cursor_uri: cursorUri,
    })

    if (result.candidates < options.batchSize) {
      // Fewer rows than requested means the (author, uri) tail is exhausted
      // for this window: there is nothing left matching the WHERE clause
      // beyond the cursor we just advanced to.
      complete = true
      break
    }
    // (the inter-batch pause was already taken above, right after the batch)
  }

  return {
    batch,
    scanned,
    updated,
    skipped_cas: skippedCas,
    counts,
    cursor_author_sha256: cursorAuthor ? sha256(cursorAuthor) : '',
    cursor_uri_sha256: cursorUri ? sha256(cursorUri) : '',
    elapsed_ms: Date.now() - startedAt,
    packet_sha256: options.packetSha256,
    // Mirrors the last batch pushed above (0 if this invocation ran zero
    // batches, e.g. an immediate empty scan) -- the top-level result is a
    // snapshot of the final progress state, not a sum across batches.
    wal_bytes: lastWalBytes,
    relation_bytes_before: lastRelationBytesBefore,
    relation_bytes_after: lastRelationBytesAfter,
    complete,
    batches,
  }
}

// Read-only preview: reports the outcome distribution the apply pass would
// produce, without ever issuing an UPDATE or taking row locks. Used by the
// CLI's --mode revalidate --dry-run path. Bounded by the same batch size so
// a preview over a large window cannot run unbounded, but has no 30-minute
// stop (each iteration is a fast plain SELECT, and callers may re-run it).
export async function previewContentTimeRevalidation(
  db: Database,
  actors: string[],
  since: Date,
  batchSize: number = REVALIDATION_LIMITS.batchSize,
  maxRows: number = 50_000,
): Promise<{ scanned: number; counts: ContentTimeRevalidationCounts; truncated: boolean }> {
  const sortedActors = [...new Set(actors)].sort()
  const sinceIso = since.toISOString()
  let cursorAuthor = ''
  let cursorUri = ''
  let scanned = 0
  let counts = emptyRevalidationCounts()
  let truncated = false

  while (scanned < maxRows) {
    const limit = Math.min(batchSize, maxRows - scanned)
    const rows = await selectRevalidationBatch(
      db,
      sortedActors,
      sinceIso,
      cursorAuthor,
      cursorUri,
      limit,
      false,
    )
    if (rows.length === 0) break
    for (const row of rows) {
      const revalidated = revalidateContentTimeCandidate(row)
      counts[revalidated.outcome] += 1
      if (revalidated.outcome === 'v1_to_v2_invalid' && revalidated.content_time_clamp_reason) {
        const reason = revalidated.content_time_clamp_reason
        counts.by_v2_invalid_reason[reason] = (counts.by_v2_invalid_reason[reason] ?? 0) + 1
      }
    }
    scanned += rows.length
    const last = rows[rows.length - 1]
    cursorAuthor = last.author
    cursorUri = last.uri
    if (rows.length < limit) break
  }
  if (scanned >= maxRows) truncated = true

  return { scanned, counts, truncated }
}

function splitCsv(value: string | undefined): string[] {
  return (value || '')
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
}

function requireValue(value: string | undefined, message: string): string {
  if (!value) throw new Error(message)
  return value
}

function parseCliArgs(argv: string[]): CliOptions {
  const flags = new Map<string, string[]>()
  let apply = false
  let json = false
  let noInsert = false

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--apply') {
      apply = true
      continue
    }
    if (arg === '--dry-run') {
      apply = false
      continue
    }
    if (arg === '--json') {
      json = true
      continue
    }
    if (arg === '--no-insert') {
      noInsert = true
      continue
    }
    if (!arg.startsWith('--')) throw new Error(`Unexpected positional argument: ${arg}`)
    const value = argv[i + 1]
    if (!value || value.startsWith('--')) throw new Error(`Missing value for ${arg}`)
    i += 1
    const key = arg.slice(2)
    flags.set(key, [...(flags.get(key) || []), value])
  }

  const actors = [
    ...splitCsv(flags.get('actors')?.join(',')),
    ...splitCsv(flags.get('dids')?.join(',')),
  ]
  if (!actors.length) throw new Error('Pass --actors or --dids with comma-separated publisher DIDs')

  const since = new Date(requireValue(flags.get('since')?.at(-1), 'Pass --since ISO timestamp'))
  const until = new Date(requireValue(flags.get('until')?.at(-1), 'Pass --until ISO timestamp'))
  if (isNaN(since.getTime())) throw new Error('--since must be an ISO timestamp')
  if (isNaN(until.getTime())) throw new Error('--until must be an ISO timestamp')
  if (until <= since) throw new Error('--until must be after --since')

  const maxPagesPerActor = Number.parseInt(flags.get('max-pages-per-actor')?.at(-1) || '50', 10)
  if (!Number.isInteger(maxPagesPerActor) || maxPagesPerActor < 1) {
    throw new Error('--max-pages-per-actor must be a positive integer')
  }

  const maxBatchesRaw = flags.get('max-batches')?.at(-1)
  let maxBatches: number | undefined
  if (maxBatchesRaw !== undefined) {
    maxBatches = Number.parseInt(maxBatchesRaw, 10)
    if (!Number.isInteger(maxBatches) || String(maxBatches) !== maxBatchesRaw.trim() || maxBatches < 1) {
      throw new Error('--max-batches must be a positive integer')
    }
  }
  const pauseBaselineRaw = flags.get('pause-baseline-bytes-per-s')?.at(-1)
  let pauseBaselineBytesPerSecond: number | undefined
  if (pauseBaselineRaw !== undefined) {
    pauseBaselineBytesPerSecond = Number.parseInt(pauseBaselineRaw, 10)
    if (!Number.isInteger(pauseBaselineBytesPerSecond) || String(pauseBaselineBytesPerSecond) !== pauseBaselineRaw.trim() || pauseBaselineBytesPerSecond < 1) {
      throw new Error('--pause-baseline-bytes-per-s must be a positive integer (bytes of WAL per second)')
    }
  }
  const windowFieldRaw = flags.get('window-field')?.at(-1) ?? 'createdAt'
  if (windowFieldRaw !== 'createdAt' && windowFieldRaw !== 'indexedAt') {
    throw new Error('--window-field must be createdAt or indexedAt')
  }

  return {
    actors,
    since,
    until,
    apply,
    json,
    apiBase: flags.get('api-base')?.at(-1) || 'https://public.api.bsky.app',
    maxPagesPerActor,
    dbUrl: flags.get('db-url')?.at(-1) || process.env.FEEDGEN_POSTGRES_URL,
    checkpointFile: flags.get('checkpoint-file')?.at(-1),
    packetSha256: flags.get('packet-sha256')?.at(-1),
    maxBatches,
    pauseBaselineBytesPerSecond,
    noInsert,
    windowField: windowFieldRaw,
  }
}

async function fetchAuthorFeedPage(
  apiBase: string,
  actor: string,
  cursor?: string,
  timeoutMs = 30_000,
): Promise<AuthorFeedPage> {
  const url = new URL('/xrpc/app.bsky.feed.getAuthorFeed', apiBase)
  url.searchParams.set('actor', actor)
  url.searchParams.set('limit', '100')
  url.searchParams.set('filter', 'posts_no_replies')
  if (cursor) url.searchParams.set('cursor', cursor)

  const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) })
  if (!response.ok) {
    throw new Error(
      `AppView request failed actor_sha256=${sha256(actor)}: ${response.status} ${response.statusText}`,
    )
  }
  const body: any = await response.json()
  return {
    posts: (body.feed || []).map((item: any) => item.post).filter(Boolean),
    cursor: typeof body.cursor === 'string' ? body.cursor : undefined,
  }
}

function readCheckpoint(
  file: string,
  packetSha256: string,
): { cursorUri: string; planSha256: string } | undefined {
  if (!fs.existsSync(file)) return undefined
  const checkpoint = JSON.parse(fs.readFileSync(file, 'utf8'))
  if (
    checkpoint.packet_sha256 !== packetSha256 ||
    typeof checkpoint.plan_sha256 !== 'string' ||
    typeof checkpoint.cursor_uri !== 'string'
  ) {
    throw new Error('checkpoint does not match the approved packet')
  }
  return { cursorUri: checkpoint.cursor_uri, planSha256: checkpoint.plan_sha256 }
}

export function writeCheckpoint(file: string, checkpoint: object) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 })
  const temporary = `${file}.tmp`
  const descriptor = fs.openSync(temporary, 'w', 0o600)
  try {
    fs.fchmodSync(descriptor, 0o600)
    fs.writeFileSync(descriptor, `${JSON.stringify(checkpoint)}\n`)
    fs.fsyncSync(descriptor)
  } finally {
    fs.closeSync(descriptor)
  }
  fs.renameSync(temporary, file)
  const directory = fs.openSync(path.dirname(file), 'r')
  try {
    fs.fsyncSync(directory)
  } finally {
    fs.closeSync(directory)
  }
}

// --- mode: revalidate CLI wiring ------------------------------------------

export type RevalidateCliOptions = {
  actors?: string[]
  since: Date
  apply: boolean
  json: boolean
  dbUrl?: string
  checkpointFile?: string
  maxPreviewRows: number
  packetSha256?: string
  maxBatches?: number
  pauseBaselineBytesPerSecond?: number
}

export function parseRevalidateCliArgs(argv: string[]): RevalidateCliOptions {
  const flags = new Map<string, string[]>()
  let apply = false
  let json = false

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--apply') {
      apply = true
      continue
    }
    if (arg === '--dry-run') {
      apply = false
      continue
    }
    if (arg === '--json') {
      json = true
      continue
    }
    if (!arg.startsWith('--')) throw new Error(`Unexpected positional argument: ${arg}`)
    const value = argv[i + 1]
    if (!value || value.startsWith('--')) throw new Error(`Missing value for ${arg}`)
    i += 1
    const key = arg.slice(2)
    flags.set(key, [...(flags.get(key) || []), value])
  }

  const actorsCsv = [
    ...splitCsv(flags.get('actors')?.join(',')),
    ...splitCsv(flags.get('dids')?.join(',')),
  ]

  const sinceRaw = flags.get('since')?.at(-1)
  const since = sinceRaw ? new Date(sinceRaw) : new Date(Date.now() - 14 * 24 * 60 * 60 * 1000)
  if (isNaN(since.getTime())) throw new Error('--since must be an ISO timestamp')

  const maxPreviewRows = Number.parseInt(flags.get('max-preview-rows')?.at(-1) || '50000', 10)
  if (!Number.isInteger(maxPreviewRows) || maxPreviewRows < 1) {
    throw new Error('--max-preview-rows must be a positive integer')
  }

  const packetSha256 = flags.get('packet-sha256')?.at(-1)
  if (packetSha256 !== undefined && !/^[0-9a-f]{64}$/.test(packetSha256)) {
    throw new Error('--packet-sha256 must be a lowercase 64-character hex SHA-256')
  }

  const maxBatchesRaw = flags.get('max-batches')?.at(-1)
  let maxBatches: number | undefined
  if (maxBatchesRaw !== undefined) {
    maxBatches = Number.parseInt(maxBatchesRaw, 10)
    if (!Number.isInteger(maxBatches) || String(maxBatches) !== maxBatchesRaw.trim() || maxBatches < 1) {
      throw new Error('--max-batches must be a positive integer')
    }
  }

  const pauseBaselineRaw = flags.get('pause-baseline-bytes-per-s')?.at(-1)
  let pauseBaselineBytesPerSecond: number | undefined
  if (pauseBaselineRaw !== undefined) {
    pauseBaselineBytesPerSecond = Number.parseInt(pauseBaselineRaw, 10)
    if (!Number.isInteger(pauseBaselineBytesPerSecond) || String(pauseBaselineBytesPerSecond) !== pauseBaselineRaw.trim() || pauseBaselineBytesPerSecond < 1) {
      throw new Error('--pause-baseline-bytes-per-s must be a positive integer (bytes of WAL per second)')
    }
  }

  return {
    actors: actorsCsv.length ? actorsCsv : undefined,
    since,
    apply,
    json,
    dbUrl: flags.get('db-url')?.at(-1) || process.env.FEEDGEN_POSTGRES_URL,
    checkpointFile: flags.get('checkpoint-file')?.at(-1),
    maxPreviewRows,
    packetSha256,
    maxBatches,
    pauseBaselineBytesPerSecond,
  }
}

export function readRevalidationCheckpoint(
  file: string,
  configSha256: string,
  packetSha256: string,
): { cursorAuthor: string; cursorUri: string } | undefined {
  if (!fs.existsSync(file)) return undefined
  const checkpoint = JSON.parse(fs.readFileSync(file, 'utf8'))
  if (
    checkpoint.config_sha256 !== configSha256 ||
    checkpoint.packet_sha256 !== packetSha256 ||
    typeof checkpoint.cursor_author !== 'string' ||
    typeof checkpoint.cursor_uri !== 'string'
  ) {
    throw new Error('checkpoint does not match the approved revalidation packet (actors, since, validator versions, or packet-sha256 changed)')
  }
  return { cursorAuthor: checkpoint.cursor_author, cursorUri: checkpoint.cursor_uri }
}

async function mainRevalidate(argv: string[]) {
  const startedAt = Date.now()
  const options = parseRevalidateCliArgs(argv)
  if (!options.dbUrl) {
    throw new Error('FEEDGEN_POSTGRES_URL or --db-url is required for --mode revalidate')
  }

  const db = createDb(options.dbUrl)
  try {
    const actors = options.actors && options.actors.length
      ? [...new Set(options.actors)].sort()
      : (await resolvePublisherDids(db)).sort()
    if (!actors.length) {
      throw new Error('no enabled publisher DIDs resolved from feedgen_ops.feed_catalog; pass --actors explicitly')
    }

    let revalidation: ContentTimeRevalidationResult | null = null
    let preview: { scanned: number; counts: ContentTimeRevalidationCounts; truncated: boolean } | null = null

    if (options.apply) {
      if (!options.checkpointFile) throw new Error('--checkpoint-file is required with --apply')
      if (!options.packetSha256 || !/^[0-9a-f]{64}$/.test(options.packetSha256)) {
        throw new Error('--packet-sha256 is required with --apply')
      }
      const configSha256 = contentTimeRevalidationConfigSha256(actors, options.since.toISOString())
      const checkpoint = readRevalidationCheckpoint(options.checkpointFile, configSha256, options.packetSha256)
      const deadlineMs = startedAt + REVALIDATION_LIMITS.maxDurationMs
      const remainingMs = deadlineMs - Date.now()
      if (remainingMs <= 0) throw new Error('content-time revalidation exhausted its own startup deadline')
      revalidation = await runContentTimeRevalidation(db, {
        actors,
        since: options.since,
        batchSize: REVALIDATION_LIMITS.batchSize,
        packetSha256: options.packetSha256,
        afterAuthor: checkpoint?.cursorAuthor,
        afterUri: checkpoint?.cursorUri,
        configSha256: checkpoint ? configSha256 : undefined,
        maxBatches: options.maxBatches,
        maxDurationMs: remainingMs,
        pauseMs: REVALIDATION_LIMITS.pauseMs,
        pauseBaselineBytesPerSecond: options.pauseBaselineBytesPerSecond,
        lockTimeoutMs: REVALIDATION_LIMITS.lockTimeoutMs,
        statementTimeoutMs: Math.min(REVALIDATION_LIMITS.statementTimeoutMs, remainingMs),
        onProgress: (progress) => console.error(JSON.stringify({ event: 'revalidate_batch', ...progress })),
        onCheckpoint: (checkpoint) => writeCheckpoint(options.checkpointFile!, checkpoint),
      })
    } else {
      preview = await previewContentTimeRevalidation(
        db,
        actors,
        options.since,
        REVALIDATION_LIMITS.batchSize,
        options.maxPreviewRows,
      )
    }

    const summary = {
      operation: 'content-time-revalidate',
      mode: options.apply ? 'apply' : 'dry-run',
      actor_count: actors.length,
      actor_sha256: actors.map(sha256).sort(),
      since: options.since.toISOString(),
      from_validator_version: CONTENT_TIME_VALIDATOR_VERSION_V1,
      to_validator_version: CONTENT_TIME_VALIDATOR_VERSION,
      packet_sha256: options.packetSha256 ?? null,
      preview,
      revalidation,
    }

    if (options.json) {
      console.log(JSON.stringify(summary, null, 2))
    } else {
      const scanned = revalidation?.scanned ?? preview?.scanned ?? 0
      const updated = revalidation?.updated ?? 0
      console.log(`${summary.mode}: scanned=${scanned} updated=${updated}`)
    }
    if (revalidation && !revalidation.complete) process.exitCode = 3
  } finally {
    await db.destroy()
  }
}

// --- mode: recover (existing AppView-backed recovery) ---------------------

async function mainRecover(argv: string[]) {
  const startedAt = Date.now()
  const options = parseCliArgs(argv)
  const deadlineMs = startedAt + RECOVERY_LIMITS.maxDurationMs
  const plan = await collectPublisherPosts({
    actors: options.actors,
    since: options.since,
    until: options.until,
    maxPagesPerActor: options.maxPagesPerActor,
    deadlineMs,
    windowField: options.windowField,
    stopWhenPageBeforeSince: true,
    fetchPage: (actor, cursor) => fetchAuthorFeedPage(
      options.apiBase,
      actor,
      cursor,
      Math.max(1, Math.min(30_000, deadlineMs - Date.now())),
    ),
  })

  let recovery: PublisherPostRecoveryResult | null = null
  let preview: PublisherPostRecoveryPreview | null = null
  if (!options.apply && options.dbUrl) {
    // Read-only DB comparison so a packet can pre-register the exact outcome.
    const db = createDb(options.dbUrl)
    try {
      preview = await previewPublisherPostRecovery(db, plan.posts, options.actors, options.since, options.until)
    } finally {
      await db.destroy()
    }
  }
  if (options.apply) {
    if (!options.checkpointFile) throw new Error('--checkpoint-file is required with --apply')
    if (!options.packetSha256 || !/^[0-9a-f]{64}$/.test(options.packetSha256)) {
      throw new Error('--packet-sha256 is required with --apply')
    }
    if (!options.dbUrl) {
      throw new Error('FEEDGEN_POSTGRES_URL or --db-url is required with --apply')
    }
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0) throw new Error('publisher recovery collection exhausted the packet deadline')
    const checkpoint = readCheckpoint(options.checkpointFile, options.packetSha256)
    const db = createDb(options.dbUrl)
    try {
      recovery = await runPublisherPostRecovery(db, {
        posts: plan.posts,
        packetSha256: options.packetSha256,
        batchSize: RECOVERY_LIMITS.batchSize,
        afterUri: checkpoint?.cursorUri,
        planSha256: checkpoint?.planSha256,
        maxDurationMs: remainingMs,
        pauseMs: RECOVERY_LIMITS.pauseMs,
        maxBatches: options.maxBatches,
        pauseBaselineBytesPerSecond: options.pauseBaselineBytesPerSecond,
        noInsert: options.noInsert,
        lockTimeoutMs: RECOVERY_LIMITS.lockTimeoutMs,
        statementTimeoutMs: Math.min(RECOVERY_LIMITS.statementTimeoutMs, remainingMs),
        onProgress: (progress) => console.error(JSON.stringify({ event: 'recovery_batch', ...progress })),
        onCheckpoint: (checkpoint) => writeCheckpoint(options.checkpointFile!, checkpoint),
      })
    } finally {
      await db.destroy()
    }
  }
  const summary = {
    operation: 'publisher-post-recover',
    mode: options.apply ? 'apply' : 'dry-run',
    actor_count: options.actors.length,
    actor_sha256: options.actors.map(sha256).sort(),
    since: options.since.toISOString(),
    until: options.until.toISOString(),
    window_field: options.windowField,
    no_insert: options.noInsert,
    scanned: plan.scanned,
    candidate_posts: plan.posts.length,
    skipped_out_of_window: plan.skipped_out_of_window,
    skipped_wrong_author: plan.skipped_wrong_author,
    by_actor: Object.fromEntries(
      Object.entries(plan.by_actor).map(([actor, counts]) => [sha256(actor), counts]),
    ),
    preview,
    recovery,
  }

  if (options.json) {
    console.log(JSON.stringify(summary, null, 2))
  } else {
    console.log(
      `${summary.mode}: scanned=${summary.scanned} candidates=${summary.candidate_posts} recovered=${recovery?.recovered ?? 0}`,
    )
  }
  if (recovery && !recovery.complete) process.exitCode = 3
}

// --- entrypoint -------------------------------------------------------

function detectMode(argv: string[]): 'recover' | 'revalidate' {
  const index = argv.indexOf('--mode')
  if (index === -1) return 'recover'
  const value = argv[index + 1]
  if (value === 'revalidate') return 'revalidate'
  if (value === 'recover' || value === undefined) return 'recover'
  throw new Error(`--mode must be "recover" or "revalidate", got "${value}"`)
}

async function main() {
  const argv = process.argv.slice(2)
  const mode = detectMode(argv)
  if (mode === 'revalidate') {
    await mainRevalidate(argv)
    return
  }
  await mainRecover(argv)
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message || err)
    process.exit(1)
  })
}
