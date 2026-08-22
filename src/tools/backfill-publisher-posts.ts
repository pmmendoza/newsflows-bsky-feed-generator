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
  CONTENT_TIME_VALIDATOR_VERSION_V2,
  CONTENT_TIME_VALIDATOR_VERSION_V3,
  validateContentTime,
  isSupportedContentTimeVersion,
  resolveActiveContentTimeContract,
  revalidationSemanticDeltaSql,
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
  content_time_clamp_reason: ContentTimeClampReason | null
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
  planFromDb: boolean
  planLimit?: number
  planUrisFile?: string
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
  version: string = CONTENT_TIME_VALIDATOR_VERSION_V2,
): BackfillPostRow | null {
  if (!post.uri || !post.cid) return null
  if (post.author?.did !== expectedAuthorDid) return null
  if (version !== CONTENT_TIME_VALIDATOR_VERSION_V2) {
    throw new Error(`recovery helper only supports validator version ${CONTENT_TIME_VALIDATOR_VERSION_V2}; got ${version}`)
  }

  const indexedAt = post.indexedAt || new Date().toISOString()
  const record = post.record || {}
  const embed = externalEmbed(record.embed)
  const contentTime = validateContentTime(record.createdAt, indexedAt, version)

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

// --- plan from the database (Belgium Step-2 recovery, 2026-08-18) ----------
//
// Instead of walking the author feed (which omits some posts and pages through
// everything), build the plan from the rows that actually need recovery: the
// actors' unclassified rows inside [since, until) by "indexedAt", fetched from
// the AppView by URI (app.bsky.feed.getPosts, 25 URIs per call). Rows the
// AppView no longer returns (deleted/taken down) are counted as unretrievable
// and left untouched -- they are reported, never guessed.
export type DbPlanOptions = {
  actors: string[]
  since: Date
  until: Date
  fetchPosts: (uris: string[]) => Promise<AppViewPost[]>
  deadlineMs?: number
  limit?: number
  // Immutable plan for resumable applies: when given, the plan is exactly this
  // URI list (materialized once by the packet runner from its prestate
  // snapshot) instead of "whatever is legacy right now" -- so the plan hash the
  // checkpoint binds stays identical across invocations while rows already
  // recovered simply come back as already_current. Rows must belong to the
  // actors (author is read from the DB); URIs not in the DB are reported as
  // unretrievable-in-db and skipped.
  planUris?: string[]
}

export type DbPlan = BackfillPlan & {
  db_legacy_in_window: number
  plan_source_rows: number
  plan_uris_not_in_db: number
  unretrievable: number
  unretrievable_uri_sha256_sample: string[]
  requests: number
}

export async function collectPublisherPostsFromDb(db: Database, options: DbPlanOptions): Promise<DbPlan> {
  const legacyInWindow = Number((await sql<{ n: string }>`
    SELECT count(*)::text AS n FROM public.post
    WHERE author = ANY(${options.actors}::text[])
      AND "indexedAt" >= ${options.since.toISOString()} AND "indexedAt" < ${options.until.toISOString()}
      AND created_at_source_raw IS NULL
      AND (content_time_status IS NULL OR content_time_status = 'legacy_unknown')
  `.execute(db)).rows[0].n)
  let planUrisNotInDb = 0
  let rows: { uri: string; author: string }[]
  if (options.planUris) {
    const wanted = [...new Set(options.planUris)].sort()
    rows = []
    for (let offset = 0; offset < wanted.length; offset += 1000) {
      const slice = wanted.slice(offset, offset + 1000)
      const found = (await sql<{ uri: string; author: string }>`
        SELECT uri, author FROM public.post
        WHERE uri = ANY(${slice}::text[]) AND author = ANY(${options.actors}::text[])
        ORDER BY uri
      `.execute(db)).rows
      rows.push(...found)
      planUrisNotInDb += slice.length - found.length
    }
  } else {
    rows = (await sql<{ uri: string; author: string }>`
      SELECT uri, author FROM public.post
      WHERE author = ANY(${options.actors}::text[])
        AND "indexedAt" >= ${options.since.toISOString()} AND "indexedAt" < ${options.until.toISOString()}
        AND created_at_source_raw IS NULL
        AND (content_time_status IS NULL OR content_time_status = 'legacy_unknown')
      ORDER BY uri
      ${options.limit ? sql`LIMIT ${options.limit}` : sql``}
    `.execute(db)).rows
  }
  const posts = new Map<string, BackfillPostRow>()
  const byActor: BackfillPlan['by_actor'] = {}
  for (const actor of options.actors) byActor[actor] = { scanned: 0, candidate_posts: 0, skipped_out_of_window: 0, skipped_wrong_author: 0 }
  let scanned = 0
  let skippedWrongAuthor = 0
  let requests = 0
  const unretrievable: string[] = []
  const authorByUri = new Map(rows.map((row) => [row.uri, row.author]))
  for (let offset = 0; offset < rows.length; offset += 25) {
    if (options.deadlineMs !== undefined && Date.now() >= options.deadlineMs) {
      throw new Error('publisher recovery collection exceeded its deadline')
    }
    const chunk = rows.slice(offset, offset + 25)
    const returned = await options.fetchPosts(chunk.map((row) => row.uri))
    requests += 1
    const seen = new Set<string>()
    for (const post of returned) {
      scanned += 1
      const expectedAuthor = post.uri ? authorByUri.get(post.uri) : undefined
      if (!expectedAuthor) { skippedWrongAuthor += 1; continue }
      const normalized = normalizeAppViewPost(post, expectedAuthor)
      if (!normalized) { skippedWrongAuthor += 1; byActor[expectedAuthor].skipped_wrong_author += 1; continue }
      seen.add(normalized.uri)
      posts.set(normalized.uri, normalized)
      byActor[expectedAuthor].scanned += 1
      byActor[expectedAuthor].candidate_posts += 1
    }
    for (const row of chunk) if (!seen.has(row.uri)) unretrievable.push(row.uri)
  }
  return {
    posts: [...posts.values()],
    scanned,
    skipped_out_of_window: 0,
    skipped_wrong_author: skippedWrongAuthor,
    by_actor: byActor,
    db_legacy_in_window: legacyInWindow,
    plan_source_rows: rows.length,
    plan_uris_not_in_db: planUrisNotInDb,
    unretrievable: unretrievable.length,
    unretrievable_uri_sha256_sample: unretrievable.slice(0, 5).map(sha256),
    requests,
  }
}

async function fetchPostsByUri(apiBase: string, uris: string[], timeoutMs = 30_000): Promise<AppViewPost[]> {
  const url = new URL('/xrpc/app.bsky.feed.getPosts', apiBase)
  for (const uri of uris) url.searchParams.append('uris', uri)
  const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) })
  if (!response.ok) {
    throw new Error(`AppView getPosts failed: ${response.status} ${response.statusText}`)
  }
  const body: any = await response.json()
  return (body.posts || []) as AppViewPost[]
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
  version: string = CONTENT_TIME_VALIDATOR_VERSION_V2,
): BackfillPostRow {
  if (current.cid !== expected.cid || current.author !== expected.author) {
    throw new Error(`content-time recovery revision conflict uri_sha256=${sha256(expected.uri)}`)
  }
  if (version !== CONTENT_TIME_VALIDATOR_VERSION_V2) {
    throw new Error(`recovery helper only supports validator version ${CONTENT_TIME_VALIDATOR_VERSION_V2}; got ${version}`)
  }
  const validated = validateContentTime(
    expected.created_at_source_raw.toString('utf8'),
    current.indexedAt,
    version,
  )
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

export async function assertPublisherRecoveryContract(db: Database): Promise<void> {
  const activeVersion = await resolveActiveContentTimeContract(db)
  if (activeVersion !== CONTENT_TIME_VALIDATOR_VERSION_V2) {
    throw new Error(
      `publisher recovery only supports active validator version ${CONTENT_TIME_VALIDATOR_VERSION_V2}; got ${activeVersion}`,
    )
  }
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

  // code-unit order, the same comparator the resume uses (`row.uri > afterUri`) -- never a locale collation
  const plan = [...options.posts].sort((left, right) => (left.uri < right.uri ? -1 : left.uri > right.uri ? 1 : 0))
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
  | 'v2_valid_to_v3_valid'
  | 'v2_skew_to_v3_clamped'
  | 'v2_invalid_to_v3_clamped'
  | 'v2_to_v3_invalid'
  | 'v3_valid_to_v2_valid'
  | 'v3_clamped_to_v2_valid'
  | 'v3_clamped_to_v2_invalid'
  | 'v3_to_v2_invalid'

export type ContentTimeRevalidationCandidate = {
  uri: string
  author: string
  indexedAt: string
  created_at_source_raw: Buffer
  content_time_status: string
  content_time_clamp_reason?: string | null
  content_time_validator_version?: string | null
}

export type RevalidatedContentTime = {
  createdAt: string
  content_time_utc: string | null
  content_time_status: 'source_valid' | 'source_invalid'
  content_time_clamp_reason: ContentTimeClampReason | null
  content_time_validator_version: string
  outcome: ContentTimeRevalidationOutcome
}

export const ALLOWED_REVALIDATION_TRANSITIONS = new Set([
  `${CONTENT_TIME_VALIDATOR_VERSION_V1}->${CONTENT_TIME_VALIDATOR_VERSION_V2}`,
  `${CONTENT_TIME_VALIDATOR_VERSION_V2}->${CONTENT_TIME_VALIDATOR_VERSION_V3}`,
  `${CONTENT_TIME_VALIDATOR_VERSION_V3}->${CONTENT_TIME_VALIDATOR_VERSION_V2}`,
])

export function validateRevalidationTransition(fromVersion: string, toVersion: string): void {
  const transitionKey = `${fromVersion}->${toVersion}`
  if (!ALLOWED_REVALIDATION_TRANSITIONS.has(transitionKey)) {
    throw new Error(
      `unsupported revalidation transition: ${transitionKey}; allowed transitions are v1->v2, v2->v3, v3->v2`,
    )
  }
}

export function validateRevalidationTarget(table: 'post' | 'engagement', fromVersion: string, toVersion: string, nativeV3Tail = false): void {
  if (nativeV3Tail && `${fromVersion}->${toVersion}` !== `${CONTENT_TIME_VALIDATOR_VERSION_V3}->${CONTENT_TIME_VALIDATOR_VERSION_V2}`) {
    throw new Error('nativeV3Tail is rollback-only (v3->v2)')
  }
  if (table === 'engagement' && fromVersion !== CONTENT_TIME_VALIDATOR_VERSION_V1) {
    throw new Error('v2/v3 semantic migration is post-only; engagement is projected at export time')
  }
}

export function revalidateContentTimeCandidate(
  candidate: Pick<
    ContentTimeRevalidationCandidate,
    'indexedAt' | 'created_at_source_raw' | 'content_time_status' | 'content_time_clamp_reason'
  >,
  toVersion: string = CONTENT_TIME_VALIDATOR_VERSION_V2,
  fromVersion: string = CONTENT_TIME_VALIDATOR_VERSION_V1,
): RevalidatedContentTime {
  validateRevalidationTransition(fromVersion, toVersion)
  const rawStr = candidate.created_at_source_raw.toString('utf8')
  const validated = validateContentTime(rawStr, candidate.indexedAt, toVersion)

  let outcome: ContentTimeRevalidationOutcome
  if (fromVersion === CONTENT_TIME_VALIDATOR_VERSION_V1 && toVersion === CONTENT_TIME_VALIDATOR_VERSION_V2) {
    outcome = validated.content_time_status === 'source_invalid'
      ? 'v1_to_v2_invalid'
      : candidate.content_time_status === 'source_valid'
        ? 'v1_valid_to_v2_valid'
        : 'v1_invalid_to_v2_valid'
  } else if (fromVersion === CONTENT_TIME_VALIDATOR_VERSION_V2 && toVersion === CONTENT_TIME_VALIDATOR_VERSION_V3) {
    if (validated.content_time_status === 'source_invalid') {
      outcome = 'v2_to_v3_invalid'
    } else if (validated.content_time_clamp_reason === 'future_skew_clamped') {
      outcome = candidate.content_time_status === 'source_valid'
        ? 'v2_skew_to_v3_clamped'
        : 'v2_invalid_to_v3_clamped'
    } else {
      outcome = 'v2_valid_to_v3_valid'
    }
  } else {
    // v3 -> v2
    if (validated.content_time_status === 'source_invalid') {
      outcome = candidate.content_time_clamp_reason === 'future_skew_clamped'
        ? 'v3_clamped_to_v2_invalid'
        : 'v3_to_v2_invalid'
    } else {
      outcome = candidate.content_time_clamp_reason === 'future_skew_clamped'
        ? 'v3_clamped_to_v2_valid'
        : 'v3_valid_to_v2_valid'
    }
  }

  return {
    createdAt: validated.legacy_created_at,
    content_time_utc: validated.content_time_utc,
    content_time_status: validated.content_time_status,
    content_time_clamp_reason: validated.content_time_clamp_reason,
    content_time_validator_version: validated.content_time_validator_version,
    outcome,
  }
}

export type ContentTimeRevalidationCounts = {
  v1_valid_to_v2_valid: number
  v1_invalid_to_v2_valid: number
  v1_to_v2_invalid: number
  v2_valid_to_v3_valid: number
  v2_skew_to_v3_clamped: number
  v2_invalid_to_v3_clamped: number
  v2_to_v3_invalid: number
  v3_valid_to_v2_valid: number
  v3_clamped_to_v2_valid: number
  v3_clamped_to_v2_invalid: number
  v3_to_v2_invalid: number
  gt_5m_restored: number
  zero_to_5m_clamped: number
  gt_5m_invalidated: number
  zero_to_5m_unclamped: number
  by_invalid_reason: Partial<Record<ContentTimeClampReason, number>>
  by_v2_invalid_reason: Partial<Record<ContentTimeClampReason, number>>
}

function emptyRevalidationCounts(): ContentTimeRevalidationCounts {
  return {
    v1_valid_to_v2_valid: 0,
    v1_invalid_to_v2_valid: 0,
    v1_to_v2_invalid: 0,
    v2_valid_to_v3_valid: 0,
    v2_skew_to_v3_clamped: 0,
    v2_invalid_to_v3_clamped: 0,
    v2_to_v3_invalid: 0,
    v3_valid_to_v2_valid: 0,
    v3_clamped_to_v2_valid: 0,
    v3_clamped_to_v2_invalid: 0,
    v3_to_v2_invalid: 0,
    gt_5m_restored: 0,
    zero_to_5m_clamped: 0,
    gt_5m_invalidated: 0,
    zero_to_5m_unclamped: 0,
    by_invalid_reason: {},
    by_v2_invalid_reason: {},
  }
}

function mergeRevalidationCounts(
  a: ContentTimeRevalidationCounts,
  b: ContentTimeRevalidationCounts,
): ContentTimeRevalidationCounts {
  const invalidReasons: Partial<Record<ContentTimeClampReason, number>> = { ...a.by_invalid_reason }
  for (const [reason, count] of Object.entries(b.by_invalid_reason)) {
    const key = reason as ContentTimeClampReason
    invalidReasons[key] = (invalidReasons[key] ?? 0) + (count ?? 0)
  }
  const v2Reasons: Partial<Record<ContentTimeClampReason, number>> = { ...a.by_v2_invalid_reason }
  for (const [reason, count] of Object.entries(b.by_v2_invalid_reason)) {
    const key = reason as ContentTimeClampReason
    v2Reasons[key] = (v2Reasons[key] ?? 0) + (count ?? 0)
  }
  return {
    v1_valid_to_v2_valid: a.v1_valid_to_v2_valid + b.v1_valid_to_v2_valid,
    v1_invalid_to_v2_valid: a.v1_invalid_to_v2_valid + b.v1_invalid_to_v2_valid,
    v1_to_v2_invalid: a.v1_to_v2_invalid + b.v1_to_v2_invalid,
    v2_valid_to_v3_valid: a.v2_valid_to_v3_valid + b.v2_valid_to_v3_valid,
    v2_skew_to_v3_clamped: a.v2_skew_to_v3_clamped + b.v2_skew_to_v3_clamped,
    v2_invalid_to_v3_clamped: a.v2_invalid_to_v3_clamped + b.v2_invalid_to_v3_clamped,
    v2_to_v3_invalid: a.v2_to_v3_invalid + b.v2_to_v3_invalid,
    v3_valid_to_v2_valid: a.v3_valid_to_v2_valid + b.v3_valid_to_v2_valid,
    v3_clamped_to_v2_valid: a.v3_clamped_to_v2_valid + b.v3_clamped_to_v2_valid,
    v3_clamped_to_v2_invalid: a.v3_clamped_to_v2_invalid + b.v3_clamped_to_v2_invalid,
    v3_to_v2_invalid: a.v3_to_v2_invalid + b.v3_to_v2_invalid,
    gt_5m_restored: a.gt_5m_restored + b.gt_5m_restored,
    zero_to_5m_clamped: a.zero_to_5m_clamped + b.zero_to_5m_clamped,
    gt_5m_invalidated: a.gt_5m_invalidated + b.gt_5m_invalidated,
    zero_to_5m_unclamped: a.zero_to_5m_unclamped + b.zero_to_5m_unclamped,
    by_invalid_reason: invalidReasons,
    by_v2_invalid_reason: v2Reasons,
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
  wal_bytes: number
  relation_bytes_before: number
  relation_bytes_after: number
}

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
  table?: 'post' | 'engagement'
  actors?: string[]
  /** Explicitly select every author in the bounded time window (post only). */
  allAuthors?: boolean
  /** Explicit rollback-only selection of every v3 row in the bounded tail. */
  nativeV3Tail?: boolean
  since: Date
  /** Exclusive receipt-time upper bound; immutable when resuming. */
  untilExclusive?: Date
  fromVersion?: string
  toVersion?: string
  batchSize: number
  packetSha256: string
  afterAuthor?: string
  afterUri?: string
  configSha256?: string
  maxBatches?: number
  maxDurationMs: number
  pauseMs: number
  pauseBaselineBytesPerSecond?: number
  lockTimeoutMs: number
  statementTimeoutMs: number
  onProgress?: (progress: ContentTimeRevalidationProgress) => void
  onCheckpoint?: (checkpoint: ContentTimeRevalidationCheckpoint) => void
}

export function contentTimeRevalidationConfigSha256(
  actors: string[],
  sinceIso: string,
  table: 'post' | 'engagement' = 'post',
  fromVersion: string = CONTENT_TIME_VALIDATOR_VERSION_V1,
  toVersion: string = CONTENT_TIME_VALIDATOR_VERSION_V2,
  allAuthors: boolean = false,
  untilExclusiveIso?: string,
  nativeV3Tail: boolean = false,
): string {
  const sortedActors = table === 'post' ? [...new Set(actors)].sort() : []
  const payload = {
    table,
    actors: sortedActors,
    since: sinceIso,
    from_validator_version: fromVersion,
    to_validator_version: toVersion,
    ...(allAuthors ? { all_authors: true } : {}),
    ...(untilExclusiveIso ? { until_exclusive: untilExclusiveIso } : {}),
    ...(nativeV3Tail ? { native_v3_tail: true } : {}),
  }
  return sha256(JSON.stringify(payload))
}

export function validateContentTimeRevalidationWindow(since: Date, untilExclusive?: Date): void {
  if (Number.isNaN(since.getTime())) throw new Error('since must be a valid timestamp')
  if (untilExclusive !== undefined) {
    if (Number.isNaN(untilExclusive.getTime())) throw new Error('untilExclusive must be a valid timestamp')
    if (untilExclusive.getTime() <= since.getTime()) {
      throw new Error('untilExclusive must be strictly after since')
    }
  }
}

/** Keep programmatic callers from accidentally widening an empty post scope. */
export function validateContentTimeRevalidationScope(
  table: 'post' | 'engagement',
  actors: string[],
  allAuthors: boolean,
): void {
  if (table === 'post') {
    if (allAuthors && actors.length > 0) {
      throw new Error('allAuthors cannot be combined with actors')
    }
    if (!allAuthors && actors.length === 0) {
      throw new Error('post revalidation requires non-empty actors or allAuthors=true')
    }
  } else if (allAuthors) {
    throw new Error('allAuthors is only valid with table=post')
  }
}

type RevalidationSelectRow = {
  uri: string
  author: string
  indexedAt: string
  created_at_source_raw: Buffer
  content_time_status: string
  content_time_clamp_reason?: string | null
}

async function selectRevalidationBatch(
  db: Database,
  table: 'post' | 'engagement',
  actors: string[],
  allAuthors: boolean,
  sinceIso: string,
  untilExclusiveIso: string | undefined,
  fromVersion: string,
  toVersion: string,
  afterAuthor: string,
  afterUri: string,
  limit: number,
  forUpdate: boolean,
  nativeV3Tail: boolean = false,
): Promise<RevalidationSelectRow[]> {
  const transitionPredicate = nativeV3Tail ? sql<boolean>`true` : revalidationSemanticDeltaSql(table, fromVersion, toVersion)
  if (table === 'post') {
    const authorPredicate = allAuthors ? sql`TRUE` : sql`author = ANY(${actors}::text[])`
    const untilPredicate = untilExclusiveIso ? sql`"indexedAt" < ${untilExclusiveIso}` : sql`TRUE`
    return forUpdate
      ? (await sql<RevalidationSelectRow>`
          SELECT uri, author, "indexedAt", created_at_source_raw, content_time_status, content_time_clamp_reason
          FROM public.post
          WHERE ${authorPredicate}
            AND "indexedAt" >= ${sinceIso}
            AND ${untilPredicate}
            AND content_time_validator_version = ${fromVersion}
            AND ${transitionPredicate}
            AND (author, uri) > (${afterAuthor}, ${afterUri})
          ORDER BY author, uri
          LIMIT ${limit}
          FOR UPDATE
        `.execute(db)).rows
      : (await sql<RevalidationSelectRow>`
          SELECT uri, author, "indexedAt", created_at_source_raw, content_time_status, content_time_clamp_reason
          FROM public.post
          WHERE ${authorPredicate}
            AND "indexedAt" >= ${sinceIso}
            AND ${untilPredicate}
            AND content_time_validator_version = ${fromVersion}
            AND ${transitionPredicate}
            AND (author, uri) > (${afterAuthor}, ${afterUri})
          ORDER BY author, uri
          LIMIT ${limit}
        `.execute(db)).rows
  } else {
    const untilPredicate = untilExclusiveIso ? sql`"indexedAt" < ${untilExclusiveIso}` : sql`TRUE`
    return forUpdate
      ? (await sql<RevalidationSelectRow>`
          SELECT uri, author, "indexedAt", created_at_source_raw, content_time_status, content_time_clamp_reason
          FROM public.engagement
          WHERE "indexedAt" >= ${sinceIso}
            AND ${untilPredicate}
            AND content_time_validator_version = ${fromVersion}
            AND ${transitionPredicate}
            AND uri > ${afterUri}
          ORDER BY uri
          LIMIT ${limit}
          FOR UPDATE
        `.execute(db)).rows
      : (await sql<RevalidationSelectRow>`
          SELECT uri, author, "indexedAt", created_at_source_raw, content_time_status, content_time_clamp_reason
          FROM public.engagement
          WHERE "indexedAt" >= ${sinceIso}
            AND ${untilPredicate}
            AND content_time_validator_version = ${fromVersion}
            AND ${transitionPredicate}
            AND uri > ${afterUri}
          ORDER BY uri
          LIMIT ${limit}
        `.execute(db)).rows
  }
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
  table: 'post' | 'engagement',
  actors: string[],
  allAuthors: boolean,
  sinceIso: string,
  untilExclusiveIso: string | undefined,
  fromVersion: string,
  toVersion: string,
  afterAuthor: string,
  afterUri: string,
  batchSize: number,
  lockTimeoutMs: number,
  statementTimeoutMs: number,
  deadlineMs: number,
  nativeV3Tail: boolean = false,
): Promise<RevalidationBatchResult> {
  return db.transaction().execute(async (trx) => {
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0) throw new Error('content-time revalidation deadline reached')
    await sql`SELECT set_config('transaction_timeout', ${`${remainingMs}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('lock_timeout', ${`${Math.min(lockTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('statement_timeout', ${`${Math.min(statementTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)

    const tableName = table === 'engagement' ? 'public.engagement' : 'public.post'
    const before = (await sql<{ lsn: string; relation_bytes: string }>`
      SELECT pg_current_wal_insert_lsn()::text AS lsn,
             pg_total_relation_size(${tableName})::text AS relation_bytes
    `.execute(trx)).rows[0]

    const rows = await selectRevalidationBatch(trx, table, actors, allAuthors, sinceIso, untilExclusiveIso, fromVersion, toVersion, afterAuthor, afterUri, batchSize, true, nativeV3Tail)
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
      const revalidated = revalidateContentTimeCandidate(row, toVersion, fromVersion)
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

    const updateResult = table === 'engagement'
      ? await sql<{
          uri: string
          outcome: string
          content_time_status: string
          content_time_clamp_reason: string | null
        }>`
          UPDATE public.engagement AS target
          SET "createdAt" = batch.created_at,
              content_time_utc = batch.content_time_utc,
              content_time_status = batch.content_time_status,
              content_time_clamp_reason = batch.content_time_clamp_reason,
              content_time_validator_version = ${toVersion}
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
            AND target.content_time_validator_version = ${fromVersion}
            AND target.created_at_source_raw = decode(batch.raw_hex, 'hex')
          RETURNING target.uri, batch.outcome AS outcome, batch.content_time_status AS content_time_status, batch.content_time_clamp_reason AS content_time_clamp_reason
        `.execute(trx)
      : await sql<{
          uri: string
          outcome: string
          content_time_status: string
          content_time_clamp_reason: string | null
        }>`
          UPDATE public.post AS target
          SET "createdAt" = batch.created_at,
              content_time_utc = batch.content_time_utc,
              content_time_status = batch.content_time_status,
              content_time_clamp_reason = batch.content_time_clamp_reason,
              content_time_validator_version = ${toVersion}
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
            AND target.content_time_validator_version = ${fromVersion}
            AND target.created_at_source_raw = decode(batch.raw_hex, 'hex')
          RETURNING target.uri, batch.outcome AS outcome, batch.content_time_status AS content_time_status, batch.content_time_clamp_reason AS content_time_clamp_reason
        `.execute(trx)

    const counts = emptyRevalidationCounts()
    for (const row of updateResult.rows) {
      const outcome = row.outcome as ContentTimeRevalidationOutcome
      if (counts[outcome] !== undefined) {
        counts[outcome] += 1
      }
      if (outcome === 'v2_invalid_to_v3_clamped') counts.gt_5m_restored += 1
      if (outcome === 'v2_skew_to_v3_clamped') counts.zero_to_5m_clamped += 1
      if (outcome === 'v3_clamped_to_v2_invalid') counts.gt_5m_invalidated += 1
      if (outcome === 'v3_clamped_to_v2_valid') counts.zero_to_5m_unclamped += 1
      if (row.content_time_status === 'source_invalid' && row.content_time_clamp_reason) {
        const reason = row.content_time_clamp_reason as ContentTimeClampReason
        counts.by_invalid_reason[reason] = (counts.by_invalid_reason[reason] ?? 0) + 1
        counts.by_v2_invalid_reason[reason] = (counts.by_v2_invalid_reason[reason] ?? 0) + 1
      }
    }

    const after = (await sql<{ wal_bytes: string; relation_bytes: string }>`
      SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), ${before.lsn}::pg_lsn)::text AS wal_bytes,
             pg_total_relation_size(${tableName})::text AS relation_bytes
    `.execute(trx)).rows[0]

    const last = rows[rows.length - 1]
    return {
      candidates: rows.length,
      updated: updateResult.rows.length,
      skipped_cas: rows.length - updateResult.rows.length,
      counts,
      cursorAuthor: last.author ?? '',
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
  const table = options.table ?? 'post'
  const fromVersion = options.fromVersion ?? CONTENT_TIME_VALIDATOR_VERSION_V1
  const toVersion = options.toVersion ?? CONTENT_TIME_VALIDATOR_VERSION_V2
  const nativeV3Tail = options.nativeV3Tail === true

  validateRevalidationTransition(fromVersion, toVersion)
  validateRevalidationTarget(table, fromVersion, toVersion, nativeV3Tail)

  if (table !== 'post' && table !== 'engagement') {
    throw new Error(`invalid table: ${table}; must be post or engagement`)
  }
  if (!/^[0-9a-f]{64}$/.test(options.packetSha256)) {
    throw new Error('packetSha256 must be a lowercase SHA-256')
  }
  const actors = options.actors ? [...new Set(options.actors)].sort() : []
  const allAuthors = options.allAuthors === true
  validateContentTimeRevalidationScope(table, actors, allAuthors)
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

  validateContentTimeRevalidationWindow(options.since, options.untilExclusive)
  const sinceIso = options.since.toISOString()
  const untilExclusiveIso = options.untilExclusive?.toISOString()
  const configSha256 = contentTimeRevalidationConfigSha256(actors, sinceIso, table, fromVersion, toVersion, allAuthors, untilExclusiveIso, nativeV3Tail)
  if (options.afterAuthor || options.afterUri) {
    if (options.configSha256 !== configSha256) {
      throw new Error('checkpoint does not match the immutable revalidation config (actors, time window, table, or validator versions changed)')
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
      table,
      actors,
      allAuthors,
      sinceIso,
      untilExclusiveIso,
      fromVersion,
      toVersion,
      cursorAuthor,
      cursorUri,
      options.batchSize,
      options.lockTimeoutMs,
      Math.min(options.statementTimeoutMs, remainingMs),
      deadlineMs,
      nativeV3Tail,
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
    const pauseRequiredMs = result.candidates === 0
      ? 0
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
      cursor_author: table === 'post' ? cursorAuthor : '',
      cursor_uri: table === 'post' ? cursorUri : (cursorUri ? sha256(cursorUri) : ''),
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
      cursor_author_sha256: table === 'post' && cursorAuthor ? sha256(cursorAuthor) : '',
      cursor_uri_sha256: cursorUri ? sha256(cursorUri) : '',
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
      complete = true
      break
    }
  }

  return {
    batch,
    scanned,
    updated,
    skipped_cas: skippedCas,
    counts,
    cursor_author_sha256: table === 'post' && cursorAuthor ? sha256(cursorAuthor) : '',
    cursor_uri_sha256: cursorUri ? sha256(cursorUri) : '',
    elapsed_ms: Date.now() - startedAt,
    packet_sha256: options.packetSha256,
    wal_bytes: lastWalBytes,
    relation_bytes_before: lastRelationBytesBefore,
    relation_bytes_after: lastRelationBytesAfter,
    complete,
    batches,
  }
}

export async function previewContentTimeRevalidation(
  db: Database,
  actors: string[],
  since: Date,
  batchSize: number = REVALIDATION_LIMITS.batchSize,
  maxRows: number = 50_000,
  table: 'post' | 'engagement' = 'post',
  fromVersion: string = CONTENT_TIME_VALIDATOR_VERSION_V1,
  toVersion: string = CONTENT_TIME_VALIDATOR_VERSION_V2,
  allAuthors: boolean = false,
  untilExclusive?: Date,
  nativeV3Tail: boolean = false,
): Promise<{ scanned: number; counts: ContentTimeRevalidationCounts; truncated: boolean }> {
  validateRevalidationTransition(fromVersion, toVersion)
  validateRevalidationTarget(table, fromVersion, toVersion, nativeV3Tail)
  const sortedActors = table === 'post' ? [...new Set(actors)].sort() : []
  validateContentTimeRevalidationScope(table, sortedActors, allAuthors)
  validateContentTimeRevalidationWindow(since, untilExclusive)
  const sinceIso = since.toISOString()
  const untilExclusiveIso = untilExclusive?.toISOString()
  let cursorAuthor = ''
  let cursorUri = ''
  let scanned = 0
  let counts = emptyRevalidationCounts()
  let truncated = false

  while (scanned < maxRows) {
    const limit = Math.min(batchSize, maxRows - scanned)
    const rows = await selectRevalidationBatch(
      db,
      table,
      sortedActors,
      allAuthors,
      sinceIso,
      untilExclusiveIso,
      fromVersion,
      toVersion,
      cursorAuthor,
      cursorUri,
      limit,
      false,
      nativeV3Tail,
    )
    if (rows.length === 0) break
    for (const row of rows) {
      const revalidated = revalidateContentTimeCandidate(row, toVersion, fromVersion)
      const outcome = revalidated.outcome
      if (counts[outcome] !== undefined) {
        counts[outcome] += 1
      }
      if (outcome === 'v2_invalid_to_v3_clamped') counts.gt_5m_restored += 1
      if (outcome === 'v2_skew_to_v3_clamped') counts.zero_to_5m_clamped += 1
      if (outcome === 'v3_clamped_to_v2_invalid') counts.gt_5m_invalidated += 1
      if (outcome === 'v3_clamped_to_v2_valid') counts.zero_to_5m_unclamped += 1
      if (revalidated.content_time_status === 'source_invalid' && revalidated.content_time_clamp_reason) {
        const reason = revalidated.content_time_clamp_reason
        counts.by_invalid_reason[reason] = (counts.by_invalid_reason[reason] ?? 0) + 1
        counts.by_v2_invalid_reason[reason] = (counts.by_v2_invalid_reason[reason] ?? 0) + 1
      }
    }
    scanned += rows.length
    const last = rows[rows.length - 1]
    cursorAuthor = last.author ?? ''
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
  let planFromDb = false

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
    if (arg === '--plan-from-db') {
      planFromDb = true
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
  const planLimitRaw = flags.get('plan-limit')?.at(-1)
  let planLimit: number | undefined
  if (planLimitRaw !== undefined) {
    planLimit = Number.parseInt(planLimitRaw, 10)
    if (!Number.isInteger(planLimit) || String(planLimit) !== planLimitRaw.trim() || planLimit < 1) {
      throw new Error('--plan-limit must be a positive integer')
    }
  }

  const planUrisFile = flags.get('plan-uris-file')?.at(-1)
  if (planUrisFile !== undefined && !planFromDb) {
    throw new Error('--plan-uris-file requires --plan-from-db')
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
    planFromDb,
    planLimit,
    planUrisFile,
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
  table: 'post' | 'engagement'
  fromVersion: string
  toVersion: string
  actors?: string[]
  allAuthors: boolean
  nativeV3Tail: boolean
  since: Date
  untilExclusive?: Date
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
  let allAuthors = false
  let nativeV3Tail = false

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
    if (arg === '--all-authors') {
      allAuthors = true
      continue
    }
    if (arg === '--native-v3-tail') {
      nativeV3Tail = true
      continue
    }
    if (!arg.startsWith('--')) throw new Error(`Unexpected positional argument: ${arg}`)
    const value = argv[i + 1]
    if (!value || value.startsWith('--')) throw new Error(`Missing value for ${arg}`)
    i += 1
    const key = arg.slice(2)
    flags.set(key, [...(flags.get(key) || []), value])
  }

  const tableRaw = flags.get('table')?.at(-1) || 'post'
  if (tableRaw !== 'post' && tableRaw !== 'engagement') {
    throw new Error('--table must be post or engagement')
  }
  const table = tableRaw as 'post' | 'engagement'
  if (allAuthors && table !== 'post') {
    throw new Error('--all-authors is only valid with --table post')
  }

  const fromVersion = flags.get('from-version')?.at(-1) || CONTENT_TIME_VALIDATOR_VERSION_V1
  const toVersion = flags.get('to-version')?.at(-1) || CONTENT_TIME_VALIDATOR_VERSION_V2

  validateRevalidationTransition(fromVersion, toVersion)
  validateRevalidationTarget(table, fromVersion, toVersion, nativeV3Tail)

  const actorsCsv = [
    ...splitCsv(flags.get('actors')?.join(',')),
    ...splitCsv(flags.get('dids')?.join(',')),
  ]
  if (allAuthors && actorsCsv.length > 0) {
    throw new Error('--all-authors cannot be combined with --actors or --dids')
  }

  const sinceRaw = flags.get('since')?.at(-1)
  const since = sinceRaw ? new Date(sinceRaw) : new Date(Date.now() - 14 * 24 * 60 * 60 * 1000)
  if (isNaN(since.getTime())) throw new Error('--since must be an ISO timestamp')
  const untilRaw = flags.get('until')?.at(-1)
  const untilExclusive = untilRaw ? new Date(untilRaw) : undefined
  if (untilRaw && Number.isNaN(untilExclusive!.getTime())) throw new Error('--until must be an ISO timestamp')
  validateContentTimeRevalidationWindow(since, untilExclusive)

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
    table,
    fromVersion,
    toVersion,
    actors: actorsCsv.length ? actorsCsv : undefined,
    allAuthors,
    nativeV3Tail,
    since,
    untilExclusive,
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
    typeof checkpoint.cursor_uri !== 'string'
  ) {
    throw new Error('checkpoint does not match the approved revalidation packet (actors, time window, table, validator versions, or packet-sha256 changed)')
  }
  return {
    cursorAuthor: typeof checkpoint.cursor_author === 'string' ? checkpoint.cursor_author : '',
    cursorUri: checkpoint.cursor_uri,
  }
}

async function mainRevalidate(argv: string[]) {
  const startedAt = Date.now()
  const options = parseRevalidateCliArgs(argv)
  if (!options.dbUrl) {
    throw new Error('FEEDGEN_POSTGRES_URL or --db-url is required for --mode revalidate')
  }

  const db = createDb(options.dbUrl)
  try {
    let actors: string[] = []
    if (options.table === 'post') {
      actors = options.allAuthors
        ? []
        : options.actors && options.actors.length
        ? [...new Set(options.actors)].sort()
        : (await resolvePublisherDids(db)).sort()
      if (!options.allAuthors && !actors.length) {
        throw new Error('no enabled publisher DIDs resolved from feedgen_ops.feed_catalog; pass --actors explicitly')
      }
    }

    let revalidation: ContentTimeRevalidationResult | null = null
    let preview: { scanned: number; counts: ContentTimeRevalidationCounts; truncated: boolean } | null = null

    if (options.apply) {
      if (!options.checkpointFile) throw new Error('--checkpoint-file is required with --apply')
      if (!options.packetSha256 || !/^[0-9a-f]{64}$/.test(options.packetSha256)) {
        throw new Error('--packet-sha256 is required with --apply')
      }
      const configSha256 = contentTimeRevalidationConfigSha256(
        actors,
        options.since.toISOString(),
        options.table,
        options.fromVersion,
        options.toVersion,
        options.allAuthors,
        options.untilExclusive?.toISOString(),
        options.nativeV3Tail,
      )
      const checkpoint = readRevalidationCheckpoint(options.checkpointFile, configSha256, options.packetSha256)
      const deadlineMs = startedAt + REVALIDATION_LIMITS.maxDurationMs
      const remainingMs = deadlineMs - Date.now()
      if (remainingMs <= 0) throw new Error('content-time revalidation exhausted its own startup deadline')
      revalidation = await runContentTimeRevalidation(db, {
        table: options.table,
        actors,
        allAuthors: options.allAuthors,
        nativeV3Tail: options.nativeV3Tail,
        since: options.since,
        untilExclusive: options.untilExclusive,
        fromVersion: options.fromVersion,
        toVersion: options.toVersion,
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
        options.table,
        options.fromVersion,
        options.toVersion,
        options.allAuthors,
        options.untilExclusive,
        options.nativeV3Tail,
      )
    }

    const summary = {
      operation: 'content-time-revalidate',
      mode: options.apply ? 'apply' : 'dry-run',
      table: options.table,
      all_authors: options.allAuthors,
      native_v3_tail: options.nativeV3Tail,
      actor_count: actors.length,
      actor_sha256: actors.map(sha256).sort(),
      since: options.since.toISOString(),
      until_exclusive: options.untilExclusive?.toISOString() ?? null,
      from_validator_version: options.fromVersion,
      to_validator_version: options.toVersion,
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
  let plan: BackfillPlan | DbPlan
  if (options.planFromDb) {
    if (!options.dbUrl) throw new Error('--plan-from-db needs FEEDGEN_POSTGRES_URL or --db-url')
    const db = createDb(options.dbUrl)
    try {
      let planUris: string[] | undefined
      if (options.planUrisFile) {
        planUris = fs.readFileSync(options.planUrisFile, 'utf8').split('\n').map((l) => l.trim()).filter(Boolean)
        if (!planUris.length) throw new Error('--plan-uris-file is empty')
      }
      plan = await collectPublisherPostsFromDb(db, {
        actors: options.actors,
        since: options.since,
        until: options.until,
        deadlineMs,
        limit: options.planLimit,
        planUris,
        fetchPosts: (uris) => fetchPostsByUri(options.apiBase, uris, Math.max(1, Math.min(30_000, deadlineMs - Date.now()))),
      })
    } finally {
      await db.destroy()
    }
  } else {
    plan = await collectPublisherPosts({
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
  }

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
      await assertPublisherRecoveryContract(db)
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
    plan_source: options.planFromDb ? (options.planUrisFile ? 'plan-uris-file+getPosts' : 'db-legacy-rows+getPosts') : 'author-feed',
    ...('db_legacy_in_window' in plan
      ? { db_legacy_in_window: plan.db_legacy_in_window, plan_source_rows: plan.plan_source_rows, plan_uris_not_in_db: plan.plan_uris_not_in_db, unretrievable: plan.unretrievable, unretrievable_uri_sha256_sample: plan.unretrievable_uri_sha256_sample, appview_requests: plan.requests }
      : {}),
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
