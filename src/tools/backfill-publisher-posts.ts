import crypto from 'crypto'
import fs from 'fs'
import path from 'path'
import { sql } from 'kysely'
import { createDb } from '../db'
import type { Database } from '../db'
import { dualWriteLinkFields } from '../util/link-fields'
import { validateContentTime } from '../util/content-time'

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
}

export type PublisherPostRecoveryProgress = {
  batch: number
  scanned: number
  inserted: number
  recovered: number
  already_current: number
  cursor_sha256: string
  elapsed_ms: number
}

export type PublisherPostRecoveryResult = PublisherPostRecoveryProgress & {
  complete: boolean
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

      for (const post of authorPage.posts) {
        scanned += 1
        byActor[actor].scanned += 1
        const normalized = normalizeAppViewPost(post, actor)
        if (!normalized) {
          skippedWrongAuthor += 1
          byActor[actor].skipped_wrong_author += 1
          continue
        }
        const created = parseDate(normalized.createdAt)
        if (!created || created < options.since || created >= options.until) {
          skippedOutOfWindow += 1
          byActor[actor].skipped_out_of_window += 1
          continue
        }
        posts.set(normalized.uri, normalized)
        byActor[actor].candidate_posts += 1
      }

      if (!authorPage.cursor) break
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
) {
  return db.transaction().execute(async (trx) => {
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0) throw new Error('content-time recovery deadline reached')
    await sql`SELECT set_config('transaction_timeout', ${`${remainingMs}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('lock_timeout', ${`${Math.min(lockTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)
    await sql`SELECT set_config('statement_timeout', ${`${Math.min(statementTimeoutMs, remainingMs)}ms`}, true)`.execute(trx)

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

    for (const row of batch) {
      const current = existing.get(row.uri)
      if (!current) inserts.push(row)
      else {
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

    return {
      inserted: inserts.length,
      recovered: recoveries.length,
      already_current: alreadyCurrent,
    }
  })
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
  let cursorUri = options.afterUri ?? ''
  let complete = true

  for (let offset = 0; offset < ordered.length; offset += options.batchSize) {
    const remainingMs = deadlineMs - Date.now()
    if (remainingMs <= 0 || (options.maxBatches !== undefined && batch >= options.maxBatches)) {
      complete = false
      break
    }
    const rows = ordered.slice(offset, offset + options.batchSize)
    const result = await applyRecoveryBatch(
      db,
      rows,
      options.lockTimeoutMs,
      Math.min(options.statementTimeoutMs, remainingMs),
      deadlineMs,
    )
    batch += 1
    scanned += rows.length
    inserted += result.inserted
    recovered += result.recovered
    alreadyCurrent += result.already_current
    cursorUri = rows[rows.length - 1].uri
    const progress: PublisherPostRecoveryProgress = {
      batch,
      scanned,
      inserted,
      recovered,
      already_current: alreadyCurrent,
      cursor_sha256: sha256(cursorUri),
      elapsed_ms: Date.now() - startedAt,
    }
    options.onProgress?.(progress)
    options.onCheckpoint?.({
      ...progress,
      packet_sha256: options.packetSha256,
      plan_sha256: planSha256,
      cursor_uri: cursorUri,
    })
    if (offset + rows.length < ordered.length && options.pauseMs > 0) {
      await sleep(Math.min(options.pauseMs, Math.max(0, deadlineMs - Date.now())))
    }
  }

  return {
    batch,
    scanned,
    inserted,
    recovered,
    already_current: alreadyCurrent,
    cursor_sha256: cursorUri ? sha256(cursorUri) : '',
    elapsed_ms: Date.now() - startedAt,
    complete,
  }
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

function writeCheckpoint(file: string, checkpoint: object) {
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

async function main() {
  const startedAt = Date.now()
  const options = parseCliArgs(process.argv.slice(2))
  const deadlineMs = startedAt + RECOVERY_LIMITS.maxDurationMs
  const plan = await collectPublisherPosts({
    actors: options.actors,
    since: options.since,
    until: options.until,
    maxPagesPerActor: options.maxPagesPerActor,
    deadlineMs,
    fetchPage: (actor, cursor) => fetchAuthorFeedPage(
      options.apiBase,
      actor,
      cursor,
      Math.max(1, Math.min(30_000, deadlineMs - Date.now())),
    ),
  })

  let recovery: PublisherPostRecoveryResult | null = null
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
    mode: options.apply ? 'apply' : 'dry-run',
    actor_count: options.actors.length,
    actor_sha256: options.actors.map(sha256).sort(),
    since: options.since.toISOString(),
    until: options.until.toISOString(),
    scanned: plan.scanned,
    candidate_posts: plan.posts.length,
    skipped_out_of_window: plan.skipped_out_of_window,
    skipped_wrong_author: plan.skipped_wrong_author,
    by_actor: Object.fromEntries(
      Object.entries(plan.by_actor).map(([actor, counts]) => [sha256(actor), counts]),
    ),
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

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message || err)
    process.exit(1)
  })
}
