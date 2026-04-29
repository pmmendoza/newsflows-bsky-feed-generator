import dotenv from 'dotenv'
import { sql } from 'kysely'
import { createDb, Database } from './db'
import { ArchiveOutbox, Post } from './db/schema'

type ArchivePayload = {
  schema_version?: number
  captured_from?: string
  request?: {
    request_id?: number
    position?: number
    feed_id?: string | null
    study_id?: string | null
    requester_did?: string | null
    requested_at?: string
    cursor_in?: string | null
    cursor_out?: string | null
    requested_limit?: number | null
    result_count?: number | null
    feedgen_build_sha?: string | null
    algo_policy_id?: string | null
    ranker_run_id?: string | null
  }
  post?: Post
}

const maybeStr = (val?: string) => (val ? val : undefined)

const maybeInt = (val: string | undefined, fallback: number) => {
  if (!val) return fallback
  const parsed = parseInt(val, 10)
  return Number.isFinite(parsed) ? parsed : fallback
}

const connectionString = () => {
  const url = maybeStr(process.env.FEEDGEN_POSTGRES_URL)
  if (url) return url

  const host = maybeStr(process.env.FEEDGEN_DB_HOST) ?? 'localhost'
  const port = maybeInt(process.env.FEEDGEN_DB_PORT, 5432)
  const user = maybeStr(process.env.FEEDGEN_DB_USER) ?? 'feedgen'
  const password = maybeStr(process.env.FEEDGEN_DB_PASSWORD) ?? 'feedgen'
  const database =
    maybeStr(process.env.FEEDGEN_DB_DATABASE) ??
    maybeStr(process.env.FEEDGEN_DB_BASE) ??
    'feedgen-db'

  return `postgres://${user}:${password}@${host}:${port}/${database}`
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const parseDate = (val?: string | Date | null): Date | null => {
  if (!val) return null
  if (val instanceof Date) return Number.isNaN(val.getTime()) ? null : val
  const parsed = new Date(val)
  return Number.isNaN(parsed.getTime()) ? null : parsed
}

const toErrorText = (error: unknown) => {
  const message = error instanceof Error ? error.stack || error.message : String(error)
  return message.slice(0, 4000)
}

const asPayload = (row: ArchiveOutbox): ArchivePayload => row.payload_json as ArchivePayload

async function drainOnce(db: Database, batchSize: number, maxAttempts: number) {
  const rows = await db
    .selectFrom('feedgen_ops.archive_outbox')
    .selectAll()
    .orderBy('enqueued_at', 'asc')
    .limit(batchSize)
    .execute()

  for (const row of rows) {
    try {
      await archiveRow(db, row)
    } catch (error) {
      await handleFailure(db, row, error, maxAttempts)
    }
  }

  return rows.length
}

async function archiveRow(db: Database, row: ArchiveOutbox) {
  const payload = asPayload(row)
  const request = payload.request ?? {}
  const post = payload.post

  if (!post) {
    throw new Error(`archive outbox row ${row.outbox_id} has no payload.post`)
  }

  const requestedAt = parseDate(row.requested_at) ?? new Date()
  const createdAt = parseDate(post.createdAt)
  const indexedAt = parseDate(post.indexedAt)

  await db.transaction().execute(async (trx) => {
    await trx
      .insertInto('research_archive.request_event')
      .values({
        request_id: row.request_id,
        feed_id: row.feed_id ?? request.feed_id ?? null,
        study_id: row.study_id ?? request.study_id ?? null,
        requester_ref: row.requester_did ?? request.requester_did ?? null,
        requested_at: requestedAt,
        cursor_in: request.cursor_in ?? null,
        cursor_out: request.cursor_out ?? null,
        requested_limit: request.requested_limit ?? null,
        result_count: request.result_count ?? null,
        feedgen_build_sha: request.feedgen_build_sha ?? process.env.FEEDGEN_BUILD_SHA ?? null,
        algo_policy_id: request.algo_policy_id ?? row.feed_id ?? null,
        ranker_run_id: request.ranker_run_id ?? null,
      })
      .onConflict((oc) => oc.column('request_id').doNothing())
      .execute()

    await trx
      .insertInto('research_archive.post_snapshot')
      .values({
        post_uri: post.uri,
        cid: post.cid,
        author_did: post.author,
        created_at: createdAt,
        indexed_at: indexedAt,
        created_at_raw: post.createdAt,
        indexed_at_raw: post.indexedAt,
        text: post.text,
        root_uri: post.rootUri,
        root_cid: post.rootCid,
        link_url: post.linkUrl,
        link_title: post.linkTitle,
        link_description: post.linkDescription,
        raw_record_json: payload,
        first_seen_at: indexedAt,
        first_captured_from: payload.captured_from ?? 'served',
      })
      .onConflict((oc) => oc.columns(['post_uri', 'cid']).doNothing())
      .execute()

    await trx
      .insertInto('research_archive.post_snapshot_capture_source')
      .values({
        post_uri: post.uri,
        cid: post.cid,
        captured_from: payload.captured_from ?? 'served',
      })
      .onConflict((oc) =>
        oc.columns(['post_uri', 'cid', 'captured_from']).doUpdateSet({
          last_captured_at: sql`now()`,
          observation_count: sql`research_archive.post_snapshot_capture_source.observation_count + 1`,
        }),
      )
      .execute()

    await trx
      .insertInto('research_archive.served_post_event')
      .values({
        request_id: row.request_id,
        position: row.position,
        feed_id: row.feed_id ?? request.feed_id ?? null,
        study_id: row.study_id ?? request.study_id ?? null,
        post_uri: post.uri,
        post_cid: post.cid,
        likes_count: post.likes_count ?? null,
        repost_count: post.repost_count ?? null,
        comments_count: post.comments_count ?? null,
        quote_count: post.quote_count ?? null,
        priority: post.priority ?? null,
        priority_source: 'public.post.priority',
        ranker_run_id: request.ranker_run_id ?? null,
        payload_status: 'present',
      })
      .onConflict((oc) => oc.columns(['request_id', 'position']).doNothing())
      .execute()

    await trx
      .deleteFrom('feedgen_ops.archive_outbox')
      .where('outbox_id', '=', row.outbox_id as any)
      .execute()
  })
}

async function handleFailure(
  db: Database,
  row: ArchiveOutbox,
  error: unknown,
  maxAttempts: number,
) {
  const nextAttempts = (row.attempts ?? 0) + 1
  const lastError = toErrorText(error)

  if (nextAttempts >= maxAttempts) {
    await db.transaction().execute(async (trx) => {
      await trx
        .insertInto('feedgen_ops.archive_outbox_dlq')
        .values({
          outbox_id: row.outbox_id as any,
          request_id: row.request_id,
          position: row.position,
          feed_id: row.feed_id ?? null,
          study_id: row.study_id ?? null,
          requested_at: row.requested_at,
          post_uri: row.post_uri,
          post_cid: row.post_cid,
          payload_json: row.payload_json,
          payload_schema_version: row.payload_schema_version ?? 1,
          attempts: nextAttempts,
          last_error: lastError,
        })
        .onConflict((oc) => oc.column('outbox_id').doUpdateSet({
          attempts: nextAttempts,
          last_error: lastError,
          failed_at: sql`now()`,
        }))
        .execute()

      await trx
        .deleteFrom('feedgen_ops.archive_outbox')
        .where('outbox_id', '=', row.outbox_id as any)
        .execute()
    })
    return
  }

  await db
    .updateTable('feedgen_ops.archive_outbox')
    .set({
      attempts: nextAttempts,
      last_attempt_at: new Date(),
      last_error: lastError,
    })
    .where('outbox_id', '=', row.outbox_id as any)
    .execute()
}

async function main() {
  dotenv.config()

  const db = createDb(connectionString())
  const batchSize = maybeInt(process.env.FEEDGEN_ARCHIVE_WORKER_BATCH_SIZE, 500)
  const idleMs = maybeInt(process.env.FEEDGEN_ARCHIVE_WORKER_IDLE_MS, 1000)
  const maxAttempts = maybeInt(process.env.FEEDGEN_ARCHIVE_WORKER_MAX_ATTEMPTS, 5)

  console.log(
    `[${new Date().toISOString()}] archive worker started batch_size=${batchSize} idle_ms=${idleMs} max_attempts=${maxAttempts}`,
  )

  try {
    while (true) {
      const drained = await drainOnce(db, batchSize, maxAttempts)
      if (drained === 0) {
        await sleep(idleMs)
      }
    }
  } finally {
    await db.destroy()
  }
}

main().catch((error) => {
  console.error('archive worker failed:', error)
  process.exit(1)
})
