import assert from 'assert'
import { Subscription } from '@atproto/xrpc-server'
import { sql } from 'kysely'
import { createDb, migrateToLatest } from '../db'
import type { Database } from '../db'
import { ids, lexicons } from '../lexicon/lexicons'
import {
  OutputSchema as RepoEvent,
  isCommit,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscription } from '../subscription'

type Options = {
  connectionString: string
  serviceUrl: string
  minStoredRows: number
  maxFrames: number
  timeoutMs: number
  minDurationMs?: number
}

export type FirehoseLiveDbRehearsalResult = {
  status: 'ok'
  transport: 'xrpc_websocket'
  store_mode: 'disposable_db'
  service_host: string
  scoped_ingestion: 'false'
  frame_count: number
  commit_count: number
  post_count: number
  engagement_count: number
  stored_total: number
  min_stored_rows: number
  lowest_seq: number
  highest_seq: number
  cursor_persisted: boolean
  cursor_value: number
  timeout_ms: number
  max_frames: number
  soak_status: 'ok' | 'skipped'
  soak_min_duration_ms: number
  soak_observed_duration_ms: number
  soak_frame_count: number
  soak_stored_total: number
  raw_values_in_output: false
}

type RowCounts = {
  posts: number
  engagements: number
}

export async function runFirehoseLiveDbRehearsal(
  options: Options,
): Promise<FirehoseLiveDbRehearsalResult> {
  assert.ok(options.connectionString, 'connectionString is required')
  assert.ok(options.minStoredRows > 0, 'minStoredRows must be positive')
  assert.ok(options.maxFrames > 0, 'maxFrames must be positive')
  assert.ok(options.timeoutMs > 0, 'timeoutMs must be positive')
  const minDurationMs = options.minDurationMs ?? 0
  assert.ok(minDurationMs >= 0, 'minDurationMs must not be negative')
  assert.ok(
    minDurationMs < options.timeoutMs,
    'minDurationMs must be lower than timeoutMs',
  )

  const previousScopedIngestion = process.env.FEEDGEN_SCOPED_INGESTION
  const previousAllowlistRefresh = process.env.FEEDGEN_ALLOWLIST_REFRESH_MS
  process.env.FEEDGEN_SCOPED_INGESTION = 'false'
  process.env.FEEDGEN_ALLOWLIST_REFRESH_MS = '0'

  const db = createDb(options.connectionString)
  try {
    await migrateToLatest(db)
    const before = await readCounts(db)
    const result = await consumeLiveRelayIntoDb({
      db,
      serviceUrl: options.serviceUrl,
      minStoredRows: options.minStoredRows,
      maxFrames: options.maxFrames,
      timeoutMs: options.timeoutMs,
      minDurationMs,
      before,
    })
    return result
  } finally {
    await db.destroy()
    if (previousScopedIngestion === undefined) {
      delete process.env.FEEDGEN_SCOPED_INGESTION
    } else {
      process.env.FEEDGEN_SCOPED_INGESTION = previousScopedIngestion
    }
    if (previousAllowlistRefresh === undefined) {
      delete process.env.FEEDGEN_ALLOWLIST_REFRESH_MS
    } else {
      process.env.FEEDGEN_ALLOWLIST_REFRESH_MS = previousAllowlistRefresh
    }
  }
}

async function consumeLiveRelayIntoDb(options: {
  db: Database
  serviceUrl: string
  minStoredRows: number
  maxFrames: number
  timeoutMs: number
  minDurationMs: number
  before: RowCounts
}): Promise<FirehoseLiveDbRehearsalResult> {
  const startedAt = Date.now()
  const abort = new AbortController()
  const serviceHost = new URL(options.serviceUrl).host
  const firehose = new FirehoseSubscription(options.db, options.serviceUrl)
  let frameCount = 0
  let commitCount = 0
  let lastCursor: number | null = null
  const seqs: number[] = []

  const timeout = setTimeout(() => {
    abort.abort(new Error('live db rehearsal timeout'))
  }, options.timeoutMs)

  const sub = new Subscription<RepoEvent>({
    service: options.serviceUrl,
    method: ids.ComAtprotoSyncSubscribeRepos,
    signal: abort.signal,
    validate: (value: unknown) => {
      try {
        return lexicons.assertValidXrpcMessage<RepoEvent>(
          ids.ComAtprotoSyncSubscribeRepos,
          value,
        )
      } catch (err) {
        console.error('live db rehearsal skipped invalid message', err)
      }
    },
  })

  try {
    try {
      for await (const event of sub) {
        frameCount += 1
        if (isCommit(event)) {
          commitCount += 1
          seqs.push(event.seq)
          await firehose.handleEvent(event)
          await firehose.updateCursor(event.seq)
          lastCursor = event.seq
        }

        const counts = await readCounts(options.db)
        const storedTotal =
          counts.posts -
          options.before.posts +
          counts.engagements -
          options.before.engagements
        if (
          storedTotal >= options.minStoredRows &&
          frameCount >= options.maxFrames &&
          Date.now() - startedAt >= options.minDurationMs
        ) {
          abort.abort(new Error('live db rehearsal frame limit reached'))
        }
      }
    } catch (err) {
      if (
        !(err instanceof Error) ||
        (err.message !== 'live db rehearsal frame limit reached' &&
          err.message !== 'live db rehearsal timeout')
      ) {
        throw err
      }
    }

    const after = await readCounts(options.db)
    const storedPostCount = after.posts - options.before.posts
    const storedEngagementCount = after.engagements - options.before.engagements
    const storedTotal = storedPostCount + storedEngagementCount
    const durationMs = Date.now() - startedAt
    assert.ok(frameCount > 0, 'relay produced no valid frames')
    assert.ok(commitCount > 0, 'relay produced no commit frames')
    assert.ok(seqs.length > 0, 'relay produced no sequence-bearing commits')
    assert.ok(
      storedTotal >= options.minStoredRows,
      'live relay did not produce enough stored disposable DB rows',
    )

    const persistedCursor = await readPersistedCursor(
      options.db,
      options.serviceUrl,
    )
    assert.ok(typeof persistedCursor === 'number', 'cursor was not persisted')
    assert.equal(persistedCursor, lastCursor)
    if (options.minDurationMs > 0) {
      assert.ok(
        durationMs >= options.minDurationMs,
        'live DB soak duration ended before the minimum duration',
      )
      assert.ok(
        frameCount >= options.maxFrames,
        'live DB soak ended before the frame floor',
      )
    }

    return {
      status: 'ok',
      transport: 'xrpc_websocket',
      store_mode: 'disposable_db',
      service_host: serviceHost,
      scoped_ingestion: 'false',
      frame_count: frameCount,
      commit_count: commitCount,
      post_count: storedPostCount,
      engagement_count: storedEngagementCount,
      stored_total: storedTotal,
      min_stored_rows: options.minStoredRows,
      lowest_seq: Math.min(...seqs),
      highest_seq: Math.max(...seqs),
      cursor_persisted: true,
      cursor_value: persistedCursor,
      timeout_ms: options.timeoutMs,
      max_frames: options.maxFrames,
      soak_status: options.minDurationMs > 0 ? 'ok' : 'skipped',
      soak_min_duration_ms: options.minDurationMs,
      soak_observed_duration_ms: durationMs,
      soak_frame_count: frameCount,
      soak_stored_total: storedTotal,
      raw_values_in_output: false,
    }
  } finally {
    clearTimeout(timeout)
  }
}

async function readCounts(db: Database): Promise<RowCounts> {
  const [posts, engagements] = await Promise.all([
    sql<{ count: string }>`SELECT COUNT(*)::text AS count FROM post`.execute(db),
    sql<{ count: string }>`SELECT COUNT(*)::text AS count FROM engagement`.execute(
      db,
    ),
  ])
  return {
    posts: Number(posts.rows[0]?.count ?? 0),
    engagements: Number(engagements.rows[0]?.count ?? 0),
  }
}

async function readPersistedCursor(
  db: Database,
  serviceUrl: string,
): Promise<number | null> {
  const cursor = await db
    .selectFrom('sub_state')
    .select('cursor')
    .where('service', '=', serviceUrl)
    .executeTakeFirst()
  return cursor ? Number(cursor.cursor) : null
}
