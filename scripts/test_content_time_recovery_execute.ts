/**
 * Destructive disposable-PostgreSQL rehearsal for bounded publisher recovery.
 *
 * FEEDGEN_TEST_DSN=postgresql://.../throwaway \
 * FEEDGEN_CONTENT_TIME_RECOVERY_REHEARSAL=1 \
 *   npx ts-node scripts/test_content_time_recovery_execute.ts
 */
import assert from 'assert'
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import {
  normalizeAppViewPost,
  RECOVERY_LIMITS,
  runPublisherPostRecovery,
  type BackfillPostRow,
} from '../src/tools/backfill-publisher-posts'

const PACKET_SHA = '1'.repeat(64)
const PUBLISHER = 'did:plc:publisher'

function recoveredPost(rkey: string, createdAt: string): BackfillPostRow {
  const row = normalizeAppViewPost({
    uri: `at://${PUBLISHER}/app.bsky.feed.post/${rkey}`,
    cid: `cid-${rkey}`,
    author: { did: PUBLISHER },
    indexedAt: '2026-08-16T12:00:00.000Z',
    record: { createdAt, text: rkey },
  }, PUBLISHER)
  assert(row)
  return row
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_CONTENT_TIME_RECOVERY_REHEARSAL !== '1') {
    console.log('SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_CONTENT_TIME_RECOVERY_REHEARSAL=1')
    return
  }

  const pool = new Pool({ connectionString: dsn })
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool }) })
  try {
    assert.deepEqual(RECOVERY_LIMITS, {
      batchSize: 500,
      maxDurationMs: 30 * 60 * 1000,
      pauseMs: 1000,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    await sql`DROP SCHEMA IF EXISTS public CASCADE`.execute(db)
    await sql`CREATE SCHEMA public`.execute(db)
    await sql`
      CREATE TABLE public.post (
        uri text PRIMARY KEY,
        cid text NOT NULL,
        "indexedAt" text NOT NULL,
        "createdAt" text NOT NULL,
        created_at_source_raw bytea,
        content_time_utc text,
        content_time_status text,
        content_time_clamp_reason text,
        content_time_validator_version text,
        author text NOT NULL,
        text text NOT NULL,
        "rootUri" text NOT NULL,
        "rootCid" text NOT NULL,
        link_uri text NOT NULL,
        link_title text NOT NULL,
        link_description text NOT NULL,
        "linkUrl" text NOT NULL,
        "linkTitle" text NOT NULL,
        "linkDescription" text NOT NULL
      )
    `.execute(db)

    const legacy = recoveredPost('legacy', '2026-08-16T11:55:00.000Z')
    const current = recoveredPost('current', '2026-08-16T11:54:00.000Z')
    const missing = recoveredPost('missing', '2026-08-16T11:53:00.000Z')
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
        link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription"
      ) VALUES (
        ${legacy.uri}, ${legacy.cid}, ${legacy.indexedAt}, '2026-08-16T12:00:00.000Z',
        ${legacy.author}, ${legacy.text}, '', '', '', '', '', '', '', ''
      )
    `.execute(db)
    await db.insertInto('post').values(current).execute()

    const columnsBefore = await sql<{ column_name: string }>`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'post'
      ORDER BY ordinal_position
    `.execute(db)
    let checkpoint: { cursor_uri: string; plan_sha256: string } | undefined
    const first = await runPublisherPostRecovery(db as any, {
      posts: [missing, current, legacy],
      packetSha256: PACKET_SHA,
      batchSize: 2,
      maxBatches: 1,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
      onCheckpoint: (value) => { checkpoint = value },
    })
    assert.equal(first.complete, false)
    assert.equal(first.scanned, 2)

    const resumed = await runPublisherPostRecovery(db as any, {
      posts: [missing, current, legacy],
      packetSha256: PACKET_SHA,
      batchSize: 2,
      afterUri: checkpoint!.cursor_uri,
      planSha256: checkpoint!.plan_sha256,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(resumed.complete, true)

    await assert.rejects(
      runPublisherPostRecovery(db as any, {
        posts: [missing, current, legacy, recoveredPost('late', '2026-08-16T11:56:00.000Z')],
        packetSha256: PACKET_SHA, batchSize: 2,
        afterUri: checkpoint!.cursor_uri, planSha256: checkpoint!.plan_sha256,
        maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000,
        statementTimeoutMs: 30_000,
      }),
      /checkpoint does not match the immutable recovery plan/,
    )

    const rows = await sql<any>`
      SELECT uri, "createdAt", encode(created_at_source_raw, 'escape') AS raw,
             content_time_utc, content_time_status, content_time_validator_version
      FROM public.post ORDER BY uri
    `.execute(db)
    assert.equal(rows.rows.length, 3)
    const recoveredLegacy = rows.rows.find((row: any) => row.uri === legacy.uri)
    assert.equal(recoveredLegacy.createdAt, '2026-08-16T12:00:00.000Z')
    assert.equal(recoveredLegacy.raw, '2026-08-16T11:55:00.000Z')
    assert.equal(recoveredLegacy.content_time_status, 'source_valid')
    assert.equal(recoveredLegacy.content_time_validator_version, 'newsflows-content-time/v2')

    const receiptMismatch = recoveredPost('receipt-mismatch', '2026-08-16T11:58:00.000Z')
    await db.insertInto('post').values({
      ...receiptMismatch,
      indexedAt: '2026-08-16T11:00:00.000Z',
      createdAt: '2026-08-16T11:00:00.000Z',
      created_at_source_raw: null,
      content_time_utc: null,
      content_time_status: null,
      content_time_clamp_reason: null,
      content_time_validator_version: null,
    }).execute()
    await runPublisherPostRecovery(db as any, {
      posts: [receiptMismatch], packetSha256: PACKET_SHA, batchSize: 1,
      maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    const receiptResult = await db.selectFrom('post')
      .select(['content_time_status', 'content_time_clamp_reason'])
      .where('uri', '=', receiptMismatch.uri).executeTakeFirstOrThrow()
    assert.deepEqual(receiptResult, {
      content_time_status: 'source_invalid',
      content_time_clamp_reason: 'future_skew',
    })

    const revisionMismatch = recoveredPost('revision-mismatch', '2026-08-16T11:57:00.000Z')
    await db.insertInto('post').values({
      ...revisionMismatch,
      cid: 'different-cid',
      created_at_source_raw: null,
      content_time_utc: null,
      content_time_status: null,
      content_time_clamp_reason: null,
      content_time_validator_version: null,
    }).execute()
    await assert.rejects(
      runPublisherPostRecovery(db as any, {
        posts: [revisionMismatch], packetSha256: PACKET_SHA, batchSize: 1,
        maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000,
        statementTimeoutMs: 30_000,
      }),
      /content-time recovery revision conflict uri_sha256=/,
    )

    const durationLimited = await runPublisherPostRecovery(db as any, {
      posts: [current, legacy], packetSha256: PACKET_SHA, batchSize: 1,
      maxDurationMs: 1000, pauseMs: 2000, lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(durationLimited.complete, false)
    assert.equal(durationLimited.scanned, 1)
    assert(durationLimited.elapsed_ms < 1500, 'duration stop did not fire promptly')

    const pauseLimited = await runPublisherPostRecovery(db as any, {
      posts: [current, legacy], packetSha256: PACKET_SHA, batchSize: 1,
      maxDurationMs: 5000, pauseMs: 1000, lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(pauseLimited.complete, true)
    assert(pauseLimited.elapsed_ms >= 1000, '1s inter-batch pause was not enforced')

    await assert.rejects(
      runPublisherPostRecovery(db as any, {
        posts: [legacy], packetSha256: PACKET_SHA, batchSize: 501,
        maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000,
        statementTimeoutMs: 30_000,
      }),
      /batchSize must be an integer from 1 to 500/,
    )

    const conflict = recoveredPost('conflict', '2026-08-16T11:52:00.000Z')
    await db.insertInto('post').values({
      ...conflict,
      created_at_source_raw: Buffer.from('different'),
      content_time_validator_version: 'newsflows-content-time/v1',
    }).execute()
    await assert.rejects(
      runPublisherPostRecovery(db as any, {
        posts: [conflict], packetSha256: PACKET_SHA, batchSize: 1,
        maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000,
        statementTimeoutMs: 30_000,
      }),
      /content-time recovery CAS conflict uri_sha256=/,
    )

    const locked = recoveredPost('locked', '2026-08-16T11:51:00.000Z')
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
        link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription"
      ) VALUES (
        ${locked.uri}, ${locked.cid}, ${locked.indexedAt}, ${locked.createdAt},
        ${locked.author}, ${locked.text}, '', '', '', '', '', '', '', ''
      )
    `.execute(db)
    const lockerPool = new Pool({ connectionString: dsn, max: 1 })
    const locker = await lockerPool.connect()
    try {
      await locker.query('BEGIN')
      await locker.query('SELECT uri FROM public.post WHERE uri = $1 FOR UPDATE', [locked.uri])
      const lockStarted = Date.now()
      await assert.rejects(
        runPublisherPostRecovery(db as any, {
          posts: [locked], packetSha256: PACKET_SHA, batchSize: 1,
          maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000,
          statementTimeoutMs: 30_000,
        }),
        /canceling statement due to lock timeout/,
      )
      const lockElapsed = Date.now() - lockStarted
      assert(lockElapsed >= 4500 && lockElapsed < 7000, '5s lock timeout was not enforced')
    } finally {
      await locker.query('ROLLBACK')
      locker.release()
      await lockerPool.end()
    }

    const rollbackRows = [
      recoveredPost('rollback-a', '2026-08-16T11:49:00.000Z'),
      recoveredPost('rollback-b', '2026-08-16T11:48:00.000Z'),
    ]
    await db.insertInto('post').values({
      ...rollbackRows[1],
      created_at_source_raw: null,
      content_time_utc: null,
      content_time_status: null,
      content_time_clamp_reason: null,
      content_time_validator_version: null,
    }).execute()
    await sql`
      CREATE FUNCTION slow_content_time_recovery() RETURNS trigger AS $$
      BEGIN
        PERFORM pg_sleep(0.1);
        RETURN NEW;
      END
      $$ LANGUAGE plpgsql
    `.execute(db)
    await sql`
      CREATE TRIGGER slow_content_time_recovery
      BEFORE UPDATE OF content_time_status ON public.post
      FOR EACH ROW EXECUTE FUNCTION slow_content_time_recovery()
    `.execute(db)
    try {
      await assert.rejects(
        runPublisherPostRecovery(db as any, {
          posts: rollbackRows, packetSha256: PACKET_SHA, batchSize: 2,
          maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000,
          statementTimeoutMs: 50,
        }),
        /canceling statement due to statement timeout/,
      )
    } finally {
      await sql`DROP TRIGGER slow_content_time_recovery ON public.post`.execute(db)
      await sql`DROP FUNCTION slow_content_time_recovery()`.execute(db)
    }
    const rolledBack = await db.selectFrom('post')
      .select(['content_time_status'])
      .where('uri', 'in', rollbackRows.map((row) => row.uri))
      .execute()
    assert.deepEqual(rolledBack, [{ content_time_status: null }])

    const columnsAfter = await sql<{ column_name: string }>`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'post'
      ORDER BY ordinal_position
    `.execute(db)
    assert.deepEqual(columnsAfter.rows, columnsBefore.rows)

    const bulk = Array.from({ length: 500 }, (_, index) =>
      recoveredPost(`bulk-${String(index).padStart(3, '0')}`, '2026-08-16T11:50:00.000Z'))
    await db.insertInto('post').values(bulk.map((row) => ({
      ...row,
      createdAt: row.indexedAt,
      created_at_source_raw: null,
      content_time_utc: null,
      content_time_status: null,
      content_time_clamp_reason: null,
      content_time_validator_version: null,
    }))).execute()
    await sql`ANALYZE public.post`.execute(db)
    const statsBefore = await sql<{ wal_lsn: string; bytes: string; dead_tuples: string }>`
      SELECT pg_current_wal_lsn()::text AS wal_lsn,
             pg_total_relation_size('public.post')::text AS bytes,
             n_dead_tup::text AS dead_tuples
      FROM pg_stat_user_tables
      WHERE schemaname = 'public' AND relname = 'post'
    `.execute(db)
    const bulkResult = await runPublisherPostRecovery(db as any, {
      posts: bulk,
      packetSha256: PACKET_SHA,
      batchSize: 500,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(bulkResult.recovered, 500)
    await sql`SELECT pg_stat_force_next_flush()`.execute(db)
    await sql`ANALYZE public.post`.execute(db)
    const statsAfter = await sql<{ wal_bytes: string; bytes: string; dead_tuples: string }>`
      SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), ${statsBefore.rows[0].wal_lsn})::text AS wal_bytes,
             pg_total_relation_size('public.post')::text AS bytes,
             n_dead_tup::text AS dead_tuples
      FROM pg_stat_user_tables
      WHERE schemaname = 'public' AND relname = 'post'
    `.execute(db)
    console.log(JSON.stringify({
      status: 'pass',
      batch_limit: 500,
      wal_bytes: Number(statsAfter.rows[0].wal_bytes),
      relation_bytes_before: Number(statsBefore.rows[0].bytes),
      relation_bytes_after: Number(statsAfter.rows[0].bytes),
      dead_tuples_before: Number(statsBefore.rows[0].dead_tuples),
      dead_tuples_after: Number(statsAfter.rows[0].dead_tuples),
      batch_elapsed_ms: bulkResult.elapsed_ms,
      old_app_created_at_preserved: true,
      schema_unchanged: true,
      resume_proven: true,
      cas_conflict_proven: true,
      lock_timeout_proven: true,
      statement_timeout_proven: true,
      transaction_rollback_proven: true,
      duration_stop_proven: true,
      pause_proven: true,
    }))
  } finally {
    await db.destroy()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
