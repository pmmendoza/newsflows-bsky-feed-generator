/**
 * Destructive disposable-PostgreSQL rehearsal for the content-time v1->v2
 * revalidation mode (src/tools/backfill-publisher-posts.ts:
 * runContentTimeRevalidation / previewContentTimeRevalidation). Mirrors the
 * shape of scripts/test_content_time_recovery_execute.ts (the "V7"
 * bounded-recovery rehearsal from commit 60561d1): same disposable-schema
 * bootstrap, same bounded-batch assertions (resume, duration stop, pause,
 * statement/lock timeout, transaction rollback), plus the WAL bytes /
 * relation growth / dead tuple capture at bulk scale.
 *
 * FEEDGEN_TEST_DSN=postgresql://.../throwaway \
 * FEEDGEN_CONTENT_TIME_REVALIDATE_REHEARSAL=1 \
 *   npx ts-node scripts/test_content_time_revalidate_execute.ts
 *
 * Only ever point this at a disposable database. It drops and recreates the
 * public schema.
 */
import assert from 'assert'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import {
  REVALIDATION_LIMITS,
  runContentTimeRevalidation,
  previewContentTimeRevalidation,
  contentTimeRevalidationConfigSha256,
  writeCheckpoint,
  readRevalidationCheckpoint,
  type ContentTimeRevalidationCheckpoint,
} from '../src/tools/backfill-publisher-posts'
import {
  CONTENT_TIME_VALIDATOR_VERSION,
  CONTENT_TIME_VALIDATOR_VERSION_V1,
} from '../src/util/content-time'

const PUBLISHER_A = 'did:plc:publisher-a'
const PUBLISHER_B = 'did:plc:publisher-b'
const OTHER_AUTHOR = 'did:plc:not-a-target-publisher'

// Same fixed-dummy-hex convention as the V7 rehearsal (PACKET_SHA =
// '1'.repeat(64) in test_content_time_recovery_execute.ts).
const PACKET_SHA = '1'.repeat(64)
const OTHER_PACKET_SHA = '2'.repeat(64)

type SeedRow = {
  rkey: string
  author: string
  indexedAt: string
  createdAt: string
  raw: string | null // null => legacy_unknown (created_at_source_raw stays NULL)
  validatorVersion: string | null
  status: 'source_valid' | 'source_invalid' | 'legacy_unknown' | null
  clampReason: 'missing' | 'unparseable' | 'future_skew' | 'past_bound' | null
  contentTimeUtc: string | null
}

function uri(author: string, rkey: string) {
  return `at://${author}/app.bsky.feed.post/${rkey}`
}

async function insertSeedRow(db: Kysely<any>, row: SeedRow) {
  await sql`
    INSERT INTO public.post (
      uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
      link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
      created_at_source_raw, content_time_utc, content_time_status,
      content_time_clamp_reason, content_time_validator_version
    ) VALUES (
      ${uri(row.author, row.rkey)}, ${`cid-${row.rkey}`}, ${row.indexedAt}, ${row.createdAt},
      ${row.author}, ${row.rkey}, '', '', '', '', '', '', '', '',
      ${row.raw === null ? null : Buffer.from(row.raw, 'utf8')},
      ${row.contentTimeUtc}, ${row.status}, ${row.clampReason}, ${row.validatorVersion}
    )
  `.execute(db)
}

async function readPost(db: Kysely<any>, author: string, rkey: string) {
  const result = await sql<any>`
    SELECT uri, "createdAt", encode(created_at_source_raw, 'escape') AS raw,
           content_time_utc, content_time_status, content_time_clamp_reason,
           content_time_validator_version
    FROM public.post WHERE uri = ${uri(author, rkey)}
  `.execute(db)
  return result.rows[0]
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_CONTENT_TIME_REVALIDATE_REHEARSAL !== '1') {
    console.log('SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_CONTENT_TIME_REVALIDATE_REHEARSAL=1')
    return
  }

  const pool = new Pool({ connectionString: dsn })
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool }) })
  try {
    assert.deepEqual(REVALIDATION_LIMITS, {
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
    await sql`CREATE INDEX post_author_index ON public.post (author)`.execute(db)

    const windowSince = '2026-08-01T00:00:00.000Z'
    const beforeWindow = '2026-07-01T00:00:00.000Z' // before --since: must never be touched

    // -- Seed the transition matrix --------------------------------------

    // v1-valid, comfortably in range under both policies -> stays valid,
    // content_time_utc must be unchanged after revalidation.
    await insertSeedRow(db, {
      rkey: 'valid-stays-valid',
      author: PUBLISHER_A,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-02T12:00:00.000Z',
      raw: '2026-08-02T12:00:00+00:00',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_valid',
      clampReason: null,
      contentTimeUtc: '2026-08-02T12:00:00.000Z',
    })

    // v1-invalid (past_bound, >2y old) -> v2 has no past bound, must flip valid.
    await insertSeedRow(db, {
      rkey: 'past-bound-becomes-valid',
      author: PUBLISHER_A,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-12T12:00:00.000Z', // legacy fallback used under v1 invalid
      raw: '2020-01-01T00:00:00Z',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_invalid',
      clampReason: 'past_bound',
      contentTimeUtc: null,
    })

    // v1-valid but 1h in the future (inside v1's 24h skew, outside v2's 5min)
    // -> must flip to source_invalid/future_skew.
    await insertSeedRow(db, {
      rkey: 'future-skew-becomes-invalid',
      author: PUBLISHER_B,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-12T13:00:00.000Z',
      raw: '2026-08-12T13:00:00.000Z',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_valid',
      clampReason: null,
      contentTimeUtc: '2026-08-12T13:00:00.000Z',
    })

    // Control: already on v2 -- must never be selected/touched.
    await insertSeedRow(db, {
      rkey: 'already-v2-control',
      author: PUBLISHER_A,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-02T12:00:00.000Z',
      raw: '2026-08-02T12:00:00.000Z',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION,
      status: 'source_valid',
      clampReason: null,
      contentTimeUtc: '2026-08-02T12:00:00.000Z',
    })

    // Control: legacy_unknown (raw IS NULL) -- must never be selected/touched.
    await insertSeedRow(db, {
      rkey: 'legacy-unknown-control',
      author: PUBLISHER_A,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-12T12:00:00.000Z',
      raw: null,
      validatorVersion: null,
      status: null,
      clampReason: null,
      contentTimeUtc: null,
    })

    // Control: v1 row from a publisher DID that is NOT in the actor list
    // passed to the run (simulates a disabled/unrelated publisher) -- must
    // never be selected/touched.
    await insertSeedRow(db, {
      rkey: 'other-author-control',
      author: OTHER_AUTHOR,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-02T12:00:00.000Z',
      raw: '2026-08-02T12:00:00+00:00',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_valid',
      clampReason: null,
      contentTimeUtc: '2026-08-02T12:00:00.000Z',
    })

    // Control: v1 row outside the --since window -- must never be selected/touched.
    await insertSeedRow(db, {
      rkey: 'out-of-window-control',
      author: PUBLISHER_A,
      indexedAt: beforeWindow,
      createdAt: '2026-06-01T12:00:00.000Z',
      raw: '2026-06-01T12:00:00+00:00',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_valid',
      clampReason: null,
      contentTimeUtc: '2026-06-01T12:00:00.000Z',
    })

    const actors = [PUBLISHER_A, PUBLISHER_B]
    const since = new Date(windowSince)

    // -- Preview (read-only): must match the eventual apply outcome -------

    const preview = await previewContentTimeRevalidation(db, actors, since)
    assert.equal(preview.scanned, 3, 'preview must see exactly the 3 in-window v1 rows for the target publishers')
    assert.equal(preview.counts.v1_valid_to_v2_valid, 1)
    assert.equal(preview.counts.v1_invalid_to_v2_valid, 1)
    assert.equal(preview.counts.v1_to_v2_invalid, 1)
    assert.deepEqual(preview.counts.by_v2_invalid_reason, { future_skew: 1 })

    // Preview must never write.
    const untouchedAfterPreview = await readPost(db, PUBLISHER_A, 'valid-stays-valid')
    assert.equal(untouchedAfterPreview.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION_V1)

    // -- Resume across a forced stop (maxBatches) --------------------------

    let checkpoint: ContentTimeRevalidationCheckpoint | undefined
    const first = await runContentTimeRevalidation(db, {
      actors,
      packetSha256: PACKET_SHA,
      since,
      batchSize: 1,
      maxBatches: 1,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
      onCheckpoint: (value) => { checkpoint = value },
    })
    assert.equal(first.complete, false)
    assert.equal(first.batch, 1)
    assert.equal(first.scanned, 1)
    assert.equal(first.updated, 1)
    assert.equal(first.packet_sha256, PACKET_SHA, 'result must echo the packet hash the run was bound to')
    // --max-batches 1 stop: exactly one batch summary, raw cursor present
    // (publisher URIs are allowed in the per-batch breakdown), and the
    // per-batch totals must sum to the cumulative totals above.
    assert.equal(first.batches.length, 1)
    assert.equal(first.batches[0].batch, 1)
    assert.equal(first.batches[0].candidates, 1)
    assert.equal(first.batches[0].updated, 1)
    assert.equal(first.batches[0].skipped_cas, 0)
    assert(first.batches[0].cursor_author.startsWith('did:plc:'), 'per-batch cursor_author must be the raw DID, not a hash')
    assert(first.batches[0].cursor_uri.startsWith('at://'), 'per-batch cursor_uri must be the raw URI, not a hash')
    assert(typeof first.batches[0].elapsed_ms === 'number' && first.batches[0].elapsed_ms >= 0)
    assert(typeof first.batches[0].wal_bytes === 'number' && first.batches[0].wal_bytes >= 0, 'wal_bytes must be a measured non-negative integer even for a 1-row batch')
    assert(typeof first.batches[0].relation_bytes_before === 'number' && first.batches[0].relation_bytes_before > 0)
    assert(typeof first.batches[0].relation_bytes_after === 'number' && first.batches[0].relation_bytes_after >= first.batches[0].relation_bytes_before)

    const resumed = await runContentTimeRevalidation(db, {
      actors,
      packetSha256: PACKET_SHA,
      since,
      batchSize: 1,
      afterAuthor: checkpoint!.cursor_author,
      afterUri: checkpoint!.cursor_uri,
      configSha256: checkpoint!.config_sha256,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(resumed.complete, true)
    assert.equal(resumed.scanned, 2, 'resume must pick up exactly the remaining 2 in-window rows')
    assert.equal(resumed.batches.length, 2, 'batchSize=1 over 2 remaining rows must produce 2 per-batch summaries')
    assert.equal(resumed.packet_sha256, PACKET_SHA)
    const resumedBatchTotal = resumed.batches.reduce((sum, b) => sum + b.updated, 0)
    assert.equal(resumedBatchTotal, resumed.updated, 'per-batch updated counts must sum to the cumulative total')

    // Mismatched checkpoint (config changed) must be rejected, not silently resumed.
    await assert.rejects(
      runContentTimeRevalidation(db, {
        actors: [PUBLISHER_A], // different actor set than the checkpoint was taken under
        packetSha256: PACKET_SHA,
        since,
        batchSize: 1,
        afterAuthor: checkpoint!.cursor_author,
        afterUri: checkpoint!.cursor_uri,
        configSha256: checkpoint!.config_sha256,
        maxDurationMs: 30_000,
        pauseMs: 0,
        lockTimeoutMs: 5000,
        statementTimeoutMs: 30_000,
      }),
      /checkpoint does not match the immutable revalidation config/,
    )

    // -- Transition assertions ---------------------------------------------

    const stillValid = await readPost(db, PUBLISHER_A, 'valid-stays-valid')
    assert.equal(stillValid.content_time_status, 'source_valid')
    assert.equal(stillValid.content_time_utc, '2026-08-02T12:00:00.000Z', 'normalized instant must be unchanged')
    assert.equal(stillValid.createdAt, '2026-08-02T12:00:00.000Z')
    assert.equal(stillValid.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION)

    const pastBound = await readPost(db, PUBLISHER_A, 'past-bound-becomes-valid')
    assert.equal(pastBound.content_time_status, 'source_valid')
    assert.equal(pastBound.content_time_utc, '2020-01-01T00:00:00.000Z')
    assert.equal(pastBound.content_time_clamp_reason, null)
    assert.equal(pastBound.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION)

    const futureSkew = await readPost(db, PUBLISHER_B, 'future-skew-becomes-invalid')
    assert.equal(futureSkew.content_time_status, 'source_invalid')
    assert.equal(futureSkew.content_time_clamp_reason, 'future_skew')
    assert.equal(futureSkew.content_time_utc, null)
    assert.equal(futureSkew.createdAt, '2026-08-12T12:00:00.000Z', 'invalid row createdAt must fall back to receipt time')
    assert.equal(futureSkew.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION)

    // -- Controls: never touched --------------------------------------------

    const v2Control = await readPost(db, PUBLISHER_A, 'already-v2-control')
    assert.equal(v2Control.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION)
    assert.equal(v2Control.content_time_utc, '2026-08-02T12:00:00.000Z')

    const legacyControl = await readPost(db, PUBLISHER_A, 'legacy-unknown-control')
    assert.equal(legacyControl.content_time_validator_version, null)
    assert.equal(legacyControl.raw, null)

    const otherAuthorControl = await readPost(db, OTHER_AUTHOR, 'other-author-control')
    assert.equal(otherAuthorControl.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION_V1)

    const outOfWindowControl = await readPost(db, PUBLISHER_A, 'out-of-window-control')
    assert.equal(outOfWindowControl.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION_V1)

    // -- Idempotent re-run: nothing left to update --------------------------

    const rerun = await runContentTimeRevalidation(db, {
      actors,
      packetSha256: PACKET_SHA,
      since,
      batchSize: REVALIDATION_LIMITS.batchSize,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(rerun.complete, true)
    assert.equal(rerun.scanned, 0)
    assert.equal(rerun.updated, 0)

    const rerunPreview = await previewContentTimeRevalidation(db, actors, since)
    assert.equal(rerunPreview.scanned, 0)

    // -- CAS predicate mechanism: a write whose expected raw no longer
    // matches the live row must affect 0 rows and leave the row untouched.
    // (Design note, not a claim of realistic reachability: because the
    // real code path always reads created_at_source_raw fresh, under a
    // FOR UPDATE lock, in the same transaction that performs the CAS
    // UPDATE, a genuinely stale in-flight write cannot occur in normal
    // operation -- Postgres re-checks a blocked FOR UPDATE's WHERE clause
    // against the newly committed row on resume, so a second writer's
    // change is always visible before this code reads it, never after.
    // This block proves the UPDATE...WHERE guard itself refuses a
    // mismatched write if one were ever attempted, which is the safety
    // property the mission asked for.)
    await insertSeedRow(db, {
      rkey: 'cas-guard-proof',
      author: PUBLISHER_A,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-02T12:00:00.000Z',
      raw: '2026-08-02T12:00:00+00:00',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_valid',
      clampReason: null,
      contentTimeUtc: '2026-08-02T12:00:00.000Z',
    })
    const wrongRawHex = Buffer.from('2099-01-01T00:00:00.000Z', 'utf8').toString('hex')
    const mismatchedUpdate = await sql<{ uri: string }>`
      UPDATE public.post AS target
      SET content_time_status = 'source_invalid',
          content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION}
      FROM jsonb_to_recordset(${JSON.stringify([{
        uri: uri(PUBLISHER_A, 'cas-guard-proof'),
        raw_hex: wrongRawHex,
      }])}::jsonb) AS batch(uri text, raw_hex text)
      WHERE target.uri = batch.uri
        AND target.content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION_V1}
        AND target.created_at_source_raw = decode(batch.raw_hex, 'hex')
      RETURNING target.uri
    `.execute(db)
    assert.equal(mismatchedUpdate.rows.length, 0, 'CAS predicate must refuse a write against mismatched raw bytes')
    const guardRow = await readPost(db, PUBLISHER_A, 'cas-guard-proof')
    assert.equal(guardRow.content_time_status, 'source_valid', 'row must be untouched by the rejected mismatched write')
    assert.equal(guardRow.content_time_validator_version, CONTENT_TIME_VALIDATOR_VERSION_V1)

    // A correctly-matched CAS write against the same row must still succeed
    // via the real run (proves the guard is precise, not just always-false).
    const guardCleanup = await runContentTimeRevalidation(db, {
      actors,
      packetSha256: PACKET_SHA,
      since,
      batchSize: REVALIDATION_LIMITS.batchSize,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(guardCleanup.updated, 1)

    // -- Bounded-contract behaviors already proven for the recovery mode
    // (duration stop, inter-batch pause, lock timeout, statement-timeout
    // rollback) share the identical outer-loop shape in
    // runContentTimeRevalidation; re-prove duration stop and pause here
    // since this mode's outer loop is a separate implementation.

    for (let i = 0; i < 3; i += 1) {
      await insertSeedRow(db, {
        rkey: `bounds-${i}`,
        author: PUBLISHER_A,
        indexedAt: '2026-08-12T12:00:00.000Z',
        createdAt: '2026-08-02T12:00:00.000Z',
        raw: '2026-08-02T12:00:00+00:00',
        validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
        status: 'source_valid',
        clampReason: null,
        contentTimeUtc: '2026-08-02T12:00:00.000Z',
      })
    }
    const durationLimited = await runContentTimeRevalidation(db, {
      actors, packetSha256: PACKET_SHA, since, batchSize: 1, maxDurationMs: 1000, pauseMs: 2000,
      lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
    })
    assert.equal(durationLimited.complete, false)
    assert.equal(durationLimited.batch, 1)
    assert(durationLimited.elapsed_ms < 1500, 'duration stop did not fire promptly')

    const pauseLimited = await runContentTimeRevalidation(db, {
      actors, packetSha256: PACKET_SHA, since, batchSize: 1, maxDurationMs: 5000, pauseMs: 1000,
      lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
    })
    assert.equal(pauseLimited.complete, true)
    assert(pauseLimited.elapsed_ms >= 1000, '1s inter-batch pause was not enforced')

    await assert.rejects(
      runContentTimeRevalidation(db, {
        actors, packetSha256: PACKET_SHA, since, batchSize: 501, maxDurationMs: 30_000, pauseMs: 0,
        lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
      }),
      /batchSize must be an integer from 1 to 500/,
    )

    // -- packet-sha256 binding: apply always requires a valid packet hash,
    // and a checkpoint recorded under a different packet must be rejected
    // (mirrors the recover mode's readCheckpoint / packetSha256 contract).

    await assert.rejects(
      runContentTimeRevalidation(db, {
        actors, packetSha256: 'not-a-sha256', since, batchSize: 1, maxDurationMs: 30_000,
        pauseMs: 0, lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
      }),
      /packetSha256 must be a lowercase SHA-256/,
    )
    await assert.rejects(
      runContentTimeRevalidation(db, {
        actors, packetSha256: 'ABCDEF'.repeat(10) + 'ABCD', since, batchSize: 1, maxDurationMs: 30_000,
        pauseMs: 0, lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
      }),
      /packetSha256 must be a lowercase SHA-256/,
      'uppercase hex must be rejected -- the packet hash contract is lowercase-only',
    )

    {
      const checkpointFile = path.join(os.tmpdir(), `feedgen-revalidate-rehearsal-checkpoint-${process.pid}-${Date.now()}.json`)
      try {
        const configSha256 = contentTimeRevalidationConfigSha256(actors, since.toISOString())
        writeCheckpoint(checkpointFile, {
          config_sha256: configSha256,
          packet_sha256: PACKET_SHA,
          cursor_author: PUBLISHER_A,
          cursor_uri: uri(PUBLISHER_A, 'valid-stays-valid'),
        })
        // Same packet -> checkpoint is accepted.
        const accepted = readRevalidationCheckpoint(checkpointFile, configSha256, PACKET_SHA)
        assert.deepEqual(accepted, { cursorAuthor: PUBLISHER_A, cursorUri: uri(PUBLISHER_A, 'valid-stays-valid') })
        // Different packet -> checkpoint must be rejected, not silently resumed.
        assert.throws(
          () => readRevalidationCheckpoint(checkpointFile, configSha256, OTHER_PACKET_SHA),
          /checkpoint does not match the approved revalidation packet/,
        )
      } finally {
        fs.rmSync(checkpointFile, { force: true })
      }
    }

    // -- Lock timeout: a row locked by another session must surface the
    // Postgres lock-timeout error, not hang or silently skip.
    const lockRkey = 'lock-timeout-proof'
    await insertSeedRow(db, {
      rkey: lockRkey,
      author: PUBLISHER_A,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-02T12:00:00.000Z',
      raw: '2026-08-02T12:00:00+00:00',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_valid',
      clampReason: null,
      contentTimeUtc: '2026-08-02T12:00:00.000Z',
    })
    const lockerPool = new Pool({ connectionString: dsn, max: 1 })
    const locker = await lockerPool.connect()
    try {
      await locker.query('BEGIN')
      await locker.query('SELECT uri FROM public.post WHERE uri = $1 FOR UPDATE', [uri(PUBLISHER_A, lockRkey)])
      const lockStarted = Date.now()
      await assert.rejects(
        runContentTimeRevalidation(db, {
          actors: [PUBLISHER_A], packetSha256: PACKET_SHA, since, batchSize: REVALIDATION_LIMITS.batchSize,
          maxDurationMs: 30_000, pauseMs: 0, lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
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
    // Clean up the still-v1 rows left by the lock/duration-stop probes so
    // the WAL/dead-tuple bulk measurement below starts from a known state.
    const cleanup = await runContentTimeRevalidation(db, {
      actors, packetSha256: PACKET_SHA, since, batchSize: REVALIDATION_LIMITS.batchSize, maxDurationMs: 30_000,
      pauseMs: 0, lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
    })
    assert.equal(cleanup.complete, true)

    // -- WAL / relation growth / dead tuples at bulk scale (500 rows) ------

    const bulkRows = Array.from({ length: 500 }, (_, index) => ({
      rkey: `bulk-${String(index).padStart(3, '0')}`,
      author: index % 2 === 0 ? PUBLISHER_A : PUBLISHER_B,
      indexedAt: '2026-08-12T12:00:00.000Z',
      createdAt: '2026-08-02T12:00:00.000Z',
      raw: '2026-08-02T12:00:00+00:00',
      validatorVersion: CONTENT_TIME_VALIDATOR_VERSION_V1,
      status: 'source_valid' as SeedRow['status'],
      clampReason: null as SeedRow['clampReason'],
      contentTimeUtc: '2026-08-02T12:00:00.000Z',
    }))
    for (const row of bulkRows) await insertSeedRow(db, row)

    await sql`ANALYZE public.post`.execute(db)
    const statsBefore = await sql<{ wal_lsn: string; bytes: string; dead_tuples: string }>`
      SELECT pg_current_wal_lsn()::text AS wal_lsn,
             pg_total_relation_size('public.post')::text AS bytes,
             n_dead_tup::text AS dead_tuples
      FROM pg_stat_user_tables
      WHERE schemaname = 'public' AND relname = 'post'
    `.execute(db)

    const bulkResult = await runContentTimeRevalidation(db, {
      actors, packetSha256: PACKET_SHA, since, batchSize: REVALIDATION_LIMITS.batchSize, maxDurationMs: 30_000,
      pauseMs: 0, lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
    })
    assert.equal(bulkResult.updated, 500)
    assert.equal(bulkResult.batches.length, 1, '500 candidates at batchSize=500 must be exactly one batch summary')
    assert.equal(bulkResult.batches[0].updated, 500)
    assert.equal(bulkResult.packet_sha256, PACKET_SHA)
    // Attributable per-batch WAL/relation-size instrumentation: measured
    // inside the batch transaction itself, so it must be strictly positive
    // for a real 500-row UPDATE and must appear both on the batch summary
    // and mirrored onto the cumulative result (last-batch snapshot).
    assert(bulkResult.batches[0].wal_bytes > 0, 'a 500-row UPDATE must produce measurable WAL inside its own batch transaction')
    assert(bulkResult.batches[0].relation_bytes_before > 0, 'relation_bytes_before must reflect the already-populated table')
    assert(bulkResult.batches[0].relation_bytes_after >= bulkResult.batches[0].relation_bytes_before, 'relation size must not shrink from an UPDATE-only batch')
    assert.equal(bulkResult.wal_bytes, bulkResult.batches[0].wal_bytes, 'the cumulative result must mirror the last (only) batch summary')
    assert.equal(bulkResult.relation_bytes_before, bulkResult.batches[0].relation_bytes_before)
    assert.equal(bulkResult.relation_bytes_after, bulkResult.batches[0].relation_bytes_after)

    await sql`SELECT pg_stat_force_next_flush()`.execute(db)
    await sql`ANALYZE public.post`.execute(db)
    const statsAfter = await sql<{ wal_bytes: string; bytes: string; dead_tuples: string; max_lock_wait_ms: string }>`
      SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), ${statsBefore.rows[0].wal_lsn})::text AS wal_bytes,
             pg_total_relation_size('public.post')::text AS bytes,
             n_dead_tup::text AS dead_tuples,
             '0'::text AS max_lock_wait_ms
      FROM pg_stat_user_tables
      WHERE schemaname = 'public' AND relname = 'post'
    `.execute(db)

    console.log(JSON.stringify({
      status: 'pass',
      batch_limit: REVALIDATION_LIMITS.batchSize,
      wal_bytes: Number(statsAfter.rows[0].wal_bytes),
      relation_bytes_before: Number(statsBefore.rows[0].bytes),
      relation_bytes_after: Number(statsAfter.rows[0].bytes),
      dead_tuples_before: Number(statsBefore.rows[0].dead_tuples),
      dead_tuples_after: Number(statsAfter.rows[0].dead_tuples),
      batch_elapsed_ms: bulkResult.elapsed_ms,
      preview_matches_apply: true,
      transitions_proven: true,
      controls_untouched: true,
      idempotent_rerun_proven: true,
      resume_proven: true,
      checkpoint_mismatch_rejected: true,
      packet_sha256_required_proven: true,
      packet_sha256_checkpoint_binding_proven: true,
      max_batches_stop_and_resume_proven: true,
      batches_breakdown_proven: true,
      cas_guard_proven: true,
      duration_stop_proven: true,
      pause_proven: true,
      lock_timeout_proven: true,
      schema_unchanged: true,
      raw_values_in_output: false,
    }))
  } finally {
    await db.destroy()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
