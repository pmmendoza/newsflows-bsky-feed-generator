/**
 * Disposable-PostgreSQL execution tests for FT-FU-1:
 * 1. Migration 013 (up, provenance guard on down, re-up)
 * 2. Two-connection advisory lock concurrency on feed_catalog mutations
 * 3. Atomic rollback on rejected catalog coherence
 * 4. Post and engagement v2 -> v3 forward revalidation and v3 -> v2 reverse rollback
 *
 * FEEDGEN_TEST_DSN=postgresql://.../throwaway \
 * FEEDGEN_CONTENT_TIME_REVALIDATE_REHEARSAL=1 \
 *   npx ts-node scripts/test_ft_fu_1_db_execute.ts
 */

import assert from 'assert'
import { Kysely, Migrator, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import { migrationProvider } from '../src/db/migrations'
import {
  CONTENT_TIME_VALIDATOR_VERSION_V2,
  CONTENT_TIME_VALIDATOR_VERSION_V3,
} from '../src/util/content-time'
import {
  assertPublisherRecoveryContract,
  runContentTimeRevalidation,
} from '../src/tools/backfill-publisher-posts'

const PACKET_SHA = '1'.repeat(64)
const PUBLISHER_A = 'did:plc:publisher-a'

function uri(author: string, rkey: string) {
  return `at://${author}/app.bsky.feed.post/${rkey}`
}

function engagementUri(author: string, rkey: string) {
  return `at://${author}/app.bsky.feed.like/${rkey}`
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
    // Reset schema
    await sql`DROP SCHEMA IF EXISTS public CASCADE`.execute(db)
    await sql`CREATE SCHEMA public`.execute(db)
    await sql`DROP SCHEMA IF EXISTS feedgen_ops CASCADE`.execute(db)
    await sql`CREATE SCHEMA feedgen_ops`.execute(db)
    // feed_catalog predates this repository's numbered migration provider;
    // migration 008 deliberately fails closed unless the owner table exists.
    await sql`
      CREATE TABLE feedgen_ops.feed_catalog (
        feed_id text PRIMARY KEY, rkey text UNIQUE NOT NULL,
        enabled boolean NOT NULL DEFAULT true,
        algo_policy_id text NOT NULL DEFAULT 'chronological',
        display_name text NOT NULL DEFAULT '', country text,
        publisher_did text, study_id text, ranker_policy_id text,
        access_policy_id text NOT NULL DEFAULT 'subscriber-default',
        created_at text NOT NULL DEFAULT CURRENT_TIMESTAMP, retired_at text
      )
    `.execute(db)

    // 1. Run migrations up to latest (includes 013)
    const migrator = new Migrator({ db, provider: migrationProvider })
    const migrationResult = await migrator.migrateToLatest()
    assert(!migrationResult.error, `migration error: ${(migrationResult.error as any)?.message}`)

    // Verify 013 is applied
    const applied = await migrator.getMigrations()
    const mig013 = applied.find((m) => m.name.startsWith('013_'))
    assert(mig013 && mig013.executedAt, 'migration 013 must be executed')

    // Test inserting v3 clamped row in post and engagement
    const receipt = '2026-08-12T12:00:00.000Z'
    const futureRaw = '2026-08-12T12:03:00.000Z'
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
        link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
        created_at_source_raw, content_time_utc, content_time_status,
        content_time_clamp_reason, content_time_validator_version
      ) VALUES (
        ${uri(PUBLISHER_A, 'v3-clamped-1')}, 'cid1', ${receipt}, ${receipt},
        ${PUBLISHER_A}, 'test', '', '', '', '', '', '', '', '',
        ${Buffer.from(futureRaw, 'utf8')}, ${receipt}, 'source_valid',
        'future_skew_clamped', ${CONTENT_TIME_VALIDATOR_VERSION_V3}
      )
    `.execute(db)
    await sql`
      INSERT INTO public.engagement (
        uri, cid, "subjectUri", "subjectCid", type, "indexedAt", "createdAt", author,
        created_at_source_raw, content_time_utc, content_time_status,
        content_time_clamp_reason, content_time_validator_version
      ) VALUES (
        ${engagementUri('did:plc:user1', 'like-1')}, 'cid2', ${uri(PUBLISHER_A, 'v3-clamped-1')}, 'cid1', 1, ${receipt}, ${receipt}, 'did:plc:user1',
        ${Buffer.from(futureRaw, 'utf8')}, ${receipt}, 'source_valid',
        'future_skew_clamped', ${CONTENT_TIME_VALIDATOR_VERSION_V3}
      )
    `.execute(db)

    // Down migration must be refused while v3 clamped rows exist
    const migDownAttempt = await migrator.migrateDown()
    assert(migDownAttempt.error, 'migration down must be refused while v3 rows exist')

    // Delete v3 rows and test down migration succeeds
    await sql`DELETE FROM public.post WHERE content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION_V3}`.execute(db)
    await sql`DELETE FROM public.engagement WHERE content_time_validator_version = ${CONTENT_TIME_VALIDATOR_VERSION_V3}`.execute(db)
    const migDownSuccess = await migrator.migrateDown()
    assert(!migDownSuccess.error, `migration down failed: ${(migDownSuccess.error as any)?.message}`)

    // Migrate back up to latest
    const migReUp = await migrator.migrateToLatest()
    assert(!migReUp.error, `migration re-up failed: ${(migReUp.error as any)?.message}`)

    // Recovery is a legacy v2-only mutation path. Once the global catalog is
    // active on v3 it must abort before the recovery writer is reached.
    await sql`
      INSERT INTO feedgen_ops.feed_catalog (
        feed_id, rkey, enabled, publisher_time_clock,
        content_time_cutover_min_valid_share, content_time_contract_version
      ) VALUES (
        'recovery-guard', 'recovery-guard', true, 'content_time_v1',
        0.9, ${CONTENT_TIME_VALIDATOR_VERSION_V3}
      )
    `.execute(db)
    await assert.rejects(
      assertPublisherRecoveryContract(db),
      /publisher recovery only supports active validator version newsflows-content-time\/v2/,
    )
    await sql`DELETE FROM feedgen_ops.feed_catalog WHERE feed_id = 'recovery-guard'`.execute(db)

    // 2. Test two-connection advisory lock concurrency on feed_catalog
    const pool2 = new Pool({ connectionString: dsn })
    const db2 = new Kysely<any>({ dialect: new PostgresDialect({ pool: pool2 }) })

    try {
      const client1 = await pool.connect()
      const client2 = await pool2.connect()
      try {
        await client1.query('BEGIN')
        await client1.query("SELECT pg_advisory_xact_lock(hashtext('feedgen_ops.feed_catalog'))")

        let client2Acquired = false
        const p2 = (async () => {
          await client2.query('BEGIN')
          await client2.query("SELECT pg_advisory_xact_lock(hashtext('feedgen_ops.feed_catalog'))")
          client2Acquired = true
          await client2.query('COMMIT')
        })()

        await new Promise((r) => setTimeout(r, 100))
        assert.equal(client2Acquired, false, 'connection 2 must be blocked while connection 1 holds advisory lock')

        await client1.query('COMMIT')
        await p2
        assert.equal(client2Acquired, true, 'connection 2 acquires lock after connection 1 commits')
      } finally {
        client1.release()
        client2.release()
      }
    } finally {
      await db2.destroy()
    }

    // 3. Post forward (v2->v3) and reverse (v3->v2) revalidation
    const since = new Date('2026-08-01T00:00:00Z')
    const untilExclusive = new Date('2026-08-13T00:00:00.000Z')

    // Seed post rows
    // a. normal valid past
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
        link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
        created_at_source_raw, content_time_utc, content_time_status,
        content_time_clamp_reason, content_time_validator_version
      ) VALUES (
        ${uri(PUBLISHER_A, 'post-past')}, 'cid1', ${receipt}, '2026-08-05T12:00:00.000Z',
        ${PUBLISHER_A}, 'test', '', '', '', '', '', '', '', '',
        ${Buffer.from('2026-08-05T12:00:00.000Z', 'utf8')}, '2026-08-05T12:00:00.000Z', 'source_valid',
        null, ${CONTENT_TIME_VALIDATOR_VERSION_V2}
      )
    `.execute(db)

    // b. +1m skew (valid in v2, clamped in v3)
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
        link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
        created_at_source_raw, content_time_utc, content_time_status,
        content_time_clamp_reason, content_time_validator_version
      ) VALUES (
        ${uri(PUBLISHER_A, 'post-skew-1m')}, 'cid2', ${receipt}, '2026-08-12T12:01:00.000Z',
        ${PUBLISHER_A}, 'test', '', '', '', '', '', '', '', '',
        ${Buffer.from('2026-08-12T12:01:00.000Z', 'utf8')}, '2026-08-12T12:01:00.000Z', 'source_valid',
        null, ${CONTENT_TIME_VALIDATOR_VERSION_V2}
      )
    `.execute(db)

    // c. +20m skew (invalid future_skew in v2, restored to clamped in v3)
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
        link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
        created_at_source_raw, content_time_utc, content_time_status,
        content_time_clamp_reason, content_time_validator_version
      ) VALUES (
        ${uri(PUBLISHER_A, 'post-skew-1h')}, 'cid3', ${receipt}, ${receipt},
        ${PUBLISHER_A}, 'test', '', '', '', '', '', '', '', '',
        ${Buffer.from('2026-08-12T12:20:00.000Z', 'utf8')}, null, 'source_invalid',
        'future_skew', ${CONTENT_TIME_VALIDATOR_VERSION_V2}
      )
    `.execute(db)

    // Missing and unparseable values have identical v2/v3 semantics and stay v2.
    for (const [rkey, raw, reason] of [
      ['post-missing', Buffer.alloc(0), 'missing'],
      ['post-unparseable', Buffer.from('not-a-time', 'utf8'), 'unparseable'],
    ] as const) {
      await sql`
        INSERT INTO public.post (
          uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
          link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
          created_at_source_raw, content_time_utc, content_time_status,
          content_time_clamp_reason, content_time_validator_version
        ) VALUES (
          ${uri(PUBLISHER_A, rkey)}, ${`cid-${rkey}`}, ${receipt}, ${receipt},
          ${PUBLISHER_A}, 'test', '', '', '', '', '', '', '', '',
          ${raw}, null, 'source_invalid', ${reason}, ${CONTENT_TIME_VALIDATOR_VERSION_V2}
        )
      `.execute(db)
    }

    // Run forward post revalidation v2 -> v3
    const forwardPostResult = await runContentTimeRevalidation(db, {
      table: 'post',
      actors: [PUBLISHER_A],
      since,
      untilExclusive,
      fromVersion: CONTENT_TIME_VALIDATOR_VERSION_V2,
      toVersion: CONTENT_TIME_VALIDATOR_VERSION_V3,
      batchSize: 500,
      packetSha256: PACKET_SHA,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(forwardPostResult.updated, 2)
    assert.equal(forwardPostResult.counts.zero_to_5m_clamped, 1)
    assert.equal(forwardPostResult.counts.gt_5m_restored, 1)
    assert.equal(forwardPostResult.counts.v2_valid_to_v3_valid, 0)
    assert.deepEqual(forwardPostResult.counts.by_invalid_reason, {}, 'clamped valid rows must not appear in invalid reasons')

    // A native v3 row at the exclusive cutoff is outside the migrated cohort.
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text, "rootUri", "rootCid",
        link_uri, link_title, link_description, "linkUrl", "linkTitle", "linkDescription",
        created_at_source_raw, content_time_utc, content_time_status,
        content_time_clamp_reason, content_time_validator_version
      ) VALUES (
        ${uri(PUBLISHER_A, 'native-v3-after-cutoff')}, 'cid-native', ${untilExclusive.toISOString()}, '2026-08-12T23:59:00.000Z',
        ${PUBLISHER_A}, 'test', '', '', '', '', '', '', '', '',
        ${Buffer.from('2026-08-12T23:59:00.000Z', 'utf8')}, '2026-08-12T23:59:00.000Z', 'source_valid',
        null, ${CONTENT_TIME_VALIDATOR_VERSION_V3}
      )
    `.execute(db)
    // Run reverse post revalidation v3 -> v2
    const reversePostResult = await runContentTimeRevalidation(db, {
      table: 'post',
      actors: [PUBLISHER_A],
      since,
      untilExclusive,
      fromVersion: CONTENT_TIME_VALIDATOR_VERSION_V3,
      toVersion: CONTENT_TIME_VALIDATOR_VERSION_V2,
      batchSize: 500,
      packetSha256: PACKET_SHA,
      maxDurationMs: 30_000,
      pauseMs: 0,
      lockTimeoutMs: 5000,
      statementTimeoutMs: 30_000,
    })
    assert.equal(reversePostResult.updated, 2)
    assert.equal(reversePostResult.counts.zero_to_5m_unclamped, 1)
    assert.equal(reversePostResult.counts.gt_5m_invalidated, 1)
    assert.equal(reversePostResult.counts.v3_valid_to_v2_valid, 0)
    assert.equal(reversePostResult.counts.by_invalid_reason.future_skew, 1)
    const nativeVersion = (await sql<{ version: string }>`
      SELECT content_time_validator_version AS version
      FROM public.post
      WHERE uri = ${uri(PUBLISHER_A, 'native-v3-after-cutoff')}
    `.execute(db)).rows[0].version
    assert.equal(nativeVersion, CONTENT_TIME_VALIDATOR_VERSION_V3, 'rollback must leave native rows at/after the cutoff untouched')
    const nativeTailPost = await runContentTimeRevalidation(db, {
      table: 'post', actors: [PUBLISHER_A], nativeV3Tail: true, since: untilExclusive,
      fromVersion: CONTENT_TIME_VALIDATOR_VERSION_V3, toVersion: CONTENT_TIME_VALIDATOR_VERSION_V2,
      batchSize: 500, packetSha256: PACKET_SHA, maxDurationMs: 30_000, pauseMs: 0,
      lockTimeoutMs: 5000, statementTimeoutMs: 30_000,
    })
    assert.equal(nativeTailPost.updated, 1, 'native-tail rollback must include non-clamped post rows at the activation floor')
    const compatibleVersions = await sql<{ version: string; rows: string }>`
      SELECT content_time_validator_version AS version, count(*)::text AS rows
      FROM public.post
      WHERE uri IN (
        ${uri(PUBLISHER_A, 'post-past')},
        ${uri(PUBLISHER_A, 'post-missing')},
        ${uri(PUBLISHER_A, 'post-unparseable')}
      )
      GROUP BY content_time_validator_version
    `.execute(db)
    assert.deepEqual(compatibleVersions.rows, [{ version: CONTENT_TIME_VALIDATOR_VERSION_V2, rows: '3' }], 'semantically unchanged rows retain v2 provenance')

    // Engagement history is projected at export time, never physically migrated.
    await assert.rejects(
      runContentTimeRevalidation(db, {
        table: 'engagement',
        since,
        fromVersion: CONTENT_TIME_VALIDATOR_VERSION_V2,
        toVersion: CONTENT_TIME_VALIDATOR_VERSION_V3,
        batchSize: 500,
        packetSha256: PACKET_SHA,
        maxDurationMs: 30_000,
        pauseMs: 0,
        lockTimeoutMs: 5000,
        statementTimeoutMs: 30_000,
      }),
      /post-only/,
    )
    console.log('ft-fu-1 db execution tests passed')
  } finally {
    await db.destroy()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
