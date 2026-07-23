/**
 * config_activation — disposable-Postgres rehearsal (design Verifications
 * 1-5, 8, 9, 12). Migration DDL, real insert/dedup semantics, the
 * append-only triggers (row + statement level), and true cross-connection
 * concurrency all need a real Postgres — that's inherent to what they test
 * (a fake in-memory db can't exercise `pg_advisory_xact_lock` or a trigger).
 * The pure-logic parts of this feature (resolvers, manifest raw-freeness,
 * completeness audit, the hook's retry/degraded-flag orchestration, and the
 * HTTP endpoint contracts) are covered without a DB in
 * scripts/test_config_activation_{manifest,completeness,hook,endpoints}.ts.
 *
 * Run only against a throwaway database:
 *   FEEDGEN_TEST_DSN=postgresql://user:pass@host:port/throwaway \
 *   FEEDGEN_CONFIG_ACTIVATION_REHEARSAL=1 \
 *     npx ts-node scripts/test_config_activation_db.ts
 */
import assert from 'assert'
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import { migrationProvider } from '../src/db/migrations'
import { Config } from '../src/config'
import {
  recordConfigActivation,
  resetConfigActivationStateForTests,
  CONFIG_ACTIVATION_ADVISORY_LOCK_KEY,
} from '../src/util/config-activation'

let failed = 0
let passed = 0

function check(cond: boolean, label: string, detail?: string) {
  if (cond) {
    passed++
  } else {
    failed++
    console.error(`FAIL: ${label}${detail ? ` — ${detail}` : ''}`)
  }
}

function baseConfig(overrides: Partial<Config> = {}): Config {
  return {
    port: 3000,
    listenhost: 'localhost',
    hostname: 'feedgen.example.com',
    subscriptionEndpoint: 'wss://bsky.network',
    serviceDid: 'did:web:feedgen.example.com',
    publisherDid: 'did:example:alice',
    subscriptionReconnectDelay: 3000,
    subscriptionIdleTimeoutMs: 0,
    readOnlyMode: false,
    autoMigrate: false,
    ...overrides,
  }
}

async function tableRows(db: Kysely<any>) {
  return db
    .selectFrom('feedgen_ops.config_activation')
    .selectAll()
    .orderBy('activated_at', 'asc')
    .orderBy('activation_id', 'asc')
    .execute()
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_CONFIG_ACTIVATION_REHEARSAL !== '1') {
    console.log('SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_CONFIG_ACTIVATION_REHEARSAL=1')
    return
  }

  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }) })
  const db2 = new Kysely<any>({ dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }) })

  try {
    // ---- Fresh schema, apply every migration up to and including 010. ----
    await sql`DROP SCHEMA IF EXISTS feedgen_ops CASCADE`.execute(db)
    await sql`DROP SCHEMA IF EXISTS ranker_prod CASCADE`.execute(db)
    await sql`DROP SCHEMA IF EXISTS research_archive CASCADE`.execute(db)
    await sql`DROP SCHEMA IF EXISTS public CASCADE`.execute(db)
    await sql`CREATE SCHEMA public`.execute(db)

    // `feedgen_ops.feed_catalog` itself predates this migrations.ts file (it
    // was created out-of-band in production before the migrator existed —
    // migration 008 explicitly RAISE EXCEPTIONs if it's absent rather than
    // create it). Stub it here so 008+ can apply on a bare rehearsal schema,
    // exactly as it would find it in production. Not part of what this
    // feature is testing — pre-existing repo gap, out of scope to fix here.
    const migrations = await migrationProvider.getMigrations()
    const orderedNames = Object.keys(migrations).sort()
    for (const name of orderedNames) {
      if (name === '008_feed_catalog_ranker_score_source') {
        await sql`
          CREATE TABLE feedgen_ops.feed_catalog (
            feed_id text PRIMARY KEY,
            rkey text NOT NULL UNIQUE,
            display_name text NOT NULL,
            country text,
            publisher_did text,
            study_id text,
            algo_policy_id text NOT NULL,
            ranker_policy_id text,
            access_policy_id text NOT NULL,
            enabled boolean NOT NULL DEFAULT true,
            created_at timestamptz NOT NULL DEFAULT now(),
            retired_at timestamptz
          )
        `.execute(db)
      }
      await migrations[name].up(db as any)
    }
    check(true, 'Verification 1: migrations 001..010 applied cleanly on a bare schema')

    const cols = await sql<{ column_name: string }>`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema = 'feedgen_ops' AND table_name = 'config_activation'
      ORDER BY column_name
    `.execute(db)
    const expectedCols = [
      'activated_at', 'activation_id', 'build_sha', 'config', 'config_hash',
      'feed_code_hash', 'image_id', 'prev_config_hash', 'ranker_code_hash', 'reason',
    ].sort()
    check(
      JSON.stringify(cols.rows.map((r) => r.column_name).sort()) === JSON.stringify(expectedCols),
      'migration 010: expected columns present',
      JSON.stringify(cols.rows.map((r) => r.column_name)),
    )

    const idx = await sql<{ indexdef: string }>`
      SELECT indexdef FROM pg_indexes
      WHERE schemaname = 'feedgen_ops' AND tablename = 'config_activation' AND indexname = 'config_activation_activated_at_idx'
    `.execute(db)
    check(
      idx.rows.length === 1 && /activated_at DESC.*activation_id DESC/.test(idx.rows[0].indexdef),
      'migration 010: (activated_at DESC, activation_id DESC) index present',
      idx.rows[0]?.indexdef,
    )

    // ---- Verification 2: first recordConfigActivation → 1 row, prev null. ----
    resetConfigActivationStateForTests()
    process.env.ENGAGEMENT_TIME_HOURS = '72'
    process.env.FEEDGEN_BUILD_SHA = 'sha-abc123'
    await recordConfigActivation(db as any, baseConfig())
    let rows = await tableRows(db)
    check(rows.length === 1, 'Verification 2: exactly one row after first activation', `rows=${rows.length}`)
    check(rows[0].prev_config_hash === null, 'Verification 2: first row prev_config_hash is null')
    check(typeof rows[0].config_hash === 'string' && rows[0].config_hash.length > 0, 'Verification 2: config_hash present')
    check(rows[0].build_sha === 'sha-abc123', 'Verification 2: build_sha stamped from env')
    const firstHash = rows[0].config_hash
    const firstConfig = rows[0].config
    check(
      !JSON.stringify(firstConfig).match(/FEEDGEN_BUILD_SHA|STUDY_JWT_SECRET|api-?key/i),
      'Verification 2: no secret-looking env NAMES leaked into stored config (spot check)',
    )

    // ---- Verification 3: identical env, second call → no new row. ----
    resetConfigActivationStateForTests()
    await recordConfigActivation(db as any, baseConfig())
    rows = await tableRows(db)
    check(rows.length === 1, 'Verification 3: identical env produces no second row (dedup)', `rows=${rows.length}`)

    // ---- Verification 4: change one behavior env → new row, prev = row-1 hash. ----
    resetConfigActivationStateForTests()
    process.env.ENGAGEMENT_TIME_HOURS = '96'
    await recordConfigActivation(db as any, baseConfig())
    rows = await tableRows(db)
    check(rows.length === 2, 'Verification 4: changed env produces a second row', `rows=${rows.length}`)
    check(rows[1].config_hash !== firstHash, 'Verification 4: second row has a different config_hash')
    check(rows[1].prev_config_hash === firstHash, 'Verification 4: second row prev_config_hash == first row config_hash')

    // ---- Verification 5: append-only — UPDATE/DELETE/TRUNCATE all rejected. ----
    let updateRejected = false
    try {
      await sql`UPDATE feedgen_ops.config_activation SET reason = 'tampered' WHERE activation_id = ${rows[0].activation_id}`.execute(db)
    } catch (error) {
      updateRejected = /append-only/.test(String((error as Error).message))
    }
    check(updateRejected, 'Verification 5: UPDATE rejected by row trigger')

    let deleteRejected = false
    try {
      await sql`DELETE FROM feedgen_ops.config_activation WHERE activation_id = ${rows[0].activation_id}`.execute(db)
    } catch (error) {
      deleteRejected = /append-only/.test(String((error as Error).message))
    }
    check(deleteRejected, 'Verification 5: DELETE rejected by row trigger')

    let truncateRejected = false
    try {
      await sql`TRUNCATE feedgen_ops.config_activation`.execute(db)
    } catch (error) {
      truncateRejected = /append-only/.test(String((error as Error).message))
    }
    check(truncateRejected, 'Verification 5: TRUNCATE rejected by statement trigger')

    const rowsAfterRejections = await tableRows(db)
    check(rowsAfterRejections.length === 2, 'Verification 5: row count unchanged after rejected mutations')

    // ---- Verification: REVOKE actually applied for a non-superuser role would
    // additionally block these at the grant level; the trigger rejection above
    // is the enforced-for-everyone layer this test can exercise without
    // provisioning a second role, and is what actually stopped the writes. ----

    // ---- Verification 8: concurrency — two racing activations, same env,
    // exactly one net insert (advisory lock serializes, dedup applies). ----
    resetConfigActivationStateForTests()
    process.env.ENGAGEMENT_TIME_HOURS = '120'
    const beforeConcurrency = await tableRows(db)
    await Promise.all([
      recordConfigActivation(db as any, baseConfig()),
      recordConfigActivation(db2 as any, baseConfig()),
    ])
    const afterConcurrency = await tableRows(db)
    check(
      afterConcurrency.length === beforeConcurrency.length + 1,
      'Verification 8: two racing activations with identical env produce exactly one new row',
      `before=${beforeConcurrency.length} after=${afterConcurrency.length}`,
    )
    const newRows = afterConcurrency.slice(beforeConcurrency.length)
    check(
      newRows[0].prev_config_hash === beforeConcurrency[beforeConcurrency.length - 1].config_hash,
      'Verification 8: no forked prev_config_hash — the new row points at the prior latest row',
    )

    // ---- Verification 9: durable fail-open + recovery. ----
    resetConfigActivationStateForTests()
    process.env.ENGAGEMENT_TIME_HOURS = '144'
    const beforeFailOpen = await tableRows(db)
    // A wrapper around the real db whose first 2 attempts fail (simulating a
    // transient outage) and then delegate to the real connection once
    // "recovered" — proves both halves of Verification 9 in one pass: fail-
    // open (serving is never blocked; fast retries exhaust) AND recovery
    // (the background retry lands exactly one row using the closure's
    // original captured activatedAt/manifest, once the DB is reachable
    // again).
    let attemptCount = 0
    const flakyDb: any = {
      transaction: () => ({
        execute: async (cb: (trx: any) => Promise<unknown>) => {
          attemptCount++
          if (attemptCount <= 2) throw new Error('simulated transient outage')
          return (db as any).transaction().execute(cb)
        },
      }),
    }
    await recordConfigActivation(flakyDb, baseConfig(), {
      fastRetries: 1,
      fastRetryBaseDelayMs: 1,
      backgroundRetryIntervalMs: 50,
    })
    check(attemptCount === 1, 'Verification 9: fast retry (1) exhausted before background kicks in')
    await new Promise((resolve) => setTimeout(resolve, 400))
    const afterFailOpen = await tableRows(db)
    check(
      afterFailOpen.length === beforeFailOpen.length + 1,
      'Verification 9: exactly one row lands once the background retry succeeds',
      `before=${beforeFailOpen.length} after=${afterFailOpen.length}`,
    )
    resetConfigActivationStateForTests()

    // ---- Verification 12: read-only mode skip. ----
    const beforeReadOnly = await tableRows(db)
    process.env.ENGAGEMENT_TIME_HOURS = '168'
    await recordConfigActivation(db as any, baseConfig({ readOnlyMode: true }))
    const afterReadOnly = await tableRows(db)
    check(
      afterReadOnly.length === beforeReadOnly.length,
      'Verification 12: read-only mode writes no activation row even with a changed behavior env',
    )

    console.log(`config_activation DB rehearsal: ${passed} passed, ${failed} failed`)
    if (failed > 0) process.exit(1)
  } finally {
    resetConfigActivationStateForTests()
    delete process.env.ENGAGEMENT_TIME_HOURS
    delete process.env.FEEDGEN_BUILD_SHA
    await db.destroy()
    await db2.destroy()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
