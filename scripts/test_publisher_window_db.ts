/**
 * Disposable-Postgres rehearsal for migration 011 and database constraints.
 *
 * FEEDGEN_TEST_DSN=postgresql://... FEEDGEN_PUBLISHER_WINDOW_REHEARSAL=1 \
 *   yarn test:publisher-window-db
 */
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Client, Pool } from 'pg'
import express from 'express'
import { migrationProvider, validateContentTimeConstraints } from '../src/db/migrations'
import registerFeedCatalogAdminEndpoint from '../src/methods/feed-catalog-admin'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

async function withAdminServer(db: any, run: (baseUrl: string) => Promise<void>) {
  const app = express()
  app.use(express.json())
  registerFeedCatalogAdminEndpoint({ xrpc: { router: app } } as any, { db } as any)
  const server = app.listen(0, '127.0.0.1')
  await new Promise<void>((resolve) => server.once('listening', resolve))
  const address = server.address()
  check(address && typeof address === 'object', 'admin rehearsal server must listen')
  try { await run(`http://127.0.0.1:${address.port}`) } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
  }
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_PUBLISHER_WINDOW_REHEARSAL !== '1') {
    console.log('SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_PUBLISHER_WINDOW_REHEARSAL=1')
    return
  }
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }) })
  try {
    await sql`DROP SCHEMA IF EXISTS feedgen_ops CASCADE`.execute(db)
    await sql`DROP TABLE IF EXISTS request_log, engagement, post CASCADE`.execute(db)
    await sql`CREATE SCHEMA feedgen_ops`.execute(db)
    await sql`
      CREATE TABLE feedgen_ops.feed_catalog (
        feed_id text PRIMARY KEY, rkey text UNIQUE NOT NULL, enabled boolean NOT NULL DEFAULT true,
        algo_policy_id text NOT NULL DEFAULT 'chronological',
        display_name text NOT NULL DEFAULT '', country text, publisher_did text, study_id text,
        ranker_policy_id text, ranker_score_source text,
        access_policy_id text NOT NULL DEFAULT 'subscriber-default',
        created_at text NOT NULL DEFAULT CURRENT_TIMESTAMP, retired_at text
      )
    `.execute(db)
    await sql`CREATE TABLE post (uri text PRIMARY KEY, createdAt text NOT NULL, indexedAt text NOT NULL)`.execute(db)
    await sql`CREATE TABLE engagement (uri text PRIMARY KEY, createdAt text NOT NULL, indexedAt text NOT NULL)`.execute(db)
    await sql`CREATE TABLE request_log (id bigserial PRIMARY KEY)`.execute(db)
    await sql`INSERT INTO post(uri, createdat, indexedat) VALUES ('at://legacy', '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')`.execute(db)
    await sql`INSERT INTO feedgen_ops.feed_catalog(feed_id, rkey) VALUES ('be-k', 'newsflow-be-k')`.execute(db)

    const migrations = await migrationProvider.getMigrations()
    await migrations['011_publisher_post_max_age'].up(db)
    await sql`
      CREATE TABLE feedgen_ops.feed_catalog_history (
        feed_id text NOT NULL, rkey text NOT NULL, revision integer NOT NULL,
        changed_at text NOT NULL DEFAULT CURRENT_TIMESTAMP, actor text NOT NULL, source text NOT NULL,
        before_row jsonb, after_row jsonb NOT NULL, changed_fields jsonb NOT NULL,
        feed_code_hash_before text, feed_code_hash_after text,
        ranker_code_hash_before text, ranker_code_hash_after text,
        PRIMARY KEY(feed_id, revision)
      )
    `.execute(db)

    const legacy = await sql<any>`SELECT content_time_status, content_time_utc FROM post WHERE uri = 'at://legacy'`.execute(db)
    check(legacy.rows[0].content_time_status === null && legacy.rows[0].content_time_utc === null, 'expand migration must not rewrite legacy hot rows')
    const eligibleLegacy = await sql<any>`SELECT count(*)::int AS count FROM post WHERE content_time_status = 'source_valid'`.execute(db)
    check(eligibleLegacy.rows[0].count === 0, 'unclassified legacy rows must remain excluded from content-time eligibility')

    const unvalidated = await sql<any>`
      SELECT conname, convalidated FROM pg_constraint
      WHERE conname IN ('post_content_time_pair', 'engagement_content_time_pair')
      ORDER BY conname
    `.execute(db)
    check(unvalidated.rows.length === 2 && unvalidated.rows.every((constraint: any) => constraint.convalidated === false), 'hot content-time constraints must start NOT VALID')

    const notifyClient = new Client({ connectionString: dsn })
    await notifyClient.connect()
    await notifyClient.query('LISTEN feed_catalog_changed')
    const notification = new Promise<string>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('feed_catalog pg_notify timeout')), 2000)
      notifyClient.once('notification', (message) => {
        clearTimeout(timer)
        resolve(message.payload ?? '')
      })
    })

    await sql`
      UPDATE feedgen_ops.feed_catalog
      SET publisher_post_max_age_days = 10,
          publisher_post_max_age_source = 'study_default',
          publisher_time_clock = 'content_time_v1',
          content_time_cutover_min_valid_share = 0.99,
          content_time_contract_version = 'newsflows-content-time/v2'
      WHERE rkey = 'newsflow-be-k'
    `.execute(db)
    const notificationPayload = JSON.parse(await notification)
    await notifyClient.end()
    check(notificationPayload.rkey === 'newsflow-be-k' && notificationPayload.op === 'update', 'feed catalog trigger must publish rkey/op')
    const row = await sql<any>`SELECT * FROM feedgen_ops.feed_catalog WHERE rkey = 'newsflow-be-k'`.execute(db)
    check(row.rows[0].publisher_post_max_age_days === 10, 'database must materialize ten days')
    check(row.rows[0].publisher_post_max_age_source === 'study_default', 'database must preserve provenance')

    let rejectedMissingRankerControls = false
    try {
      await sql`UPDATE feedgen_ops.feed_catalog SET algo_policy_id = 'ranker-priority' WHERE rkey = 'newsflow-be-k'`.execute(db)
    } catch { rejectedMissingRankerControls = true }
    check(rejectedMissingRankerControls, 'database must reject ranker feeds without owner-provided score controls')
    await sql`
      UPDATE feedgen_ops.feed_catalog
      SET algo_policy_id = 'ranker-priority',
          ranker_score_max_age_hours = 24,
          ranker_score_max_age_source = 'study_default',
          ranker_min_score_backed_share = 0.8,
          ranker_min_score_backed_source = 'study_default'
      WHERE rkey = 'newsflow-be-k'
    `.execute(db)

    let rejectedPair = false
    try {
      await sql`UPDATE feedgen_ops.feed_catalog SET publisher_post_max_age_source = NULL WHERE rkey = 'newsflow-be-k'`.execute(db)
    } catch { rejectedPair = true }
    check(rejectedPair, 'database must reject value without provenance')

    let rejectedSourceOnly = false
    try {
      await sql`
        UPDATE feedgen_ops.feed_catalog
        SET publisher_post_max_age_days = NULL,
            publisher_post_max_age_source = 'study_default'
        WHERE rkey = 'newsflow-be-k'
      `.execute(db)
    } catch { rejectedSourceOnly = true }
    check(rejectedSourceOnly, 'database must reject provenance without a value')

    let rejectedBound = false
    try {
      await sql`UPDATE feedgen_ops.feed_catalog SET publisher_post_max_age_days = 366 WHERE rkey = 'newsflow-be-k'`.execute(db)
    } catch { rejectedBound = true }
    check(rejectedBound, 'database must reject unsafe bounds')

    await sql`
      UPDATE feedgen_ops.feed_catalog
      SET publisher_post_max_age_days = 7,
          publisher_post_max_age_source = 'feed_override',
          publisher_time_clock = 'receipt_time',
          content_time_cutover_min_valid_share = NULL,
          content_time_contract_version = NULL
      WHERE rkey = 'newsflow-be-k'
    `.execute(db)
    const rollback = await sql<any>`
      SELECT publisher_post_max_age_days, publisher_post_max_age_source, publisher_time_clock
      FROM feedgen_ops.feed_catalog WHERE rkey = 'newsflow-be-k'
    `.execute(db)
    check(rollback.rows[0].publisher_post_max_age_days === 7, 'paired rollback must restore the prior value')
    check(rollback.rows[0].publisher_post_max_age_source === 'feed_override', 'paired rollback must restore provenance')
    check(rollback.rows[0].publisher_time_clock === 'receipt_time', 'rollback must restore the prior clock')

    await sql`
      INSERT INTO feedgen_ops.feed_catalog(
        feed_id, rkey, display_name, algo_policy_id, access_policy_id,
        publisher_post_max_age_days, publisher_post_max_age_source)
      VALUES
        ('be-x', 'newsflow-be-x', 'BE X', 'chronological', 'subscriber-default', 7, 'feed_override'),
        ('be-y', 'newsflow-be-y', 'BE Y', 'chronological', 'subscriber-default', 7, 'feed_override')
    `.execute(db)
    const bulkNotifyClient = new Client({ connectionString: dsn })
    await bulkNotifyClient.connect()
    await bulkNotifyClient.query('LISTEN feed_catalog_changed')
    const bulkNotifications: string[] = []
    bulkNotifyClient.on('notification', (message) => bulkNotifications.push(message.payload ?? ''))
    process.env.FEEDGEN_ADMIN_API_KEY = 'postgres-rehearsal-admin'
    await withAdminServer(db, async (baseUrl) => {
      const applyBulk = (updates: unknown[]) => fetch(`${baseUrl}/api/admin/feed_catalog/bulk`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'postgres-rehearsal-admin' },
        body: JSON.stringify({ updates }),
      })
      const update = (rkey: string, days: number, expectedDays: number) => ({
        op: 'update', rkey,
        publisher_post_max_age_days: days,
        publisher_post_max_age_source: 'feed_override',
        if_current: {
          publisher_post_max_age_days: expectedDays,
          publisher_post_max_age_source: 'feed_override',
        },
      })

      const conflict = await applyBulk([
        update('newsflow-be-x', 8, 7),
        update('newsflow-be-y', 9, 6),
      ])
      check(conflict.status === 409, 'second-row CAS conflict must abort bulk owner path')
      await new Promise((resolve) => setTimeout(resolve, 100))
      let bulkRows = await sql<any>`
        SELECT rkey, publisher_post_max_age_days, catalog_revision
        FROM feedgen_ops.feed_catalog WHERE rkey IN ('newsflow-be-x', 'newsflow-be-y') ORDER BY rkey
      `.execute(db)
      check(bulkRows.rows.every((candidate: any) => candidate.publisher_post_max_age_days === 7 && Number(candidate.catalog_revision) === 0), 'CAS abort must commit zero partial feed rows')
      let historyCount = await sql<any>`SELECT count(*)::int AS count FROM feedgen_ops.feed_catalog_history WHERE feed_id IN ('be-x', 'be-y')`.execute(db)
      check(historyCount.rows[0].count === 0, 'CAS abort must commit zero history rows')
      check(bulkNotifications.length === 0, 'CAS abort must commit zero notifications')

      await sql`
        CREATE FUNCTION feedgen_ops.fail_be_y_bulk_update() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.rkey = 'newsflow-be-y' THEN RAISE EXCEPTION 'injected second-row failure'; END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER fail_be_y_bulk_update BEFORE UPDATE ON feedgen_ops.feed_catalog
        FOR EACH ROW EXECUTE FUNCTION feedgen_ops.fail_be_y_bulk_update()
      `.execute(db)
      const failed = await applyBulk([
        update('newsflow-be-x', 8, 7),
        update('newsflow-be-y', 9, 7),
      ])
      check(failed.status === 500, 'injected second-row database failure must abort bulk owner path')
      await new Promise((resolve) => setTimeout(resolve, 100))
      bulkRows = await sql<any>`
        SELECT rkey, publisher_post_max_age_days, catalog_revision
        FROM feedgen_ops.feed_catalog WHERE rkey IN ('newsflow-be-x', 'newsflow-be-y') ORDER BY rkey
      `.execute(db)
      check(bulkRows.rows.every((candidate: any) => candidate.publisher_post_max_age_days === 7 && Number(candidate.catalog_revision) === 0), 'database failure must commit zero partial feed rows')
      historyCount = await sql<any>`SELECT count(*)::int AS count FROM feedgen_ops.feed_catalog_history WHERE feed_id IN ('be-x', 'be-y')`.execute(db)
      check(historyCount.rows[0].count === 0, 'database failure must commit zero history rows')
      check(bulkNotifications.length === 0, 'database failure must commit zero notifications')
    })
    await bulkNotifyClient.end()
    delete process.env.FEEDGEN_ADMIN_API_KEY

    await sql`
      INSERT INTO post(uri, createdAt, indexedAt, created_at_source_raw, content_time_utc,
        content_time_status, content_time_validator_version)
      VALUES ('valid', '2026-08-02T00:00:00Z', '2026-08-12T00:00:00Z',
        convert_to('2026-08-02T00:00:00Z', 'UTF8'), '2026-08-02T00:00:00.000Z',
        'source_valid', 'newsflows-content-time/v2')
    `.execute(db)
    let rejectedContentPair = false
    try {
      await sql`
        INSERT INTO post(uri, createdAt, indexedAt, content_time_status)
        VALUES ('invalid', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z', 'source_invalid')
      `.execute(db)
    } catch { rejectedContentPair = true }
    check(rejectedContentPair, 'database must reject invalid content time without reason')

    let rejectedValidRaw = false
    try {
      await sql`
        INSERT INTO post(uri, createdAt, indexedAt, content_time_utc,
          content_time_status, content_time_validator_version)
        VALUES ('valid-no-raw', '2026-08-02T00:00:00Z', '2026-08-12T00:00:00Z',
          '2026-08-02T00:00:00.000Z', 'source_valid', 'newsflows-content-time/v2')
      `.execute(db)
    } catch { rejectedValidRaw = true }
    check(rejectedValidRaw, 'database must reject source_valid without raw provenance')

    let rejectedInvalidVersion = false
    try {
      await sql`
        INSERT INTO engagement(uri, createdAt, indexedAt, created_at_source_raw,
          content_time_status, content_time_clamp_reason)
        VALUES ('invalid-no-version', '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z',
          convert_to('', 'UTF8'), 'source_invalid', 'missing')
      `.execute(db)
    } catch { rejectedInvalidVersion = true }
    check(rejectedInvalidVersion, 'database must reject source_invalid without validator version')

    check(!migrations['012_validate_content_time_constraints'], 'hot-table validation must not be an automatic migration')
    await validateContentTimeConstraints(db)
    const validated = await sql<any>`
      SELECT conname, convalidated FROM pg_constraint
      WHERE conname IN ('post_content_time_pair', 'engagement_content_time_pair')
      ORDER BY conname
    `.execute(db)
    check(validated.rows.length === 2 && validated.rows.every((constraint: any) => constraint.convalidated === true), 'follow-up migration must validate constraints')

    console.log('publisher window disposable Postgres rehearsal passed')
  } finally {
    await db.destroy()
  }
}

main().catch((error) => { console.error(error); process.exit(1) })
