/** Verify the BE internal-ID migration, its replay, and its rollback on a disposable DB. */
import assert from 'assert'
import { Migrator, sql } from 'kysely'
import { createDb, migrateToLatest } from '../src/db'
import { migrationProvider } from '../src/db/migrations'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_SUBSCRIPTION_TEST_CONFIRM !== 'disposable') {
    throw new Error('requires FEEDGEN_TEST_DSN and FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable')
  }
  const db = createDb(dsn)
  try {
    await migrateToLatest(db)
    await sql`
      CREATE SCHEMA IF NOT EXISTS feedgen_ops;
      CREATE SCHEMA IF NOT EXISTS ranker_prod;
      CREATE TABLE IF NOT EXISTS feedgen_ops.feed_catalog (
        feed_id varchar PRIMARY KEY, rkey varchar UNIQUE NOT NULL,
        display_name varchar NOT NULL, country varchar, publisher_did varchar,
        study_id varchar, algo_policy_id varchar NOT NULL, ranker_policy_id varchar,
        access_policy_id varchar NOT NULL, enabled boolean NOT NULL,
        created_at timestamptz DEFAULT now(), retired_at timestamptz
      );
      CREATE TABLE IF NOT EXISTS ranker_prod.feed_current_priority (
        feed_id varchar NOT NULL, post_uri varchar NOT NULL, score double precision,
        run_id varchar NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (feed_id, post_uri)
      );
    `.execute(db)

    const migrator = new Migrator({ db, provider: migrationProvider })
    // Target the migration immediately before 006 explicitly (not a bare
    // migrateDown(), which only undoes the single most-recently-applied
    // migration — 007_subscriber_state_and_kind now sits after 006 in the
    // chain, so a single migrateDown() would undo 007, not 006).
    const down = await migrator.migrateTo('005_canonical_link_columns')
    if (down.error) throw down.error

    await db.deleteFrom('feedgen_ops.subscriber_feed_assignment').where('did', '=', 'did:plc:semantic-feed-id-test').execute()
    await db.deleteFrom('ranker_prod.feed_current_priority').where('run_id', '=', 'semantic-test').execute()
    await db.deleteFrom('feedgen_ops.feed_catalog').where('rkey', 'in', ['newsflow-be-k', 'newsflow-be-m']).execute()
    await db.deleteFrom('subscriber').where('did', '=', 'did:plc:semantic-feed-id-test').execute()

    for (const [feed_id, rkey, display_name] of [
      ['newsflow-be-k', 'newsflow-be-k', 'Vlaamsinfuus K'],
      ['newsflow-be-m', 'newsflow-be-m', 'Vlaamsinfuus M'],
    ]) {
      await db.insertInto('feedgen_ops.feed_catalog').values({
        feed_id, rkey, display_name, algo_policy_id: 'ranker-priority',
        access_policy_id: 'subscriber-default', enabled: true,
      }).execute()
    }
    await db.insertInto('subscriber').values({ handle: 'semantic-test', did: 'did:plc:semantic-feed-id-test' }).execute()
    await db.insertInto('feedgen_ops.subscriber_feed_assignment').values({
      feed_id: 'newsflow-be-k', did: 'did:plc:semantic-feed-id-test',
      active_from: new Date(), status: 'active',
    }).execute()
    await db.insertInto('ranker_prod.feed_current_priority').values({
      feed_id: 'newsflow-be-m', post_uri: 'at://did:plc:test/app.bsky.feed.post/semantic', run_id: 'semantic-test',
    }).execute()

    await migrateToLatest(db)
    const catalog = await db.selectFrom('feedgen_ops.feed_catalog').select(['feed_id', 'rkey', 'display_name']).where('rkey', 'in', ['newsflow-be-k', 'newsflow-be-m']).orderBy('rkey').execute()
    assert.deepStrictEqual(catalog, [
      { feed_id: 'be-k-conventional', rkey: 'newsflow-be-k', display_name: 'Vlaamsinfuus K' },
      { feed_id: 'be-m-party-diversity', rkey: 'newsflow-be-m', display_name: 'Vlaamsinfuus M' },
    ])
    assert.equal((await db.selectFrom('feedgen_ops.subscriber_feed_assignment').select('feed_id').where('did', '=', 'did:plc:semantic-feed-id-test').executeTakeFirstOrThrow()).feed_id, 'be-k-conventional')
    assert.equal((await db.selectFrom('ranker_prod.feed_current_priority').select('feed_id').where('run_id', '=', 'semantic-test').executeTakeFirstOrThrow()).feed_id, 'be-m-party-diversity')

    await migrateToLatest(db) // applied migrations are a replay-safe no-op
    const rollback = await migrator.migrateTo('005_canonical_link_columns')
    if (rollback.error) throw rollback.error
    assert.equal((await db.selectFrom('feedgen_ops.feed_catalog').select('feed_id').where('rkey', '=', 'newsflow-be-k').executeTakeFirstOrThrow()).feed_id, 'newsflow-be-k')
  } finally {
    await db.destroy()
  }
}

main().then(() => console.log('semantic BE feed-ID migration: PASS')).catch((error) => {
  console.error(error)
  process.exit(1)
})
