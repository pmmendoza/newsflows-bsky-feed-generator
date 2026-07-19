import { Migrator, sql } from 'kysely'
import jwt from 'jsonwebtoken'
import { createDb, migrateToLatest } from '../src/db'
import { migrationProvider } from '../src/db/migrations'
import { AppContext } from '../src/config'
import {
  executeSubscription,
  inspectSubscription,
  resolveSubscriptionIdentity,
  SubscriptionError,
} from '../src/util/exact-subscription'
import { evaluateAccessPolicy, invalidatePolicyCache } from '../src/util/access-policy'
import { validateAuth } from '../src/auth'

const HANDLE = process.env.FEEDGEN_SUBSCRIPTION_TEST_HANDLE || 'luciemartel.bsky.social'

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

async function rollbackThroughSubscriptionMigration(db: ReturnType<typeof createDb>) {
  const migrator = new Migrator({ db, provider: migrationProvider })
  while ((await sql`
    SELECT 1 FROM kysely_migration
    WHERE name = '004_exact_feed_subscriptions'
  `.execute(db)).rows.length) {
    const down = await migrator.migrateDown()
    if (down.error) throw down.error
    const irreversible = down.results?.find((result) => result.status === 'NotExecuted')
    if (irreversible) {
      // The disposable schema keeps the idempotent expand migration's columns;
      // remove only its migration record so this test can exercise migration 004.
      await sql`DELETE FROM kysely_migration WHERE name = ${irreversible.migrationName}`.execute(db)
    }
  }
}

async function testMalformedOwnerSchemaIsRejected(db: ReturnType<typeof createDb>) {
  await migrateToLatest(db)
  await rollbackThroughSubscriptionMigration(db)

  await sql`
    ALTER TABLE subscriber ADD COLUMN access_scope varchar;
    CREATE SCHEMA IF NOT EXISTS feedgen_ops;
    CREATE TABLE feedgen_ops.subscriber_feed_assignment (
      assignment_id bigserial PRIMARY KEY,
      feed_id varchar NOT NULL,
      did varchar NOT NULL REFERENCES subscriber(did) ON DELETE CASCADE,
      active_from timestamptz NOT NULL DEFAULT now(),
      active_until timestamptz,
      source varchar,
      status varchar NOT NULL DEFAULT 'active'
    )
  `.execute(db)

  let migrationError: unknown
  try {
    await migrateToLatest(db)
  } catch (error) {
    migrationError = error
  }
  assert(migrationError instanceof Error, 'malformed owner-applied subscription schema must fail migration')
  assert(
    migrationError.message.includes('004_exact_feed_subscriptions schema mismatch'),
    'malformed owner-applied schema must fail with an actionable migration error',
  )

  await sql`
    DROP TABLE feedgen_ops.subscriber_feed_assignment;
    ALTER TABLE subscriber DROP CONSTRAINT IF EXISTS subscriber_access_scope_check;
    ALTER TABLE subscriber DROP COLUMN access_scope
  `.execute(db)
}

async function testIneffectiveOwnerConstraintsAreRejected(db: ReturnType<typeof createDb>) {
  await sql`
    ALTER TABLE subscriber
      ADD COLUMN access_scope varchar NOT NULL DEFAULT 'omni',
      ADD CONSTRAINT subscriber_access_scope_check CHECK (true);
    CREATE TABLE feedgen_ops.subscriber_feed_assignment (
      assignment_id bigserial PRIMARY KEY,
      feed_id varchar NOT NULL,
      did varchar NOT NULL REFERENCES subscriber(did) ON DELETE CASCADE,
      active_from timestamptz NOT NULL DEFAULT now(),
      active_until timestamptz,
      source varchar,
      status varchar NOT NULL DEFAULT 'active',
      CONSTRAINT subscriber_feed_assignment_interval_check CHECK (true),
      CONSTRAINT subscriber_feed_assignment_status_check CHECK (true)
    );
    CREATE UNIQUE INDEX subscriber_feed_assignment_active_uq
      ON feedgen_ops.subscriber_feed_assignment (feed_id, did)
      WHERE active_until IS NULL;
    CREATE INDEX subscriber_feed_assignment_did_active_idx
      ON feedgen_ops.subscriber_feed_assignment (did)
      WHERE active_until IS NULL
  `.execute(db)

  let migrationError: unknown
  try {
    await migrateToLatest(db)
  } catch (error) {
    migrationError = error
  }
  assert(migrationError instanceof Error, 'ineffective owner-applied constraints must fail migration')
  assert(
    migrationError.message.includes('004_exact_feed_subscriptions schema mismatch'),
    'ineffective owner-applied constraints must fail with an actionable migration error',
  )

  await sql`
    DROP TABLE feedgen_ops.subscriber_feed_assignment;
    ALTER TABLE subscriber DROP CONSTRAINT IF EXISTS subscriber_access_scope_check;
    ALTER TABLE subscriber DROP COLUMN access_scope
  `.execute(db)
}

async function testValidOwnerSchemaSurvivesRollback(db: ReturnType<typeof createDb>) {
  await sql`
    ALTER TABLE subscriber
      ADD COLUMN access_scope varchar NOT NULL DEFAULT 'omni',
      ADD CONSTRAINT subscriber_access_scope_check
        CHECK (access_scope IN ('omni', 'assigned', 'none'));
    COMMENT ON COLUMN subscriber.access_scope IS 'owner-applied subscription schema';
    CREATE TABLE feedgen_ops.subscriber_feed_assignment (
      assignment_id bigserial PRIMARY KEY,
      feed_id varchar NOT NULL,
      did varchar NOT NULL REFERENCES subscriber(did) ON DELETE CASCADE,
      active_from timestamptz NOT NULL DEFAULT now(),
      active_until timestamptz,
      source varchar,
      status varchar NOT NULL DEFAULT 'active',
      CONSTRAINT subscriber_feed_assignment_interval_check
        CHECK (active_until IS NULL OR active_until > active_from),
      CONSTRAINT subscriber_feed_assignment_status_check
        CHECK (
          (active_until IS NULL AND status = 'active') OR
          (active_until IS NOT NULL AND status IN ('removed', 'replaced', 'omni'))
        )
    );
    CREATE UNIQUE INDEX subscriber_feed_assignment_active_uq
      ON feedgen_ops.subscriber_feed_assignment (feed_id, did)
      WHERE active_until IS NULL;
    CREATE INDEX subscriber_feed_assignment_did_active_idx
      ON feedgen_ops.subscriber_feed_assignment (did)
      WHERE active_until IS NULL;
    COMMENT ON TABLE feedgen_ops.subscriber_feed_assignment IS 'owner-applied subscription schema'
  `.execute(db)

  await migrateToLatest(db)
  await rollbackThroughSubscriptionMigration(db)

  const objects = await sql<{ table_exists: boolean; column_exists: boolean }>`
    SELECT
      to_regclass('feedgen_ops.subscriber_feed_assignment') IS NOT NULL AS table_exists,
      EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'subscriber'
          AND column_name = 'access_scope'
      ) AS column_exists
  `.execute(db)
  assert(
    objects.rows[0]?.table_exists && objects.rows[0]?.column_exists,
    'rollback must preserve valid owner-applied subscription objects',
  )

  await sql`
    DROP TABLE feedgen_ops.subscriber_feed_assignment;
    ALTER TABLE subscriber DROP COLUMN access_scope
  `.execute(db)
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_SUBSCRIPTION_TEST_CONFIRM !== 'disposable') {
    throw new Error('integration gate requires FEEDGEN_TEST_DSN and FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable')
  }

  const db = createDb(dsn)
  const ctx = { db } as AppContext
  let did = ''
  const listDids = [
    'did:plc:subscriptionlistomni',
    'did:plc:subscriptionlistassigned',
    'did:plc:subscriptionlistnone',
  ]

  try {
    await testMalformedOwnerSchemaIsRejected(db)
    await testIneffectiveOwnerConstraintsAreRejected(db)
    await testValidOwnerSchemaSurvivesRollback(db)
    const identity = await resolveSubscriptionIdentity({ handle: HANDLE })
    did = identity.did
    await migrateToLatest(db)
    await sql`
      CREATE SCHEMA IF NOT EXISTS feedgen_ops;
      CREATE TABLE IF NOT EXISTS feedgen_ops.feed_catalog (
        feed_id varchar PRIMARY KEY,
        rkey varchar UNIQUE NOT NULL,
        display_name varchar NOT NULL,
        country varchar,
        publisher_did varchar,
        study_id varchar,
        algo_policy_id varchar NOT NULL,
        ranker_policy_id varchar,
        access_policy_id varchar NOT NULL,
        enabled boolean NOT NULL,
        created_at timestamptz DEFAULT now(),
        retired_at timestamptz
      )
    `.execute(db)

    await db
      .deleteFrom('feedgen_ops.subscriber_feed_assignment')
      .where('did', '=', did)
      .execute()
    await db.deleteFrom('subscriber').where('did', '=', did).execute()

    await rollbackThroughSubscriptionMigration(db)
    await db.insertInto('subscriber').values({ handle: identity.handle, did }).execute()
    await migrateToLatest(db)
    const backfill = await db
      .selectFrom('subscriber')
      .select('access_scope')
      .where('did', '=', did)
      .executeTakeFirstOrThrow()
    assert(backfill.access_scope === 'omni', 'migration must backfill existing subscribers to omni')
    await db.deleteFrom('subscriber').where('did', '=', did).execute()

    for (const feed of [
      { id: 'test-feed-be-1', rkey: 'test-be-1', study: 'test-study-be' },
      { id: 'test-feed-be-2', rkey: 'test-be-2', study: 'test-study-be' },
      { id: 'test-feed-nl-1', rkey: 'test-nl-1', study: 'test-study-nl' },
    ]) {
      await db
        .insertInto('feedgen_ops.feed_catalog')
        .values({
          feed_id: feed.id,
          rkey: feed.rkey,
          display_name: feed.rkey,
          country: null,
          publisher_did: null,
          study_id: feed.study,
          algo_policy_id: 'chronological',
          ranker_policy_id: null,
          access_policy_id: 'subscriber-default',
          enabled: true,
          retired_at: null,
        })
        .onConflict((oc) => oc.column('feed_id').doUpdateSet({
          rkey: feed.rkey,
          study_id: feed.study,
          enabled: true,
          access_policy_id: 'subscriber-default',
        }))
        .execute()
    }

    await db.insertInto('subscriber').values({ handle: identity.handle, did, access_scope: 'omni' }).execute()
    const addFromOmni = await executeSubscription(
      ctx,
      { did, feed: 'test-be-1', mode: 'add', source: 'subscription-test' },
      true,
      false,
    )
    assert(
      addFromOmni.access_scope === 'assigned' && addFromOmni.assignments.map((row) => row.feed).join() === 'test-be-1',
      'add from migrated omni scope must enter exact-feed scope',
    )
    const feedlessOmni = await executeSubscription(ctx, { did, mode: 'omni' }, true, false)
    assert(feedlessOmni.feed === null, 'omni must not require or invent a feed')
    let omniWithFeedRejected = false
    try {
      await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'omni' }, false)
    } catch (error) {
      omniWithFeedRejected = error instanceof SubscriptionError && error.code === 'invalid_feed'
    }
    assert(omniWithFeedRejected, 'omni must reject a feed argument')
    await db.deleteFrom('subscriber').where('did', '=', did).execute()
    const removeMissing = await executeSubscription(
      ctx,
      { did, feed: 'test-be-1', mode: 'remove' },
      true,
      false,
    )
    assert(!removeMissing.changed && removeMissing.access_scope === 'none', 'remove must be idempotent for a missing subscriber')

    await db
      .updateTable('feedgen_ops.feed_catalog')
      .set({ retired_at: new Date() })
      .where('feed_id', '=', 'test-feed-be-2')
      .execute()
    let retiredRejected = false
    try {
      await executeSubscription(ctx, { did, feed: 'test-be-2', mode: 'replace' }, false)
    } catch (error) {
      retiredRejected = error instanceof SubscriptionError && error.code === 'feed_disabled'
    }
    assert(retiredRejected, 'retired feed must be rejected even if enabled')
    await db
      .updateTable('feedgen_ops.feed_catalog')
      .set({ retired_at: null })
      .where('feed_id', '=', 'test-feed-be-2')
      .execute()

    const replace = await executeSubscription(
      ctx,
      { handle: HANDLE, feed: 'test-be-1', mode: 'replace', source: 'subscription-test' },
      true,
      false,
    )
    assert(replace.access_scope === 'assigned', 'replace must enter assigned scope')
    assert(replace.assignments.map((row) => row.feed).join() === 'test-be-1', 'replace must assign exact feed')

    const byDid = await executeSubscription(
      ctx,
      { did, feed: 'test-be-1', mode: 'replace', source: 'subscription-test' },
      true,
      false,
    )
    assert(byDid.did === replace.did && !byDid.changed, 'handle and DID must be equivalent and idempotent')
    const byUrl = await executeSubscription(
      ctx,
      { did, feed: 'https://bsky.app/profile/test.example/feed/test-be-1', mode: 'replace' },
      false,
    )
    assert(byUrl.feed === 'test-be-1' && !byUrl.changed, 'canonical Bluesky feed URL must resolve to its exact rkey')
    let invalidUrlRejected = false
    try {
      await executeSubscription(
        ctx,
        { did, feed: 'https://bsky.app/profile/test.example/feed/%', mode: 'replace' },
        false,
      )
    } catch (error) {
      invalidUrlRejected = error instanceof SubscriptionError && error.code === 'invalid_feed'
    }
    assert(invalidUrlRejected, 'malformed feed URL must return a stable client error')

    for (let index = 0; index < 3; index += 1) {
      await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'remove' }, true, false)
      await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'add' }, true, false)
    }
    const historyCount = await db
      .selectFrom('feedgen_ops.subscriber_feed_assignment')
      .select(({ fn }) => fn.countAll<number>().as('count'))
      .where('did', '=', did)
      .where('feed_id', '=', 'test-feed-be-1')
      .executeTakeFirstOrThrow()
    assert(Number(historyCount.count) === 4, 'rapid reactivation must retain collision-free temporal history')

    invalidatePolicyCache()
    assert((await evaluateAccessPolicy(db, 'test-be-1', did)).allowed, 'assigned feed must be allowed')
    invalidatePolicyCache()
    assert(!(await evaluateAccessPolicy(db, 'test-be-2', did)).allowed, 'sibling feed must be denied')

    const added = await executeSubscription(ctx, { did, feed: 'test-be-2', mode: 'add' }, true, false)
    assert(added.assignments.length === 2, 'add must preserve existing exact assignment')
    const removed = await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'remove' }, true, false)
    assert(removed.assignments.map((row) => row.feed).join() === 'test-be-2', 'remove must close only target feed')
    const otherStudy = await executeSubscription(ctx, { did, feed: 'test-nl-1', mode: 'replace' }, true, false)
    assert(otherStudy.assignments.length === 2, 'replace must preserve other-study assignments')
    await executeSubscription(ctx, { did, feed: 'test-be-2', mode: 'remove' }, true, false)
    const none = await executeSubscription(ctx, { did, feed: 'test-nl-1', mode: 'remove' }, true, false)
    assert(none.access_scope === 'none' && none.assignments.length === 0, 'removing last assignment must deny all')
    const omni = await executeSubscription(ctx, { did, mode: 'omni' }, true, false)
    assert(omni.access_scope === 'omni' && omni.assignments.length === 0, 'omni must clear exact assignments')

    let conflict = false
    try {
      await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'remove' }, true, false)
    } catch (error) {
      conflict = error instanceof SubscriptionError && error.code === 'mode_conflict'
    }
    assert(conflict, 'remove from omni must fail explicitly')

    const concurrent = await Promise.all([
      executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'replace' }, true, false),
      executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'replace' }, true, false),
    ])
    assert(
      concurrent.filter((result) => result.changed).length === 1,
      'concurrent identical requests must truthfully report one mutation and one no-op',
    )
    const finalState = await inspectSubscription(db, identity)
    assert(finalState.assignments.length === 1, 'concurrent idempotent replace must leave one active row')

    await executeSubscription(ctx, { did, mode: 'omni' }, true, false)
    await sql`
      CREATE OR REPLACE FUNCTION feedgen_ops.reject_forced_subscription_test()
      RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.source = 'force-rollback' THEN
          RAISE EXCEPTION 'forced subscription rollback';
        END IF;
        RETURN NEW;
      END
      $$;
      CREATE TRIGGER reject_forced_subscription_test
      BEFORE INSERT ON feedgen_ops.subscriber_feed_assignment
      FOR EACH ROW EXECUTE FUNCTION feedgen_ops.reject_forced_subscription_test()
    `.execute(db)
    let rollbackRejected = false
    try {
      await executeSubscription(
        ctx,
        { did, feed: 'test-be-1', mode: 'replace', source: 'force-rollback' },
        true,
        false,
      )
    } catch {
      rollbackRejected = true
    }
    assert(rollbackRejected, 'forced database failure must reject the request')
    const rolledBack = await inspectSubscription(db, identity)
    assert(rolledBack.access_scope === 'omni' && rolledBack.assignments.length === 0, 'failed transaction must not partially mutate state')
    await sql`
      DROP TRIGGER reject_forced_subscription_test ON feedgen_ops.subscriber_feed_assignment;
      DROP FUNCTION feedgen_ops.reject_forced_subscription_test()
    `.execute(db)

    process.env.FEEDGEN_ADMIN_API_KEY = 'subscription-admin-test-key'
    process.env.FEEDGEN_READ_API_KEY = 'subscription-read-test-key'
    process.env.STUDY_TOKEN_API_KEY = 'subscription-token-test-key'
    process.env.STUDY_JWT_SECRET = 'subscription-test-secret-that-is-long-enough'
    const FeedGenerator = (await import('../src/server')).default
    const app = FeedGenerator.create({
      port: 0,
      listenhost: '127.0.0.1',
      hostname: 'localhost',
      postgresUrl: dsn,
      subscriptionEndpoint: 'ws://127.0.0.1:1',
      serviceDid: 'did:web:localhost',
      publisherDid: 'did:plc:test-publisher',
      subscriptionReconnectDelay: 1000,
      subscriptionIdleTimeoutMs: 0,
      readOnlyMode: true,
    })
    await app.start()
    try {
      const address = app.server?.address()
      assert(address && typeof address === 'object', 'test server must expose a port')
      const base = `http://127.0.0.1:${address.port}`
      const body = { handle: HANDLE, feed: 'test-be-1', mode: 'replace' }
      const mint = await fetch(`${base}/api/subscription-token`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-token-test-key' },
        body: JSON.stringify({ handle: HANDLE }),
      })
      const tokenPayload = await mint.json() as any
      assert(mint.status === 200 && typeof tokenPayload.token === 'string', 'study authority must mint a per-user token')

      const subscribe = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${tokenPayload.token}` },
        body: JSON.stringify(body),
      })
      assert(subscribe.status === 200, 'per-user token must subscribe to an exact feed')
      const sourceRow = await db
        .selectFrom('feedgen_ops.subscriber_feed_assignment')
        .select('source')
        .where('did', '=', did)
        .where('feed_id', '=', 'test-feed-be-1')
        .where('active_until', 'is', null)
        .executeTakeFirstOrThrow()
      assert(sourceRow.source === 'subscription-token', 'token caller must not spoof audit source')

      const tokenAdd = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${tokenPayload.token}` },
        body: JSON.stringify({ ...body, feed: 'test-be-2', mode: 'add' }),
      })
      assert(tokenAdd.status === 200, 'per-user token must support exact-feed modes')

      const tokenOmni = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${tokenPayload.token}` },
        body: JSON.stringify({ handle: HANDLE, mode: 'omni' }),
      })
      assert(tokenOmni.status === 200, 'per-user token must currently support omni mode')
      const tokenOmniWithFeed = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${tokenPayload.token}` },
        body: JSON.stringify({ ...body, mode: 'omni' }),
      })
      assert(tokenOmniWithFeed.status === 400, 'omni with a feed must fail before mutation')

      const ruben = await resolveSubscriptionIdentity({ handle: 'rubencallahan.bsky.social' })
      const identityMismatch = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${tokenPayload.token}` },
        body: JSON.stringify({ did: ruben.did, feed: body.feed, mode: body.mode }),
      })
      assert(identityMismatch.status === 403, 'per-user token must remain identity-bound')

      const parts = tokenPayload.token.split('.')
      const signatureIndex = Math.floor(parts[2].length / 2)
      parts[2] = `${parts[2].slice(0, signatureIndex)}${parts[2][signatureIndex] === 'a' ? 'b' : 'a'}${parts[2].slice(signatureIndex + 1)}`
      const tampered = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${parts.join('.')}` },
        body: JSON.stringify(body),
      })
      assert(tampered.status === 401, 'tampered per-user token must be rejected')

      const expiredToken = jwt.sign({ sub: did, scope: 'subscription:write' }, process.env.STUDY_JWT_SECRET!, {
        algorithm: 'HS256',
        issuer: 'newsflows-bsky-feed-generator',
        audience: 'newsflows-subscription',
        expiresIn: -1,
      })
      const expired = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${expiredToken}` },
        body: JSON.stringify(body),
      })
      assert(expired.status === 401, 'expired per-user token must be rejected')

      const unauthorized = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      })
      assert(unauthorized.status === 401, 'unauthenticated subscription must be rejected')

      const unsignedLike = jwt.sign({
        iss: did,
        aud: 'did:web:localhost',
        lxm: 'app.bsky.feed.getFeedSkeleton',
        exp: Math.floor(Date.now() / 1000) + 60,
      }, 'not-the-account-signing-key', { algorithm: 'HS256' })
      let requesterRejected = false
      try {
        await validateAuth({
          headers: { authorization: `Bearer ${unsignedLike}` },
          originalUrl: '/xrpc/app.bsky.feed.getFeedSkeleton',
        } as any, 'did:web:localhost', {
          resolveAtprotoKey: async () => 'did:key:z6MkhInvalidTestKey',
        } as any)
      } catch {
        requesterRejected = true
      }
      assert(requesterRejected, 'decode-only or forged requester JWT must be rejected')

      await sql`
        CREATE TABLE IF NOT EXISTS feedgen_ops.study_registry (
          study_id varchar NOT NULL,
          did varchar NOT NULL,
          active_from timestamptz NOT NULL,
          active_until timestamptz,
          source varchar,
          status varchar NOT NULL
        )
      `.execute(db)
      await db.deleteFrom('feedgen_ops.study_registry').where('did', 'in', listDids).execute()
      await db.deleteFrom('feedgen_ops.subscriber_feed_assignment').where('did', 'in', listDids).execute()
      await db.deleteFrom('subscriber').where('did', 'in', listDids).execute()
      await db.insertInto('subscriber').values([
        { did: listDids[0], handle: 'list-omni.test', access_scope: 'omni' },
        { did: listDids[1], handle: 'list-assigned.test', access_scope: 'assigned' },
        { did: listDids[2], handle: 'list-none.test', access_scope: 'none' },
      ]).execute()
      await db.insertInto('feedgen_ops.subscriber_feed_assignment').values([
        { did: listDids[1], feed_id: 'test-feed-be-1', active_from: new Date('2026-06-30T00:00:00Z'), active_until: new Date('2026-07-01T00:00:00Z'), source: 'subscription-test', status: 'replaced' },
        { did: listDids[1], feed_id: 'test-feed-be-1', active_from: new Date('2026-07-01T00:00:00Z'), active_until: null, source: 'subscription-test', status: 'active' },
        { did: listDids[1], feed_id: 'test-feed-be-2', active_from: new Date('2026-07-02T00:00:00Z'), active_until: null, source: 'subscription-test', status: 'active' },
      ]).execute()

      const unauthorizedList = await fetch(`${base}/api/admin/subscribers`)
      assert(unauthorizedList.status === 401, 'bulk subscriber owner readback must require authentication')
      const firstPage = await fetch(`${base}/api/admin/subscribers?limit=1`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      const firstPagePayload = await firstPage.json() as any
      assert(firstPage.status === 200, 'read key must authorize bulk subscriber owner readback')
      assert(
        firstPagePayload.subscribers.length === 1 && Number.isInteger(firstPagePayload.next_cursor),
        'bulk readback must paginate with an integer offset cursor',
      )
      const secondPage = await fetch(`${base}/api/admin/subscribers?limit=1&cursor=${firstPagePayload.next_cursor}`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      const secondPagePayload = await secondPage.json() as any
      assert(secondPage.status === 200, 'subscriber cursor must retrieve the next page')
      assert(secondPagePayload.subscribers[0].did > firstPagePayload.subscribers[0].did, 'subscriber pages must use stable DID ordering')
      const invalidCursor = await fetch(`${base}/api/admin/subscribers?cursor=-1`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      assert(invalidCursor.status === 400, 'subscriber cursor must be a non-negative integer offset')
      const nonIntegerCursor = await fetch(`${base}/api/admin/subscribers?cursor=next`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      assert(nonIntegerCursor.status === 400, 'subscriber cursor must reject non-integer values')

      const unauthorizedHistory = await fetch(`${base}/api/admin/subscribers/history?did=${encodeURIComponent(listDids[1])}`)
      assert(unauthorizedHistory.status === 401, 'subscriber history must require the read key')
      const firstHistory = await fetch(`${base}/api/admin/subscribers/history?did=${encodeURIComponent(listDids[1])}&limit=1`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      const firstHistoryPayload = await firstHistory.json() as any
      assert(firstHistory.status === 200, 'read key must authorize DID-keyed subscriber history')
      assert(firstHistoryPayload.did === listDids[1], 'history must echo the requested canonical DID')
      assert(firstHistoryPayload.assignments.length === 1 && firstHistoryPayload.assignments[0].feed === 'test-be-2', 'history must order rows by immutable assignment ID descending')
      assert(Number.isInteger(firstHistoryPayload.next_cursor) && Number.isInteger(firstHistoryPayload.through_assignment_id), 'history must return integer offset and snapshot boundaries')
      assert(firstHistoryPayload.raw_values_in_output === false, 'history must declare raw-free output')
      const missingSnapshot = await fetch(`${base}/api/admin/subscribers/history?did=${encodeURIComponent(listDids[1])}&limit=1&cursor=1`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      assert(missingSnapshot.status === 400, 'history later pages must require the first-page snapshot boundary')
      await db.insertInto('feedgen_ops.subscriber_feed_assignment').values({
        did: listDids[1], feed_id: 'test-feed-nl-1', active_from: new Date(), active_until: null,
        source: 'subscription-test', status: 'active',
      }).execute()
      const secondHistory = await fetch(`${base}/api/admin/subscribers/history?did=${encodeURIComponent(listDids[1])}&limit=1&cursor=${firstHistoryPayload.next_cursor}&through_assignment_id=${firstHistoryPayload.through_assignment_id}`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      const secondHistoryPayload = await secondHistory.json() as any
      assert(secondHistory.status === 200 && secondHistoryPayload.assignments[0].feed === 'test-be-1', 'history cursor must exclude later assignments and page within the owner snapshot')
      await db.updateTable('feedgen_ops.subscriber_feed_assignment')
        .set({ active_until: new Date(), status: 'replaced' })
        .where('did', '=', listDids[1])
        .where('feed_id', '=', 'test-feed-nl-1')
        .execute()
      const fullHistory = await fetch(`${base}/api/admin/subscribers/history?did=${encodeURIComponent(listDids[1])}&limit=200&through_assignment_id=${firstHistoryPayload.through_assignment_id}`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      }).then((response) => response.json() as Promise<any>)
      assert(fullHistory.assignments.some((row: any) => row.status === 'replaced' && row.active_until), 'history must retain closed temporal assignment rows')
      const invalidHistory = await fetch(`${base}/api/admin/subscribers/history?did=not-a-did`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      assert(invalidHistory.status === 400, 'history must reject non-canonical DID input')

      const assignedList = await fetch(`${base}/api/admin/subscribers?scope=assigned&limit=100`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      const assignedPayload = await assignedList.json() as any
      const assignedRow = assignedPayload.subscribers.find((row: any) => row.did === listDids[1])
      assert(assignedList.status === 200 && assignedRow, 'scope filter must return assigned subscribers')
      assert(
        assignedRow.assignments.map((row: any) => row.feed).join(',') === 'test-be-1,test-be-2',
        'bulk readback must return sorted active assignments',
      )
      assert(assignedRow.assignments[0].active_from, 'bulk readback must expose truthful assignment timestamps')
      assert(assignedRow.scope_since === null, 'bulk readback must not fabricate a scope timestamp')

      const defaultFeedList = await fetch(`${base}/api/admin/subscribers?feed=test-be-1&limit=100`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      const defaultFeedPayload = await defaultFeedList.json() as any
      const defaultFeedDids = defaultFeedPayload.subscribers.map((row: any) => row.did)
      assert(defaultFeedDids.includes(listDids[0]) && defaultFeedDids.includes(listDids[1]), 'feed filter must include omni and exact access')
      assert(!defaultFeedDids.includes(listDids[2]), 'feed filter must exclude none scope')

      await db.updateTable('feedgen_ops.feed_catalog')
        .set({ access_policy_id: 'study-only' })
        .where('feed_id', '=', 'test-feed-be-2')
        .execute()
      const beforeLifecycle = await fetch(`${base}/api/admin/subscribers?feed=test-be-2&limit=100`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      }).then((response) => response.json() as Promise<any>)
      assert(!beforeLifecycle.subscribers.some((row: any) => row.did === listDids[1]), 'study-only feed filter must require active lifecycle membership')
      await db.insertInto('feedgen_ops.study_registry').values({
        study_id: 'test-study-be', did: listDids[1], active_from: new Date('2026-07-01T00:00:00Z'), active_until: null, source: 'subscription-test', status: 'active',
      }).execute()
      const afterLifecycle = await fetch(`${base}/api/admin/subscribers?feed=test-be-2&limit=100`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      }).then((response) => response.json() as Promise<any>)
      assert(afterLifecycle.subscribers.some((row: any) => row.did === listDids[1]), 'study-only feed filter must include active assigned members')
      assert(!afterLifecycle.subscribers.some((row: any) => row.did === listDids[0]), 'study-only feed filter must reject omni scope without active lifecycle')
      await db.insertInto('feedgen_ops.study_registry').values({
        study_id: 'test-study-be', did: listDids[0], active_from: new Date('2026-07-01T00:00:00Z'), active_until: null, source: 'subscription-test', status: 'active',
      }).execute()
      const omniWithLifecycle = await fetch(`${base}/api/admin/subscribers?feed=test-be-2&limit=100`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      }).then((response) => response.json() as Promise<any>)
      assert(omniWithLifecycle.subscribers.some((row: any) => row.did === listDids[0]), 'study-only feed filter must include omni scope with active lifecycle')

      const unknownFeedList = await fetch(`${base}/api/admin/subscribers?feed=not-a-feed`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      assert(unknownFeedList.status === 404, 'unknown feed filters must fail closed')
      await db.updateTable('feedgen_ops.feed_catalog').set({ enabled: false }).where('feed_id', '=', 'test-feed-be-1').execute()
      const disabledFeedList = await fetch(`${base}/api/admin/subscribers?feed=test-be-1`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      assert(disabledFeedList.status === 409, 'disabled feed filters must fail closed')
      await db.updateTable('feedgen_ops.feed_catalog').set({ enabled: true }).where('feed_id', '=', 'test-feed-be-1').execute()

      const readCatalog = await fetch(`${base}/api/admin/feed_catalog?subscribable=true`, {
        headers: { 'api-key': 'subscription-read-test-key' },
      })
      assert(readCatalog.status === 200, 'read key must authorize feed catalog GET')
      const writeCatalogWithReadKey = await fetch(`${base}/api/admin/feed_catalog`, {
        method: 'POST', headers: { 'content-type': 'application/json', 'api-key': 'subscription-read-test-key' }, body: '{}',
      })
      assert(writeCatalogWithReadKey.status === 401, 'read key must not authorize feed catalog mutation')
      const planWithReadKey = await fetch(`${base}/api/admin/subscribers/plan`, {
        method: 'POST', headers: { 'content-type': 'application/json', 'api-key': 'subscription-read-test-key' }, body: JSON.stringify(body),
      })
      assert(planWithReadKey.status === 401, 'read key must not authorize subscriber planning')
      const subscribeWithReadKey = await fetch(`${base}/api/subscribe`, {
        method: 'POST', headers: { 'content-type': 'application/json', 'api-key': 'subscription-read-test-key' }, body: JSON.stringify(body),
      })
      assert(subscribeWithReadKey.status === 401, 'read key must not authorize subscription mutation')

      const omniResponse = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify({ handle: HANDLE, mode: 'omni' }),
      })
      assert(omniResponse.status === 200, 'administrator must be able to restore omni')
      const adminExact = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify(body),
      })
      assert(adminExact.status === 200, 'administrator must support exact-feed modes')
      const inspect = await fetch(`${base}/api/admin/subscribers/inspect?did=${encodeURIComponent(did)}`, {
        headers: { 'api-key': 'subscription-admin-test-key' },
      })
      assert(inspect.status === 200, 'admin subscriber inspect must remain available')
      const plan = await fetch(`${base}/api/admin/subscribers/plan`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify({ handle: HANDLE, mode: 'omni' }),
      })
      assert(plan.status === 200, 'admin subscriber plan must remain available')
      const beforeRetiredApply = await inspectSubscription(db, identity)
      const unauthorizedRetiredApply = await fetch(`${base}/api/admin/subscribers/apply`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ ...body, mode: 'omni' }),
      })
      const unauthorizedRetiredApplyPayload = await unauthorizedRetiredApply.json() as any
      assert(
        unauthorizedRetiredApply.status === 410 && unauthorizedRetiredApplyPayload.error === 'retired_endpoint',
        'retired admin subscriber apply must return the same stable 410 without a key',
      )
      const retiredApply = await fetch(`${base}/api/admin/subscribers/apply`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify({ ...body, mode: 'omni' }),
      })
      const retiredApplyPayload = await retiredApply.json() as any
      assert(
        retiredApply.status === 410 && retiredApplyPayload.error === 'retired_endpoint',
        'admin subscriber apply must be retired with a stable 410 response',
      )
      const afterRetiredApply = await inspectSubscription(db, identity)
      assert(
        JSON.stringify(afterRetiredApply) === JSON.stringify(beforeRetiredApply),
        'retired admin subscriber apply must not mutate subscription state',
      )
      await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify({ handle: HANDLE, mode: 'omni' }),
      })
      assert((await fetch(`${base}/api/subscribe?handle=${encodeURIComponent(HANDLE)}`)).status === 410, 'legacy GET must be retired')
      await new Promise((resolve) => setTimeout(resolve, 1000))
    } finally {
      await app.stop()
    }

    console.log('PASS: exact-feed migration, modes, isolation, concurrency, and endpoint security')
  } finally {
    await db.deleteFrom('feedgen_ops.study_registry').where('did', 'in', listDids).execute().catch(() => undefined)
    await db.deleteFrom('feedgen_ops.subscriber_feed_assignment').where('did', 'in', listDids).execute().catch(() => undefined)
    await db.deleteFrom('subscriber').where('did', 'in', listDids).execute().catch(() => undefined)
    await db
      .deleteFrom('feedgen_ops.subscriber_feed_assignment')
      .where('did', '=', did)
      .execute()
      .catch(() => undefined)
    await db.deleteFrom('subscriber').where('did', '=', did).execute().catch(() => undefined)
    await db.destroy()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
