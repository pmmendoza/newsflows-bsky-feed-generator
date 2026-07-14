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

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_SUBSCRIPTION_TEST_CONFIRM !== 'disposable') {
    throw new Error('integration gate requires FEEDGEN_TEST_DSN and FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable')
  }

  const db = createDb(dsn)
  const ctx = { db } as AppContext
  const identity = await resolveSubscriptionIdentity({ handle: HANDLE })
  const did = identity.did

  try {
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

    const migrator = new Migrator({ db, provider: migrationProvider })
    const down = await migrator.migrateDown()
    if (down.error) throw down.error
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
    await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'omni' }, true, false)
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
    const omni = await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'omni' }, true, false)
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

    await executeSubscription(ctx, { did, feed: 'test-be-1', mode: 'omni' }, true, false)
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
        body: JSON.stringify({ ...body, mode: 'omni' }),
      })
      assert(tokenOmni.status === 200, 'per-user token must currently support omni mode')

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

      const omniResponse = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify({ ...body, mode: 'omni' }),
      })
      assert(omniResponse.status === 200, 'administrator must be able to restore omni')
      const adminExact = await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify(body),
      })
      assert(adminExact.status === 200, 'administrator must support exact-feed modes')
      await fetch(`${base}/api/subscribe`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'api-key': 'subscription-admin-test-key' },
        body: JSON.stringify({ ...body, mode: 'omni' }),
      })
      assert((await fetch(`${base}/api/subscribe?handle=${encodeURIComponent(HANDLE)}`)).status === 410, 'legacy GET must be retired')
      await new Promise((resolve) => setTimeout(resolve, 1000))
    } finally {
      await app.stop()
    }

    console.log('PASS: exact-feed migration, modes, isolation, concurrency, and endpoint security')
  } finally {
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
