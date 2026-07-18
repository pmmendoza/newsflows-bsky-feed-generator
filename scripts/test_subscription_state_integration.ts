// DB-backed integration test for the atomic setSubscription op (disposable DB).
// Covers the red-team fixes: first-time enrollment (change + CAS), lifecycle
// omni<->assigned<->none, structural CAS 409, idempotency, preview-no-write.
//
//   FEEDGEN_TEST_DSN=postgresql://... FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable \
//     ts-node scripts/test_subscription_state_integration.ts
import assert from 'assert'
import { sql } from 'kysely'
import { AppContext } from '../src/config'
import { createDb, migrateToLatest } from '../src/db'
import {
  inspectSubscription,
  resolveSubscriptionIdentity,
  setSubscription,
  SubscriptionError,
} from '../src/util/exact-subscription'

const HANDLE = process.env.FEEDGEN_SUBSCRIPTION_TEST_HANDLE || 'luciemartel.bsky.social'
const FEED_A = 'itfeed-a'
const FEED_B = 'itfeed-b'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_SUBSCRIPTION_TEST_CONFIRM !== 'disposable') {
    throw new Error('integration gate requires FEEDGEN_TEST_DSN and FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable')
  }
  const db = createDb(dsn)
  const ctx = { db } as AppContext
  await migrateToLatest(db)
  // feed_catalog is feedgen runtime state (not in the app migrations); create
  // it the same way the existing subscription integration harness does.
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
  for (const rkey of [FEED_A, FEED_B]) {
    await db
      .insertInto('feedgen_ops.feed_catalog')
      .values({
        feed_id: rkey,
        rkey,
        display_name: `IT ${rkey}`,
        algo_policy_id: 'chronological',
        access_policy_id: 'subscriber-default',
        enabled: true,
      })
      .onConflict((oc) => oc.column('feed_id').doNothing())
      .execute()
  }

  const identity = await resolveSubscriptionIdentity({ handle: HANDLE })
  const did = identity.did
  const ident = { did }

  const clean = async () => {
    await sql`DELETE FROM feedgen_ops.subscriber_feed_assignment WHERE did = ${did}`.execute(db)
    await sql`DELETE FROM subscriber WHERE did = ${did}`.execute(db)
  }
  const state = async () => {
    const s = await inspectSubscription(db, await resolveSubscriptionIdentity({ did }))
    return { scope: s.access_scope, feeds: s.assignments.map((a) => a.feed).sort() }
  }
  const expectErr = async (fn: () => Promise<unknown>, code: string) => {
    try {
      await fn()
      throw new Error(`expected ${code}`)
    } catch (e) {
      assert(e instanceof SubscriptionError && e.code === code, `wanted ${code}, got ${e}`)
    }
  }

  // 1. First-time omni enrollment: changed=true (HIGH 2).
  await clean()
  let r = await setSubscription(ctx, { ...ident, state: { scope: 'omni' } }, true)
  assert(r.changed === true, '1: first omni must be a change')
  assert((await state()).scope === 'omni', '1: scope omni')

  // 1b. First-time state:none must PERSIST none, not leave the bootstrap omni
  //     row (deploy red-team BLOCKER 1).
  await clean()
  const rnone = await setSubscription(ctx, { ...ident, state: { scope: 'none' } }, true)
  assert((await state()).scope === 'none', '1b: first-time none must persist none, not omni')
  assert(rnone.access_scope === 'none', '1b: response scope none matches persisted')

  // 2. First-time enrollment with expected none/[] must succeed, not 409 (HIGH 1).
  await clean()
  r = await setSubscription(
    ctx,
    { ...ident, state: { scope: 'assigned', feeds: [FEED_A] }, expected: { scope: 'none', feeds: [] } },
    true,
  )
  assert(r.changed === true, '2: first assigned with expected none must apply')
  assert.deepStrictEqual((await state()), { scope: 'assigned', feeds: [FEED_A] }, '2: assigned [a]')

  // 3. assigned -> exact set {a,b} -> none.
  r = await setSubscription(ctx, { ...ident, state: { scope: 'assigned', feeds: [FEED_B, FEED_A] } }, true)
  assert.deepStrictEqual((await state()), { scope: 'assigned', feeds: [FEED_A, FEED_B] }, '3: assigned [a,b]')
  await setSubscription(ctx, { ...ident, state: { scope: 'none' } }, true)
  assert((await state()).scope === 'none', '3: none')

  // 4. Structural CAS 409: current none, expected assigned[a].
  await expectErr(
    () =>
      setSubscription(
        ctx,
        { ...ident, state: { scope: 'omni' }, expected: { scope: 'assigned', feeds: [FEED_A] } },
        true,
      ),
    'stale_state',
  )

  // 5. Idempotency: same absolute state twice -> second is a no-op.
  await clean()
  await setSubscription(ctx, { ...ident, state: { scope: 'assigned', feeds: [FEED_A] } }, true)
  r = await setSubscription(ctx, { ...ident, state: { scope: 'assigned', feeds: [FEED_A] } }, true)
  assert(r.changed === false, '5: re-applying identical state is a no-op')

  // 6. Preview does not write.
  await clean()
  await setSubscription(ctx, { ...ident, state: { scope: 'omni' } }, true)
  const preview = await setSubscription(ctx, { ...ident, state: { scope: 'none' } }, false)
  assert(preview.apply_performed === false && preview.changed === true, '6: preview shows change')
  assert((await state()).scope === 'omni', '6: preview did not write')

  // 7. Unknown/disabled feed rejected.
  await expectErr(
    () => setSubscription(ctx, { ...ident, state: { scope: 'assigned', feeds: ['no-such-feed'] } }, true),
    'feed_not_found',
  )

  // 8. expected.subscribed membership CAS (batch-ops safety).
  // 8a. Not a member + expected.subscribed:true -> 409 (batch-mutate on removed).
  await clean()
  await expectErr(
    () => setSubscription(ctx, { ...ident, state: { scope: 'omni' }, expected: { scope: 'none', feeds: [], subscribed: true } }, true),
    'stale_state',
  )
  // 8b. Not a member + expected.subscribed:false -> enrolls (batch-add new).
  await clean()
  r = await setSubscription(ctx, { ...ident, state: { scope: 'assigned', feeds: [FEED_A] }, expected: { scope: 'none', feeds: [], subscribed: false } }, true)
  assert(r.changed === true && (await state()).scope === 'assigned', '8b: enroll-if-absent succeeds')
  // 8c. Now a member + expected.subscribed:false -> 409 (batch-add must not touch existing).
  await expectErr(
    () => setSubscription(ctx, { ...ident, state: { scope: 'omni' }, expected: { scope: 'assigned', feeds: [FEED_A], subscribed: false } }, true),
    'stale_state',
  )
  // 8d. Now a member + expected.subscribed:true + correct state -> mutates.
  r = await setSubscription(ctx, { ...ident, state: { scope: 'omni' }, expected: { scope: 'assigned', feeds: [FEED_A], subscribed: true } }, true)
  assert((await state()).scope === 'omni', '8d: mutate-existing succeeds')

  await clean()
  await new Promise((r) => setTimeout(r, 400)) // let async follows updates settle
  console.log('test_subscription_state_integration OK')
  await db.destroy()
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
