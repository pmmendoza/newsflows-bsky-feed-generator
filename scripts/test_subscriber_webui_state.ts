// Stage 0 feedgen backend tests for the WebUI subscriber-state foundation
// (INFRA-WEB-024 timestamps, INFRA-WEB-026 status filter, INFRA-WEB-029 list
// sort/search/feed-filter, INFRA-WEB-030 kind + researcher guard,
// INFRA-WEB-032 handle-history). Uses real Bluesky identities where a
// mutation must go through the apply-time AppView resolution (same
// constraint the existing exact-subscription integration tests operate
// under) and a disposable DB for everything else.
//
//   FEEDGEN_TEST_DSN=postgresql://... FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable \
//     ts-node scripts/test_subscriber_webui_state.ts
import assert from 'assert'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { Migrator, sql } from 'kysely'
import { AppContext } from '../src/config'
import { createDb, migrateToLatest } from '../src/db'
import { migrationProvider } from '../src/db/migrations'
import {
  executeSubscription,
  inspectSubscription,
  resolveSubscriptionIdentity,
  setSubscription,
  SubscriptionError,
} from '../src/util/exact-subscription'
import { importSubscribersFromCSV } from '../src/util/import-subscribers'

// Must match src/db/migrations.ts's 007_subscriber_state_and_kind seed lists.
const PUBLISHER_DIDS = [
  'did:plc:toz4no26o2x4vsbum7cp4bxp',
  'did:plc:kzmukwaf72iwepygposicgt3',
  'did:plc:cegiy4pfghh4rjs7ks7pbnkm',
  'did:plc:vzmnljt7otfbbgrmachtefxh',
  'did:plc:tlmi333azel2jcornp2qeolm',
]
const TESTING_DIDS = ['did:plc:weksrderzzdyxdh26pu5jyqo', 'did:plc:u7d6u2a5wu7dbjp6wruttlrv']
const RESEARCHER_DIDS = [
  'did:plc:3vomhawgkjhtvw4euuxbll3r',
  'did:plc:df5sxbescomzxz7fwovti4vd',
  'did:plc:upgwmkhteysqu2n7mar2w4rk',
]
const RUBEN_TESTING_HANDLE = 'rubencallahan.bsky.social' // TESTING_DIDS[1]
const JGRUBER_HANDLE = 'jbgruber.bsky.social' // plain participant, not in any seed list

function fail(message: string): never {
  throw new Error(`FAIL: ${message}`)
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_SUBSCRIPTION_TEST_CONFIRM !== 'disposable') {
    throw new Error('requires FEEDGEN_TEST_DSN and FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable')
  }
  const db = createDb(dsn)
  const ctx = { db } as AppContext
  await migrateToLatest(db)

  await sql`
    CREATE SCHEMA IF NOT EXISTS feedgen_ops;
    CREATE TABLE IF NOT EXISTS feedgen_ops.feed_catalog (
      feed_id varchar PRIMARY KEY, rkey varchar UNIQUE NOT NULL,
      display_name varchar NOT NULL, country varchar, publisher_did varchar,
      study_id varchar, algo_policy_id varchar NOT NULL, ranker_policy_id varchar,
      access_policy_id varchar NOT NULL, enabled boolean NOT NULL,
      created_at timestamptz DEFAULT now(), retired_at timestamptz
    )
  `.execute(db)
  for (const rkey of ['webui-a', 'webui-b']) {
    await db
      .insertInto('feedgen_ops.feed_catalog')
      .values({
        feed_id: rkey,
        rkey,
        display_name: rkey,
        algo_policy_id: 'chronological',
        access_policy_id: 'subscriber-default',
        enabled: true,
      })
      .onConflict((oc) => oc.column('feed_id').doNothing())
      .execute()
  }

  const ruben = await resolveSubscriptionIdentity({ handle: RUBEN_TESTING_HANDLE })
  const jgruber = await resolveSubscriptionIdentity({ handle: JGRUBER_HANDLE })
  const researcherDid = RESEARCHER_DIDS[0]
  const researcher = await resolveSubscriptionIdentity({ did: researcherDid })

  const FIXTURE_DIDS = [ruben.did, jgruber.did, researcher.did]
  const cleanupFixtures = async () => {
    await db.deleteFrom('subscriber_handle_history').where('did', 'in', FIXTURE_DIDS).execute()
    await db.deleteFrom('feedgen_ops.subscriber_feed_assignment').where('did', 'in', FIXTURE_DIDS).execute()
    await db.deleteFrom('subscriber').where('did', 'in', FIXTURE_DIDS).execute()
  }
  await cleanupFixtures()

  let app: any
  try {
    // -----------------------------------------------------------------
    // 1. Timestamps (INFRA-WEB-024)
    // -----------------------------------------------------------------

    // 1a. Absent subscriber: no row at all before a first subscribe.
    const absentRow = await db.selectFrom('subscriber').select(['did']).where('did', '=', ruben.did).executeTakeFirst()
    assert(!absentRow, '1a: fixture must start absent')

    // 1a continued: a genuine first subscribe stamps BOTH timestamps.
    const firstSub = await setSubscription(ctx, { did: ruben.did, state: { scope: 'omni' } }, true, false)
    assert(firstSub.changed, '1a: first-ever subscribe must be a change')
    const stampedRow = await db
      .selectFrom('subscriber')
      .select(['first_subscribed_at', 'scope_changed_at'])
      .where('did', '=', ruben.did)
      .executeTakeFirstOrThrow()
    assert(stampedRow.first_subscribed_at, '1a: first_subscribed_at must be stamped on genuine first subscribe')
    assert(stampedRow.scope_changed_at, '1a: scope_changed_at must be stamped on genuine first subscribe')
    const originalFirstSubscribedAt = stampedRow.first_subscribed_at

    // 1b. Legacy-null subscriber (simulates a pre-migration row with no
    // recorded dates) unsub -> resub: scope_changed_at advances, but
    // first_subscribed_at correctly stays NULL forever (genuinely unknown —
    // not a bug; see date-columns plan Non-goals).
    await db
      .updateTable('subscriber')
      .set({ first_subscribed_at: null, scope_changed_at: null })
      .where('did', '=', ruben.did)
      .execute()
    await setSubscription(ctx, { did: ruben.did, state: { scope: 'none' } }, true, false)
    await setSubscription(ctx, { did: ruben.did, state: { scope: 'omni' } }, true, false)
    const legacyRow = await db
      .selectFrom('subscriber')
      .select(['first_subscribed_at', 'scope_changed_at'])
      .where('did', '=', ruben.did)
      .executeTakeFirstOrThrow()
    assert(legacyRow.first_subscribed_at === null, '1b: legacy-null first_subscribed_at must stay NULL across resubscribe')
    assert(legacyRow.scope_changed_at, '1b: scope_changed_at must advance from NULL on a real scope change')

    // Restore a realistic first_subscribed_at for later list-endpoint tests.
    await db
      .updateTable('subscriber')
      .set({ first_subscribed_at: originalFirstSubscribedAt })
      .where('did', '=', ruben.did)
      .execute()

    // 1c. CSV import must not stamp a false "today" (RT-2).
    const csvDid = 'did:plc:webuicsvimporttest00001'
    await db.deleteFrom('subscriber').where('did', '=', csvDid).execute()
    const tmpCsv = path.join(os.tmpdir(), `webui-import-test-${Date.now()}.csv`)
    fs.writeFileSync(tmpCsv, `handle,did\ncsv-import-test.example,${csvDid}\n`)
    try {
      await importSubscribersFromCSV(db, { csvPath: tmpCsv })
      const csvRow = await db
        .selectFrom('subscriber')
        .select(['first_subscribed_at', 'scope_changed_at'])
        .where('did', '=', csvDid)
        .executeTakeFirstOrThrow() as any
      assert(csvRow.first_subscribed_at === null, '1c: CSV import must leave first_subscribed_at NULL')
      assert(csvRow.scope_changed_at === null, '1c: CSV import must leave scope_changed_at NULL')
    } finally {
      fs.unlinkSync(tmpCsv)
      await db.deleteFrom('subscriber').where('did', '=', csvDid).execute()
    }

    // -----------------------------------------------------------------
    // 2. Handle-change detection + history (INFRA-WEB-032, RT-8)
    // -----------------------------------------------------------------

    // Force a stale stored handle on a real, resolvable DID.
    await db.updateTable('subscriber').set({ handle: 'stale-handle.invalid' }).where('did', '=', jgruber.did).execute()
    await db.deleteFrom('subscriber').where('did', '=', jgruber.did).where('did', '!=', jgruber.did).execute() // no-op guard
    let jgruberRow = await db.selectFrom('subscriber').select('did').where('did', '=', jgruber.did).executeTakeFirst()
    if (!jgruberRow) {
      await db
        .insertInto('subscriber')
        .values({ handle: 'stale-handle.invalid', did: jgruber.did, access_scope: 'omni' })
        .execute()
    }

    // 2a. DB-only read paths (inspect via inspectSubscription, and preview via
    // setSubscription apply=false) must NOT re-resolve via AppView — they
    // must keep returning the stale stored handle unchanged.
    const dbOnlyIdentity = { did: jgruber.did, handle: 'irrelevant-for-db-only-lookup', input: jgruber.did }
    const inspected = await inspectSubscription(db, dbOnlyIdentity as any)
    assert(inspected.access_scope === 'omni', '2a: inspect must read the existing row')
    const preview = await setSubscription(ctx, { did: jgruber.did, state: { scope: 'none' } }, false, false)
    assert(preview.handle === 'stale-handle.invalid', '2a: preview must return the DB-first (stale) handle, not re-resolve via AppView')

    // 2b. Apply resolves via AppView exactly once; if the resolved handle
    // differs from stored, the row is corrected and a history row appended.
    const applyResult = await setSubscription(ctx, { did: jgruber.did, state: { scope: 'omni' } }, true, false)
    assert(applyResult.handle === jgruber.handle, '2b: apply must correct the handle to the AppView-resolved value')
    const storedAfterApply = await db.selectFrom('subscriber').select('handle').where('did', '=', jgruber.did).executeTakeFirstOrThrow()
    assert(storedAfterApply.handle === jgruber.handle, '2b: stored handle must be corrected on apply')
    const historyRows = await db
      .selectFrom('subscriber_handle_history')
      .selectAll()
      .where('did', '=', jgruber.did)
      .execute()
    assert(historyRows.length === 1, '2b: exactly one handle-history transition must be appended')
    assert(
      historyRows[0].old_handle === 'stale-handle.invalid' && historyRows[0].new_handle === jgruber.handle,
      '2b: history row must record old -> new handle',
    )

    // 2c. A second apply with no handle drift must not append another row.
    await setSubscription(ctx, { did: jgruber.did, state: { scope: 'none' } }, true, false)
    const historyAfterSecondApply = await db
      .selectFrom('subscriber_handle_history')
      .selectAll()
      .where('did', '=', jgruber.did)
      .execute()
    assert(historyAfterSecondApply.length === 1, '2c: no new history row when the handle has not changed')

    // -----------------------------------------------------------------
    // 3. Researcher guard (RT-1) — both mutation paths + preview + no-demote
    // -----------------------------------------------------------------
    await db.deleteFrom('subscriber').where('did', '=', researcher.did).execute()
    await db
      .insertInto('subscriber')
      .values({ handle: researcher.handle, did: researcher.did, access_scope: 'omni', kind: 'researcher' })
      .execute()

    const expectResearcherLocked = async (fn: () => Promise<unknown>, label: string) => {
      try {
        await fn()
        fail(`${label}: expected researcher_locked`)
      } catch (e) {
        assert(e instanceof SubscriptionError && e.code === 'researcher_locked', `${label}: wanted researcher_locked, got ${e}`)
      }
    }
    // setSubscription (atomic) preview + apply.
    await expectResearcherLocked(
      () => setSubscription(ctx, { did: researcher.did, state: { scope: 'none' } }, false, false),
      '3a: setSubscription preview',
    )
    await expectResearcherLocked(
      () => setSubscription(ctx, { did: researcher.did, state: { scope: 'none' } }, true, false),
      '3b: setSubscription apply',
    )
    // executeSubscription (legacy verbs) preview + apply.
    await expectResearcherLocked(
      () => executeSubscription(ctx, { did: researcher.did, mode: 'omni' }, false, false),
      '3c: executeSubscription preview',
    )
    await expectResearcherLocked(
      () => executeSubscription(ctx, { did: researcher.did, feed: 'webui-a', mode: 'replace' }, true, false),
      '3d: executeSubscription apply',
    )
    const researcherAfterGuard = await db
      .selectFrom('subscriber')
      .select('access_scope')
      .where('did', '=', researcher.did)
      .executeTakeFirstOrThrow()
    assert(researcherAfterGuard.access_scope === 'omni', '3e: researcher must remain omni after all rejected attempts')

    // -----------------------------------------------------------------
    // 4. HTTP endpoints: list (status/q/feed[]/sort/NULLS LAST), handle-history,
    //    kind setter (+ no-demote), unknown-feed fail-closed.
    // -----------------------------------------------------------------
    process.env.FEEDGEN_ADMIN_API_KEY = 'webui-admin-test-key'
    process.env.FEEDGEN_READ_API_KEY = 'webui-read-test-key'
    process.env.STUDY_TOKEN_API_KEY = 'webui-token-test-key'
    process.env.STUDY_JWT_SECRET = 'webui-test-secret-that-is-long-enough'
    const FeedGenerator = (await import('../src/server')).default
    app = FeedGenerator.create({
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
    const address = app.server?.address()
    assert(address && typeof address === 'object', 'test server must expose a port')
    const base = `http://127.0.0.1:${address.port}`
    const readHeaders = { 'api-key': 'webui-read-test-key' }
    const adminHeaders = { 'content-type': 'application/json', 'api-key': 'webui-admin-test-key' }

    // 4a. status filter.
    const activeList = await fetch(`${base}/api/admin/subscribers?status=active&limit=500`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(activeList.subscribers.every((s: any) => s.access_scope !== 'none'), '4a: status=active must exclude none scope')
    const jgruberInActive = activeList.subscribers.some((s: any) => s.did === jgruber.did)
    assert(!jgruberInActive, '4a: jgruber (currently none) must be excluded from status=active')
    const formerList = await fetch(`${base}/api/admin/subscribers?status=former&limit=500`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(formerList.subscribers.some((s: any) => s.did === jgruber.did), '4a: status=former must include jgruber (none scope)')
    const allList = await fetch(`${base}/api/admin/subscribers?limit=500`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(allList.subscribers.some((s: any) => s.did === jgruber.did), '4a: default status must remain all (back-compat)')
    assert(typeof allList.total_count === 'number', '4a: list must return a total_count')

    // 4b. kind + timestamps surfaced; NULLS LAST ordering on a nullable sort col.
    const rubenRow = allList.subscribers.find((s: any) => s.did === ruben.did)
    assert(rubenRow && rubenRow.first_subscribed_at, '4b: ruben must show a real first_subscribed_at')
    const researcherRow = allList.subscribers.find((s: any) => s.did === researcher.did)
    assert(researcherRow && researcherRow.kind === 'researcher', '4b: researcher kind must surface in the list')

    const sortedDesc = await fetch(
      `${base}/api/admin/subscribers?sort=first_subscribed_at&dir=desc&limit=500`,
      { headers: readHeaders },
    ).then((r) => r.json() as Promise<any>)
    const dates = sortedDesc.subscribers.map((s: any) => s.first_subscribed_at)
    const firstNullIndex = dates.findIndex((d: any) => d === null)
    if (firstNullIndex !== -1) {
      assert(
        dates.slice(firstNullIndex).every((d: any) => d === null),
        '4b: NULLS LAST must keep every null after the last non-null value on a DESC sort',
      )
    }

    // 4c. bounded regex q: length cap, valid regex match, and 2201B -> literal fallback.
    const tooLongQ = 'x'.repeat(129)
    const rejectedQ = await fetch(`${base}/api/admin/subscribers?q=${tooLongQ}`, { headers: readHeaders })
    assert(rejectedQ.status === 400, '4c: q over 128 chars must be rejected')
    const validRegexQ = await fetch(`${base}/api/admin/subscribers?q=${encodeURIComponent('^' + jgruber.handle.slice(0, 4))}`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(validRegexQ.subscribers.some((s: any) => s.did === jgruber.did), '4c: a valid regex must match by handle')
    const brokenRegexQ = await fetch(`${base}/api/admin/subscribers?q=${encodeURIComponent('foo(')}`, { headers: readHeaders })
    assert(brokenRegexQ.status === 200, '4c: an invalid regex must fall back to a literal match, not 500')
    // 4c-escape: the 2201B ILIKE fallback must escape %/_ so it is a TRUE literal
    // substring match, never a wildcard over-match (regression for `%${q}%`).
    // `a%b(` is an invalid regex (unbalanced paren) -> fallback, and contains `%`.
    await db.insertInto('subscriber').values([
      { did: 'did:plc:esc-wild', handle: 'aXb(wild.test', access_scope: 'omni' },
      { did: 'did:plc:esc-lit', handle: 'a%b(lit.test', access_scope: 'omni' },
    ]).execute()
    const escFallback = await fetch(`${base}/api/admin/subscribers?q=${encodeURIComponent('a%b(')}&limit=500`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(escFallback.subscribers.some((s: any) => s.did === 'did:plc:esc-lit'), '4c-escape: fallback must match the literal "a%b(" handle')
    assert(!escFallback.subscribers.some((s: any) => s.did === 'did:plc:esc-wild'), '4c-escape: fallback must NOT wildcard-match "aXb(" — the % must be escaped')

    // 4d. feed[] AND-predicate + omni-in-all + fail-closed on unknown rkey.
    await setSubscription(ctx, { did: ruben.did, state: { scope: 'assigned', feeds: ['webui-a', 'webui-b'] } }, true, false)
    const feedAndBoth = await fetch(`${base}/api/admin/subscribers?feed=webui-a&feed=webui-b&limit=500`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(feedAndBoth.subscribers.some((s: any) => s.did === ruben.did), '4d: AND filter must include a subscriber assigned to both feeds')
    const feedAndImpossible = await fetch(`${base}/api/admin/subscribers?feed=webui-a&feed=webui-b`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(
      feedAndImpossible.subscribers.every((s: any) => s.access_scope === 'omni' || s.did === ruben.did),
      '4d: AND filter must exclude subscribers assigned to only one of the two feeds',
    )
    await setSubscription(ctx, { did: jgruber.did, state: { scope: 'omni' } }, true, false)
    const feedAndOmni = await fetch(`${base}/api/admin/subscribers?feed=webui-a&feed=webui-b&limit=500`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(feedAndOmni.subscribers.some((s: any) => s.did === jgruber.did), '4d: omni must show under every feed[] filter (omni-in-all)')
    const unknownFeedAnd = await fetch(`${base}/api/admin/subscribers?feed=webui-a&feed=not-a-real-feed`, { headers: readHeaders })
    assert(unknownFeedAnd.status === 400, '4d: an unknown rkey in feed[] must fail closed, not silently drop')
    const legacyFeedCombinedWithQ = await fetch(`${base}/api/admin/subscribers?feed=webui-a&q=x`, { headers: readHeaders })
    assert(legacyFeedCombinedWithQ.status === 400, '4d: legacy scalar feed= must reject combination with q/sort/feed[]')
    // 4d-empty: an empty singular feed= means "no filter" (back-compat with
    // origin/main), not 400 — a WebUI "all feeds" control may submit feed="".
    const emptyFeed = await fetch(`${base}/api/admin/subscribers?feed=&limit=500`, { headers: readHeaders })
    assert(emptyFeed.status === 200, '4d-empty: empty singular feed= must return 200 (no filter), not 400')
    const emptyFeedBody = (await emptyFeed.json()) as any
    assert(emptyFeedBody.subscribers.some((s: any) => s.did === jgruber.did), '4d-empty: empty feed= must return the unfiltered list')

    // 4e. handle-history endpoint.
    const handleHistoryResp = await fetch(`${base}/api/admin/subscribers/handle-history?did=${encodeURIComponent(jgruber.did)}`, { headers: readHeaders })
      .then((r) => r.json() as Promise<any>)
    assert(handleHistoryResp.transitions.length === 1, '4e: handle-history must return the one recorded transition')
    assert(handleHistoryResp.transitions[0].old_handle === 'stale-handle.invalid', '4e: handle-history must report the recorded old handle')
    const unauthorizedHandleHistory = await fetch(`${base}/api/admin/subscribers/handle-history?did=${encodeURIComponent(jgruber.did)}`)
    assert(unauthorizedHandleHistory.status === 401, '4e: handle-history must require the read key')

    // 4f. kind setter + no-demotion guard.
    const setToTesting = await fetch(`${base}/api/admin/subscribers/kind`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({ did: jgruber.did, kind: 'testing' }),
    })
    assert(setToTesting.status === 200, '4f: kind setter must accept a valid kind change')
    const jgruberKindRow = await db.selectFrom('subscriber').select('kind').where('did', '=', jgruber.did).executeTakeFirstOrThrow()
    assert(jgruberKindRow.kind === 'testing', '4f: kind setter must persist the new kind')
    const demoteAttempt = await fetch(`${base}/api/admin/subscribers/kind`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({ did: researcher.did, kind: 'participant' }),
    })
    assert(demoteAttempt.status === 409, '4f: kind setter must reject demoting a researcher')
    const researcherKindRow = await db.selectFrom('subscriber').select('kind').where('did', '=', researcher.did).executeTakeFirstOrThrow()
    assert(researcherKindRow.kind === 'researcher', '4f: researcher kind must be unchanged after a blocked demotion')
    const promoteToResearcher = await fetch(`${base}/api/admin/subscribers/kind`, {
      method: 'POST',
      headers: adminHeaders,
      body: JSON.stringify({ did: jgruber.did, kind: 'researcher' }),
    })
    assert(promoteToResearcher.status === 200, '4f: promoting TO researcher must be allowed')
    // Clean up the promotion so the researcher-guard test below is unaffected.
    await db.updateTable('subscriber').set({ kind: 'participant' }).where('did', '=', jgruber.did).execute()

    console.log('test_subscriber_webui_state OK')
  } finally {
    if (app) await app.stop()
    await cleanupFixtures()
    await db.destroy()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
