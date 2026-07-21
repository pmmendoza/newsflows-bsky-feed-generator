import { Transaction } from 'kysely'
import { AppContext } from '../config'
import { Database } from '../db'
import { DatabaseSchema, FeedCatalog } from '../db/schema'
import { getProfile } from './queries'
import { triggerFollowsUpdateForSubscriber } from './scheduled-updater'
import { isSubscribableFeed } from './subscribable-feed'

export type SubscriptionMode = 'replace' | 'add' | 'remove' | 'omni'
export type AccessScope = 'omni' | 'assigned' | 'none'

export type SubscriptionInput = {
  identifier?: string
  did?: string
  handle?: string
  feed?: string
  rkey?: string
  mode?: string
  source?: string
}

export type ResolvedIdentity = {
  input: string
  handle: string
  did: string
  avatar?: string
}

export type AssignmentView = {
  feed_id: string
  feed: string
  study_id: string | null
}

export type SubscriptionResult = {
  ok: true
  message: string
  handle: string
  did: string
  avatar?: string
  feed: string | null
  mode: SubscriptionMode
  access_scope: AccessScope
  changed: boolean
  assignments: AssignmentView[]
  current_access_scope?: AccessScope
  current_assignments?: AssignmentView[]
  apply_performed: boolean
}

type Db = Database | Transaction<DatabaseSchema>

export class SubscriptionError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message)
  }
}

function values(input: SubscriptionInput, keys: Array<keyof SubscriptionInput>): string[] {
  return keys
    .map((key) => input[key])
    .filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
    .map((value) => value.trim())
}

export function parseSubscriptionMode(value: unknown): SubscriptionMode {
  if (value === 'replace' || value === 'add' || value === 'remove' || value === 'omni') {
    return value
  }
  throw new SubscriptionError(400, 'invalid_mode', 'mode must be replace, add, remove, or omni')
}

export async function resolveSubscriptionIdentity(input: SubscriptionInput): Promise<ResolvedIdentity> {
  const identifiers = values(input, ['identifier', 'did', 'handle'])
  if (identifiers.length !== 1) {
    throw new SubscriptionError(400, 'invalid_identity', 'provide exactly one of identifier, did, or handle')
  }
  const actor = identifiers[0]
  try {
    const profile = await getProfile(actor)
    if (!profile?.did || !profile.handle) throw new Error('profile missing DID or handle')
    return {
      input: actor,
      handle: profile.handle,
      did: profile.did,
      avatar: profile.avatar,
    }
  } catch {
    throw new SubscriptionError(404, 'identity_not_found', `could not resolve Bluesky identity: ${actor}`)
  }
}

// RT-8: read-only mutation paths (inspect/preview/plan/readback) must not
// each spend an AppView lookup — a 100-row batch must trigger <=100
// resolutions total, not ~300 (preview + apply + readback per account).
// Known subscribers (the common batch case) resolve from the stored DB row;
// only a genuinely new identity (never subscribed, e.g. an "Add subscribers"
// preview) falls through to the single AppView lookup. The apply path always
// calls resolveSubscriptionIdentity directly (exactly one lookup, at apply
// time) so handle-change detection compares against a value fetched in the
// same call, never a stale DB-only guess.
export async function resolveSubscriptionIdentityDbFirst(db: Db, input: SubscriptionInput): Promise<ResolvedIdentity> {
  const identifiers = values(input, ['identifier', 'did', 'handle'])
  if (identifiers.length !== 1) {
    throw new SubscriptionError(400, 'invalid_identity', 'provide exactly one of identifier, did, or handle')
  }
  const actor = identifiers[0]
  const column = actor.startsWith('did:') ? 'did' : 'handle'
  const row = await db
    .selectFrom('subscriber')
    .select(['did', 'handle'])
    .where(column, '=', actor)
    .executeTakeFirst()
  if (row) {
    return { input: actor, handle: row.handle, did: row.did }
  }
  return resolveSubscriptionIdentity(input)
}

// RT-1: researcher accounts are locked to omni — no mutation may change their
// scope/assignments, in either mutation path or the preview/plan paths.
function assertNotResearcher(kind: string | null | undefined, did: string): void {
  if (kind === 'researcher') {
    throw new SubscriptionError(409, 'researcher_locked', `subscriber is a researcher account locked to omni: ${did}`)
  }
}

async function assertSubscriberNotResearcher(db: Db, did: string): Promise<void> {
  const row = await db.selectFrom('subscriber').select('kind').where('did', '=', did).executeTakeFirst()
  assertNotResearcher(row?.kind, did)
}

// RT-8: called once per apply, after the row is locked, comparing the
// AppView-resolved handle (from the single apply-time resolution) against
// the handle already stored on the locked row. A rename updates the row and
// appends an explicit old->new transition to the append-only history table.
async function recordHandleChangeIfNeeded(
  trx: Transaction<DatabaseSchema>,
  did: string,
  storedHandle: string | null | undefined,
  resolvedHandle: string,
  source: string,
): Promise<void> {
  if (!storedHandle || storedHandle === resolvedHandle) return
  await trx.updateTable('subscriber').set({ handle: resolvedHandle }).where('did', '=', did).execute()
  await trx
    .insertInto('subscriber_handle_history')
    .values({ did, old_handle: storedHandle, new_handle: resolvedHandle, source })
    .execute()
}

export async function resolveSubscriptionFeed(db: Db, input: SubscriptionInput): Promise<FeedCatalog> {
  const feeds = values(input, ['feed', 'rkey'])
  if (feeds.length !== 1) {
    throw new SubscriptionError(400, 'invalid_feed', 'provide exactly one of feed or rkey')
  }
  const requested = feeds[0]
  const urlMatch = requested.match(/\/feed\/([^/?#]+)/)
  let rkey = requested
  if (urlMatch) {
    try {
      rkey = decodeURIComponent(urlMatch[1])
    } catch {
      throw new SubscriptionError(400, 'invalid_feed', `invalid feed URL: ${requested}`)
    }
  }
  const feed = await db
    .selectFrom('feedgen_ops.feed_catalog')
    .selectAll()
    .where('rkey', '=', rkey)
    .executeTakeFirst()
  if (!feed) throw new SubscriptionError(404, 'feed_not_found', `unknown feed: ${requested}`)
  if (!isSubscribableFeed(feed)) {
    throw new SubscriptionError(409, 'feed_disabled', `feed is disabled: ${requested}`)
  }
  return feed
}

async function readAssignments(db: Db, did: string): Promise<AssignmentView[]> {
  const rows = await db
    .selectFrom('feedgen_ops.subscriber_feed_assignment as assignment')
    .innerJoin('feedgen_ops.feed_catalog as feed', 'feed.feed_id', 'assignment.feed_id')
    .select([
      'assignment.feed_id as feed_id',
      'feed.rkey as feed',
      'feed.study_id as study_id',
    ])
    .where('assignment.did', '=', did)
    .where('assignment.active_until', 'is', null)
    .orderBy('feed.rkey', 'asc')
    .execute()
  return rows.map((row) => ({
    feed_id: row.feed_id,
    feed: row.feed,
    study_id: row.study_id ?? null,
  }))
}

async function readScope(db: Db, did: string): Promise<AccessScope | null> {
  const row = await db
    .selectFrom('subscriber')
    .select('access_scope')
    .where('did', '=', did)
    .executeTakeFirst()
  return row ? row.access_scope ?? 'omni' : null
}

export async function inspectSubscription(
  db: Db,
  identity: ResolvedIdentity,
): Promise<{ access_scope: AccessScope; assignments: AssignmentView[]; subscribed: boolean }> {
  const [scope, assignments] = await Promise.all([
    readScope(db, identity.did),
    readAssignments(db, identity.did),
  ])
  return {
    access_scope: scope ?? 'none',
    assignments,
    subscribed: scope !== null,
  }
}

function sameStudy(assignment: AssignmentView, feed: FeedCatalog): boolean {
  return feed.study_id === null || feed.study_id === undefined
    ? assignment.feed_id === feed.feed_id
    : assignment.study_id === feed.study_id
}

function project(
  currentScope: AccessScope,
  current: AssignmentView[],
  feed: FeedCatalog | null,
  mode: SubscriptionMode,
): { scope: AccessScope; assignments: AssignmentView[] } {
  if (mode === 'omni') return { scope: 'omni', assignments: [] }
  if (!feed) throw new SubscriptionError(400, 'invalid_feed', `${mode} requires a feed`)
  const target: AssignmentView = {
    feed_id: feed.feed_id,
    feed: feed.rkey,
    study_id: feed.study_id ?? null,
  }
  if (mode === 'add') {
    if (currentScope === 'omni') return { scope: 'assigned', assignments: [target] }
    const assignments = current.some((row) => row.feed_id === feed.feed_id)
      ? current
      : [...current, target].sort((a, b) => a.feed.localeCompare(b.feed))
    return { scope: 'assigned', assignments }
  }
  if (mode === 'remove') {
    if (currentScope === 'omni') {
      throw new SubscriptionError(409, 'mode_conflict', 'remove requires exact-feed scope; use replace first')
    }
    const assignments = current.filter((row) => row.feed_id !== feed.feed_id)
    return { scope: assignments.length ? 'assigned' : 'none', assignments }
  }
  const assignments = [
    ...current.filter((row) => !sameStudy(row, feed)),
    target,
  ].sort((a, b) => a.feed.localeCompare(b.feed))
  return { scope: 'assigned', assignments }
}

function changed(
  currentScope: AccessScope,
  current: AssignmentView[],
  nextScope: AccessScope,
  next: AssignmentView[],
): boolean {
  if (currentScope !== nextScope || current.length !== next.length) return true
  return current.some((row, index) => row.feed_id !== next[index]?.feed_id)
}

async function applyProjectedState(
  trx: Transaction<DatabaseSchema>,
  identity: ResolvedIdentity,
  mode: SubscriptionMode,
  source: string,
  before: AssignmentView[],
  next: { scope: AccessScope; assignments: AssignmentView[] },
  now: Date,
): Promise<void> {
  const nextIds = new Set(next.assignments.map((row) => row.feed_id))
  const closeIds = before.filter((row) => !nextIds.has(row.feed_id)).map((row) => row.feed_id)
  if (closeIds.length) {
    await trx
      .updateTable('feedgen_ops.subscriber_feed_assignment')
      .set({
        active_until: now,
        source,
        status: mode === 'omni' ? 'omni' : mode === 'remove' ? 'removed' : 'replaced',
      })
      .where('did', '=', identity.did)
      .where('feed_id', 'in', closeIds)
      .where('active_until', 'is', null)
      .execute()
  }

  const beforeIds = new Set(before.map((row) => row.feed_id))
  const open = next.assignments.filter((row) => !beforeIds.has(row.feed_id))
  for (const assignment of open) {
    await trx
      .insertInto('feedgen_ops.subscriber_feed_assignment')
      .values({
        feed_id: assignment.feed_id,
        did: identity.did,
        active_from: now,
        active_until: null,
        source,
        status: 'active',
      })
      .execute()
  }

  // Handle rename is written separately by recordHandleChangeIfNeeded (once
  // per apply, alongside its history row); this UPDATE stamps scope_changed_at
  // only when a real scope/assignment change is being applied (INFRA-WEB-024).
  const update = await trx
    .updateTable('subscriber')
    .set({ access_scope: next.scope, scope_changed_at: now })
    .where('did', '=', identity.did)
    .executeTakeFirst()
  if (mode === 'remove' && Number(update.numUpdatedRows) === 0) {
    throw new SubscriptionError(404, 'subscriber_not_found', `identity is not subscribed: ${identity.did}`)
  }
}

export async function executeSubscription(
  ctx: AppContext,
  input: SubscriptionInput,
  apply: boolean,
  updateFollows = true,
  boundDid?: string,
): Promise<SubscriptionResult> {
  const mode = parseSubscriptionMode(input.mode)
  // RT-8: apply resolves via AppView (exactly once, at apply time); preview
  // resolves DB-first so a batch's preview/plan calls spend no AppView budget.
  const identity = apply
    ? await resolveSubscriptionIdentity(input)
    : await resolveSubscriptionIdentityDbFirst(ctx.db, input)
  if (boundDid && boundDid !== identity.did) {
    throw new SubscriptionError(403, 'identity_mismatch', 'subscription identity does not match token subject')
  }
  const suppliedFeeds = values(input, ['feed', 'rkey'])
  if (mode === 'omni' && suppliedFeeds.length > 0) {
    throw new SubscriptionError(400, 'invalid_feed', 'omni does not accept feed or rkey')
  }
  const feed = mode === 'omni' ? null : await resolveSubscriptionFeed(ctx.db, input)
  const source = input.source?.trim() || 'feedgen-subscription-api'

  // RT-1: reject researcher-account mutation in the preview/plan path too,
  // not only inside the apply transaction.
  await assertSubscriberNotResearcher(ctx.db, identity.did)

  const beforeState = await inspectSubscription(ctx.db, identity)
  const next = project(beforeState.access_scope, beforeState.assignments, feed, mode)
  const willChange = changed(
    beforeState.access_scope,
    beforeState.assignments,
    next.scope,
    next.assignments,
  )
  let didChange = willChange

  if (apply) {
    didChange = await ctx.db.transaction().execute(async (trx) => {
      const now = new Date()
      const inserted = mode === 'remove'
        ? undefined
        : await trx
          .insertInto('subscriber')
          .values({ handle: identity.handle, did: identity.did, access_scope: 'omni', first_subscribed_at: now })
          .onConflict((oc) => oc.column('did').doNothing())
          .returning('did')
          .executeTakeFirst()
      const locked = await trx
        .selectFrom('subscriber')
        .select(['did', 'kind', 'handle'])
        .where('did', '=', identity.did)
        .forUpdate()
        .executeTakeFirst()
      if (mode === 'remove' && !locked) return false
      // RT-1: authoritative, race-safe check on the row just locked FOR UPDATE.
      assertNotResearcher(locked?.kind, identity.did)
      // RT-8: identity.handle was already resolved once (via AppView, above);
      // no second lookup here — just compare to the locked row's stored value.
      await recordHandleChangeIfNeeded(trx, identity.did, locked?.handle, identity.handle, source)
      const current = await inspectSubscription(trx, identity)
      const effectiveScope = inserted ? 'none' : current.access_scope
      const projected = project(effectiveScope, current.assignments, feed, mode)
      const transactionChanged = changed(
        effectiveScope,
        current.assignments,
        projected.scope,
        projected.assignments,
      )
      if (!transactionChanged) return false
      await applyProjectedState(
        trx,
        identity,
        mode,
        source,
        current.assignments,
        projected,
        now,
      )
      return true
    })
    if (didChange && updateFollows && mode !== 'remove') {
      triggerFollowsUpdateForSubscriber(ctx.db, identity.did)
    }
  }

  const finalState = apply ? await inspectSubscription(ctx.db, identity) : {
    access_scope: next.scope,
    assignments: next.assignments,
  }
  return {
    ok: true,
    message: apply ? 'Subscription updated' : 'Subscription plan',
    handle: identity.handle,
    did: identity.did,
    avatar: identity.avatar,
    feed: feed?.rkey ?? null,
    mode,
    access_scope: finalState.access_scope,
    changed: didChange,
    assignments: finalState.assignments,
    current_access_scope: apply ? undefined : beforeState.access_scope,
    current_assignments: apply ? undefined : beforeState.assignments,
    apply_performed: apply,
  }
}

// ---------------------------------------------------------------------------
// Atomic desired-state subscription (FEEDGEN-SUBSCRIBE-ATOMIC).
//
// One operation sets the subscriber's ABSOLUTE desired state — no add/remove/
// replace/omni verbs. Naturally idempotent (re-applying the same state is a
// no-op) with an optional expected-state CAS guard for concurrency. Reuses the
// proven applyProjectedState writer; the legacy mode API remains as a shim.
// ---------------------------------------------------------------------------

export type DesiredState = { scope: AccessScope; feeds: string[] }

export type SetSubscriptionInput = SubscriptionInput & {
  state?: { scope?: unknown; feeds?: unknown }
  expected?: { scope?: unknown; feeds?: unknown; subscribed?: unknown } | null
}

// `subscribed` is an optional membership predicate for compare-and-set: it
// distinguishes "subscriber row absent" (false) from "present" (true),
// independent of access scope. Batch enrollment sets false (enroll only if not
// already a member); batch mutation sets true (reject if removed since preview).
type ExpectedState = { scope: AccessScope; feeds: string[]; subscribed?: boolean }

function parseFeedList(value: unknown, ctxLabel: string): string[] {
  if (value === undefined) return []
  if (!Array.isArray(value)) {
    throw new SubscriptionError(400, 'invalid_state', `${ctxLabel}.feeds must be an array of feed rkeys`)
  }
  const out: string[] = []
  for (const f of value) {
    if (typeof f !== 'string' || f.trim().length === 0) {
      throw new SubscriptionError(400, 'invalid_state', `${ctxLabel}.feeds entries must be non-empty strings`)
    }
    out.push(f.trim())
  }
  return out
}

export function parseDesiredState(input: SetSubscriptionInput): DesiredState {
  // A state body must not also carry legacy verbs — that ambiguity is a defect.
  if (input.mode !== undefined || input.feed !== undefined || input.rkey !== undefined) {
    throw new SubscriptionError(400, 'invalid_state', 'state cannot be combined with legacy mode/feed/rkey')
  }
  const raw = input.state
  if (!raw || typeof raw !== 'object') {
    throw new SubscriptionError(400, 'invalid_state', 'state is required: {scope, feeds?}')
  }
  const scope = raw.scope
  if (scope !== 'omni' && scope !== 'none' && scope !== 'assigned') {
    throw new SubscriptionError(400, 'invalid_state', 'state.scope must be omni, none, or assigned')
  }
  const feeds = parseFeedList(raw.feeds, 'state')
  if (scope === 'assigned') {
    const deduped = Array.from(new Set(feeds)).sort()
    if (!deduped.length) {
      throw new SubscriptionError(400, 'invalid_state', 'state.scope=assigned requires a non-empty feeds list')
    }
    return { scope, feeds: deduped }
  }
  if (feeds.length > 0) {
    throw new SubscriptionError(400, 'invalid_state', `state.scope=${scope} does not accept feeds`)
  }
  return { scope, feeds: [] }
}

function parseExpectedState(raw: { scope?: unknown; feeds?: unknown; subscribed?: unknown }): ExpectedState {
  if (!raw || typeof raw !== 'object') {
    throw new SubscriptionError(400, 'invalid_state', 'expected must be {scope, feeds?}')
  }
  const scope = raw.scope
  if (scope !== 'omni' && scope !== 'none' && scope !== 'assigned') {
    throw new SubscriptionError(400, 'invalid_state', 'expected.scope must be omni, none, or assigned')
  }
  const feeds = Array.from(new Set(parseFeedList(raw.feeds, 'expected'))).sort()
  if (scope !== 'assigned' && feeds.length > 0) {
    throw new SubscriptionError(400, 'invalid_state', `expected.scope=${scope} does not accept feeds`)
  }
  let subscribed: boolean | undefined
  if (raw.subscribed !== undefined) {
    if (typeof raw.subscribed !== 'boolean') {
      throw new SubscriptionError(400, 'invalid_state', 'expected.subscribed must be a boolean')
    }
    subscribed = raw.subscribed
  }
  return { scope, feeds, subscribed }
}

function currentFeeds(assignments: AssignmentView[]): string[] {
  return assignments.map((a) => a.feed).sort()
}

// Structural state comparison (no delimiter serialization — feeds compared
// element-wise on sorted rkeys).
function statesEqual(scope: AccessScope, feeds: string[], expected: ExpectedState): boolean {
  if (scope !== expected.scope) return false
  if (feeds.length !== expected.feeds.length) return false
  return feeds.every((f, i) => f === expected.feeds[i])
}

async function resolveFeedRow(
  db: Db,
  rkey: string,
  lock: boolean,
): Promise<{ feed_id: string; feed: string; study_id: string | null }> {
  let q = db.selectFrom('feedgen_ops.feed_catalog').selectAll().where('rkey', '=', rkey)
  if (lock) q = q.forShare()
  const feed = await q.executeTakeFirst()
  if (!feed) throw new SubscriptionError(404, 'feed_not_found', `unknown feed: ${rkey}`)
  if (!isSubscribableFeed(feed)) {
    throw new SubscriptionError(409, 'feed_disabled', `feed is disabled: ${rkey}`)
  }
  return { feed_id: feed.feed_id, feed: feed.rkey, study_id: feed.study_id ?? null }
}

// Resolve requested rkeys to assignments. When `lock` (inside the write
// transaction) each catalog row is locked FOR SHARE and rechecked, closing the
// eligibility check-to-write race; rkeys are visited in deterministic order.
async function resolveDesiredAssignments(
  db: Db,
  rkeys: string[],
  lock: boolean,
): Promise<AssignmentView[]> {
  const seen = new Set<string>()
  const out: AssignmentView[] = []
  for (const rkey of [...rkeys].sort()) {
    const row = await resolveFeedRow(db, rkey, lock)
    if (seen.has(row.feed_id)) continue
    seen.add(row.feed_id)
    out.push(row)
  }
  return out.sort((a, b) => a.feed.localeCompare(b.feed))
}

// Close-row status for the temporal audit trail, per the desired scope
// (constraint allows removed/replaced/omni on closed rows).
function closeMode(scope: AccessScope): SubscriptionMode {
  return scope === 'omni' ? 'omni' : scope === 'none' ? 'remove' : 'replace'
}

export async function setSubscription(
  ctx: AppContext,
  input: SetSubscriptionInput,
  apply: boolean,
  updateFollows = true,
  boundDid?: string,
): Promise<SubscriptionResult> {
  // RT-8: apply resolves via AppView (exactly once, at apply time); preview
  // resolves DB-first so a batch's preview/plan calls spend no AppView budget.
  const identity = apply
    ? await resolveSubscriptionIdentity(input)
    : await resolveSubscriptionIdentityDbFirst(ctx.db, input)
  if (boundDid && boundDid !== identity.did) {
    throw new SubscriptionError(403, 'identity_mismatch', 'subscription identity does not match token subject')
  }
  const desired = parseDesiredState(input)
  const source = input.source?.trim() || 'feedgen-subscription-api'
  const expected =
    input.expected !== undefined && input.expected !== null ? parseExpectedState(input.expected) : null

  // RT-1: reject researcher-account mutation in the preview/plan path too,
  // not only inside the apply transaction.
  await assertSubscriberNotResearcher(ctx.db, identity.did)

  const beforeState = await inspectSubscription(ctx.db, identity)

  if (!apply) {
    // Preview validates expected against the observed before-state too.
    if (
      expected &&
      (!statesEqual(beforeState.access_scope, currentFeeds(beforeState.assignments), expected) ||
        (expected.subscribed !== undefined && expected.subscribed !== beforeState.subscribed))
    ) {
      throw new SubscriptionError(409, 'stale_state', 'current state does not match expected; re-read and retry')
    }
    const nextAssignments =
      desired.scope === 'assigned' ? await resolveDesiredAssignments(ctx.db, desired.feeds, false) : []
    const didChange = changed(
      beforeState.access_scope,
      beforeState.assignments,
      desired.scope,
      nextAssignments,
    )
    return {
      ok: true,
      message: 'Subscription plan',
      handle: identity.handle,
      did: identity.did,
      avatar: identity.avatar,
      feed: null,
      mode: 'replace',
      access_scope: desired.scope,
      changed: didChange,
      assignments: nextAssignments,
      current_access_scope: beforeState.access_scope,
      current_assignments: beforeState.assignments,
      apply_performed: false,
    }
  }

  // Apply: one locked transaction. It returns the state it committed (its
  // linearization point) so the response never reflects a concurrent writer.
  const result = await ctx.db.transaction().execute(async (trx) => {
    const now = new Date()
    const inserted = await trx
      .insertInto('subscriber')
      .values({ handle: identity.handle, did: identity.did, access_scope: 'omni', first_subscribed_at: now })
      .onConflict((oc) => oc.column('did').doNothing())
      .returning('did')
      .executeTakeFirst()
    const locked = await trx
      .selectFrom('subscriber')
      .select(['did', 'kind', 'handle'])
      .where('did', '=', identity.did)
      .forUpdate()
      .executeTakeFirst()
    // RT-1: authoritative, race-safe check on the row just locked FOR UPDATE.
    assertNotResearcher(locked?.kind, identity.did)
    // RT-8: identity.handle was already resolved once (via AppView, above);
    // no second lookup here — just compare to the locked row's stored value.
    await recordHandleChangeIfNeeded(trx, identity.did, locked?.handle, identity.handle, source)
    // Two views of "before":
    //  - PHYSICAL: what the row actually holds now, incl. the bootstrap omni
    //    row a fresh insert just created. Drives whether we must write, so a
    //    first-time state:none normalizes the bootstrap omni row to none.
    //  - LOGICAL: a freshly-inserted subscriber is none/[] (never subscribed),
    //    used for CAS and for whether the participant's effective access
    //    changed (follows trigger + the reported `changed`).
    const observed = await inspectSubscription(trx, identity)
    const physScope: AccessScope = observed.access_scope
    const physAssignments = observed.assignments
    const logicalScope: AccessScope = inserted ? 'none' : observed.access_scope
    const logicalAssignments = inserted ? [] : observed.assignments
    // Logical membership: a row we just bootstrapped was NOT a subscriber before.
    const logicalSubscribed = !inserted
    if (
      expected &&
      (!statesEqual(logicalScope, currentFeeds(logicalAssignments), expected) ||
        (expected.subscribed !== undefined && expected.subscribed !== logicalSubscribed))
    ) {
      throw new SubscriptionError(409, 'stale_state', 'current state does not match expected; re-read and retry')
    }
    const nextAssignments =
      desired.scope === 'assigned' ? await resolveDesiredAssignments(trx, desired.feeds, true) : []
    const next = { scope: desired.scope, assignments: nextAssignments }
    const physChanged = changed(physScope, physAssignments, next.scope, next.assignments)
    const logicalChanged = changed(logicalScope, logicalAssignments, next.scope, next.assignments)
    if (physChanged) {
      await applyProjectedState(trx, identity, closeMode(next.scope), source, physAssignments, next, now)
    } else if (inserted && logicalChanged) {
      // A brand-new subscriber whose first desired state is 'omni' matches
      // the bootstrap INSERT's default (physScope was already 'omni') —
      // nothing to physically write, but it is still the participant's
      // first real scope decision (INFRA-WEB-024: stamp scope_changed_at).
      await trx.updateTable('subscriber').set({ scope_changed_at: now }).where('did', '=', identity.did).execute()
    }
    return { changed: physChanged || logicalChanged, followsRelevant: logicalChanged, scope: next.scope, assignments: next.assignments }
  })

  if (result.followsRelevant && updateFollows && result.scope !== 'none') {
    triggerFollowsUpdateForSubscriber(ctx.db, identity.did)
  }
  return {
    ok: true,
    message: 'Subscription updated',
    handle: identity.handle,
    did: identity.did,
    avatar: identity.avatar,
    feed: null,
    mode: 'replace',
    access_scope: result.scope,
    changed: result.changed,
    assignments: result.assignments,
    current_access_scope: undefined,
    current_assignments: undefined,
    apply_performed: true,
  }
}
