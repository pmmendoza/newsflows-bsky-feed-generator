import { Server } from '../lexicon'
import { AppContext } from '../config'
import { FeedCatalog, StudyRegistry, Subscriber } from '../db/schema'
import { getProfile } from '../util/queries'
import {
  ApiKeyAuthConfig,
  isApiKeyAuthorized,
  logUnauthorized,
} from '../util/api-auth'

const adminWriteAuth: ApiKeyAuthConfig = {
  primaryEnv: ['FEEDGEN_ADMIN_API_KEY'],
}

type SubscriberAdminAction = 'add' | 'remove'

type SubscriberAdminBody = {
  identifier?: string
  did?: string
  handle?: string
  feed?: string
  rkey?: string
  study_id?: string
  action?: SubscriberAdminAction
  source?: string
}

type ResolvedIdentity = {
  input: string
  handle: string
  did: string
}

type FeedTarget = {
  feed: FeedCatalog
  study_id: string | null
}

type MembershipPlan = {
  ok: true
  mode: 'dry_run' | 'apply'
  action: SubscriberAdminAction
  identifier: string
  resolvedHandle: string
  resolvedDid: string
  feed: string | null
  studyId: string | null
  accessPolicyId: string | null
  ownerEndpoint: string
  currentSubscriber: Subscriber | null
  currentMemberships: StudyRegistry[]
  plannedAction: string
  applyPerformed: boolean
  warnings: string[]
}

function firstString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return undefined
}

function unauthorized(res: any, endpoint: string) {
  logUnauthorized(endpoint)
  return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
}

async function resolveIdentity(bodyOrQuery: SubscriberAdminBody): Promise<ResolvedIdentity> {
  const input = firstString(bodyOrQuery.identifier, bodyOrQuery.did, bodyOrQuery.handle)
  if (!input) {
    const err = new Error('identifier, did, or handle is required')
    ;(err as any).status = 400
    throw err
  }
  try {
    const profile = await getProfile(input)
    if (!profile?.handle || !profile?.did) {
      throw new Error(`Profile did not include handle and DID for: ${input}`)
    }
    return {
      input,
      handle: profile.handle,
      did: profile.did,
    }
  } catch {
    const err = new Error(`Could not resolve Bluesky identity: ${input}`)
    ;(err as any).status = 404
    throw err
  }
}

async function readSubscriber(ctx: AppContext, did: string): Promise<Subscriber | null> {
  const row = await ctx.db
    .selectFrom('subscriber')
    .selectAll()
    .where('did', '=', did)
    .executeTakeFirst()
  return row ?? null
}

async function readMemberships(ctx: AppContext, did: string): Promise<StudyRegistry[]> {
  return await ctx.db
    .selectFrom('feedgen_ops.study_registry')
    .selectAll()
    .where('did', '=', did)
    .orderBy('study_id', 'asc')
    .execute()
}

async function resolveFeedTarget(
  ctx: AppContext,
  body: SubscriberAdminBody,
): Promise<FeedTarget> {
  const rkey = firstString(body.feed, body.rkey)
  if (!rkey) {
    const err = new Error('feed or rkey is required')
    ;(err as any).status = 400
    throw err
  }
  const feed = await ctx.db
    .selectFrom('feedgen_ops.feed_catalog')
    .selectAll()
    .where('rkey', '=', rkey)
    .executeTakeFirst()
  if (!feed) {
    const err = new Error(`No feed_catalog row found for rkey: ${rkey}`)
    ;(err as any).status = 404
    throw err
  }
  const studyId = firstString(body.study_id) ?? feed.study_id ?? null
  if (feed.access_policy_id === 'study-only' && !studyId) {
    const err = new Error(`Feed ${rkey} uses study-only access but has no study_id`)
    ;(err as any).status = 400
    throw err
  }
  return { feed, study_id: studyId }
}

function activeMembership(memberships: StudyRegistry[], studyId: string | null): StudyRegistry | null {
  if (!studyId) return null
  return memberships.find((row) => row.study_id === studyId && !row.status.includes(':stop_tracking')) ?? null
}

async function buildMembershipPlan(
  ctx: AppContext,
  body: SubscriberAdminBody,
  mode: 'dry_run' | 'apply',
): Promise<MembershipPlan> {
  const action = body.action
  if (action !== 'add' && action !== 'remove') {
    const err = new Error("action must be 'add' or 'remove'")
    ;(err as any).status = 400
    throw err
  }
  const identity = await resolveIdentity(body)
  const feedTarget = await resolveFeedTarget(ctx, body)
  const currentSubscriber = await readSubscriber(ctx, identity.did)
  const currentMemberships = await readMemberships(ctx, identity.did)
  const membership = activeMembership(currentMemberships, feedTarget.study_id)
  const warnings: string[] = []
  if (feedTarget.feed.access_policy_id === 'subscriber-default') {
    warnings.push('feed uses subscriber-default access; study membership is advisory for this feed')
  }
  if (!feedTarget.study_id) {
    warnings.push('feed has no study_id; only subscriber allowlist add/remove will be planned')
  }

  const plannedAction =
    action === 'add'
      ? membership
        ? 'subscriber_and_membership_already_present'
        : 'add_subscriber_and_study_membership'
      : membership
        ? 'mark_study_membership_stop_tracking'
        : 'study_membership_not_present'

  return {
    ok: true,
    mode,
    action,
    identifier: identity.input,
    resolvedHandle: identity.handle,
    resolvedDid: identity.did,
    feed: feedTarget.feed.rkey,
    studyId: feedTarget.study_id,
    accessPolicyId: feedTarget.feed.access_policy_id,
    ownerEndpoint: mode === 'apply' ? '/api/admin/subscribers/apply' : '/api/admin/subscribers/plan',
    currentSubscriber,
    currentMemberships,
    plannedAction,
    applyPerformed: false,
    warnings,
  }
}

async function applyMembership(ctx: AppContext, plan: MembershipPlan, source: string | undefined): Promise<MembershipPlan> {
  const now = new Date()
  await ctx.db.transaction().execute(async (trx) => {
    if (plan.action === 'add') {
      await trx
        .insertInto('subscriber')
        .values({
          handle: plan.resolvedHandle,
          did: plan.resolvedDid,
        })
        .onConflict((oc) => oc.column('did').doUpdateSet({ handle: plan.resolvedHandle }))
        .execute()
      if (plan.studyId) {
        await trx
          .deleteFrom('feedgen_ops.study_registry')
          .where('study_id', '=', plan.studyId)
          .where('did', '=', plan.resolvedDid)
          .execute()
        await trx
          .insertInto('feedgen_ops.study_registry')
          .values({
            study_id: plan.studyId,
            did: plan.resolvedDid,
            active_from: now,
            active_until: null,
            source: source || 'bskyops-manual-feed-access',
            status: 'active',
          } as any)
          .execute()
      }
      return
    }

    if (plan.studyId) {
      await trx
        .updateTable('feedgen_ops.study_registry')
        .set({
          active_until: now,
          status: 'operator:stop_tracking',
          source: source || 'bskyops-manual-feed-access',
        } as any)
        .where('study_id', '=', plan.studyId)
        .where('did', '=', plan.resolvedDid)
        .execute()
    }
  })

  return {
    ...plan,
    mode: 'apply',
    ownerEndpoint: '/api/admin/subscribers/apply',
    applyPerformed: true,
  }
}

function endpointError(res: any, err: unknown) {
  const status = typeof (err as any)?.status === 'number' ? (err as any).status : 500
  const message = err instanceof Error ? err.message : 'Unexpected subscriber admin error'
  if (status >= 500) {
    console.error(`[${new Date().toISOString()}] - subscriber-admin: ${message}`)
  }
  return res.status(status).json({
    ok: false,
    error: status >= 500 ? 'InternalServerError' : 'BadRequest',
    message,
  })
}

export default function registerSubscriberAdminEndpoints(server: Server, ctx: AppContext) {
  server.xrpc.router.get('/api/admin/subscribers/inspect', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      return unauthorized(res, '/api/admin/subscribers/inspect')
    }
    try {
      const identity = await resolveIdentity(req.query as SubscriberAdminBody)
      const currentSubscriber = await readSubscriber(ctx, identity.did)
      const currentMemberships = await readMemberships(ctx, identity.did)
      return res.json({
        ok: true,
        mode: 'read',
        identifier: identity.input,
        resolvedHandle: identity.handle,
        resolvedDid: identity.did,
        ownerEndpoint: '/api/admin/subscribers/inspect',
        currentSubscriber,
        currentMemberships,
        applyPerformed: false,
      })
    } catch (err) {
      return endpointError(res, err)
    }
  })

  server.xrpc.router.post('/api/admin/subscribers/plan', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      return unauthorized(res, '/api/admin/subscribers/plan')
    }
    try {
      const plan = await buildMembershipPlan(ctx, req.body as SubscriberAdminBody, 'dry_run')
      return res.json(plan)
    } catch (err) {
      return endpointError(res, err)
    }
  })

  server.xrpc.router.post('/api/admin/subscribers/apply', async (req, res) => {
    if (!isApiKeyAuthorized(req, adminWriteAuth)) {
      return unauthorized(res, '/api/admin/subscribers/apply')
    }
    try {
      const plan = await buildMembershipPlan(ctx, req.body as SubscriberAdminBody, 'apply')
      const applied = await applyMembership(ctx, plan, firstString((req.body as SubscriberAdminBody)?.source))
      return res.json(applied)
    } catch (err) {
      return endpointError(res, err)
    }
  })
}
