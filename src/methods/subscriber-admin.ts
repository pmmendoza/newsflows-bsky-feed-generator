import { Server } from '../lexicon'
import { AppContext } from '../config'
import { accessVerdictForState, readFeedAccessStates } from '../util/access-policy'
import {
  executeSubscription,
  inspectSubscription,
  resolveSubscriptionIdentity,
  resolveSubscriptionFeed,
  SubscriptionError,
  SubscriptionInput,
} from '../util/exact-subscription'
import { ApiKeyAuthConfig, isApiKeyAuthorized, logUnauthorized } from '../util/api-auth'

const adminAuth: ApiKeyAuthConfig = { primaryEnv: ['FEEDGEN_ADMIN_API_KEY'] }
const readAuth: ApiKeyAuthConfig = { primaryEnv: ['FEEDGEN_READ_API_KEY', 'FEEDGEN_ADMIN_API_KEY'] }

type LegacyAdminInput = SubscriptionInput & { action?: 'add' | 'remove' }

function normalize(body: LegacyAdminInput): SubscriptionInput {
  return {
    ...body,
    mode: body.mode ?? (body.action === 'add' ? 'add' : body.action === 'remove' ? 'remove' : undefined),
  }
}

function unauthorized(res: any, endpoint: string) {
  logUnauthorized(endpoint)
  return res.status(401).json({ ok: false, error: 'unauthorized' })
}

function endpointError(res: any, error: unknown) {
  const err = error instanceof SubscriptionError
    ? error
    : new SubscriptionError(500, 'internal_error', 'an unexpected error occurred')
  if (err.status >= 500) console.error(`[${new Date().toISOString()}] - subscriber-admin: ${err.message}`)
  return res.status(err.status).json({ ok: false, error: err.code, message: err.message })
}

export default function registerSubscriberAdminEndpoints(server: Server, ctx: AppContext) {
  server.xrpc.router.get('/api/admin/subscribers', async (req, res) => {
    const endpoint = '/api/admin/subscribers'
    if (!isApiKeyAuthorized(req, readAuth)) return unauthorized(res, endpoint)
    try {
      const scope = req.query?.scope
      if (scope !== undefined && scope !== 'omni' && scope !== 'assigned' && scope !== 'none') {
        throw new SubscriptionError(400, 'invalid_scope', 'scope must be omni, assigned, or none')
      }
      const parseInteger = (value: unknown, fallback: number, name: string) => {
        if (value === undefined) return fallback
        if (typeof value !== 'string' || !/^\d+$/.test(value)) {
          throw new SubscriptionError(400, `invalid_${name}`, `${name} must be a non-negative integer`)
        }
        const parsed = Number(value)
        if (!Number.isSafeInteger(parsed)) {
          throw new SubscriptionError(400, `invalid_${name}`, `${name} must be a non-negative integer`)
        }
        return parsed
      }
      const cursor = parseInteger(req.query?.cursor, 0, 'cursor')
      const limit = parseInteger(req.query?.limit, 100, 'limit')
      if (limit < 1 || limit > 500) {
        throw new SubscriptionError(400, 'invalid_limit', 'limit must be between 1 and 500')
      }

      if (req.query?.feed !== undefined && typeof req.query.feed !== 'string') {
        throw new SubscriptionError(400, 'invalid_feed', 'feed must be one rkey or feed URL')
      }
      const requestedFeed = req.query?.feed
      const feed = requestedFeed
        ? await resolveSubscriptionFeed(ctx.db, { feed: requestedFeed })
        : null

      let query = ctx.db
        .selectFrom('subscriber as subscriber')
        .select(['subscriber.handle', 'subscriber.did', 'subscriber.access_scope'])
        .orderBy('subscriber.did', 'asc')
      if (scope) query = query.where('subscriber.access_scope', '=', scope)
      let candidateRows
      if (feed) {
        const rows = await query.execute()
        const policyRow = {
          feed_id: feed.feed_id,
          access_policy_id: feed.access_policy_id,
          study_id: feed.study_id ?? null,
          enabled: feed.enabled,
          retired_at: feed.retired_at ?? null,
        }
        const states = await readFeedAccessStates(ctx.db, policyRow, rows.map((row) => row.did))
        // ponytail: bounded operator dataset scan; compile the shared predicate to SQL if scale demands it.
        candidateRows = rows.filter((row) => accessVerdictForState(
          policyRow,
          states.get(row.did) ?? { scope: null, hasActiveAssignment: false, activeStudy: false },
        ).allowed).slice(cursor, cursor + limit + 1)
      } else {
        candidateRows = await query.offset(cursor).limit(limit + 1).execute()
      }
      const page = candidateRows.slice(0, limit)
      const dids = page.map((row) => row.did)
      const assignments = dids.length === 0 ? [] : await ctx.db
        .selectFrom('feedgen_ops.subscriber_feed_assignment as assignment')
        .innerJoin('feedgen_ops.feed_catalog as feed', 'feed.feed_id', 'assignment.feed_id')
        .select([
          'assignment.did',
          'assignment.feed_id',
          'assignment.active_from',
          'feed.rkey as feed',
          'feed.study_id',
        ])
        .where('assignment.did', 'in', dids)
        .where('assignment.active_until', 'is', null)
        .orderBy('assignment.did', 'asc')
        .orderBy('feed.rkey', 'asc')
        .execute()

      return res.json({
        schema_version: 1,
        subscribers: page.map((row) => ({
          handle: row.handle,
          did: row.did,
          access_scope: row.access_scope ?? 'omni',
          // ponytail: bounded 500-row page scan; group by DID if feed counts grow materially.
          assignments: assignments
            .filter((assignment) => assignment.did === row.did)
            .map((assignment) => ({
              feed_id: assignment.feed_id,
              feed: assignment.feed,
              study_id: assignment.study_id ?? null,
              active_from: assignment.active_from,
            })),
          scope_since: null,
        })),
        returned_count: page.length,
        next_cursor: candidateRows.length > limit ? cursor + limit : null,
        filters: { scope: scope ?? null, feed: feed?.rkey ?? null },
        owner_endpoint: endpoint,
        apply_performed: false,
        raw_values_in_output: false,
      })
    } catch (error) {
      return endpointError(res, error)
    }
  })

  server.xrpc.router.get('/api/admin/subscribers/inspect', async (req, res) => {
    const endpoint = '/api/admin/subscribers/inspect'
    if (!isApiKeyAuthorized(req, adminAuth)) return unauthorized(res, endpoint)
    try {
      const identity = await resolveSubscriptionIdentity(req.query as SubscriptionInput)
      const state = await inspectSubscription(ctx.db, identity)
      return res.json({
        ok: true,
        mode: 'read',
        handle: identity.handle,
        did: identity.did,
        access_scope: state.access_scope,
        assignments: state.assignments,
        subscribed: state.subscribed,
        owner_endpoint: endpoint,
        apply_performed: false,
      })
    } catch (error) {
      return endpointError(res, error)
    }
  })

  server.xrpc.router.post('/api/admin/subscribers/plan', async (req, res) => {
    const endpoint = '/api/admin/subscribers/plan'
    if (!isApiKeyAuthorized(req, adminAuth)) return unauthorized(res, endpoint)
    try {
      return res.json(await executeSubscription(ctx, normalize(req.body as LegacyAdminInput), false))
    } catch (error) {
      return endpointError(res, error)
    }
  })

  server.xrpc.router.post('/api/admin/subscribers/apply', (_req, res) => {
    return res.status(410).json({
      ok: false,
      error: 'retired_endpoint',
      message: 'Use authenticated POST /api/subscribe for subscription mutations',
    })
  })
}
