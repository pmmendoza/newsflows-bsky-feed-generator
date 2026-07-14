import { Server } from '../lexicon'
import { AppContext } from '../config'
import {
  executeSubscription,
  inspectSubscription,
  resolveSubscriptionIdentity,
  SubscriptionError,
  SubscriptionInput,
} from '../util/exact-subscription'
import { ApiKeyAuthConfig, isApiKeyAuthorized, logUnauthorized } from '../util/api-auth'

const adminAuth: ApiKeyAuthConfig = { primaryEnv: ['FEEDGEN_ADMIN_API_KEY'] }

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

  server.xrpc.router.post('/api/admin/subscribers/apply', async (req, res) => {
    const endpoint = '/api/admin/subscribers/apply'
    if (!isApiKeyAuthorized(req, adminAuth)) return unauthorized(res, endpoint)
    try {
      return res.json(await executeSubscription(ctx, normalize(req.body as LegacyAdminInput), true))
    } catch (error) {
      return endpointError(res, error)
    }
  })
}
