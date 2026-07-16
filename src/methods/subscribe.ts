import crypto from 'crypto'
import express from 'express'
import jwt from 'jsonwebtoken'
import { Server } from '../lexicon'
import { AppContext } from '../config'
import {
  executeSubscription,
  parseSubscriptionMode,
  resolveSubscriptionIdentity,
  SubscriptionError,
  SubscriptionInput,
} from '../util/exact-subscription'
import { ApiKeyAuthConfig, isApiKeyAuthorized } from '../util/api-auth'

const adminAuth: ApiKeyAuthConfig = { primaryEnv: ['FEEDGEN_ADMIN_API_KEY'] }
const studyTokenAuth: ApiKeyAuthConfig = { primaryEnv: ['STUDY_TOKEN_API_KEY'] }
const ISSUER = 'newsflows-bsky-feed-generator'
const AUDIENCE = 'newsflows-subscription'

type SubscriptionTokenPayload = {
  sub: string
  scope: 'subscription:write'
}

function ttlSeconds(): number {
  const value = Number(process.env.SUBSCRIPTION_TOKEN_TTL_SECONDS || 600)
  return Number.isInteger(value) && value > 0 && value <= 3600 ? value : 600
}

function secret(): string {
  const value = process.env.STUDY_JWT_SECRET?.trim()
  if (!value) throw new SubscriptionError(500, 'server_not_configured', 'study JWT secret is not configured')
  return value
}

function tokenSubject(req: express.Request): string {
  const authorization = req.headers.authorization
  if (typeof authorization !== 'string' || !authorization.startsWith('Bearer ')) {
    throw new SubscriptionError(401, 'unauthorized', 'subscription authorization is required')
  }
  try {
    const decoded = jwt.verify(authorization.slice(7).trim(), secret(), {
      algorithms: ['HS256'],
      issuer: ISSUER,
      audience: AUDIENCE,
    })
    if (!decoded || typeof decoded !== 'object') throw new Error('invalid payload')
    const payload = decoded as Partial<SubscriptionTokenPayload>
    if (payload.scope !== 'subscription:write' || typeof payload.sub !== 'string') {
      throw new SubscriptionError(403, 'forbidden', 'token lacks subscription scope')
    }
    return payload.sub
  } catch (error) {
    if (error instanceof SubscriptionError && error.status === 500) throw error
    if (error instanceof SubscriptionError && error.status === 403) throw error
    throw new SubscriptionError(401, 'invalid_subscription_token', 'subscription token is invalid or expired')
  }
}
function endpointError(res: express.Response, error: unknown) {
  const err = error instanceof SubscriptionError
    ? error
    : new SubscriptionError(500, 'internal_error', 'an unexpected error occurred')
  if (err.status >= 500) console.error(`[${new Date().toISOString()}] - subscription: ${err.message}`)
  return res.status(err.status).json({ ok: false, error: err.code, message: err.message })
}

export default function registerSubscribeEndpoint(server: Server, ctx: AppContext) {
  server.xrpc.router.get('/api/subscribe', (_req, res) => {
    return res.status(410).json({
      ok: false,
      error: 'retired_endpoint',
      message: 'Use authenticated POST /api/subscribe with identity and mode; exact-feed modes also require feed',
    })
  })

  server.xrpc.router.post('/api/subscription-token', async (req, res) => {
    if (!isApiKeyAuthorized(req, studyTokenAuth)) {
      return res.status(401).json({ ok: false, error: 'unauthorized' })
    }
    try {
      const identity = await resolveSubscriptionIdentity(req.body as SubscriptionInput)
      const ttl = ttlSeconds()
      const exp = Math.floor(Date.now() / 1000) + ttl
      const token = jwt.sign({ sub: identity.did, scope: 'subscription:write' }, secret(), {
        algorithm: 'HS256',
        issuer: ISSUER,
        audience: AUDIENCE,
        expiresIn: ttl,
        jwtid: crypto.randomUUID(),
      })
      res.header('Cache-Control', 'no-store')
      return res.json({
        ok: true,
        handle: identity.handle,
        did: identity.did,
        token,
        token_type: 'Bearer',
        exp,
      })
    } catch (error) {
      return endpointError(res, error)
    }
  })

  server.xrpc.router.post('/api/subscribe', async (req, res) => {
    try {
      const input = req.body as SubscriptionInput
      const admin = isApiKeyAuthorized(req, adminAuth)
      parseSubscriptionMode(input.mode)
      const boundDid = admin ? undefined : tokenSubject(req)
      const trustedInput = admin
        ? input
        : { ...input, source: 'subscription-token' }
      return res.json(await executeSubscription(ctx, trustedInput, true, true, boundDid))
    } catch (error) {
      return endpointError(res, error)
    }
  })
}
