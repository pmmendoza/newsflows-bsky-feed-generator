import express from 'express'
import crypto from 'crypto'
import jwt from 'jsonwebtoken'
import { sql } from 'kysely'
import { Server } from '../lexicon'
import { AppContext } from '../config'

type StudyScope = 'compliance:read'

type StudyTokenPayload = {
  sub: string
  scope: StudyScope
}

const STUDY_JWT_ISSUER = 'newsflows-bsky-feed-generator'
const STUDY_JWT_AUDIENCE = 'newsflows-study'

type RateLimitState = {
  count: number
  resetAt: number
}

type RateLimitConfig = {
  max: number
  windowMs: number
}

const complianceSummaryRateLimit = new Map<string, RateLimitState>()
let lastComplianceSummaryRateLimitPruneMs = 0

function parseIntOrUndefined(raw: string | undefined): number | undefined {
  if (!raw) return undefined
  const parsed = parseInt(raw, 10)
  return Number.isFinite(parsed) ? parsed : undefined
}

function getComplianceSummaryRateLimitConfig(): RateLimitConfig | null {
  // Defaults: 60 requests per 60 seconds per DID
  const maxRaw = parseIntOrUndefined(process.env.STUDY_COMPLIANCE_RATE_LIMIT_MAX)
  if (maxRaw === 0) return null
  const max = typeof maxRaw === 'number' && maxRaw > 0 ? maxRaw : 60

  const windowSecondsRaw = parseIntOrUndefined(
    process.env.STUDY_COMPLIANCE_RATE_LIMIT_WINDOW_SECONDS,
  )
  const windowSeconds =
    typeof windowSecondsRaw === 'number' && windowSecondsRaw > 0 ? windowSecondsRaw : 60

  return { max, windowMs: windowSeconds * 1000 }
}

function pruneComplianceSummaryRateLimit(nowMs: number, windowMs: number) {
  // Prevent unbounded growth if many unique DIDs hit the endpoint.
  if (nowMs - lastComplianceSummaryRateLimitPruneMs < 10 * 60 * 1000) return
  lastComplianceSummaryRateLimitPruneMs = nowMs

  for (const [key, state] of complianceSummaryRateLimit) {
    if (nowMs > state.resetAt + windowMs) {
      complianceSummaryRateLimit.delete(key)
    }
  }
}

function checkComplianceSummaryRateLimit(
  key: string,
  config: RateLimitConfig,
): { ok: true } | { ok: false; retryAfterSeconds: number } {
  const nowMs = Date.now()
  pruneComplianceSummaryRateLimit(nowMs, config.windowMs)

  const state = complianceSummaryRateLimit.get(key)
  if (!state || nowMs >= state.resetAt) {
    complianceSummaryRateLimit.set(key, { count: 1, resetAt: nowMs + config.windowMs })
    return { ok: true }
  }

  if (state.count >= config.max) {
    return {
      ok: false,
      retryAfterSeconds: Math.max(1, Math.ceil((state.resetAt - nowMs) / 1000)),
    }
  }

  state.count += 1
  return { ok: true }
}

function parseHeaderValue(value: unknown): string | undefined {
  if (typeof value === 'string') return value
  if (Array.isArray(value) && typeof value[0] === 'string') return value[0]
  return undefined
}

function parseBearerToken(req: express.Request): string | undefined {
  const authorization = parseHeaderValue(req.headers.authorization)
  if (!authorization?.startsWith('Bearer ')) return undefined
  const token = authorization.slice('Bearer '.length).trim()
  return token.length > 0 ? token : undefined
}

function parseTtlSeconds(): number {
  const raw = process.env.STUDY_JWT_TTL_SECONDS
  if (!raw) return 60 * 60 * 24 * 4 // 4 days
  const parsed = parseInt(raw, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) return 60 * 60 * 24 * 4
  return parsed
}

function assertDid(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !value.startsWith('did:') || value.length < 6) {
    throw new Error('Invalid DID')
  }
}

function extractDid(req: express.Request): string {
  const body = req.body
  const didFromBody =
    body && typeof body === 'object' && 'did' in body ? (body as any).did : undefined
  const didFromQuery = req.query?.did
  const didRaw = didFromBody ?? didFromQuery
  assertDid(didRaw)
  return didRaw
}

export default function registerStudyEndpoints(server: Server, ctx: AppContext) {
  // Server-to-server: mint participant token
  server.xrpc.router.post(
    '/api/study/token',
    async (req: express.Request, res: express.Response) => {
      const expectedApiKey = process.env.PRIORITIZE_API_KEY
      if (!expectedApiKey) {
        return res.status(500).json({
          error: 'InternalServerError',
          message: 'PRIORITIZE_API_KEY is not configured',
        })
      }

      const apiKey = parseHeaderValue(req.headers['api-key'])
      if (!apiKey || apiKey !== expectedApiKey) {
        return res.status(401).json({ error: 'Unauthorized' })
      }

      try {
        const did = extractDid(req)

        const subscriber = await ctx.db
          .selectFrom('subscriber')
          .select('did')
          .where('did', '=', did)
          .executeTakeFirst()

        if (!subscriber) {
          return res.status(404).json({
            error: 'NotFound',
            message: 'DID is not subscribed',
          })
        }

        const secret = process.env.STUDY_JWT_SECRET
        if (!secret) {
          return res.status(500).json({
            error: 'InternalServerError',
            message: 'Study JWT secret is not configured',
          })
        }

        const ttlSeconds = parseTtlSeconds()
        const nowSeconds = Math.floor(Date.now() / 1000)
        const exp = nowSeconds + ttlSeconds
        const tokenId = crypto.randomUUID()

        const payload: StudyTokenPayload = { sub: did, scope: 'compliance:read' }
        const token = jwt.sign(payload, secret, {
          algorithm: 'HS256',
          issuer: STUDY_JWT_ISSUER,
          audience: STUDY_JWT_AUDIENCE,
          expiresIn: ttlSeconds,
          jwtid: tokenId,
        })

        res.header('Cache-Control', 'no-store')
        return res.json({
          requester_did: did,
          token,
          token_type: 'Bearer',
          exp,
        })
      } catch (error) {
        const message = error instanceof Error ? error.message : 'BadRequest'
        return res.status(400).json({ error: 'BadRequest', message })
      }
    },
  )

  // Browser-safe: compliance summary for token-bound DID
  server.xrpc.router.get(
    '/api/study/compliance-summary',
    async (req: express.Request, res: express.Response) => {
      const token = parseBearerToken(req)
      if (!token) return res.status(401).json({ error: 'Unauthorized' })

      let did: string
      try {
        const secret = process.env.STUDY_JWT_SECRET
        if (!secret) {
          return res.status(500).json({
            error: 'InternalServerError',
            message: 'Study JWT secret is not configured',
          })
        }

        const decoded = jwt.verify(token, secret, {
          algorithms: ['HS256'],
          issuer: STUDY_JWT_ISSUER,
          audience: STUDY_JWT_AUDIENCE,
        })

        if (!decoded || typeof decoded !== 'object') {
          return res.status(401).json({ error: 'Unauthorized' })
        }

        const sub = (decoded as any).sub
        const scope = (decoded as any).scope
        if (scope !== 'compliance:read') {
          return res.status(403).json({ error: 'Forbidden' })
        }

        assertDid(sub)
        did = sub
      } catch (_err) {
        return res.status(401).json({ error: 'Unauthorized' })
      }

      const rateLimitConfig = getComplianceSummaryRateLimitConfig()
      if (rateLimitConfig) {
        const check = checkComplianceSummaryRateLimit(did, rateLimitConfig)
        if (!check.ok) {
          res.header('Cache-Control', 'no-store')
          res.header('Retry-After', String(check.retryAfterSeconds))
          res.header('X-RateLimited', '1')
          return res.status(429).json({
            error: 'TooManyRequests',
            message: 'Rate limit exceeded',
          })
        }
      }

      const minDate =
        typeof req.query?.min_date === 'string' && req.query.min_date.length > 0
          ? req.query.min_date
          : undefined

      try {
        let query = ctx.db
          .selectFrom('request_log')
          .select([
            sql<number>`COUNT(*)::int`.as('count'),
            sql<string | null>`MAX(timestamp)::text`.as('last_timestamp'),
            sql<string[]>`COALESCE(ARRAY_AGG(DISTINCT algo), '{}')`.as('algos'),
          ])
          .where('requester_did', '=', did)

        if (minDate) {
          query = query.where('timestamp', '>', minDate)
        }

        const summary = await query.executeTakeFirst()

        return res.json({
          requester_did: did,
          count: summary?.count ?? 0,
          algos: summary?.algos ?? [],
          last_timestamp: summary?.last_timestamp ?? null,
        })
      } catch (error) {
        console.error('Error retrieving compliance summary:', error)
        return res.status(500).json({
          error: 'InternalServerError',
          message: 'An unexpected error occurred',
        })
      }
    },
  )
}
