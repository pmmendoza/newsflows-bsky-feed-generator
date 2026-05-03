import express from 'express'
import crypto from 'crypto'
import jwt from 'jsonwebtoken'
import { sql } from 'kysely'
import { Server } from '../lexicon'
import { AppContext } from '../config'
import { ApiKeyAuthConfig, hasConfiguredApiKey, isApiKeyAuthorized } from '../util/api-auth'
import { getPublisherDidsFromEnv } from '../util/ingestion-scope'

type StudyScope = 'compliance:read'

type StudyTokenPayload = {
  sub: string
  scope: StudyScope
}

const STUDY_JWT_ISSUER = 'newsflows-bsky-feed-generator'
const STUDY_JWT_AUDIENCE = 'newsflows-study'
const STUDY_TIMEZONE = 'Europe/Amsterdam'
const STUDY_DAY_BOUNDARY_HOUR = 6
const DEFAULT_STUDY_DAYS = 2
const MAX_STUDY_DAYS = 7

const studyTokenAuth: ApiKeyAuthConfig = {
  primaryEnv: ['STUDY_TOKEN_API_KEY'],
}

type RateLimitState = {
  count: number
  resetAt: number
}

type RateLimitConfig = {
  max: number
  windowMs: number
}

type StudyWindow = {
  start: string
  end: string
  timezone: string
  day_boundary_hour: number
  study_days: number
}

type ParticipantAuthResult =
  | { ok: true; did: string }
  | { ok: false; status: 401 | 403 | 500; body: Record<string, string> }

type AmsterdamDateParts = {
  year: number
  month: number
  day: number
  hour: number
  minute: number
  second: number
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

function normalizeCount(value: unknown): number {
  if (typeof value === 'number') return value
  if (typeof value === 'bigint') return Number(value)
  if (typeof value === 'string') {
    const parsed = parseInt(value, 10)
    return Number.isFinite(parsed) ? parsed : 0
  }
  return 0
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

function verifyStudyParticipant(req: express.Request): ParticipantAuthResult {
  const token = parseBearerToken(req)
  if (!token) return { ok: false, status: 401, body: { error: 'Unauthorized' } }

  try {
    const secret = process.env.STUDY_JWT_SECRET
    if (!secret) {
      return {
        ok: false,
        status: 500,
        body: {
          error: 'InternalServerError',
          message: 'Study JWT secret is not configured',
        },
      }
    }

    const decoded = jwt.verify(token, secret, {
      algorithms: ['HS256'],
      issuer: STUDY_JWT_ISSUER,
      audience: STUDY_JWT_AUDIENCE,
    })

    if (!decoded || typeof decoded !== 'object') {
      return { ok: false, status: 401, body: { error: 'Unauthorized' } }
    }

    const sub = (decoded as any).sub
    const scope = (decoded as any).scope
    if (scope !== 'compliance:read') {
      return { ok: false, status: 403, body: { error: 'Forbidden' } }
    }

    assertDid(sub)
    return { ok: true, did: sub }
  } catch (_err) {
    return { ok: false, status: 401, body: { error: 'Unauthorized' } }
  }
}

function applyComplianceRateLimit(
  did: string,
  res: express.Response,
): boolean {
  const rateLimitConfig = getComplianceSummaryRateLimitConfig()
  if (!rateLimitConfig) return true

  const check = checkComplianceSummaryRateLimit(did, rateLimitConfig)
  if (check.ok) return true

  res.header('Cache-Control', 'no-store')
  res.header('Retry-After', String(check.retryAfterSeconds))
  res.header('X-RateLimited', '1')
  res.status(429).json({
    error: 'TooManyRequests',
    message: 'Rate limit exceeded',
  })
  return false
}

function getAmsterdamDateParts(date: Date): AmsterdamDateParts {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: STUDY_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date)

  const byType: Record<string, string> = {}
  for (const part of parts) {
    if (part.type !== 'literal') byType[part.type] = part.value
  }

  return {
    year: parseInt(byType.year, 10),
    month: parseInt(byType.month, 10),
    day: parseInt(byType.day, 10),
    hour: parseInt(byType.hour, 10),
    minute: parseInt(byType.minute, 10),
    second: parseInt(byType.second, 10),
  }
}

function addLocalDays(parts: AmsterdamDateParts, days: number): AmsterdamDateParts {
  const shifted = new Date(Date.UTC(parts.year, parts.month - 1, parts.day + days))
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
    hour: parts.hour,
    minute: parts.minute,
    second: parts.second,
  }
}

function getAmsterdamOffsetMs(date: Date): number {
  const parts = getAmsterdamDateParts(date)
  const asUtc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  )
  return asUtc - Math.floor(date.getTime() / 1000) * 1000
}

function amsterdamLocalTimeToUtc(parts: AmsterdamDateParts): Date {
  const utcGuess = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  )
  const firstPass = new Date(utcGuess - getAmsterdamOffsetMs(new Date(utcGuess)))
  return new Date(utcGuess - getAmsterdamOffsetMs(firstPass))
}

function parseStudyDays(raw: unknown): number {
  if (raw === undefined) return DEFAULT_STUDY_DAYS
  if (typeof raw !== 'string' || raw.length === 0) {
    throw new Error('study_days must be a positive integer')
  }
  const parsed = parseInt(raw, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error('study_days must be a positive integer')
  }
  if (parsed > MAX_STUDY_DAYS) {
    throw new Error(`study_days must be <= ${MAX_STUDY_DAYS}`)
  }
  return parsed
}

function getStudyWindow(rawStudyDays: unknown, now = new Date()): StudyWindow {
  const studyDays = parseStudyDays(rawStudyDays)
  const nowParts = getAmsterdamDateParts(now)
  const boundaryDate =
    nowParts.hour < STUDY_DAY_BOUNDARY_HOUR ? addLocalDays(nowParts, -1) : nowParts
  const currentBoundaryLocal: AmsterdamDateParts = {
    year: boundaryDate.year,
    month: boundaryDate.month,
    day: boundaryDate.day,
    hour: STUDY_DAY_BOUNDARY_HOUR,
    minute: 0,
    second: 0,
  }
  const startBoundaryLocal = addLocalDays(currentBoundaryLocal, -(studyDays - 1))
  const start = amsterdamLocalTimeToUtc(startBoundaryLocal).toISOString()

  return {
    start,
    end: now.toISOString(),
    timezone: STUDY_TIMEZONE,
    day_boundary_hour: STUDY_DAY_BOUNDARY_HOUR,
    study_days: studyDays,
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
      if (!hasConfiguredApiKey(studyTokenAuth)) {
        return res.status(500).json({
          error: 'InternalServerError',
          message: 'Study token API key is not configured',
        })
      }

      if (!isApiKeyAuthorized(req, studyTokenAuth)) {
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
      const auth = verifyStudyParticipant(req)
      if (!auth.ok) return res.status(auth.status).json(auth.body)
      const did = auth.did

      if (!applyComplianceRateLimit(did, res)) return

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

  server.xrpc.router.get(
    '/api/study/compliance-activity-summary',
    async (req: express.Request, res: express.Response) => {
      const auth = verifyStudyParticipant(req)
      if (!auth.ok) return res.status(auth.status).json(auth.body)
      const did = auth.did

      if (!applyComplianceRateLimit(did, res)) return

      let window: StudyWindow
      try {
        window = getStudyWindow(req.query?.study_days)
      } catch (error) {
        const message = error instanceof Error ? error.message : 'BadRequest'
        return res.status(400).json({ error: 'BadRequest', message })
      }

      const publisherDids = getPublisherDidsFromEnv()
      const publisherTargetExpr =
        publisherDids.length > 0
          ? sql<boolean>`split_part(ev.target_uri, '/', 3) in (${sql.join(publisherDids)})`
          : sql<boolean>`false`

      try {
        res.header('Cache-Control', 'no-store')

        const queryStartedAt = Date.now()
        const { rows } = await sql<{
          retrieval_count: number | string | bigint
          last_timestamp: string | null
          algos: string[] | null
          feed_post_count: number | string | bigint
          publisher_post_count: number | string | bigint
          publisher_like_repost_count: number | string | bigint
          like_count: number | string | bigint
          repost_count: number | string | bigint
          quote_count: number | string | bigint
          comment_count: number | string | bigint
        }>`
          WITH served AS MATERIALIZED (
            SELECT DISTINCT rp.post_uri
            FROM request_log rl
            JOIN request_posts rp ON rp.request_id = rl.id
            WHERE
              rl.requester_did = ${did}
              AND rl.timestamp >= ${window.start}
              AND rl.timestamp < ${window.end}
              AND COALESCE(rl.result_count, 0) > 0
          ),
          retrievals AS (
            SELECT
              COUNT(*)::int AS retrieval_count,
              MAX(timestamp)::text AS last_timestamp,
              COALESCE(
                ARRAY_AGG(DISTINCT algo) FILTER (WHERE algo IS NOT NULL),
                '{}'::varchar[]
              ) AS algos
            FROM request_log
            WHERE
              requester_did = ${did}
              AND timestamp >= ${window.start}
              AND timestamp < ${window.end}
              AND COALESCE(result_count, 0) > 0
          ),
          events AS (
            SELECT
              CASE e.type
                WHEN 1 THEN 'repost'
                WHEN 2 THEN 'like'
                WHEN 3 THEN 'quote'
                ELSE 'unknown'
              END AS type,
              e.uri AS event_uri,
              e."subjectUri" AS target_uri
            FROM engagement e
            WHERE
              e.author = ${did}
              AND e."createdAt" >= ${window.start}
              AND e."createdAt" < ${window.end}
              AND e.type IN (1, 2, 3)
            UNION ALL
            SELECT
              'comment' AS type,
              p.uri AS event_uri,
              p."rootUri" AS target_uri
            FROM post p
            WHERE
              p.author = ${did}
              AND p."rootUri" != ''
              AND p."createdAt" >= ${window.start}
              AND p."createdAt" < ${window.end}
          ),
          engagement_counts AS (
            SELECT
              COUNT(*) FILTER (
                WHERE EXISTS (SELECT 1 FROM served s WHERE s.post_uri = ev.target_uri)
              )::int AS feed_post_count,
              COUNT(*) FILTER (WHERE ${publisherTargetExpr})::int AS publisher_post_count,
              COUNT(*) FILTER (
                WHERE ev.type IN ('like', 'repost') AND ${publisherTargetExpr}
              )::int AS publisher_like_repost_count,
              COUNT(*) FILTER (WHERE ev.type = 'like')::int AS like_count,
              COUNT(*) FILTER (WHERE ev.type = 'repost')::int AS repost_count,
              COUNT(*) FILTER (WHERE ev.type = 'quote')::int AS quote_count,
              COUNT(*) FILTER (WHERE ev.type = 'comment')::int AS comment_count
            FROM events ev
          )
          SELECT
            r.retrieval_count,
            r.last_timestamp,
            r.algos,
            e.feed_post_count,
            e.publisher_post_count,
            e.publisher_like_repost_count,
            e.like_count,
            e.repost_count,
            e.quote_count,
            e.comment_count
          FROM retrievals r
          CROSS JOIN engagement_counts e
        `.execute(ctx.db)

        const summary = rows[0]
        const durationMs = Date.now() - queryStartedAt
        console.log(
          `[${new Date().toISOString()}] - Study compliance activity summary requester=${did} window=${window.start}..${window.end} retrievals=${normalizeCount(summary?.retrieval_count)} engagements=${normalizeCount(summary?.like_count) + normalizeCount(summary?.repost_count) + normalizeCount(summary?.quote_count) + normalizeCount(summary?.comment_count)} duration_ms=${durationMs}`,
        )

        return res.json({
          requester_did: did,
          window,
          retrievals: {
            count: normalizeCount(summary?.retrieval_count),
            last_timestamp: summary?.last_timestamp ?? null,
            algos: summary?.algos ?? [],
          },
          engagements: {
            feed_post_count: normalizeCount(summary?.feed_post_count),
            publisher_post_count: normalizeCount(summary?.publisher_post_count),
            publisher_like_repost_count: normalizeCount(summary?.publisher_like_repost_count),
            by_type: {
              like: normalizeCount(summary?.like_count),
              repost: normalizeCount(summary?.repost_count),
              quote: normalizeCount(summary?.quote_count),
              comment: normalizeCount(summary?.comment_count),
            },
          },
        })
      } catch (error) {
        console.error('Error retrieving compliance activity summary:', error)
        return res.status(500).json({
          error: 'InternalServerError',
          message: 'An unexpected error occurred',
        })
      }
    },
  )
}
