import { Server } from '../lexicon'
import { AppContext } from '../config'
import { sql } from 'kysely'
import { createHash } from 'crypto'
import fs from 'fs'
import path from 'path'
import { Database } from '../db'
import {
  allowlistRefreshMs,
  getPublisherDidsFromEnv,
  restrictPublisherEngagementToSubscribersEnabled,
  scopedIngestionEnabled,
  trackSubscriberActivityEnabled,
} from '../util/ingestion-scope'
import { getRetentionConfig } from '../util/retention'

type EngagementExportType = 'like' | 'repost' | 'comment' | 'quote'
type EngagementExportScope = 'union' | 'publisher' | 'subscriber' | 'subscriber_on_publisher'

type EngagementExportEvent = {
  type: EngagementExportType
  event_uri: string
  subject_uri: string
  author_did: string
  created_at: string
  comment_root_uri: string | null
  quote_subject_uri: string | null
  publisher_target_any: boolean
  publisher_target_comment_root: boolean
  publisher_target_quote_subject: boolean
}

type EngagementFilters = {
  since: string
  until: string
  scope: EngagementExportScope
  limit: number
  types: EngagementExportType[]
  subscriberDid: string | null
  includeOtherSubscriberActivity: boolean
}

type EngagementCursor = {
  created_at: string
  event_uri: string
  filter_sig: string
}

let cachedPackageVersion: string | null | undefined

function getPackageVersion(): string | undefined {
  if (cachedPackageVersion !== undefined) {
    return cachedPackageVersion ?? undefined
  }
  try {
    const pkgPath = path.resolve(__dirname, '../../package.json')
    const raw = fs.readFileSync(pkgPath, 'utf8')
    const parsed = JSON.parse(raw)
    cachedPackageVersion = typeof parsed?.version === 'string' ? parsed.version : null
  } catch (error) {
    cachedPackageVersion = null
  }
  return cachedPackageVersion ?? undefined
}

function getEngagementTimeHours(): number {
  const raw = process.env.ENGAGEMENT_TIME_HOURS
  if (!raw) return 72
  const parsed = parseInt(raw, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 72
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

function base64UrlEncode(input: string): string {
  return Buffer.from(input, 'utf8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '')
}

function base64UrlDecode(input: string): string {
  let base64 = input.replace(/-/g, '+').replace(/_/g, '/')
  while (base64.length % 4 !== 0) {
    base64 += '='
  }
  return Buffer.from(base64, 'base64').toString('utf8')
}

function buildFilterSignature(filters: EngagementFilters): string {
  const normalized = {
    since: filters.since,
    until: filters.until,
    scope: filters.scope,
    limit: filters.limit,
    types: [...filters.types].sort(),
    subscriber_did: filters.subscriberDid ?? null,
    include_other_subscriber_activity: filters.includeOtherSubscriberActivity,
  }
  const json = JSON.stringify(normalized)
  return createHash('sha256').update(json).digest('hex')
}

function encodeEngagementCursor(cursor: EngagementCursor): string {
  return base64UrlEncode(JSON.stringify(cursor))
}

function decodeEngagementCursor(raw: string): EngagementCursor {
  let parsed: any
  try {
    parsed = JSON.parse(base64UrlDecode(raw))
  } catch (error) {
    throw new Error('cursor must be valid base64url JSON')
  }
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('cursor must be a JSON object')
  }
  const created_at = parseIsoOrThrow(parsed.created_at, 'cursor.created_at')
  const event_uri = typeof parsed.event_uri === 'string' ? parsed.event_uri : ''
  const filter_sig = typeof parsed.filter_sig === 'string' ? parsed.filter_sig : ''
  if (!event_uri) {
    throw new Error('cursor.event_uri must be a non-empty string')
  }
  if (!filter_sig) {
    throw new Error('cursor.filter_sig must be a non-empty string')
  }
  return { created_at, event_uri, filter_sig }
}

function parseHeaderValue(value: unknown): string | undefined {
  if (typeof value === 'string') return value
  if (Array.isArray(value) && typeof value[0] === 'string') return value[0]
  return undefined
}

function parseBoolParam(raw: unknown): boolean | undefined {
  if (raw === undefined) return undefined
  if (typeof raw !== 'string') return undefined
  if (raw.toLowerCase() === 'true') return true
  if (raw.toLowerCase() === 'false') return false
  if (raw === '1') return true
  if (raw === '0') return false
  return undefined
}

function parseNonNegInt(raw: unknown): number | undefined {
  if (raw === undefined) return undefined
  if (typeof raw !== 'string' || raw.length === 0) return undefined
  const parsed = parseInt(raw, 10)
  if (!Number.isFinite(parsed) || parsed < 0) return undefined
  return parsed
}

function parseDidParam(raw: unknown): string | undefined {
  if (raw === undefined) return undefined
  if (typeof raw !== 'string' || raw.length === 0) return undefined
  if (!raw.startsWith('did:') || raw.length < 6) return undefined
  return raw
}

function parseIsoOrThrow(raw: unknown, name: string): string {
  if (typeof raw !== 'string' || raw.length === 0) {
    throw new Error(`${name} must be a non-empty ISO timestamp string`)
  }
  const d = new Date(raw)
  if (Number.isNaN(d.getTime())) {
    throw new Error(`${name} must be a valid ISO timestamp string`)
  }
  return d.toISOString()
}

function getDefaultSinceUntil(): { since: string; until: string } {
  const untilDate = new Date()
  const sinceDate = new Date(untilDate.getTime() - 24 * 60 * 60 * 1000)
  return { since: sinceDate.toISOString(), until: untilDate.toISOString() }
}

export default function registerMonitorEndpoints(server: Server, ctx: AppContext) {
  server.xrpc.router.get('/api/subscribers', async (req, res) => {
    const apiKey = req.headers['api-key']

    if (!apiKey || apiKey !== process.env.PRIORITIZE_API_KEY) {
      console.log(`[${new Date().toISOString()}] - Attempted unauthorized access to subscribers with API key ${apiKey}`);
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    try {
      const subscribers = await ctx.db
        .selectFrom('subscriber')
        .selectAll()
        .orderBy('handle', 'asc')
        .execute()

      console.log(`[${new Date().toISOString()}] - Retrieved ${subscribers.length} subscribers`);

      return res.json({
        count: subscribers.length,
        subscribers: subscribers
      });
    } catch (error) {
      console.error('Error retrieving subscribers:', error);
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred'
      });
    }
  });

  server.xrpc.router.get('/api/follows', async (req, res) => {
    const apiKey = req.headers['api-key']

    if (!apiKey || apiKey !== process.env.PRIORITIZE_API_KEY) {
      console.log(`[${new Date().toISOString()}] - Attempted unauthorized access to follows with API key ${apiKey}`);
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    try {
      const follows = await ctx.db
        .selectFrom('follows')
        .selectAll()
        .orderBy('subject', 'asc')
        .execute()

      console.log(`[${new Date().toISOString()}] - Retrieved ${follows.length} follows`);

      return res.json({
        count: follows.length,
        follows: follows
      });
    } catch (error) {
      console.error('Error retrieving follows:', error);
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred'
      });
    }
  });

  server.xrpc.router.get('/api/config', async (req, res) => {
    const apiKey = parseHeaderValue(req.headers['api-key'])
    if (!apiKey || apiKey !== process.env.PRIORITIZE_API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' })
    }

    try {
      res.header('Cache-Control', 'no-store')

      const publisherDids = getPublisherDidsFromEnv().sort()
      const retention = getRetentionConfig()
      const version = getPackageVersion()
      const buildSha = process.env.FEEDGEN_BUILD_SHA || undefined

      const subscriberCountRow = await ctx.db
        .selectFrom('subscriber')
        .select(sql<number>`count(*)`.as('count'))
        .executeTakeFirst()
      const followsCountRow = await ctx.db
        .selectFrom('follows')
        .select(sql<number>`count(*)`.as('count'))
        .executeTakeFirst()

      const subscriberCount = normalizeCount(subscriberCountRow?.count)
      const followsCount = normalizeCount(followsCountRow?.count)

      const warnings: string[] = []
      if (publisherDids.length === 0) {
        warnings.push('no_publisher_dids_configured')
      }
      if (scopedIngestionEnabled() && publisherDids.length === 0 && followsCount === 0) {
        warnings.push('scoped_ingestion_enabled_but_allowlist_empty')
      }

      let firehoseHost: string | undefined
      try {
        firehoseHost = new URL(ctx.cfg.subscriptionEndpoint).host
      } catch (error) {
        firehoseHost = undefined
      }

      return res.json({
        service_did: ctx.cfg.serviceDid,
        hostname: ctx.cfg.hostname,
        version: version ?? null,
        build_sha: buildSha ?? null,
        ingestion: {
          scoped_ingestion_enabled: scopedIngestionEnabled(),
          track_subscriber_activity: trackSubscriberActivityEnabled(),
          publisher_engagement_subscriber_only: restrictPublisherEngagementToSubscribersEnabled(),
          allowlist_refresh_ms: allowlistRefreshMs(),
        },
        retention: {
          enabled: retention.enabled,
          post_retention_days: retention.postRetentionDays,
          engagement_retention_days: retention.engagementRetentionDays,
          delete_batch_size: retention.deleteBatchSize,
        },
        engagement: {
          time_hours: getEngagementTimeHours(),
        },
        publisher_dids: publisherDids,
        subscriber_count: subscriberCount,
        firehose: {
          subscription_endpoint: ctx.cfg.subscriptionEndpoint,
          subscription_endpoint_host: firehoseHost ?? null,
          reconnect_delay_ms: ctx.cfg.subscriptionReconnectDelay,
        },
        warnings,
      })
    } catch (error) {
      console.error('Error retrieving config:', error)
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred',
      })
    }
  })

  server.xrpc.router.get('/api/compliance', async (req, res) => {
    const apiKey = req.headers['api-key']

    if (!apiKey || apiKey !== process.env.PRIORITIZE_API_KEY) {
      console.log(`[${new Date().toISOString()}] - Attempted unauthorized access to compliance with API key ${apiKey}`);
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    try {
      const { min_date, user_did } = req.query;

      // Build the query with JSON aggregation for posts (only from request_posts table)
      let query = ctx.db
        .selectFrom('request_log as rl')
        .leftJoin('request_posts as rp', 'rl.id', 'rp.request_id')
        .select([
          'rl.id',
          'rl.algo',
          'rl.requester_did',
          'rl.timestamp',
          sql<any>`COALESCE(
            JSON_AGG(
              JSON_BUILD_OBJECT('uri', rp.post_uri, 'position', rp.position)
            ) FILTER (WHERE rp.post_uri IS NOT NULL),
            '[]'::json
          )`.as('posts')
        ])
        .groupBy(['rl.id', 'rl.algo', 'rl.requester_did', 'rl.timestamp'])

      // Apply optional filters
      if (user_did) {
        query = query.where('rl.requester_did', '=', user_did as string)
      }

      if (min_date) {
        query = query.where('rl.timestamp', '>', min_date as string)
      }

      const compliance = await query.execute()

      console.log(`[${new Date().toISOString()}] - Retrieved ${compliance.length} compliance records`);

      return res.json({
        count: compliance.length,
        compliance: compliance
      });
    } catch (error) {
      console.error('Error retrieving compliance data:', error);
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred'
      });
    }
  });

  const handleEngagementExport = async (
    req: any,
    res: any,
    db: Database,
    responseSource?: string,
  ) => {
    const apiKey = parseHeaderValue(req.headers['api-key'])
    if (!apiKey || apiKey !== process.env.PRIORITIZE_API_KEY) {
      return res.status(401).json({ error: 'Unauthorized' })
    }

    const publisherDids = getPublisherDidsFromEnv()

    const { since: defaultSince, until: defaultUntil } = getDefaultSinceUntil()

    let since: string
    let until: string
    let scope: EngagementExportScope
    let page: number
    let limit: number
    let subscriberDid: string | undefined
    let includeOtherSubscriberActivity = false
    let types: EngagementExportType[]
    let cursor: EngagementCursor | null = null
    let otherCursor: EngagementCursor | null = null
    let filterSig = ''
    let useCursor = false
    let useOtherCursor = false

    try {
      const cursorRaw = typeof req.query?.cursor === 'string' ? req.query.cursor : undefined
      if (cursorRaw !== undefined && cursorRaw.length === 0) {
        throw new Error('cursor must be a non-empty string')
      }
      const otherCursorRaw =
        typeof req.query?.other_cursor === 'string' ? req.query.other_cursor : undefined
      if (otherCursorRaw !== undefined && otherCursorRaw.length === 0) {
        throw new Error('other_cursor must be a non-empty string')
      }
      useCursor = cursorRaw !== undefined
      const otherCursorProvided = otherCursorRaw !== undefined

      since = req.query?.since ? parseIsoOrThrow(req.query.since, 'since') : defaultSince
      until = req.query?.until ? parseIsoOrThrow(req.query.until, 'until') : defaultUntil

      if (since >= until) {
        throw new Error('since must be earlier than until')
      }

      const scopeRaw = req.query?.scope
      if (scopeRaw === undefined) {
        scope = 'union'
      } else if (typeof scopeRaw === 'string') {
        if (
          scopeRaw === 'publisher' ||
          scopeRaw === 'subscriber' ||
          scopeRaw === 'subscriber_on_publisher' ||
          scopeRaw === 'union'
        ) {
          scope = scopeRaw
        } else {
          throw new Error('scope must be one of: union|publisher|subscriber|subscriber_on_publisher')
        }
      } else {
        throw new Error('scope must be one of: union|publisher|subscriber|subscriber_on_publisher')
      }

      if (!useCursor) {
        const pageRaw = req.query?.page
        if (pageRaw === undefined) {
          page = 0
        } else {
          const parsed = parseNonNegInt(pageRaw)
          if (parsed === undefined) throw new Error('page must be a non-negative integer')
          page = parsed
        }
      } else {
        page = 0
      }

      const limitRaw = req.query?.limit
      if (limitRaw === undefined) {
        limit = 1000
      } else {
        const parsed = parseNonNegInt(limitRaw)
        if (parsed === undefined) throw new Error('limit must be a non-negative integer')
        limit = parsed
      }
      if (limit <= 0) throw new Error('limit must be a positive integer')
      if (limit > 5000) throw new Error('limit must be <= 5000')

      subscriberDid = parseDidParam(req.query?.subscriber_did)
      if (req.query?.subscriber_did !== undefined && !subscriberDid) {
        throw new Error('subscriber_did must be a DID (did:...)')
      }

      const includeRaw = parseBoolParam(req.query?.include_other_subscriber_activity)
      if (req.query?.include_other_subscriber_activity !== undefined && includeRaw === undefined) {
        throw new Error('include_other_subscriber_activity must be true|false|1|0')
      }
      includeOtherSubscriberActivity = includeRaw ?? false
      if (otherCursorProvided && !includeOtherSubscriberActivity) {
        throw new Error('other_cursor requires include_other_subscriber_activity=true')
      }

      const typesRaw = typeof req.query?.types === 'string' ? req.query.types : undefined
      if (!typesRaw) {
        types = ['like', 'repost', 'comment', 'quote']
      } else {
        const parsed = typesRaw
          .split(',')
          .map((t) => t.trim().toLowerCase())
          .filter((t) => t.length > 0)
        const allowed: EngagementExportType[] = []
        for (const t of parsed) {
          if (t === 'like' || t === 'repost' || t === 'comment' || t === 'quote') {
            allowed.push(t)
          } else {
            throw new Error(`types contains invalid value: ${t}`)
          }
        }
        types = Array.from(new Set(allowed))
        if (types.length === 0) throw new Error('types must include at least one type')
      }
      const filters: EngagementFilters = {
        since,
        until,
        scope,
        limit,
        types,
        subscriberDid: subscriberDid ?? null,
        includeOtherSubscriberActivity,
      }
      filterSig = buildFilterSignature(filters)

      if (useCursor && cursorRaw) {
        cursor = decodeEngagementCursor(cursorRaw)
        if (cursor.filter_sig !== filterSig) {
          throw new Error('cursor does not match request filters')
        }
      }
      if (otherCursorProvided && otherCursorRaw) {
        otherCursor = decodeEngagementCursor(otherCursorRaw)
        if (otherCursor.filter_sig !== filterSig) {
          throw new Error('other_cursor does not match request filters')
        }
      }
      useOtherCursor = includeOtherSubscriberActivity && (otherCursorProvided || useCursor)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'BadRequest'
      return res.status(400).json({ error: 'BadRequest', message })
    }

    const offset = useCursor ? 0 : page * limit
    const effectiveLimit = limit + 1

    const publisherTargetExpr =
      publisherDids.length > 0
        ? sql<boolean>`split_part(b.subject_uri, '/', 3) in (${sql.join(publisherDids)})`
        : sql<boolean>`false`

    const typeFilter = sql<boolean>`type in (${sql.join(types)})`
    const subscriberDidFilter = subscriberDid
      ? sql<boolean>`author_did = ${subscriberDid}`
      : sql<boolean>`true`

    const scopeFilter =
      scope === 'publisher'
        ? sql<boolean>`is_publisher_target`
        : scope === 'subscriber'
          ? sql<boolean>`is_subscriber_actor`
          : scope === 'subscriber_on_publisher'
            ? sql<boolean>`is_publisher_target AND is_subscriber_actor`
            : sql<boolean>`is_publisher_target OR is_subscriber_actor`

    const cursorFilter =
      useCursor && cursor
        ? sql<boolean>`(d.created_at < ${cursor.created_at} OR (d.created_at = ${cursor.created_at} AND d.event_uri < ${cursor.event_uri}))`
        : sql<boolean>`true`
    const otherCursorForFilter = useOtherCursor ? otherCursor ?? cursor : null
    const otherCursorFilter =
      useOtherCursor && otherCursorForFilter
        ? sql<boolean>`(d.created_at < ${otherCursorForFilter.created_at} OR (d.created_at = ${otherCursorForFilter.created_at} AND d.event_uri < ${otherCursorForFilter.event_uri}))`
        : sql<boolean>`true`
    const otherOffset = useOtherCursor ? 0 : offset
    const otherEffectiveLimit = limit + 1

    try {
      res.header('Cache-Control', 'no-store')

      const { rows } = await sql<EngagementExportEvent>`
        WITH base AS (
          SELECT
            CASE e.type
              WHEN 1 THEN 'repost'
              WHEN 2 THEN 'like'
              WHEN 3 THEN 'quote'
              ELSE 'unknown'
            END AS type,
            e.uri AS event_uri,
            e."subjectUri" AS subject_uri,
            e.author AS author_did,
            e."createdAt" AS created_at
          FROM engagement e
          WHERE e."createdAt" >= ${since} AND e."createdAt" < ${until}

          UNION ALL

          SELECT
            'comment' AS type,
            p.uri AS event_uri,
            p."rootUri" AS subject_uri,
            p.author AS author_did,
            p."createdAt" AS created_at
          FROM post p
          WHERE p."rootUri" != '' AND p."createdAt" >= ${since} AND p."createdAt" < ${until}
        ),
        enriched AS (
          SELECT
            b.type,
            b.event_uri,
            b.subject_uri,
            b.author_did,
            b.created_at,
            (${publisherTargetExpr}) AS is_publisher_target,
            EXISTS (SELECT 1 FROM subscriber s WHERE s.did = b.author_did) AS is_subscriber_actor,
            CASE b.type
              WHEN 'like' THEN 1
              WHEN 'repost' THEN 2
              WHEN 'comment' THEN 3
              WHEN 'quote' THEN 4
              ELSE 0
            END AS type_rank
          FROM base b
        ),
        scoped AS (
          SELECT * FROM enriched
          WHERE (${typeFilter}) AND (${subscriberDidFilter}) AND (${scopeFilter})
        ),
        aggregated AS (
          SELECT
            event_uri,
            MAX(CASE WHEN type = 'comment' THEN subject_uri ELSE NULL END) AS comment_root_uri,
            MAX(CASE WHEN type = 'quote' THEN subject_uri ELSE NULL END) AS quote_subject_uri,
            BOOL_OR(is_publisher_target) AS publisher_target_any,
            BOOL_OR(type = 'comment' AND is_publisher_target) AS publisher_target_comment_root,
            BOOL_OR(type = 'quote' AND is_publisher_target) AS publisher_target_quote_subject
          FROM scoped
          GROUP BY event_uri
        ),
        deduped AS (
          SELECT DISTINCT ON (event_uri)
            type,
            event_uri,
            subject_uri,
            author_did,
            created_at
          FROM scoped
          ORDER BY event_uri, created_at DESC, type_rank DESC
        )
        SELECT
          d.type,
          d.event_uri,
          d.subject_uri,
          d.author_did,
          d.created_at,
          a.comment_root_uri,
          a.quote_subject_uri,
          COALESCE(a.publisher_target_any, false) AS publisher_target_any,
          COALESCE(a.publisher_target_comment_root, false) AS publisher_target_comment_root,
          COALESCE(a.publisher_target_quote_subject, false) AS publisher_target_quote_subject
        FROM deduped d
        LEFT JOIN aggregated a ON a.event_uri = d.event_uri
        WHERE (${cursorFilter})
        ORDER BY d.created_at DESC, d.event_uri DESC
        LIMIT ${effectiveLimit} OFFSET ${offset}
      `.execute(db)

      let events = rows
      let nextCursor: string | undefined
      if (rows.length > limit) {
        events = rows.slice(0, limit)
        const last = events[events.length - 1]
        if (last) {
          nextCursor = encodeEngagementCursor({
            created_at: last.created_at,
            event_uri: last.event_uri,
            filter_sig: filterSig,
          })
        }
      }

      const response: any = {
        since,
        until,
        scope,
        page,
        limit,
        count: events.length,
        events,
      }
      if (nextCursor) {
        response.next_cursor = nextCursor
      }
      if (responseSource) {
        response.source = responseSource
      }

      if (
        includeOtherSubscriberActivity &&
        (scope === 'publisher' || scope === 'subscriber_on_publisher')
      ) {
        const otherScopeFilter = sql<boolean>`is_subscriber_actor AND NOT is_publisher_target`

        const other = await sql<EngagementExportEvent>`
          WITH base AS (
          SELECT
            CASE e.type
              WHEN 1 THEN 'repost'
              WHEN 2 THEN 'like'
              WHEN 3 THEN 'quote'
              ELSE 'unknown'
            END AS type,
            e.uri AS event_uri,
            e."subjectUri" AS subject_uri,
            e.author AS author_did,
            e."createdAt" AS created_at
            FROM engagement e
            WHERE e."createdAt" >= ${since} AND e."createdAt" < ${until}

            UNION ALL

            SELECT
              'comment' AS type,
              p.uri AS event_uri,
              p."rootUri" AS subject_uri,
              p.author AS author_did,
              p."createdAt" AS created_at
            FROM post p
            WHERE p."rootUri" != '' AND p."createdAt" >= ${since} AND p."createdAt" < ${until}
          ),
          enriched AS (
            SELECT
              b.type,
              b.event_uri,
              b.subject_uri,
              b.author_did,
              b.created_at,
              (${publisherTargetExpr}) AS is_publisher_target,
              EXISTS (SELECT 1 FROM subscriber s WHERE s.did = b.author_did) AS is_subscriber_actor,
              CASE b.type
                WHEN 'like' THEN 1
                WHEN 'repost' THEN 2
                WHEN 'comment' THEN 3
                WHEN 'quote' THEN 4
                ELSE 0
              END AS type_rank
            FROM base b
          ),
          scoped AS (
            SELECT * FROM enriched
            WHERE (${typeFilter}) AND (${subscriberDidFilter}) AND (${otherScopeFilter})
          ),
          aggregated AS (
            SELECT
              event_uri,
              MAX(CASE WHEN type = 'comment' THEN subject_uri ELSE NULL END) AS comment_root_uri,
              MAX(CASE WHEN type = 'quote' THEN subject_uri ELSE NULL END) AS quote_subject_uri,
              BOOL_OR(is_publisher_target) AS publisher_target_any,
              BOOL_OR(type = 'comment' AND is_publisher_target) AS publisher_target_comment_root,
              BOOL_OR(type = 'quote' AND is_publisher_target) AS publisher_target_quote_subject
            FROM scoped
            GROUP BY event_uri
          ),
          deduped AS (
            SELECT DISTINCT ON (event_uri)
              type,
              event_uri,
              subject_uri,
              author_did,
              created_at
            FROM scoped
            ORDER BY event_uri, created_at DESC, type_rank DESC
          )
          SELECT
            d.type,
            d.event_uri,
            d.subject_uri,
            d.author_did,
            d.created_at,
            a.comment_root_uri,
            a.quote_subject_uri,
            COALESCE(a.publisher_target_any, false) AS publisher_target_any,
            COALESCE(a.publisher_target_comment_root, false) AS publisher_target_comment_root,
            COALESCE(a.publisher_target_quote_subject, false) AS publisher_target_quote_subject
          FROM deduped d
          LEFT JOIN aggregated a ON a.event_uri = d.event_uri
          WHERE (${otherCursorFilter})
          ORDER BY d.created_at DESC, d.event_uri DESC
          LIMIT ${otherEffectiveLimit} OFFSET ${otherOffset}
        `.execute(db)

        let otherEvents = other.rows
        let otherNextCursor: string | undefined
        if (other.rows.length > limit) {
          otherEvents = other.rows.slice(0, limit)
          const last = otherEvents[otherEvents.length - 1]
          if (last) {
            otherNextCursor = encodeEngagementCursor({
              created_at: last.created_at,
              event_uri: last.event_uri,
              filter_sig: filterSig,
            })
          }
        }

        response.other_subscriber_count = otherEvents.length
        response.other_subscriber_events = otherEvents
        if (otherNextCursor) {
          response.other_next_cursor = otherNextCursor
        }
      }

      return res.json(response)
    } catch (error) {
      console.error('Error retrieving compliance engagement export:', error)
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred',
      })
    }
  }

  server.xrpc.router.get('/api/compliance/engagement', async (req, res) => {
    return handleEngagementExport(req, res, ctx.db)
  })

  server.xrpc.router.get('/api/compliance/engagement_legacy', async (req, res) => {
    if (!ctx.legacyDb) {
      return res.status(503).json({
        error: 'LegacyDbUnavailable',
        message: 'Legacy database is not configured for this service',
      })
    }
    return handleEngagementExport(req, res, ctx.legacyDb, 'legacy_db')
  })

  server.xrpc.router.get('/api/engagement', async (req, res) => {
    const apiKey = req.headers['api-key']

    if (!apiKey || apiKey !== process.env.PRIORITIZE_API_KEY) {
      console.log(`[${new Date().toISOString()}] - Attempted unauthorized access to engagement with API key ${apiKey}`);
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    try {
      const { requester_did, publisher_did, page } = req.query;

      // Validate that exactly one of requester_did or publisher_did is provided
      if ((!requester_did && !publisher_did) || (requester_did && publisher_did)) {
        return res.status(400).json({
          error: 'BadRequest',
          message: 'Exactly one of requester_did or publisher_did must be provided'
        });
      }

      // Parse page parameter (default to 0)
      const pageNum = page ? parseInt(page as string, 10) : 0;
      if (isNaN(pageNum) || pageNum < 0) {
        return res.status(400).json({
          error: 'BadRequest',
          message: 'page must be a non-negative integer'
        });
      }

      const limit = 100;
      const offset = pageNum * limit;

      // Get engagement time window from environment
      const engagementTimeHours = process.env.ENGAGEMENT_TIME_HOURS ?
        parseInt(process.env.ENGAGEMENT_TIME_HOURS, 10) : 72;
      const timeLimit = new Date(Date.now() - engagementTimeHours * 60 * 60 * 1000).toISOString();

      let posts: any[];
      let queryType: string;

      if (publisher_did) {
        // Query for posts by the specified publisher
        queryType = 'publisher';
        posts = await ctx.db
          .selectFrom('post')
          .select([
            'uri',
            'indexedAt',
            'likes_count',
            'repost_count',
            'comments_count',
            // Base engagement score
            sql<number>`
              COALESCE(
                (COALESCE(likes_count, 0) +
                 COALESCE(repost_count, 0) * 1.5 +
                 COALESCE(comments_count, 0)),
                0
              )
            `.as('base_engagement_score'),
            // Time-decayed engagement score
            sql<number>`
              COALESCE(
                (COALESCE(likes_count, 0) +
                 COALESCE(repost_count, 0) * 1.5 +
                 COALESCE(comments_count, 0)),
                0
              )
              *
              (1 - POWER(
                (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM "indexedAt"::timestamp)) /
                (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM ${timeLimit}::timestamp)),
                2
              ))
            `.as('time_decayed_engagement_score')
          ])
          .where('author', '=', publisher_did as string)
          .where('post.indexedAt', '>=', timeLimit)
          .orderBy('time_decayed_engagement_score', 'desc')
          .orderBy('indexedAt', 'desc')
          .orderBy('cid', 'desc')
          .offset(offset)
          .limit(limit)
          .execute();
      } else {
        // Query for posts by people the requester follows
        queryType = 'follows';
        const { getFollows } = await import('../util/queries');
        const requesterFollows = await getFollows(requester_did as string, ctx.db);

        posts = await ctx.db
          .selectFrom('post')
          .select([
            'uri',
            'indexedAt',
            'likes_count',
            'repost_count',
            'comments_count',
            // Base engagement score
            sql<number>`
              COALESCE(
                (COALESCE(likes_count, 0) +
                 COALESCE(repost_count, 0) * 1.5 +
                 COALESCE(comments_count, 0)),
                0
              )
            `.as('base_engagement_score'),
            // Time-decayed engagement score
            sql<number>`
              COALESCE(
                (COALESCE(likes_count, 0) +
                 COALESCE(repost_count, 0) * 1.5 +
                 COALESCE(comments_count, 0)),
                0
              )
              *
              (1 - POWER(
                (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM "indexedAt"::timestamp)) /
                (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM ${timeLimit}::timestamp)),
                2
              ))
            `.as('time_decayed_engagement_score')
          ])
          .where('post.indexedAt', '>=', timeLimit)
          .where((eb) => eb('author', 'in', requesterFollows))
          .orderBy('time_decayed_engagement_score', 'desc')
          .orderBy('indexedAt', 'desc')
          .orderBy('cid', 'desc')
          .offset(offset)
          .limit(limit)
          .execute();
      }

      console.log(`[${new Date().toISOString()}] - Retrieved ${posts.length} ${queryType} posts with engagement scores, page ${pageNum}`);

      return res.json({
        count: posts.length,
        page: pageNum,
        limit: limit,
        query_type: queryType,
        requester_did: requester_did || null,
        publisher_did: publisher_did || null,
        time_limit: timeLimit,
        engagement_time_hours: engagementTimeHours,
        posts: posts
      });
    } catch (error) {
      console.error('Error retrieving engagement data:', error);
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred'
      });
    }
  });

  server.xrpc.router.get('/api/priorities', async (req, res) => {
    const apiKey = req.headers['api-key']

    if (!apiKey || apiKey !== process.env.PRIORITIZE_API_KEY) {
      console.log(`[${new Date().toISOString()}] - Attempted unauthorized access to priorities with API key ${apiKey}`);
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    try {
      const { requester_did, publisher_did, page, min_priority } = req.query;

      // Validate that exactly one of requester_did or publisher_did is provided
      if ((!requester_did && !publisher_did) || (requester_did && publisher_did)) {
        return res.status(400).json({
          error: 'BadRequest',
          message: 'Exactly one of requester_did or publisher_did must be provided'
        });
      }

      // Parse page parameter (default to 0)
      const pageNum = page ? parseInt(page as string, 10) : 0;
      if (isNaN(pageNum) || pageNum < 0) {
        return res.status(400).json({
          error: 'BadRequest',
          message: 'page must be a non-negative integer'
        });
      }

      // Parse min_priority parameter (default to 1)
      const minPriority = min_priority ? parseInt(min_priority as string, 10) : 1;
      if (isNaN(minPriority)) {
        return res.status(400).json({
          error: 'BadRequest',
          message: 'min_priority must be an integer'
        });
      }

      const limit = 100;
      const offset = pageNum * limit;

      let posts: any[];
      let queryType: string;

      if (publisher_did) {
        // Query for posts by the specified publisher
        queryType = 'publisher';
        posts = await ctx.db
          .selectFrom('post')
          .select([
            'uri',
            'indexedAt',
            'priority',
            'likes_count',
            'repost_count',
            'comments_count',
            sql<number>`COALESCE(priority, 0)`.as('priority_value')
          ])
          .where('author', '=', publisher_did as string)
          .where((eb) => eb('priority', '>=', minPriority))
          .orderBy(sql`COALESCE(priority, 0)`, 'desc')
          .orderBy('indexedAt', 'desc')
          .orderBy('cid', 'desc')
          .offset(offset)
          .limit(limit)
          .execute();
      } else {
        // Query for posts by people the requester follows
        queryType = 'follows';
        const { getFollows } = await import('../util/queries');
        const requesterFollows = await getFollows(requester_did as string, ctx.db);

        posts = await ctx.db
          .selectFrom('post')
          .select([
            'uri',
            'indexedAt',
            'priority',
            'likes_count',
            'repost_count',
            'comments_count',
            sql<number>`COALESCE(priority, 0)`.as('priority_value')
          ])
          .where((eb) => eb('author', 'in', requesterFollows))
          .where((eb) => eb('priority', '>=', minPriority))
          .orderBy(sql`COALESCE(priority, 0)`, 'desc')
          .orderBy('indexedAt', 'desc')
          .orderBy('cid', 'desc')
          .offset(offset)
          .limit(limit)
          .execute();
      }

      console.log(`[${new Date().toISOString()}] - Retrieved ${posts.length} ${queryType} posts with priorities >= ${minPriority}, page ${pageNum}`);

      return res.json({
        count: posts.length,
        page: pageNum,
        limit: limit,
        query_type: queryType,
        requester_did: requester_did || null,
        publisher_did: publisher_did || null,
        min_priority: minPriority,
        posts: posts
      });
    } catch (error) {
      console.error('Error retrieving priorities data:', error);
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred'
      });
    }
  });
}
