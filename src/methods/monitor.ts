import { Server } from '../lexicon'
import { AppContext } from '../config'
import { sql } from 'kysely'
import { getPublisherDidsFromEnv } from '../util/ingestion-scope'

type EngagementExportType = 'like' | 'repost' | 'comment' | 'quote'
type EngagementExportScope = 'union' | 'publisher' | 'subscriber' | 'subscriber_on_publisher'

type EngagementExportEvent = {
  type: EngagementExportType
  event_uri: string
  subject_uri: string
  author_did: string
  created_at: string
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

  server.xrpc.router.get('/api/compliance/engagement', async (req, res) => {
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

    try {
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

      const pageRaw = req.query?.page
      if (pageRaw === undefined) {
        page = 0
      } else {
        const parsed = parseNonNegInt(pageRaw)
        if (parsed === undefined) throw new Error('page must be a non-negative integer')
        page = parsed
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
        // De-dupe
        types = Array.from(new Set(allowed))
        if (types.length === 0) throw new Error('types must include at least one type')
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'BadRequest'
      return res.status(400).json({ error: 'BadRequest', message })
    }

    const offset = page * limit

    // When no publisher DIDs are configured, publisher-target scopes cannot match anything.
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
          type,
          event_uri,
          subject_uri,
          author_did,
          created_at
        FROM deduped
        ORDER BY created_at DESC, event_uri DESC
        LIMIT ${limit} OFFSET ${offset}
      `.execute(ctx.db)

      const response: any = {
        since,
        until,
        scope,
        page,
        limit,
        count: rows.length,
        events: rows,
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
            type,
            event_uri,
            subject_uri,
            author_did,
            created_at
          FROM deduped
          ORDER BY created_at DESC, event_uri DESC
          LIMIT ${limit} OFFSET ${offset}
        `.execute(ctx.db)

        response.other_subscriber_count = other.rows.length
        response.other_subscriber_events = other.rows
      }

      return res.json(response)
    } catch (error) {
      console.error('Error retrieving compliance engagement export:', error)
      return res.status(500).json({
        error: 'InternalServerError',
        message: 'An unexpected error occurred',
      })
    }
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
