import { sql } from 'kysely'
import { Server } from '../lexicon'
import { AppContext } from '../config'
import { accessVerdictForState, readFeedAccessStates } from '../util/access-policy'
import {
  executeSubscription,
  inspectSubscription,
  resolveSubscriptionIdentityDbFirst,
  resolveSubscriptionFeed,
  setSubscription,
  SetSubscriptionInput,
  SubscriptionError,
  SubscriptionInput,
} from '../util/exact-subscription'
import { ApiKeyAuthConfig, isApiKeyAuthorized, logUnauthorized } from '../util/api-auth'

const adminAuth: ApiKeyAuthConfig = { primaryEnv: ['FEEDGEN_ADMIN_API_KEY'] }
const readAuth: ApiKeyAuthConfig = { primaryEnv: ['FEEDGEN_READ_API_KEY', 'FEEDGEN_ADMIN_API_KEY'] }
const DID_RE = /^did:(?:plc|web):[a-zA-Z0-9._:%-]{1,256}$/

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

// ---------------------------------------------------------------------------
// List-endpoint helpers (INFRA-WEB-024/026/029/030/032). RT-4: the endpoint
// keeps its existing cursor (integer offset) + limit contract and existing
// scope=omni|assigned|none filter — no page_size/page params.
// ---------------------------------------------------------------------------

const SUBSCRIBER_KINDS = ['participant', 'publisher', 'testing', 'researcher'] as const
type SubscriberKindParam = (typeof SUBSCRIBER_KINDS)[number]

const SORT_COLUMNS: Record<string, string> = {
  handle: 'subscriber.handle',
  did: 'subscriber.did',
  first_subscribed_at: 'subscriber.first_subscribed_at',
  scope_changed_at: 'subscriber.scope_changed_at',
  access_scope: 'subscriber.access_scope',
}
const Q_MAX_LENGTH = 128

function parseIntegerParam(value: unknown, fallback: number, name: string): number {
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

function parseStatus(value: unknown): 'active' | 'former' | 'all' {
  // RT-5: API default stays 'all' to preserve current behavior for the
  // existing console during the deploy window; a new console explicitly
  // requests 'active' or 'former'.
  if (value === undefined) return 'all'
  if (value !== 'active' && value !== 'former' && value !== 'all') {
    throw new SubscriptionError(400, 'invalid_status', 'status must be active, former, or all')
  }
  return value
}

function parseSort(value: unknown): string {
  if (value === undefined) return 'did'
  if (typeof value !== 'string' || !(value in SORT_COLUMNS)) {
    throw new SubscriptionError(400, 'invalid_sort', `sort must be one of ${Object.keys(SORT_COLUMNS).join(', ')}`)
  }
  return value
}

function parseDir(value: unknown): 'asc' | 'desc' {
  if (value === undefined) return 'asc'
  if (value !== 'asc' && value !== 'desc') {
    throw new SubscriptionError(400, 'invalid_dir', 'dir must be asc or desc')
  }
  return value
}

function parseQ(value: unknown): string | undefined {
  if (value === undefined) return undefined
  if (typeof value !== 'string') {
    throw new SubscriptionError(400, 'invalid_q', 'q must be a string')
  }
  if (value.length === 0) return undefined
  // RT-3: bound the pattern length before it ever reaches Postgres.
  if (value.length > Q_MAX_LENGTH) {
    throw new SubscriptionError(400, 'invalid_q', `q must be at most ${Q_MAX_LENGTH} characters`)
  }
  return value
}

// RT-3: q is only ever bound as a parameter (never sql.raw'd) and is probed
// once, inside a transaction-local statement_timeout, before it is trusted.
// Postgres invalid_regular_expression (SQLSTATE 2201B) falls back to a bound
// literal substring match rather than 500ing on a half-typed pattern. The
// SAME resulting fragment is then reused for both the count and the page
// query (RT-7's "one predicate, reused for count and page" rule applies here
// too — a filter must never differ between how many rows exist and which
// rows are returned).
// Returns { predicate } rather than the raw fragment directly: an `async
// function` that `return`s a Kysely expression triggers the engine's
// thenable check as part of Promise resolution, which walks straight into
// Kysely's "don't await a query builder" guard (the RawBuilder proxy throws
// on any `.then` access, including this indirect one) and fails with
// "don't await RawBuilder instances directly". Wrapping in a plain object
// sidesteps the thenable check entirely.
async function buildQPredicate(ctx: AppContext, q: string): Promise<{ predicate: ReturnType<typeof sql<boolean>> }> {
  const regexPredicate = sql<boolean>`(subscriber.handle ~* ${q} OR subscriber.did ~* ${q})`
  try {
    await ctx.db.transaction().execute(async (trx) => {
      await sql`SET LOCAL statement_timeout = '2s'`.execute(trx)
      await trx.selectFrom('subscriber').select('did').where(regexPredicate).limit(1).execute()
    })
    return { predicate: regexPredicate }
  } catch (err: any) {
    if (err?.code === '2201B') {
      // Escape ILIKE's own metacharacters (\, %, _) so the fallback is a TRUE
      // literal substring match — otherwise a % or _ in the (invalid-regex)
      // search term would still act as a SQL wildcard and over-match.
      const escaped = q.replace(/[\\%_]/g, '\\$&')
      const literal = `%${escaped}%`
      return { predicate: sql<boolean>`(subscriber.handle ILIKE ${literal} ESCAPE '\\' OR subscriber.did ILIKE ${literal} ESCAPE '\\')` }
    }
    throw err
  }
}

// RT-7: one parameterized AND predicate, reused for both the count and the
// page query. access_scope='omni' subscribers pass unconditionally (omni
// implicitly carries every feed); an 'assigned' subscriber must have an
// active assignment (active_until IS NULL) for EVERY requested feed_id.
function buildFeedAndPredicate(feedIds: string[]) {
  return sql<boolean>`(
    subscriber.access_scope = 'omni' OR (
      subscriber.access_scope = 'assigned' AND (
        SELECT count(DISTINCT assignment.feed_id)
        FROM feedgen_ops.subscriber_feed_assignment assignment
        WHERE assignment.did = subscriber.did
          AND assignment.active_until IS NULL
          AND assignment.feed_id = ANY(${feedIds}::varchar[])
      ) = ${feedIds.length}
    )
  )`
}

async function resolveFeedIdsOrFailClosed(ctx: AppContext, rkeys: string[]): Promise<string[]> {
  const deduped = Array.from(new Set(rkeys))
  const feedRows = await ctx.db
    .selectFrom('feedgen_ops.feed_catalog')
    .select(['feed_id', 'rkey'])
    .where('rkey', 'in', deduped)
    .execute()
  const known = new Map(feedRows.map((row) => [row.rkey, row.feed_id]))
  const missing = deduped.filter((rkey) => !known.has(rkey))
  // RT-7: fail closed — an unknown rkey must reject the whole request, never
  // be silently dropped (a silent drop would widen the AND filter).
  if (missing.length) {
    throw new SubscriptionError(400, 'unknown_feed', `unknown feed rkey(s): ${missing.join(', ')}`)
  }
  return deduped.map((rkey) => known.get(rkey)!)
}

type SubscriberRow = {
  handle: string
  did: string
  access_scope: 'omni' | 'assigned' | 'none' | null
  first_subscribed_at: string | Date | null
  scope_changed_at: string | Date | null
  kind: SubscriberKindParam | null
}

function subscriberSelect(query: any) {
  return query.select([
    'subscriber.handle',
    'subscriber.did',
    'subscriber.access_scope',
    'subscriber.first_subscribed_at',
    'subscriber.scope_changed_at',
    'subscriber.kind',
  ])
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
      const cursor = parseIntegerParam(req.query?.cursor, 0, 'cursor')
      const limit = parseIntegerParam(req.query?.limit, 100, 'limit')
      if (limit < 1 || limit > 500) {
        throw new SubscriptionError(400, 'invalid_limit', 'limit must be between 1 and 500')
      }
      const status = parseStatus(req.query?.status)
      const sort = parseSort(req.query?.sort)
      const dir = parseDir(req.query?.dir)
      const q = parseQ(req.query?.q)

      // Back-compat: an empty singular `feed=` meant "no filter" (falsy) on
      // origin/main; treat it as absent so a WebUI "all feeds" control that
      // submits feed="" gets the unfiltered list, not a 400 (feed[] arrays and
      // non-empty values are unaffected).
      const rawFeed = req.query?.feed === '' ? undefined : req.query?.feed
      const usingNewParams = sort !== 'did' || dir !== 'asc' || q !== undefined || Array.isArray(rawFeed)
      let legacyFeed: Awaited<ReturnType<typeof resolveSubscriptionFeed>> | null = null
      let andFeedIds: string[] | null = null
      if (Array.isArray(rawFeed)) {
        if (!rawFeed.every((v) => typeof v === 'string')) {
          throw new SubscriptionError(400, 'invalid_feed', 'feed entries must be strings')
        }
        andFeedIds = await resolveFeedIdsOrFailClosed(ctx, rawFeed as string[])
      } else if (rawFeed !== undefined) {
        if (typeof rawFeed !== 'string') {
          throw new SubscriptionError(400, 'invalid_feed', 'feed must be one rkey or feed URL')
        }
        if (usingNewParams) {
          // The legacy singular `feed` param is access-policy-aware (study-only
          // lifecycle etc.) and JS-evaluated; it cannot be composed with the new
          // SQL-composed sort/q/feed[] path without silently changing its
          // semantics. Reject the combination instead of guessing.
          throw new SubscriptionError(
            400,
            'invalid_feed',
            'feed cannot be combined with q/sort/dir or a repeated feed[] filter; use feed[] for the new filtered/sorted list',
          )
        }
        legacyFeed = await resolveSubscriptionFeed(ctx.db, { feed: rawFeed })
      }

      let page: SubscriberRow[]
      let total: number
      let hasMore: boolean

      if (legacyFeed) {
        // ---- legacy JS-evaluated access-policy path (unchanged contract) ----
        let query: any = subscriberSelect(ctx.db.selectFrom('subscriber as subscriber')).orderBy('subscriber.did', 'asc')
        if (scope) query = query.where('subscriber.access_scope', '=', scope)
        if (status === 'active') query = query.where('subscriber.access_scope', '!=', 'none')
        if (status === 'former') query = query.where('subscriber.access_scope', '=', 'none')
        const rows = (await query.execute()) as SubscriberRow[]
        const policyRow = {
          feed_id: legacyFeed.feed_id,
          access_policy_id: legacyFeed.access_policy_id,
          study_id: legacyFeed.study_id ?? null,
          enabled: legacyFeed.enabled,
          retired_at: legacyFeed.retired_at ?? null,
        }
        const states = await readFeedAccessStates(ctx.db, policyRow, rows.map((row) => row.did))
        // ponytail: bounded operator dataset scan; compile the shared predicate to SQL if scale demands it.
        const filtered = rows.filter((row) => accessVerdictForState(
          policyRow,
          states.get(row.did) ?? { scope: null, hasActiveAssignment: false, activeStudy: false },
        ).allowed)
        total = filtered.length
        const candidateRows = filtered.slice(cursor, cursor + limit + 1)
        page = candidateRows.slice(0, limit)
        hasMore = candidateRows.length > limit
      } else {
        // ---- new SQL-composed path: scope + status + q + feed[] + sort ----
        let baseQuery: any = ctx.db.selectFrom('subscriber as subscriber')
        let countQuery: any = ctx.db.selectFrom('subscriber as subscriber')
          .select(({ fn }: any) => fn.countAll().as('count'))
        if (scope) {
          baseQuery = baseQuery.where('subscriber.access_scope', '=', scope)
          countQuery = countQuery.where('subscriber.access_scope', '=', scope)
        }
        if (status === 'active') {
          baseQuery = baseQuery.where('subscriber.access_scope', '!=', 'none')
          countQuery = countQuery.where('subscriber.access_scope', '!=', 'none')
        } else if (status === 'former') {
          baseQuery = baseQuery.where('subscriber.access_scope', '=', 'none')
          countQuery = countQuery.where('subscriber.access_scope', '=', 'none')
        }
        if (andFeedIds) {
          const predicate = buildFeedAndPredicate(andFeedIds)
          baseQuery = baseQuery.where(predicate)
          countQuery = countQuery.where(predicate)
        }
        if (q !== undefined) {
          const { predicate } = await buildQPredicate(ctx, q)
          baseQuery = baseQuery.where(predicate)
          countQuery = countQuery.where(predicate)
        }

        const orderColumn = SORT_COLUMNS[sort]
        // RT-4: NULLS LAST on every sort (the two timestamp columns are
        // nullable for ~934 legacy omni subscribers) plus a deterministic did
        // tiebreaker, so those rows never float to the top on a DESC sort.
        baseQuery = baseQuery.orderBy(sql.raw(`${orderColumn} ${dir.toUpperCase()} NULLS LAST`))
        if (sort !== 'did') baseQuery = baseQuery.orderBy('subscriber.did', 'asc')
        baseQuery = subscriberSelect(baseQuery)

        const totalRow = await countQuery.executeTakeFirstOrThrow()
        total = Number((totalRow as any).count)
        const candidateRows = (await baseQuery.offset(cursor).limit(limit + 1).execute()) as SubscriberRow[]
        page = candidateRows.slice(0, limit)
        hasMore = candidateRows.length > limit
      }

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
          first_subscribed_at: row.first_subscribed_at ?? null,
          scope_changed_at: row.scope_changed_at ?? null,
          kind: row.kind ?? 'participant',
          // Deprecated alias of scope_changed_at, kept for one release for any
          // consumer still reading the pre-INFRA-WEB-024 field name.
          scope_since: row.scope_changed_at ?? null,
        })),
        returned_count: page.length,
        total_count: total,
        next_cursor: hasMore ? cursor + limit : null,
        filters: {
          scope: scope ?? null,
          feed: legacyFeed?.rkey ?? null,
          status,
          q: q ?? null,
          sort,
          dir,
        },
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
      // RT-8: inspect is a readback — resolve DB-first (no AppView spend) so
      // repeated readbacks across a batch don't burn the resolution budget
      // reserved for apply time.
      const identity = await resolveSubscriptionIdentityDbFirst(ctx.db, req.query as SubscriptionInput)
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

  server.xrpc.router.get('/api/admin/subscribers/history', async (req, res) => {
    const endpoint = '/api/admin/subscribers/history'
    if (!isApiKeyAuthorized(req, readAuth)) return unauthorized(res, endpoint)
    try {
      const did = req.query?.did
      if (typeof did !== 'string' || !DID_RE.test(did)) {
        throw new SubscriptionError(400, 'invalid_did', 'did must be a canonical DID')
      }
      const cursor = parseIntegerParam(req.query?.cursor, 0, 'cursor')
      const hasSnapshotBoundary = req.query?.through_assignment_id !== undefined
      if (cursor > 0 && !hasSnapshotBoundary) {
        throw new SubscriptionError(
          400,
          'history_snapshot_required',
          'through_assignment_id from the first page is required when cursor is greater than zero',
        )
      }
      const limit = parseIntegerParam(req.query?.limit, 100, 'limit')
      if (limit < 1 || limit > 200) {
        throw new SubscriptionError(400, 'invalid_limit', 'limit must be between 1 and 200')
      }
      const subscriber = await ctx.db
        .selectFrom('subscriber')
        .select(['did', 'handle'])
        .where('did', '=', did)
        .executeTakeFirst()
      if (!subscriber) throw new SubscriptionError(404, 'identity_not_found', 'subscriber DID was not found')
      const firstPage = !hasSnapshotBoundary
      const throughAssignmentId = firstPage
        ? Number((await ctx.db
          .selectFrom('feedgen_ops.subscriber_feed_assignment as assignment')
          .select('assignment.assignment_id')
          .where('assignment.did', '=', did)
          .orderBy('assignment.assignment_id', 'desc')
          .executeTakeFirst())?.assignment_id ?? 0)
        : parseIntegerParam(req.query?.through_assignment_id, 0, 'through_assignment_id')
      if (!Number.isSafeInteger(throughAssignmentId)) {
        throw new SubscriptionError(500, 'history_snapshot_unavailable', 'history snapshot is unavailable')
      }
      const rows = await ctx.db
        .selectFrom('feedgen_ops.subscriber_feed_assignment as assignment')
        .leftJoin('feedgen_ops.feed_catalog as feed', 'feed.feed_id', 'assignment.feed_id')
        .select([
          'assignment.assignment_id as assignment_id',
          'assignment.feed_id as feed_id',
          'feed.rkey as feed',
          'assignment.active_from as active_from',
          'assignment.active_until as active_until',
          'assignment.status as status',
        ])
        .where('assignment.did', '=', did)
        .where('assignment.assignment_id', '<=', throughAssignmentId)
        .orderBy('assignment.assignment_id', 'desc')
        .offset(cursor)
        .limit(limit + 1)
        .execute()
      const page = rows.slice(0, limit)
      return res.json({
        schema_version: 1,
        did: subscriber.did,
        handle: subscriber.handle,
        assignments: page.map((row) => ({
          assignment_id: row.assignment_id,
          feed: row.feed ?? row.feed_id,
          active_from: row.active_from,
          active_until: row.active_until ?? null,
          status: row.status,
        })),
        next_cursor: rows.length > limit ? cursor + limit : null,
        through_assignment_id: throughAssignmentId,
        generated_at: new Date().toISOString(),
        owner_endpoint: endpoint,
        apply_performed: false,
        raw_values_in_output: false,
      })
    } catch (error) {
      return endpointError(res, error)
    }
  })

  // INFRA-WEB-032: append-only handle-rename transition log. DB-only readback
  // (no identity resolution at all, matching /history's pattern) — the did
  // is required and validated directly.
  server.xrpc.router.get('/api/admin/subscribers/handle-history', async (req, res) => {
    const endpoint = '/api/admin/subscribers/handle-history'
    if (!isApiKeyAuthorized(req, readAuth)) return unauthorized(res, endpoint)
    try {
      const did = req.query?.did
      if (typeof did !== 'string' || !DID_RE.test(did)) {
        throw new SubscriptionError(400, 'invalid_did', 'did must be a canonical DID')
      }
      const cursor = parseIntegerParam(req.query?.cursor, 0, 'cursor')
      const limit = parseIntegerParam(req.query?.limit, 100, 'limit')
      if (limit < 1 || limit > 200) {
        throw new SubscriptionError(400, 'invalid_limit', 'limit must be between 1 and 200')
      }
      const subscriber = await ctx.db
        .selectFrom('subscriber')
        .select(['did', 'handle'])
        .where('did', '=', did)
        .executeTakeFirst()
      if (!subscriber) throw new SubscriptionError(404, 'identity_not_found', 'subscriber DID was not found')
      const rows = await ctx.db
        .selectFrom('subscriber_handle_history')
        .select(['id', 'old_handle', 'new_handle', 'observed_at', 'source'])
        .where('did', '=', did)
        .orderBy('id', 'desc')
        .offset(cursor)
        .limit(limit + 1)
        .execute()
      const page = rows.slice(0, limit)
      return res.json({
        schema_version: 1,
        did: subscriber.did,
        handle: subscriber.handle,
        transitions: page.map((row) => ({
          old_handle: row.old_handle,
          new_handle: row.new_handle,
          observed_at: row.observed_at,
          source: row.source ?? null,
        })),
        next_cursor: rows.length > limit ? cursor + limit : null,
        owner_endpoint: endpoint,
        apply_performed: false,
        raw_values_in_output: false,
      })
    } catch (error) {
      return endpointError(res, error)
    }
  })

  // INFRA-WEB-030: owner setter for special-group membership. RT-1: a
  // researcher can never be demoted through this endpoint — that would be a
  // guard bypass (demote, then mutate the now-unlocked subscription).
  server.xrpc.router.post('/api/admin/subscribers/kind', async (req, res) => {
    const endpoint = '/api/admin/subscribers/kind'
    if (!isApiKeyAuthorized(req, adminAuth)) return unauthorized(res, endpoint)
    try {
      const body = req.body as { did?: unknown; kind?: unknown }
      const did = typeof body.did === 'string' ? body.did.trim() : ''
      if (!DID_RE.test(did)) {
        throw new SubscriptionError(400, 'invalid_did', 'did must be a canonical DID')
      }
      const rawKind = body.kind
      if (typeof rawKind !== 'string' || !(SUBSCRIBER_KINDS as readonly string[]).includes(rawKind)) {
        throw new SubscriptionError(400, 'invalid_kind', `kind must be one of ${SUBSCRIBER_KINDS.join(', ')}`)
      }
      const kind = rawKind as SubscriberKindParam
      const updated = await ctx.db.transaction().execute(async (trx) => {
        const row = await trx
          .selectFrom('subscriber')
          .select(['did', 'kind'])
          .where('did', '=', did)
          .forUpdate()
          .executeTakeFirst()
        if (!row) throw new SubscriptionError(404, 'subscriber_not_found', `subscriber DID was not found: ${did}`)
        if (row.kind === 'researcher' && kind !== 'researcher') {
          throw new SubscriptionError(
            409,
            'researcher_demotion_blocked',
            'researcher accounts cannot be demoted through the kind setter',
          )
        }
        await trx.updateTable('subscriber').set({ kind }).where('did', '=', did).execute()
        return { did, kind }
      })
      return res.json({
        ok: true,
        did: updated.did,
        kind: updated.kind,
        owner_endpoint: endpoint,
        apply_performed: true,
      })
    } catch (error) {
      return endpointError(res, error)
    }
  })

  server.xrpc.router.post('/api/admin/subscribers/plan', async (req, res) => {
    const endpoint = '/api/admin/subscribers/plan'
    if (!isApiKeyAuthorized(req, adminAuth)) return unauthorized(res, endpoint)
    try {
      const body = req.body as SetSubscriptionInput
      // Atomic desired-state preview (apply=false) when a state is supplied.
      if (body.state !== undefined) {
        return res.json(await setSubscription(ctx, body, false))
      }
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
