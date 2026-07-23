/**
 * `GET /api/admin/config_activation` — read the config_activation history
 * (design §4). Mirrors the `/api/admin/feed_catalog/:rkey/history` endpoint
 * in feed-catalog-admin.ts: same auth, same deterministic ordering, same
 * `{schema_version, ..., returned_count, limit, offset}` pagination shape.
 */
import { Server } from '../lexicon'
import { AppContext } from '../config'
import { logUnauthorized, isApiKeyAuthorized } from '../util/api-auth'
import { monitorReadAuth } from './monitor'

const DEFAULT_LIMIT = 50
const MAX_LIMIT = 200

function parseIntegerQuery(value: unknown, name: string, defaultValue: number, minimum: number): number {
  if (value === undefined) return defaultValue
  if (typeof value !== 'string' || !/^\d+$/.test(value)) {
    throw new Error(`${name} must be a non-negative integer`)
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    throw new Error(`${name} must be ${minimum === 0 ? 'a non-negative' : 'a positive'} integer`)
  }
  return parsed
}

function parsePagination(limit: unknown, offset: unknown) {
  return {
    limit: Math.min(parseIntegerQuery(limit, 'limit', DEFAULT_LIMIT, 1), MAX_LIMIT),
    offset: parseIntegerQuery(offset, 'offset', 0, 0),
  }
}

export default function registerConfigActivationAdminEndpoint(server: Server, ctx: AppContext) {
  server.xrpc.router.get('/api/admin/config_activation', async (req, res) => {
    if (!isApiKeyAuthorized(req, monitorReadAuth)) {
      logUnauthorized('/api/admin/config_activation')
      return res.status(401).json({ error: 'Unauthorized: Invalid API key' })
    }

    let pagination: ReturnType<typeof parsePagination>
    try {
      pagination = parsePagination(req.query?.limit, req.query?.offset)
    } catch (err) {
      return res.status(400).json({ error: err instanceof Error ? err.message : 'invalid pagination' })
    }

    try {
      const activations = await ctx.db
        .selectFrom('feedgen_ops.config_activation')
        .selectAll()
        .orderBy('activated_at', 'desc')
        .orderBy('activation_id', 'desc')
        .limit(pagination.limit)
        .offset(pagination.offset)
        .execute()

      return res.json({
        schema_version: 1,
        activations,
        returned_count: activations.length,
        limit: pagination.limit,
        offset: pagination.offset,
        raw_values_in_output: false,
      })
    } catch (err) {
      console.error(
        `[${new Date().toISOString()}] - config-activation-admin: read error. ${
          err instanceof Error ? err.message : String(err)
        }`,
      )
      return res.status(500).json({ error: 'InternalServerError' })
    }
  })
}
