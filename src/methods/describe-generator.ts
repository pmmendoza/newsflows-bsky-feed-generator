import { Server } from '../lexicon'
import { AppContext } from '../config'
import algos from '../algos'
import { AtUri } from '@atproto/syntax'

/**
 * describeFeedGenerator — Sprint 6 Lane D rewrite (2026-05-03).
 *
 * Background: prior to Sprint 6, this endpoint enumerated feeds from
 * `Object.keys(algos)` — the static handler registry under
 * `src/algos/index.ts`. Adding or retiring a feed required a code
 * change + redeploy. Sprint 6 introduces `feedgen_ops.feed_catalog`
 * (migration 003) as the data-driven source of feed identity. Adding
 * a feed becomes a catalog row insert, plus (only if the algo policy
 * is new) a handler addition.
 *
 * Compatibility:
 *   - When env `FEEDGEN_DESCRIBE_FROM_CATALOG=true`, the endpoint reads
 *     enabled rows from `feedgen_ops.feed_catalog`.
 *   - When the env is unset / 'false', behaviour is unchanged: the
 *     static handler registry is enumerated. This is the default
 *     during the Sprint 6 cut-over window so an operator can verify
 *     catalog parity before flipping the flag.
 *   - When the catalog query fails OR returns zero rows, the endpoint
 *     logs a warning and falls back to the static registry. This is
 *     a safety net so a misconfigured catalog never empties the feed
 *     directory in production.
 *
 * Cross-check guarantee: every enumerated feed must have a known
 * handler in the static `algos` registry. Catalog rows whose `rkey`
 * is unknown are dropped with a warning. The matching reverse warning
 * (handler present, no catalog row) is logged once at startup if the
 * env flag is on.
 *
 * Plan reference:
 *   - dev/storage/plan_storage_refactor/plan_feed_catalog_and_registry.md
 *     "Lane D: describe-generator and access-check rewrite".
 *   - migration 003_feed_catalog_and_study_registry.sql.
 */

const useCatalog = (): boolean =>
  String(process.env.FEEDGEN_DESCRIBE_FROM_CATALOG ?? '').toLowerCase() === 'true'

let parityWarningLogged = false

async function readCatalogRkeys(ctx: AppContext): Promise<string[] | null> {
  try {
    const rows = await ctx.db
      .selectFrom('feedgen_ops.feed_catalog')
      .select('rkey')
      .where('enabled', '=', true)
      .execute()
    return rows.map((r) => r.rkey)
  } catch (err) {
    console.warn(
      `[${new Date().toISOString()}] - describeFeedGenerator: feed_catalog query failed; falling back to static registry. error=${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    return null
  }
}

function staticRkeys(): string[] {
  return Object.keys(algos)
}

function logParityWarningOnce(catalogRkeys: string[]): void {
  if (parityWarningLogged) return
  parityWarningLogged = true

  const staticSet = new Set(staticRkeys())
  const catalogSet = new Set(catalogRkeys)

  const inCatalogButNoHandler = catalogRkeys.filter((r) => !staticSet.has(r))
  const inHandlerButNoCatalog = staticRkeys().filter((r) => !catalogSet.has(r))

  if (inCatalogButNoHandler.length > 0) {
    console.warn(
      `[${new Date().toISOString()}] - describeFeedGenerator: ${inCatalogButNoHandler.length} catalog rows have no handler: ${inCatalogButNoHandler.join(',')}. They will be omitted from describe.`,
    )
  }
  if (inHandlerButNoCatalog.length > 0) {
    console.warn(
      `[${new Date().toISOString()}] - describeFeedGenerator: ${inHandlerButNoCatalog.length} handlers have no catalog row: ${inHandlerButNoCatalog.join(',')}. They will not appear in describe (catalog-driven mode).`,
    )
  }
}

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.describeFeedGenerator(async () => {
    let rkeys = staticRkeys()

    if (useCatalog()) {
      const catalogRkeys = await readCatalogRkeys(ctx)
      if (catalogRkeys && catalogRkeys.length > 0) {
        logParityWarningOnce(catalogRkeys)
        const known = new Set(staticRkeys())
        rkeys = catalogRkeys.filter((r) => known.has(r))
      } else {
        console.warn(
          `[${new Date().toISOString()}] - describeFeedGenerator: catalog returned 0 rows; falling back to static registry`,
        )
      }
    }

    const feeds = rkeys.map((shortname) => ({
      uri: AtUri.make(
        ctx.cfg.publisherDid,
        'app.bsky.feed.generator',
        shortname,
      ).toString(),
    }))

    return {
      encoding: 'application/json',
      body: {
        did: ctx.cfg.serviceDid,
        feeds,
      },
    }
  })
}
