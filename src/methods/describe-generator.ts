import { Server } from '../lexicon'
import { AppContext } from '../config'
import algos from '../algos'
import { AtUri } from '@atproto/syntax'
import { KNOWN_POLICIES } from '../algos/catalog-dispatch'
import type { Policy } from '../algos/make-handler'

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
 * Safety guarantee: catalog-driven describe enumerates enabled catalog
 * rows directly, but only for algo policies supported by dynamic
 * dispatch. Unsupported policy rows fail closed and are omitted with
 * a warning.
 *
 * Plan reference:
 *   - dev/storage/plan_storage_refactor/plan_feed_catalog_and_registry.md
 *     "Lane D: describe-generator and access-check rewrite".
 *   - migration 003_feed_catalog_and_study_registry.sql.
 */

const useCatalog = (): boolean =>
  String(process.env.FEEDGEN_DESCRIBE_FROM_CATALOG ?? '').toLowerCase() === 'true'

let parityWarningLogged = false

type DescribeCatalogRow = {
  rkey: string
  algo_policy_id: string | null
  enabled?: boolean | null
}

export type DescribeCatalogSelection = {
  rkeys: string[]
  unsupportedRkeys: string[]
  rowCount: number
}

export function selectDescribeRkeysFromCatalogRows(
  rows: DescribeCatalogRow[],
): DescribeCatalogSelection {
  const rkeys: string[] = []
  const unsupportedRkeys: string[] = []

  for (const row of rows) {
    if (row.enabled === false) continue
    const policy = String(row.algo_policy_id ?? '')
    if (!KNOWN_POLICIES.has(policy as Policy)) {
      unsupportedRkeys.push(row.rkey)
      continue
    }
    rkeys.push(row.rkey)
  }

  return { rkeys, unsupportedRkeys, rowCount: rows.length }
}

async function readCatalogSelection(ctx: AppContext): Promise<DescribeCatalogSelection | null> {
  try {
    const rows = await ctx.db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['rkey', 'algo_policy_id', 'enabled'])
      .where('enabled', '=', true)
      .execute()
    return selectDescribeRkeysFromCatalogRows(rows)
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

function logCatalogWarningOnce(selection: DescribeCatalogSelection): void {
  if (parityWarningLogged) return
  parityWarningLogged = true

  if (selection.unsupportedRkeys.length > 0) {
    console.warn(
      `[${new Date().toISOString()}] - describeFeedGenerator: ${selection.unsupportedRkeys.length} enabled catalog rows have unsupported algo_policy_id values and will be omitted: ${selection.unsupportedRkeys.join(',')}.`,
    )
  }
}

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.describeFeedGenerator(async () => {
    let rkeys = staticRkeys()

    if (useCatalog()) {
      const selection = await readCatalogSelection(ctx)
      if (selection && selection.rowCount > 0) {
        logCatalogWarningOnce(selection)
        rkeys = selection.rkeys
      } else if (selection) {
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
