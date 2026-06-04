import { Server } from '../lexicon'
import { AppContext } from '../config'
import algos from '../algos'
import { AtUri } from '@atproto/syntax'

/**
 * describeFeedGenerator — Sprint 6 Lane D rewrite (2026-05-03).
 *
 * Current behavior:
 *   - Feed serving is catalog-driven through `src/algos/catalog-dispatch.ts`.
 *   - `src/algos/index.ts` is now an intentionally empty compatibility
 *     boundary after static shim retirement.
 *   - describe therefore validates catalog rows against the dynamic dispatch
 *     policy set, not the old static registry.
 *
 * Compatibility:
 *   - When env `FEEDGEN_DESCRIBE_FROM_CATALOG=true`, or when the static
 *     registry is empty, the endpoint reads rows from
 *     `feedgen_ops.feed_catalog`.
 *   - When the env is unset / 'false' and a non-empty static registry exists,
 *     behaviour stays compatible with the old static registry path.
 *   - When the catalog query fails OR returns zero rows, the endpoint
 *     logs a warning and falls back to the static registry. This is
 *     a safety net so a misconfigured catalog never empties the feed
 *     directory in production while the static registry still exists.
 *
 * Plan reference:
 *   - dev/storage/plan_storage_refactor/plan_feed_catalog_and_registry.md
 *     "Lane D: describe-generator and access-check rewrite".
 *   - migration 003_feed_catalog_and_study_registry.sql.
 */

const DESCRIBE_SUPPORTED_POLICIES = new Set([
  'chronological',
  'ranker-priority',
  'engagement-sorted',
])

export type DescribeCatalogRow = {
  rkey: string | null
  enabled: boolean | null
  algo_policy_id: string | null
}

let parityWarningLogged = false

function staticRkeys(): string[] {
  return Object.keys(algos)
}

export function shouldUseCatalogForDescribe(
  staticRegistryRkeys: string[],
  env: Record<string, string | undefined> = process.env,
): boolean {
  return (
    String(env.FEEDGEN_DESCRIBE_FROM_CATALOG ?? '').toLowerCase() === 'true' ||
    staticRegistryRkeys.length === 0
  )
}

export function describeRkeysFromCatalogRows(rows: DescribeCatalogRow[]): {
  rkeys: string[]
  unsupportedPolicyRkeys: string[]
} {
  const rkeys: string[] = []
  const unsupportedPolicyRkeys: string[] = []

  for (const row of rows) {
    const rkey = String(row.rkey ?? '').trim()
    if (!rkey || row.enabled !== true) continue

    const policy = String(row.algo_policy_id ?? '').trim()
    if (!DESCRIBE_SUPPORTED_POLICIES.has(policy)) {
      unsupportedPolicyRkeys.push(rkey)
      continue
    }

    rkeys.push(rkey)
  }

  return { rkeys, unsupportedPolicyRkeys }
}

async function readCatalogRows(ctx: AppContext): Promise<DescribeCatalogRow[] | null> {
  try {
    const rows = await ctx.db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['rkey', 'enabled', 'algo_policy_id'])
      .execute()
    return rows.map((r) => ({
      rkey: r.rkey,
      enabled: r.enabled,
      algo_policy_id: r.algo_policy_id,
    }))
  } catch (err) {
    console.warn(
      `[${new Date().toISOString()}] - describeFeedGenerator: feed_catalog query failed; falling back to static registry. error=${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    return null
  }
}

function logCatalogWarningOnce(result: {
  unsupportedPolicyRkeys: string[]
}): void {
  if (parityWarningLogged) return
  parityWarningLogged = true

  if (result.unsupportedPolicyRkeys.length > 0) {
    console.warn(
      `[${new Date().toISOString()}] - describeFeedGenerator: ${result.unsupportedPolicyRkeys.length} catalog rows have unsupported algo_policy_id values: ${result.unsupportedPolicyRkeys.join(',')}. They will be omitted from describe.`,
    )
  }
}

export default function (server: Server, ctx: AppContext) {
  server.app.bsky.feed.describeFeedGenerator(async () => {
    const registryRkeys = staticRkeys()
    let rkeys = registryRkeys

    if (shouldUseCatalogForDescribe(registryRkeys)) {
      const catalogRows = await readCatalogRows(ctx)
      if (catalogRows && catalogRows.length > 0) {
        const result = describeRkeysFromCatalogRows(catalogRows)
        logCatalogWarningOnce(result)
        rkeys = result.rkeys
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
