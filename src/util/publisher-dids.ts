import { Database } from '../db'

export type PublisherDidSource = 'feed_catalog' | 'env_fallback'

export type PublisherDidInfo = {
  dids: string[]
  source: PublisherDidSource
  error?: string
}

type CachedPublisherDids = {
  expiresAtMs: number
  info: PublisherDidInfo
}

const CACHE_TTL_MS = 60_000
let cached: CachedPublisherDids | null = null

export function invalidatePublisherDidCache(): void {
  cached = null
}

export const getPublisherDidsFromEnv = (): string[] => {
  const dids: string[] = []
  for (const key of Object.keys(process.env)) {
    if (key.startsWith('NEWSBOT_') && key.endsWith('_DID')) {
      const did = process.env[key]
      if (did) dids.push(did)
    }
  }
  return Array.from(new Set(dids))
}

function uniqueEnabledPublisherDids(
  rows: Array<{ publisher_did?: string | null; enabled?: boolean | null }>,
): string[] {
  const dids: string[] = []
  const seen = new Set<string>()
  for (const row of rows) {
    if (row.enabled === false) continue
    const did = String(row.publisher_did ?? '').trim()
    if (!did || seen.has(did)) continue
    seen.add(did)
    dids.push(did)
  }
  return dids
}

function cacheInfo(info: PublisherDidInfo): PublisherDidInfo {
  cached = {
    expiresAtMs: Date.now() + CACHE_TTL_MS,
    info,
  }
  return info
}

export async function resolvePublisherDidInfo(db: Database): Promise<PublisherDidInfo> {
  if (cached && cached.expiresAtMs > Date.now()) {
    return cached.info
  }

  try {
    const rows = await db
      .selectFrom('feedgen_ops.feed_catalog')
      .select(['publisher_did', 'enabled'])
      .where('enabled', '=', true)
      .execute()
    const catalogDids = uniqueEnabledPublisherDids(rows)
    if (catalogDids.length > 0) {
      return cacheInfo({ dids: catalogDids, source: 'feed_catalog' })
    }
  } catch (error) {
    return {
      dids: getPublisherDidsFromEnv(),
      source: 'env_fallback',
      error: error instanceof Error ? error.message : String(error),
    }
  }

  return cacheInfo({ dids: getPublisherDidsFromEnv(), source: 'env_fallback' })
}

export async function resolvePublisherDids(db: Database): Promise<string[]> {
  const info = await resolvePublisherDidInfo(db)
  return info.dids
}
