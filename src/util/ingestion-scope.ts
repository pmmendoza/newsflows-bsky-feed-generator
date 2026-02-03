import { Database } from '../db'

export type IngestionScope = {
  allowlistedAuthorDids: Set<string>
  publisherDids: Set<string>
  subscriberDids: Set<string>
}

type CachedScope = {
  loadedAtMs: number
  scope: IngestionScope
}

let cached: CachedScope | null = null
let refreshInFlight: Promise<IngestionScope> | null = null

const getBoolEnv = (key: string, defaultValue: boolean): boolean => {
  const raw = process.env[key]
  if (raw === undefined || raw === '') return defaultValue
  return raw.toLowerCase() === 'true'
}

const getIntEnv = (key: string, defaultValue: number): number => {
  const raw = process.env[key]
  if (!raw) return defaultValue
  const n = parseInt(raw, 10)
  return Number.isFinite(n) ? n : defaultValue
}

export const scopedIngestionEnabled = (): boolean => {
  return getBoolEnv('FEEDGEN_SCOPED_INGESTION', false)
}

export const trackSubscriberActivityEnabled = (): boolean => {
  return getBoolEnv('FEEDGEN_TRACK_SUBSCRIBER_ACTIVITY', false)
}

export const restrictPublisherEngagementToSubscribersEnabled = (): boolean => {
  return getBoolEnv('FEEDGEN_PUBLISHER_ENGAGEMENT_SUBSCRIBER_ONLY', false)
}

export const allowlistRefreshMs = (): number => {
  return getIntEnv('FEEDGEN_ALLOWLIST_REFRESH_MS', 60_000)
}

export const getPublisherDidsFromEnv = (): string[] => {
  const dids: string[] = []
  for (const key of Object.keys(process.env)) {
    if (key.startsWith('NEWSBOT_') && key.endsWith('_DID')) {
      const did = process.env[key]
      if (did) dids.push(did)
    }
  }
  return dids
}

export const didFromAtUri = (uri: string | undefined | null): string | null => {
  if (!uri) return null
  const match = /^at:\/\/([^/]+)\//.exec(uri)
  return match ? match[1] : null
}

export async function getIngestionScope(db: Database): Promise<IngestionScope> {
  const now = Date.now()
  const refreshMs = allowlistRefreshMs()

  if (cached && now - cached.loadedAtMs < refreshMs) {
    return cached.scope
  }

  if (refreshInFlight) {
    return refreshInFlight
  }

  refreshInFlight = (async () => {
    const publisherDids = new Set(getPublisherDidsFromEnv())

    const followsRows = await db.selectFrom('follows').select('follows').execute()
    const allowlistedAuthorDids = new Set<string>(followsRows.map((r) => r.follows))
    for (const did of publisherDids) allowlistedAuthorDids.add(did)

    const subscriberDids = new Set<string>()
    if (trackSubscriberActivityEnabled() || restrictPublisherEngagementToSubscribersEnabled()) {
      const subs = await db.selectFrom('subscriber').select('did').execute()
      for (const row of subs) subscriberDids.add(row.did)
    }

    const scope: IngestionScope = {
      allowlistedAuthorDids,
      publisherDids,
      subscriberDids,
    }

    cached = { loadedAtMs: Date.now(), scope }
    return scope
  })()

  try {
    return await refreshInFlight
  } finally {
    refreshInFlight = null
  }
}

