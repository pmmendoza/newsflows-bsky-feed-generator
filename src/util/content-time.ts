import { Database } from '../db'
import { ContentTimeClampReason, ContentTimeStatus } from '../db/schema'

export const CONTENT_TIME_VALIDATOR_VERSION_V1 = 'newsflows-content-time/v1'
export const CONTENT_TIME_VALIDATOR_VERSION_V2 = 'newsflows-content-time/v2'
export const CONTENT_TIME_VALIDATOR_VERSION_V3 = 'newsflows-content-time/v3'
export const CONTENT_TIME_VALIDATOR_VERSION = 'newsflows-content-time/v2'

export const SUPPORTED_CONTENT_TIME_VERSIONS = [
  CONTENT_TIME_VALIDATOR_VERSION_V2,
  CONTENT_TIME_VALIDATOR_VERSION_V3,
] as const

export type SupportedContentTimeVersion =
  typeof SUPPORTED_CONTENT_TIME_VERSIONS[number]

export function isSupportedContentTimeVersion(
  version: unknown,
): version is SupportedContentTimeVersion {
  return (
    version === CONTENT_TIME_VALIDATOR_VERSION_V2 ||
    version === CONTENT_TIME_VALIDATOR_VERSION_V3
  )
}

export type ContentTimePolicy = {
  maxFutureSkewMs: number
  maxPastAgeMs: number | null
}

export const CONTENT_TIME_POLICY_V2: ContentTimePolicy = {
  maxFutureSkewMs: 5 * 60 * 1000,
  maxPastAgeMs: null,
}

export type ValidatedContentTime = {
  created_at_source_raw: Buffer
  content_time_utc: string | null
  content_time_status: Exclude<ContentTimeStatus, 'legacy_unknown'>
  content_time_clamp_reason: ContentTimeClampReason | null
  content_time_validator_version: string
  legacy_created_at: string
}

export function validateContentTime(
  raw: string | undefined | null,
  indexedAt: string,
  versionOrPolicy: string | ContentTimePolicy = CONTENT_TIME_VALIDATOR_VERSION_V2,
  policyArg?: ContentTimePolicy,
): ValidatedContentTime {
  let version = CONTENT_TIME_VALIDATOR_VERSION_V2
  let policy = CONTENT_TIME_POLICY_V2

  if (typeof versionOrPolicy === 'string') {
    version = versionOrPolicy as any
    policy = policyArg ?? CONTENT_TIME_POLICY_V2
  } else if (typeof versionOrPolicy === 'object' && versionOrPolicy !== null) {
    policy = versionOrPolicy
    version = CONTENT_TIME_VALIDATOR_VERSION_V2
  }

  if (
    version !== CONTENT_TIME_VALIDATOR_VERSION_V2 &&
    version !== CONTENT_TIME_VALIDATOR_VERSION_V3
  ) {
    throw new Error(`unsupported content-time validator version: ${version}`)
  }

  const receiptMs = Date.parse(indexedAt)
  if (!Number.isFinite(receiptMs)) throw new Error('indexedAt must be valid ISO time')
  const normalizedReceipt = new Date(receiptMs).toISOString()

  // Empty bytes record an absent source value while keeping every classified
  // row distinguishable from untouched legacy rows (which retain NULL).
  const source = Buffer.from(raw ?? '', 'utf8')
  let reason: ContentTimeClampReason | null = null
  let contentMs = NaN
  if (raw == null || raw === '') reason = 'missing'
  else {
    contentMs = Date.parse(raw)
    if (!Number.isFinite(contentMs)) reason = 'unparseable'
    else if (policy.maxPastAgeMs !== null && contentMs < receiptMs - policy.maxPastAgeMs) {
      reason = 'past_bound'
    } else if (version === CONTENT_TIME_VALIDATOR_VERSION_V2) {
      if (contentMs > receiptMs + policy.maxFutureSkewMs) reason = 'future_skew'
    } else if (version === CONTENT_TIME_VALIDATOR_VERSION_V3) {
      if (contentMs > receiptMs) {
        reason = 'future_skew_clamped'
      }
    }
  }

  if (version === CONTENT_TIME_VALIDATOR_VERSION_V3 && reason === 'future_skew_clamped') {
    return {
      created_at_source_raw: source,
      content_time_utc: normalizedReceipt,
      content_time_status: 'source_valid',
      content_time_clamp_reason: 'future_skew_clamped',
      content_time_validator_version: CONTENT_TIME_VALIDATOR_VERSION_V3,
      legacy_created_at: normalizedReceipt,
    }
  }

  const valid = reason === null
  const normalized = valid ? new Date(contentMs).toISOString() : null
  return {
    created_at_source_raw: source,
    content_time_utc: normalized,
    content_time_status: valid ? 'source_valid' : 'source_invalid',
    content_time_clamp_reason: reason,
    content_time_validator_version: version,
    // Compatibility only. Content-age queries use content_time_utc plus status.
    legacy_created_at: normalized ?? normalizedReceipt,
  }
}

export function resolveActiveContentTimeVersionFromRows(
  rows: Array<{
    enabled?: boolean | null
    publisher_time_clock?: string | null
    content_time_contract_version?: string | null
  }>,
): SupportedContentTimeVersion {
  const contentTimeRows = rows.filter(
    (row) => row.enabled === true && row.publisher_time_clock === 'content_time_v1',
  )
  if (contentTimeRows.length === 0) {
    throw new Error('no enabled content_time_v1 feeds found in feed catalog')
  }

  const versions = new Set<string>()
  for (const row of contentTimeRows) {
    const version = String(row.content_time_contract_version ?? '').trim()
    if (!version) {
      throw new Error('enabled content_time_v1 feed is missing content_time_contract_version')
    }
    versions.add(version)
  }

  if (versions.size > 1) {
    throw new Error(
      `mixed content-time contract versions in enabled catalog feeds: ${[...versions].join(', ')}`,
    )
  }

  const [version] = versions
  if (!isSupportedContentTimeVersion(version)) {
    throw new Error(`unsupported content-time contract version in catalog: ${version}`)
  }

  return version
}

const CONTRACT_CACHE_TTL_MS = 60_000

type CachedContract = {
  version: SupportedContentTimeVersion
  expiresAtMs: number
  generation: number
}

let cachedContract: CachedContract | null = null
let currentCacheGeneration = 0

export function invalidateActiveContentTimeContractCache(): void {
  currentCacheGeneration += 1
  cachedContract = null
}

export async function resolveActiveContentTimeContract(
  db: Database,
): Promise<SupportedContentTimeVersion> {
  const currentGen = currentCacheGeneration
  if (
    cachedContract &&
    cachedContract.generation === currentGen &&
    cachedContract.expiresAtMs > Date.now()
  ) {
    return cachedContract.version
  }

  const rows = await db
    .selectFrom('feedgen_ops.feed_catalog')
    .select(['enabled', 'publisher_time_clock', 'content_time_contract_version'])
    .execute()

  const version = resolveActiveContentTimeVersionFromRows(rows)
  if (currentCacheGeneration !== currentGen) {
    return resolveActiveContentTimeContract(db)
  }
  cachedContract = {
    version,
    expiresAtMs: Date.now() + CONTRACT_CACHE_TTL_MS,
    generation: currentGen,
  }
  return version
}
