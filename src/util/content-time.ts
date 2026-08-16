import { ContentTimeClampReason, ContentTimeStatus } from '../db/schema'

export const CONTENT_TIME_VALIDATOR_VERSION = 'newsflows-content-time/v2'

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
  policy: ContentTimePolicy = CONTENT_TIME_POLICY_V2,
): ValidatedContentTime {
  const receiptMs = Date.parse(indexedAt)
  if (!Number.isFinite(receiptMs)) throw new Error('indexedAt must be valid ISO time')

  // Empty bytes record an absent source value while keeping every classified
  // row distinguishable from untouched legacy rows (which retain NULL).
  const source = Buffer.from(raw ?? '', 'utf8')
  let reason: ContentTimeClampReason | null = null
  let contentMs = NaN
  if (raw == null || raw === '') reason = 'missing'
  else {
    contentMs = Date.parse(raw)
    if (!Number.isFinite(contentMs)) reason = 'unparseable'
    else if (contentMs > receiptMs + policy.maxFutureSkewMs) reason = 'future_skew'
    else if (policy.maxPastAgeMs !== null && contentMs < receiptMs - policy.maxPastAgeMs) reason = 'past_bound'
  }

  const valid = reason === null
  const normalized = valid ? new Date(contentMs).toISOString() : null
  return {
    created_at_source_raw: source,
    content_time_utc: normalized,
    content_time_status: valid ? 'source_valid' : 'source_invalid',
    content_time_clamp_reason: reason,
    content_time_validator_version: CONTENT_TIME_VALIDATOR_VERSION,
    // Compatibility only. Content-age queries use content_time_utc plus status.
    legacy_created_at: normalized ?? new Date(receiptMs).toISOString(),
  }
}
