import { PublisherPostMaxAgeSource } from '../db/schema'

export const PUBLISHER_POST_MAX_AGE_DAYS_MIN = 1
export const PUBLISHER_POST_MAX_AGE_DAYS_MAX = 365
const MAX_COMPATIBILITY_HOURS = PUBLISHER_POST_MAX_AGE_DAYS_MAX * 24

export type PublisherServingWindow = {
  effectiveHours: number
  effectiveDays: number
  source: PublisherPostMaxAgeSource
  compatibilityFallbackActive: boolean
  compatibilityEnvKey: string | null
}

export function isPublisherPostMaxAgeDays(value: unknown): value is number {
  return Number.isSafeInteger(value)
    && Number(value) >= PUBLISHER_POST_MAX_AGE_DAYS_MIN
    && Number(value) <= PUBLISHER_POST_MAX_AGE_DAYS_MAX
}

export function parseStrictPositiveHours(raw: string | undefined): number | null {
  if (!raw || !/^\d+$/.test(raw)) return null
  const parsed = Number(raw)
  return Number.isSafeInteger(parsed) && parsed > 0 && parsed <= MAX_COMPATIBILITY_HOURS
    ? parsed
    : null
}

export function resolvePublisherServingWindow(
  catalogDays: number | null | undefined,
  catalogSource: PublisherPostMaxAgeSource | null | undefined,
): PublisherServingWindow {
  if (!isPublisherPostMaxAgeDays(catalogDays)
    || (catalogSource !== 'study_default' && catalogSource !== 'feed_override')) {
    throw new Error('publisher serving window requires valid catalog days and provenance')
  }

  return {
    effectiveHours: catalogDays * 24,
    effectiveDays: catalogDays,
    source: catalogSource,
    compatibilityFallbackActive: false,
    compatibilityEnvKey: null,
  }
}

export function cutoffFromHours(referenceTimeMs: number, hours: number): string {
  if (!Number.isFinite(referenceTimeMs) || !Number.isSafeInteger(hours) || hours <= 0 || hours > MAX_COMPATIBILITY_HOURS) {
    throw new Error('publisher serving window cannot produce a valid cutoff')
  }
  const cutoffMs = referenceTimeMs - hours * 60 * 60 * 1000
  if (!Number.isFinite(cutoffMs)) throw new Error('publisher serving cutoff is outside the supported date range')
  return new Date(cutoffMs).toISOString()
}
