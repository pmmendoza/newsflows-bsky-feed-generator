import { PublisherPostMaxAgeSource } from '../db/schema'
import { rkeyToEnvSuffix } from './ranker-priority-helper'

export const PUBLISHER_POST_MAX_AGE_DAYS_MIN = 1
export const PUBLISHER_POST_MAX_AGE_DAYS_MAX = 365
const MAX_COMPATIBILITY_HOURS = PUBLISHER_POST_MAX_AGE_DAYS_MAX * 24

export type PublisherServingWindow = {
  effectiveHours: number
  effectiveDays: number
  source: PublisherPostMaxAgeSource | 'compatibility_feed_env' | 'compatibility_global_env' | 'compatibility_default'
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
  rkey: string,
  catalogDays: number | null | undefined,
  catalogSource: PublisherPostMaxAgeSource | null | undefined,
): PublisherServingWindow {
  if (isPublisherPostMaxAgeDays(catalogDays) && catalogSource) {
    return {
      effectiveHours: catalogDays * 24,
      effectiveDays: catalogDays,
      source: catalogSource,
      compatibilityFallbackActive: false,
      compatibilityEnvKey: null,
    }
  }

  const feedEnvKey = `FEEDGEN_SERVING_TIME_HOURS_${rkeyToEnvSuffix(rkey)}`
  const feedHours = parseStrictPositiveHours(process.env[feedEnvKey])
  if (feedHours !== null) {
    return {
      effectiveHours: feedHours,
      effectiveDays: feedHours / 24,
      source: 'compatibility_feed_env',
      compatibilityFallbackActive: true,
      compatibilityEnvKey: feedEnvKey,
    }
  }

  const globalHours = parseStrictPositiveHours(process.env.ENGAGEMENT_TIME_HOURS)
  const effectiveHours = globalHours ?? 72
  return {
    effectiveHours,
    effectiveDays: effectiveHours / 24,
    source: globalHours === null ? 'compatibility_default' : 'compatibility_global_env',
    compatibilityFallbackActive: true,
    compatibilityEnvKey: globalHours === null ? null : 'ENGAGEMENT_TIME_HOURS',
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
