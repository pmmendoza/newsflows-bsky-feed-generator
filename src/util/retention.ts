import { Database } from '../db'
import { sql } from 'kysely'

type RetentionConfig = {
  enabled: boolean
  postRetentionDays: number
  engagementRetentionDays: number
  deleteBatchSize: number
}

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

// Exact copy of server.ts's pre-existing scheduler-setup gate
// (`process.env.FEEDGEN_RETENTION_ENABLED === 'true'`, case-SENSITIVE),
// extracted to a shared name so server.ts and the config-activation
// manifest read the identical resolved value. Deliberately NOT unified with
// getRetentionConfig().enabled below, which is case-INSENSITIVE
// (`.toLowerCase() === 'true'`) and gates runRetentionOnce()'s own delete
// behavior — that is a pre-existing divergence between "is the scheduler
// even started" and "would a retention pass delete rows", not something
// this refactor may change (serving behavior must stay byte-for-byte
// identical). Both resolved values are recorded in the manifest under
// distinct keys so the divergence is visible, not silently unified away.
export const isRetentionSchedulerEnabledStrict = (): boolean => {
  return process.env.FEEDGEN_RETENTION_ENABLED === 'true'
}

export const getRetentionConfig = (): RetentionConfig => {
  return {
    enabled: getBoolEnv('FEEDGEN_RETENTION_ENABLED', false),
    postRetentionDays: getIntEnv('FEEDGEN_RETENTION_POST_DAYS', 14),
    engagementRetentionDays: getIntEnv('FEEDGEN_RETENTION_ENGAGEMENT_DAYS', 14),
    deleteBatchSize: getIntEnv('FEEDGEN_RETENTION_DELETE_BATCH_SIZE', 50_000),
  }
}

const isoDaysAgo = (days: number): string => {
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString()
}

const numDeleted = (res: any): number => {
  const v =
    res?.numUpdatedOrDeletedRows ??
    res?.rowCount ??
    res?.numDeletedRows ??
    0
  return typeof v === 'bigint' ? Number(v) : Number(v)
}

export async function runRetentionOnce(db: Database): Promise<void> {
  const cfg = getRetentionConfig()
  if (!cfg.enabled) return

  const start = Date.now()
  const postCutoff = isoDaysAgo(cfg.postRetentionDays)
  const engagementCutoff = isoDaysAgo(cfg.engagementRetentionDays)

  console.log(
    `[${new Date().toISOString()}] - Retention: starting (posts>${cfg.postRetentionDays}d, engagement>${cfg.engagementRetentionDays}d, batch=${cfg.deleteBatchSize})`,
  )

  let totalEngagementDeleted = 0
  while (true) {
    const res = await sql`
      DELETE FROM engagement
      WHERE ctid IN (
        SELECT ctid
        FROM engagement
        WHERE "indexedAt" < ${engagementCutoff}
        LIMIT ${cfg.deleteBatchSize}
      )
    `.execute(db)
    const deleted = numDeleted(res)
    totalEngagementDeleted += deleted
    if (deleted < cfg.deleteBatchSize) break
  }

  let totalPostsDeleted = 0
  while (true) {
    const res = await sql`
      DELETE FROM post
      WHERE ctid IN (
        SELECT ctid
        FROM post
        WHERE "indexedAt" < ${postCutoff}
        LIMIT ${cfg.deleteBatchSize}
      )
    `.execute(db)
    const deleted = numDeleted(res)
    totalPostsDeleted += deleted
    if (deleted < cfg.deleteBatchSize) break
  }

  const durationMs = Date.now() - start
  console.log(
    `[${new Date().toISOString()}] - Retention: done (deleted engagement=${totalEngagementDeleted}, posts=${totalPostsDeleted}) in ${Math.round(
      durationMs / 1000,
    )}s`,
  )
}

