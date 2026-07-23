/**
 * config_activation — startup hook (design §2).
 *
 * Called once from index.ts, right after `await server.start()`, via
 * `server.recordConfigActivation()` (`FeedGenerator.create()` builds `ctx`
 * locally and does not return it — index.ts only holds `server`, so this
 * module takes `db`/`cfg` directly rather than an `AppContext`).
 *
 * Contract (fail-open — this must never block or crash serving):
 *   - SKIPPED entirely under read-only mode (`cfg.readOnlyMode`): read-only
 *     mode already skips migrations/firehose/subscriber-import/schedulers —
 *     i.e. ALL writes — and may run against a physically read-only replica.
 *   - `activated_at` is captured ONCE, in code, at hook entry — not the
 *     column's `DEFAULT now()` (which would record retry-success time).
 *   - Read-latest / compare / conditional-insert happens in ONE transaction
 *     under `pg_advisory_xact_lock(CONFIG_ACTIVATION_ADVISORY_LOCK_KEY)`, so
 *     two racing process starts can't both insert or fork prev_config_hash.
 *   - Dedup: insert only if the table is empty or the latest row's
 *     config_hash differs from this process's.
 *   - A few fast, bounded, backed-off retries at startup; if still
 *     unpersisted, `configActivationDegraded` flips true and slow (~5 min)
 *     background retries continue for the life of the process until the row
 *     lands. The flag is a plain in-memory boolean — read it via
 *     isConfigActivationDegraded(), which never touches the DB (so it stays
 *     observable through the exact DB outage this retry loop is handling —
 *     see methods/monitor.ts's /api/config handler).
 */
import { sql } from 'kysely'
import { Database } from '../db'
import { Config } from '../config'
import { buildConfigManifest, computeConfigHash } from './config-manifest'

// Arbitrary fixed key, scoped to this feature only (no other
// pg_advisory_xact_lock use exists in this codebase to collide with).
export const CONFIG_ACTIVATION_ADVISORY_LOCK_KEY = 472813009

const DEFAULT_FAST_RETRIES = 5
const DEFAULT_FAST_RETRY_BASE_DELAY_MS = 500
const DEFAULT_FAST_RETRY_MAX_DELAY_MS = 30_000
const DEFAULT_BACKGROUND_RETRY_INTERVAL_MS = 5 * 60 * 1000

export type RecordConfigActivationOptions = {
  fastRetries?: number
  fastRetryBaseDelayMs?: number
  fastRetryMaxDelayMs?: number
  backgroundRetryIntervalMs?: number
}

let degraded = false
let backgroundTimer: NodeJS.Timeout | null = null

/** Pure in-memory read — never touches the DB. Safe to call during a DB outage. */
export function isConfigActivationDegraded(): boolean {
  return degraded
}

/** Test-only: reset module singleton state between scenarios. */
export function resetConfigActivationStateForTests(): void {
  degraded = false
  if (backgroundTimer) {
    clearInterval(backgroundTimer)
    backgroundTimer = null
  }
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

type InsertOutcome = 'inserted' | 'skipped_dedup'

async function insertIfChanged(
  db: Database,
  activatedAt: string,
  manifest: unknown,
  configHash: string,
): Promise<InsertOutcome> {
  return db.transaction().execute(async (trx) => {
    await sql`SELECT pg_advisory_xact_lock(${CONFIG_ACTIVATION_ADVISORY_LOCK_KEY})`.execute(trx)

    const latest = await trx
      .selectFrom('feedgen_ops.config_activation')
      .select(['config_hash'])
      .orderBy('activated_at', 'desc')
      .orderBy('activation_id', 'desc')
      .limit(1)
      .executeTakeFirst()

    if (latest && latest.config_hash === configHash) {
      return 'skipped_dedup'
    }

    const manifestAny = manifest as {
      build: { build_sha: string | null; image_id: string | null; feed_code_hash: string | null; ranker_code_hash: string | null }
    }

    await trx
      .insertInto('feedgen_ops.config_activation')
      .values({
        activated_at: activatedAt,
        build_sha: manifestAny.build.build_sha,
        image_id: manifestAny.build.image_id,
        feed_code_hash: manifestAny.build.feed_code_hash,
        ranker_code_hash: manifestAny.build.ranker_code_hash,
        // jsonb columns need an explicit JSON serialization + ::jsonb cast:
        // node-postgres renders a plain JS value as a Postgres array/text
        // literal, not JSON, without this (see feed-catalog-admin.ts).
        config: sql`${JSON.stringify(manifest)}::jsonb`,
        config_hash: configHash,
        prev_config_hash: latest?.config_hash ?? null,
        reason: 'process_start',
      } as any)
      .execute()

    return 'inserted'
  })
}

/**
 * Record this process's resolved behavior config. Fail-open: resolves once
 * fast retries are exhausted (never rejects), leaving background retries
 * running if persistence hasn't succeeded yet.
 */
export async function recordConfigActivation(
  db: Database,
  cfg: Config,
  opts: RecordConfigActivationOptions = {},
): Promise<void> {
  if (cfg.readOnlyMode) {
    console.log(
      `[${new Date().toISOString()}] - config-activation: skipped (read-only mode)`,
    )
    return
  }

  const fastRetries = opts.fastRetries ?? DEFAULT_FAST_RETRIES
  const fastRetryBaseDelayMs = opts.fastRetryBaseDelayMs ?? DEFAULT_FAST_RETRY_BASE_DELAY_MS
  const fastRetryMaxDelayMs = opts.fastRetryMaxDelayMs ?? DEFAULT_FAST_RETRY_MAX_DELAY_MS
  const backgroundRetryIntervalMs = opts.backgroundRetryIntervalMs ?? DEFAULT_BACKGROUND_RETRY_INTERVAL_MS

  // Captured ONCE, here, at hook entry — inserted explicitly below rather
  // than relying on the column's DEFAULT now() (which would record the
  // retry-success time, not the activation time).
  const activatedAt = new Date().toISOString()
  const manifest = buildConfigManifest(cfg)
  const configHash = computeConfigHash(manifest)

  const attempt = async (): Promise<boolean> => {
    try {
      const outcome = await insertIfChanged(db, activatedAt, manifest, configHash)
      console.log(
        `[${new Date().toISOString()}] - config-activation: ${outcome} config_hash=${configHash}`,
      )
      return true
    } catch (error) {
      console.error(
        `[${new Date().toISOString()}] - config-activation: persist attempt failed. error=${
          error instanceof Error ? error.message : String(error)
        }`,
      )
      return false
    }
  }

  let persisted = false
  for (let i = 0; i < fastRetries && !persisted; i++) {
    if (i > 0) {
      await sleep(Math.min(fastRetryMaxDelayMs, fastRetryBaseDelayMs * 2 ** (i - 1)))
    }
    persisted = await attempt()
  }

  if (persisted) {
    degraded = false
    return
  }

  degraded = true
  console.warn(
    `[${new Date().toISOString()}] - config-activation: DEGRADED — not yet persisted after ${fastRetries} attempts; ` +
      `continuing background retry every ${Math.round(backgroundRetryIntervalMs / 1000)}s for the life of the process`,
  )

  if (backgroundTimer) {
    clearInterval(backgroundTimer)
  }
  backgroundTimer = setInterval(() => {
    attempt()
      .then((ok) => {
        if (ok) {
          degraded = false
          if (backgroundTimer) {
            clearInterval(backgroundTimer)
            backgroundTimer = null
          }
        }
      })
      .catch(() => {
        /* attempt() already logs; keep retrying */
      })
  }, backgroundRetryIntervalMs)
  // Retry runs for the life of the process, but must never be the reason the
  // process can't exit (e.g. in tests or a clean shutdown).
  backgroundTimer.unref?.()
}
