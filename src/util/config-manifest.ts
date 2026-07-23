/**
 * config_activation — behavior manifest (design §3).
 *
 * Builds the explicit-whitelist, non-secret "resolved config" object that
 * gets hashed and stored in `feedgen_ops.config_activation.config` on every
 * process start (see util/config-activation.ts for the write path).
 *
 * THE LOAD-BEARING RULE: every field below is produced by calling the SAME
 * shared resolver function the corresponding serving/describe/subscribe/
 * study/ingestion code path calls — never a separate re-parse of the env
 * var. That is what makes this manifest trustworthy: it can't drift from
 * what serving actually does, because it IS what serving actually does.
 *
 * Deliberately excluded (see CONFIG_MANIFEST_EXCLUDED_ENV_KEYS below for the
 * full classified list with reasons):
 *   - all secrets (API keys, JWT secret, DSNs/DB passwords)
 *   - DB-derived, transiently-failable state (publisher_dids from the
 *     feed_catalog table — that's a live /api/config overlay only)
 *   - live stats (counts, warnings, uptime)
 *   - the dead flag FEEDGEN_PRIORITY_FROM_RANKER_PROD (migration 024 made
 *     useRankerPriority() unconditionally true)
 *   - network bind config (FEEDGEN_PORT/LISTENHOST) — doesn't affect served
 *     content, unlike everything else here
 *   - the archive-worker process's own env (separate process/entrypoint)
 *
 * The subscription endpoint is stored as structural parts (scheme/host/
 * port/path) plus a sha256 fingerprint of the full URL — never the raw URL,
 * which may carry userinfo/query credentials.
 */
import { createHash } from 'crypto'
import { Config } from '../config'
import { resolveEngagementTimeHours, archiveOutboxEnabled } from '../algos/feed-builder'
import { freshnessHours } from '../algos/ranker-priority-helper'
import { killSwitchDisabled as politicianFilterKillSwitchDisabled } from '../algos/politician-filter'
import { scoreSourceRefreshMs } from './score-source-cache'
import {
  scopedIngestionEnabled,
  trackSubscriberActivityEnabled,
  restrictPublisherEngagementToSubscribersEnabled,
  allowlistRefreshMs,
} from './ingestion-scope'
import { getRetentionConfig, isRetentionSchedulerEnabledStrict } from './retention'
import { followsUpdateIntervalMs, retentionIntervalMs } from './scheduled-updater'
import { catalogListenNotifyEnabled } from './catalog-listener'
import { getPublisherDidsFromEnv } from './publisher-dids'
import { ttlSeconds as subscriptionTokenTtlSeconds } from '../methods/subscribe'
import { parseTtlSeconds as studyJwtTtlSeconds, getComplianceSummaryRateLimitConfig } from '../methods/study'
import { shouldUseCatalogForDescribe, staticRkeys } from '../methods/describe-generator'

export const CONFIG_MANIFEST_SCHEMA_VERSION = 1

function subscriptionEndpointManifest(endpoint: string) {
  const fingerprint = createHash('sha256').update(endpoint).digest('hex')
  try {
    const u = new URL(endpoint)
    return {
      scheme: u.protocol ? u.protocol.replace(/:$/, '') : null,
      host: u.hostname || null,
      port: u.port ? Number(u.port) : null,
      path: u.pathname || '/',
      fingerprint,
    }
  } catch (error) {
    return { scheme: null, host: null, port: null, path: null, fingerprint }
  }
}

export function buildConfigManifest(cfg: Config) {
  return {
    schema_version: CONFIG_MANIFEST_SCHEMA_VERSION,
    service: {
      service_did: cfg.serviceDid,
      hostname: cfg.hostname,
      publisher_did: cfg.publisherDid,
    },
    runtime: {
      read_only_mode: Boolean(cfg.readOnlyMode),
      auto_migrate: Boolean(cfg.autoMigrate),
    },
    subscription: {
      endpoint: subscriptionEndpointManifest(cfg.subscriptionEndpoint),
      reconnect_delay_ms: cfg.subscriptionReconnectDelay,
      idle_timeout_ms: cfg.subscriptionIdleTimeoutMs,
      token_ttl_seconds: subscriptionTokenTtlSeconds(),
    },
    study: {
      jwt_ttl_seconds: studyJwtTtlSeconds(),
      compliance_rate_limit: getComplianceSummaryRateLimitConfig(),
    },
    describe: {
      describe_from_catalog: shouldUseCatalogForDescribe(staticRkeys()),
    },
    engagement: {
      time_hours: resolveEngagementTimeHours(),
    },
    politician_filter: {
      kill_switch_disabled: politicianFilterKillSwitchDisabled(),
    },
    ranker: {
      freshness_hours: freshnessHours(),
      score_source_refresh_ms: scoreSourceRefreshMs(),
    },
    ingestion: {
      scoped_ingestion_enabled: scopedIngestionEnabled(),
      track_subscriber_activity: trackSubscriberActivityEnabled(),
      publisher_engagement_subscriber_only: restrictPublisherEngagementToSubscribersEnabled(),
      allowlist_refresh_ms: allowlistRefreshMs(),
    },
    retention: getRetentionConfig(),
    scheduling: {
      follows_update_interval_ms: followsUpdateIntervalMs(),
      retention_scheduler_enabled_strict: isRetentionSchedulerEnabledStrict(),
      retention_interval_ms: retentionIntervalMs(),
    },
    archive_outbox: {
      enabled: archiveOutboxEnabled(),
    },
    catalog_listener: {
      listen_notify_enabled: catalogListenNotifyEnabled(),
    },
    // Static NEWSBOT_*_DID env fallback (distinct from the DB-derived
    // feed_catalog.publisher_did state, which is excluded — see module
    // doc). Sorted + deduped: env/compose enumeration order must not
    // produce spurious config-hash churn on an otherwise-identical config.
    publisher_dids_env: Array.from(new Set(getPublisherDidsFromEnv())).sort(),
    build: {
      build_sha: process.env.FEEDGEN_BUILD_SHA || null,
      image_id: process.env.FEEDGEN_IMAGE_ID || null,
      feed_code_hash: process.env.FEEDGEN_FEED_CODE_HASH || null,
      ranker_code_hash: process.env.FEEDGEN_RANKER_CODE_HASH || null,
    },
  }
}

export type ConfigManifest = ReturnType<typeof buildConfigManifest>

/** Recursively sort object keys; arrays keep their (already-normalized) order. */
export function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize)
  if (value !== null && typeof value === 'object' && !(value instanceof Date)) {
    const input = value as Record<string, unknown>
    const sorted: Record<string, unknown> = {}
    for (const key of Object.keys(input).sort()) {
      sorted[key] = canonicalize(input[key])
    }
    return sorted
  }
  return value
}

export function canonicalJSONStringify(value: unknown): string {
  return JSON.stringify(canonicalize(value))
}

export function computeConfigHash(manifest: unknown): string {
  return createHash('sha256').update(canonicalJSONStringify(manifest)).digest('hex')
}

// ---------------------------------------------------------------------------
// Completeness-audit support (design Verification 11). CONFIG_MANIFEST_ENV_KEYS
// lists every literal env var name read (directly or via a shared resolver)
// by buildConfigManifest() above. CONFIG_MANIFEST_EXCLUDED_ENV_KEYS lists
// every OTHER behavior-relevant env var read in the serving/describe/
// subscribe/study/ingestion modules, each with a stated reason it is not in
// the manifest. scripts/test_config_activation_completeness.ts scans those
// modules' source for every process.env.KEY / process.env['KEY'] /
// get{Bool,Int}Env('KEY', ...) read and fails if any key is on neither list —
// so a new unclassified behavior env fails the build instead of silently
// falling behind the manifest.
// ---------------------------------------------------------------------------

export const CONFIG_MANIFEST_ENV_KEYS: readonly string[] = [
  'FEEDGEN_SUBSCRIPTION_ENDPOINT',
  'FEEDGEN_SUBSCRIPTION_RECONNECT_DELAY',
  'FEEDGEN_SUBSCRIPTION_IDLE_TIMEOUT_MS',
  'FEEDGEN_SERVICE_DID',
  'FEEDGEN_HOSTNAME',
  'FEEDGEN_PUBLISHER_DID',
  'FEEDGEN_READ_ONLY_MODE',
  'FEEDGEN_AUTO_MIGRATE',
  'SUBSCRIPTION_TOKEN_TTL_SECONDS',
  'STUDY_JWT_TTL_SECONDS',
  'STUDY_COMPLIANCE_RATE_LIMIT_MAX',
  'STUDY_COMPLIANCE_RATE_LIMIT_WINDOW_SECONDS',
  'FEEDGEN_DESCRIBE_FROM_CATALOG',
  'ENGAGEMENT_TIME_HOURS',
  'FEEDGEN_BE_POLITICIAN_FILTER',
  'FEEDGEN_RANKER_PROD_FRESHNESS_HOURS',
  'FEEDGEN_SCORE_SOURCE_REFRESH_MS',
  'FEEDGEN_SCOPED_INGESTION',
  'FEEDGEN_TRACK_SUBSCRIBER_ACTIVITY',
  'FEEDGEN_PUBLISHER_ENGAGEMENT_SUBSCRIBER_ONLY',
  'FEEDGEN_ALLOWLIST_REFRESH_MS',
  'FEEDGEN_RETENTION_ENABLED',
  'FEEDGEN_RETENTION_POST_DAYS',
  'FEEDGEN_RETENTION_ENGAGEMENT_DAYS',
  'FEEDGEN_RETENTION_DELETE_BATCH_SIZE',
  'FOLLOWS_UPDATE_INTERVAL_MS',
  'FEEDGEN_RETENTION_INTERVAL_MS',
  'FEEDGEN_ARCHIVE_OUTBOX_ENABLED',
  'FEEDGEN_CATALOG_LISTEN_NOTIFY',
  'FEEDGEN_BUILD_SHA',
  'FEEDGEN_IMAGE_ID',
  'FEEDGEN_FEED_CODE_HASH',
  'FEEDGEN_RANKER_CODE_HASH',
]

// NEWSBOT_*_DID is a dynamic-key pattern (one env var per country bot, e.g.
// NEWSBOT_NL_DID) matched by pattern rather than enumerated individually.
export const CONFIG_MANIFEST_DYNAMIC_ENV_KEY_PATTERNS: readonly RegExp[] = [/^NEWSBOT_[A-Z0-9_]*_DID$/]

export const CONFIG_MANIFEST_EXCLUDED_ENV_KEYS: Readonly<Record<string, string>> = {
  FEEDGEN_PRIORITY_FROM_RANKER_PROD:
    'dead flag — migration 024 made useRankerPriority() unconditionally return true; including it would misrepresent behavior',
  FEEDGEN_POSTGRES_URL: 'secret — credential-bearing DSN',
  FEEDGEN_DB_HOST: 'DB connection component; excluded with the rest of the DSN group (see FEEDGEN_POSTGRES_URL)',
  FEEDGEN_DB_PORT: 'DB connection component; excluded with the rest of the DSN group',
  FEEDGEN_DB_USER: 'DB connection component; excluded with the rest of the DSN group',
  FEEDGEN_DB_PASSWORD: 'secret',
  FEEDGEN_DB_DATABASE: 'DB connection component; excluded with the rest of the DSN group',
  FEEDGEN_LEGACY_POSTGRES_URL: 'secret — credential-bearing DSN',
  FEEDGEN_LEGACY_DB_HOST: 'DB connection component; excluded with the rest of the DSN group',
  FEEDGEN_LEGACY_DB_PORT: 'DB connection component; excluded with the rest of the DSN group',
  FEEDGEN_LEGACY_DB_USER: 'DB connection component; excluded with the rest of the DSN group',
  FEEDGEN_LEGACY_DB_PASSWORD: 'secret',
  FEEDGEN_LEGACY_DB_DATABASE: 'DB connection component; excluded with the rest of the DSN group',
  FEEDGEN_RESEARCH_DB_URL: 'secret — credential-bearing DSN (archive-worker process only, not the serving process)',
  FEEDGEN_PORT: 'network bind config — does not affect served content',
  FEEDGEN_LISTENHOST: 'network bind config — does not affect served content',
  STUDY_JWT_SECRET: 'secret',
  STUDY_TOKEN_API_KEY: 'secret (API key)',
  FEEDGEN_ADMIN_API_KEY: 'secret (API key)',
  FEEDGEN_READ_API_KEY: 'secret (API key)',
  FEEDGEN_MONITOR_API_KEY: 'secret (API key)',
  FEEDGEN_RANKER_API_KEY: 'secret (API key)',
  FEEDGEN_ARCHIVE_WORKER_BATCH_SIZE: 'archive-worker is a separate process/entrypoint (yarn archiveWorker), not the feedgen serving process this activation record covers',
  FEEDGEN_ARCHIVE_WORKER_IDLE_MS: 'archive-worker is a separate process; see FEEDGEN_ARCHIVE_WORKER_BATCH_SIZE',
  FEEDGEN_ARCHIVE_WORKER_MAX_ATTEMPTS: 'archive-worker is a separate process; see FEEDGEN_ARCHIVE_WORKER_BATCH_SIZE',
}

/** Config (src/config.ts) fields carried into the manifest 1:1 (already resolved by index.ts before cfg is built). */
export const CONFIG_CFG_KEYS_COVERED: readonly (keyof Config)[] = [
  'hostname',
  'subscriptionEndpoint',
  'serviceDid',
  'publisherDid',
  'subscriptionReconnectDelay',
  'subscriptionIdleTimeoutMs',
  'readOnlyMode',
  'autoMigrate',
]

/**
 * Config fields deliberately excluded from the manifest — network bind or
 * DB-connection/secret. Every `keyof Config` must appear in exactly one of
 * CONFIG_CFG_KEYS_COVERED / CONFIG_CFG_KEYS_EXCLUDED (checked by
 * scripts/test_config_activation_completeness.ts); a field satisfying
 * neither is a completeness gap.
 */
export const CONFIG_CFG_KEYS_EXCLUDED: Readonly<Partial<Record<keyof Config, string>>> = {
  port: 'network bind config — does not affect served content',
  listenhost: 'network bind config — does not affect served content',
  postgresUrl: 'secret — credential-bearing DSN',
  pgHost: 'DB connection component; excluded with the DSN group',
  pgPort: 'DB connection component; excluded with the DSN group',
  pgUser: 'DB connection component; excluded with the DSN group',
  pgPassword: 'secret',
  pgDatabase: 'DB connection component; excluded with the DSN group',
  legacyPostgresUrl: 'secret — credential-bearing DSN',
  legacyPgHost: 'DB connection component; excluded with the DSN group',
  legacyPgPort: 'DB connection component; excluded with the DSN group',
  legacyPgUser: 'DB connection component; excluded with the DSN group',
  legacyPgPassword: 'secret',
  legacyPgDatabase: 'DB connection component; excluded with the DSN group',
}
