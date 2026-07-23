/**
 * config_activation manifest — pure-logic unit tests (no DB required).
 *
 * Run: `npx ts-node scripts/test_config_activation_manifest.ts`
 *
 * Covers design Verifications 6, 7, and the DID-array requirement from §3:
 *   - resolver parity: buildConfigManifest()'s resolved values equal what
 *     the actual serving code path computes, via the SAME shared resolver
 *     function — including an INVALID ENGAGEMENT_TIME_HOURS, where the
 *     manifest must carry the raw (possibly NaN) value feed-builder.ts uses,
 *     not the normalized display-only value from methods/monitor.ts.
 *   - the dead flag FEEDGEN_PRIORITY_FROM_RANKER_PROD never appears.
 *   - raw-free SENTINEL: a unique sentinel in every secret env (incl. a
 *     credentialed wss://user:pass@host/path?token=SENTINEL subscription
 *     URL) never appears anywhere in the serialized manifest.
 *   - the subscription endpoint is stored as structural parts + a sha256
 *     fingerprint of the full URL, never the raw URL.
 *   - NEWSBOT_*_DID is recorded sorted and deduped.
 *   - identical env -> identical config_hash (dedup precondition);
 *     a changed behavior env -> a different config_hash.
 */
import assert from 'assert'
import { createHash } from 'crypto'
import { Config } from '../src/config'
import { resolveEngagementTimeHours } from '../src/algos/feed-builder'
import {
  buildConfigManifest,
  canonicalJSONStringify,
  computeConfigHash,
} from '../src/util/config-manifest'

let failed = 0
let passed = 0

function check(cond: boolean, label: string, detail?: string) {
  if (cond) {
    passed++
  } else {
    failed++
    console.error(`FAIL: ${label}${detail ? ` — ${detail}` : ''}`)
  }
}

async function withEnv<T>(env: Record<string, string | undefined>, fn: () => T | Promise<T>): Promise<T> {
  const saved: Record<string, string | undefined> = {}
  for (const [key, value] of Object.entries(env)) {
    saved[key] = process.env[key]
    if (value === undefined) delete process.env[key]
    else process.env[key] = value
  }
  try {
    return await fn()
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  }
}

function baseConfig(overrides: Partial<Config> = {}): Config {
  return {
    port: 3000,
    listenhost: 'localhost',
    hostname: 'feedgen.example.com',
    subscriptionEndpoint: 'wss://bsky.network',
    serviceDid: 'did:web:feedgen.example.com',
    publisherDid: 'did:example:alice',
    subscriptionReconnectDelay: 3000,
    subscriptionIdleTimeoutMs: 0,
    readOnlyMode: false,
    autoMigrate: false,
    ...overrides,
  }
}

async function main() {
  // ---- Resolver parity: valid ENGAGEMENT_TIME_HOURS ----
  await withEnv({ ENGAGEMENT_TIME_HOURS: '48' }, () => {
    const manifest = buildConfigManifest(baseConfig())
    check(
      manifest.engagement.time_hours === resolveEngagementTimeHours(),
      'resolver parity: engagement.time_hours (valid input)',
      `manifest=${manifest.engagement.time_hours} resolver=${resolveEngagementTimeHours()}`,
    )
    check(manifest.engagement.time_hours === 48, 'resolver parity: valid input resolves to 48')
  })

  // ---- Resolver parity: INVALID ENGAGEMENT_TIME_HOURS (the divergence R1-F1 found) ----
  await withEnv({ ENGAGEMENT_TIME_HOURS: 'not-a-number' }, () => {
    const manifest = buildConfigManifest(baseConfig())
    const servingValue = resolveEngagementTimeHours()
    check(Number.isNaN(servingValue), 'sanity: raw parseInt on invalid input is NaN (matches feed-builder.ts:51 behavior)')
    check(
      Number.isNaN(manifest.engagement.time_hours) === Number.isNaN(servingValue),
      'resolver parity: manifest matches serving on INVALID input (both NaN) — NOT the normalized 72 from monitor.ts',
    )
  })

  // ---- Dead flag never appears ----
  await withEnv({ FEEDGEN_PRIORITY_FROM_RANKER_PROD: 'false' }, () => {
    const manifest = buildConfigManifest(baseConfig())
    const json = canonicalJSONStringify(manifest)
    check(!json.includes('FEEDGEN_PRIORITY_FROM_RANKER_PROD'), 'dead flag name absent from manifest JSON')
    check(!('priority_from_ranker_prod' in JSON.parse(json)), 'dead flag has no manifest field')
  })

  // ---- Subscription endpoint: structural parts + fingerprint, never the raw URL ----
  await withEnv({}, () => {
    const rawUrl = 'wss://svc-user:svc-pass@relay.example.com:8443/subscribe/reasons?token=abc123secret'
    const manifest = buildConfigManifest(baseConfig({ subscriptionEndpoint: rawUrl }))
    const json = canonicalJSONStringify(manifest)
    check(!json.includes('svc-user'), 'subscription endpoint: userinfo user not in manifest')
    check(!json.includes('svc-pass'), 'subscription endpoint: userinfo password not in manifest')
    check(!json.includes('abc123secret'), 'subscription endpoint: query token not in manifest')
    check(!json.includes(rawUrl), 'subscription endpoint: full raw URL not in manifest')
    check(manifest.subscription.endpoint.scheme === 'wss', 'subscription endpoint: scheme captured')
    check(manifest.subscription.endpoint.host === 'relay.example.com', 'subscription endpoint: host captured')
    check(manifest.subscription.endpoint.port === 8443, 'subscription endpoint: port captured')
    check(manifest.subscription.endpoint.path === '/subscribe/reasons', 'subscription endpoint: path captured')
    check(
      manifest.subscription.endpoint.fingerprint === createHash('sha256').update(rawUrl).digest('hex'),
      'subscription endpoint: fingerprint is sha256 of the exact full URL',
    )
    // Changing only the query/path (host+scheme+port unchanged) must change
    // the fingerprint — a lossy host-only capture would miss this (R2-F1).
    const rawUrl2 = 'wss://svc-user:svc-pass@relay.example.com:8443/subscribe/other?token=abc123secret'
    const manifest2 = buildConfigManifest(baseConfig({ subscriptionEndpoint: rawUrl2 }))
    check(
      manifest.subscription.endpoint.fingerprint !== manifest2.subscription.endpoint.fingerprint,
      'subscription endpoint: fingerprint changes when only the path changes (host-only capture would miss this)',
    )
  })

  // ---- Raw-free SENTINEL across every secret env ----
  await withEnv(
    {
      FEEDGEN_ADMIN_API_KEY: 'SENTINEL-admin-key',
      FEEDGEN_READ_API_KEY: 'SENTINEL-read-key',
      FEEDGEN_MONITOR_API_KEY: 'SENTINEL-monitor-key',
      FEEDGEN_RANKER_API_KEY: 'SENTINEL-ranker-key',
      STUDY_JWT_SECRET: 'SENTINEL-jwt-secret',
      STUDY_TOKEN_API_KEY: 'SENTINEL-study-token-key',
      FEEDGEN_POSTGRES_URL: 'postgres://u:SENTINEL-pg-pass@host:5432/db',
      FEEDGEN_DB_PASSWORD: 'SENTINEL-db-password',
      FEEDGEN_LEGACY_POSTGRES_URL: 'postgres://u:SENTINEL-legacy-pass@host:5432/db',
      FEEDGEN_RESEARCH_DB_URL: 'postgres://u:SENTINEL-research-pass@host:5432/db',
    },
    () => {
      const rawUrl = 'wss://user:pass@relay.example.com/path?token=SENTINEL-subscription-token'
      const manifest = buildConfigManifest(baseConfig({ subscriptionEndpoint: rawUrl }))
      const json = canonicalJSONStringify(manifest)
      const sentinels = [
        'SENTINEL-admin-key',
        'SENTINEL-read-key',
        'SENTINEL-monitor-key',
        'SENTINEL-ranker-key',
        'SENTINEL-jwt-secret',
        'SENTINEL-study-token-key',
        'SENTINEL-pg-pass',
        'SENTINEL-db-password',
        'SENTINEL-legacy-pass',
        'SENTINEL-research-pass',
        'SENTINEL-subscription-token',
      ]
      for (const sentinel of sentinels) {
        check(!json.includes(sentinel), `raw-free: ${sentinel} absent from serialized manifest`)
      }
    },
  )

  // ---- publisher_dids state (DB-derived) is absent; only the static env array appears ----
  await withEnv({}, () => {
    const manifest = buildConfigManifest(baseConfig())
    const json = JSON.parse(canonicalJSONStringify(manifest))
    check(!('publisher_dids' in json), 'DB-derived publisher_dids key absent from manifest (env-only publisher_dids_env is the whitelisted field)')
    check(Array.isArray(json.publisher_dids_env), 'publisher_dids_env present as an array')
  })

  // ---- NEWSBOT_*_DID: sorted + deduped ----
  await withEnv(
    {
      NEWSBOT_NL_DID: 'did:plc:nl-bot',
      NEWSBOT_FR_DID: 'did:plc:fr-bot',
      NEWSBOT_CZ_DID: 'did:plc:nl-bot', // duplicate value under a different key
    },
    () => {
      const manifest = buildConfigManifest(baseConfig())
      const dids = manifest.publisher_dids_env
      check(JSON.stringify(dids) === JSON.stringify([...dids].sort()), 'NEWSBOT_*_DID array is sorted')
      check(new Set(dids).size === dids.length, 'NEWSBOT_*_DID array is deduped')
      check(dids.includes('did:plc:nl-bot') && dids.includes('did:plc:fr-bot'), 'NEWSBOT_*_DID array contains the configured DIDs')
    },
  )

  // ---- Hash stability / change detection (dedup precondition) ----
  await withEnv({ ENGAGEMENT_TIME_HOURS: '72' }, () => {
    const m1 = buildConfigManifest(baseConfig())
    const m2 = buildConfigManifest(baseConfig())
    check(computeConfigHash(m1) === computeConfigHash(m2), 'identical env -> identical config_hash')
  })
  await withEnv({ ENGAGEMENT_TIME_HOURS: '72' }, () => {
    const before = computeConfigHash(buildConfigManifest(baseConfig()))
    process.env.ENGAGEMENT_TIME_HOURS = '96'
    const after = computeConfigHash(buildConfigManifest(baseConfig()))
    check(before !== after, 'changed behavior env -> different config_hash')
  })

  // ---- Canonical JSON: key order in source does not affect hash ----
  {
    const a = { z: 1, a: { y: 2, b: 3 } }
    const b = { a: { b: 3, y: 2 }, z: 1 }
    check(canonicalJSONStringify(a) === canonicalJSONStringify(b), 'canonical JSON is stable under source key-order permutation')
  }

  console.log(`config_activation manifest tests: ${passed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
