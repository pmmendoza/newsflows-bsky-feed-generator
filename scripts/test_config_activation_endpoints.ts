/**
 * config_activation HTTP-surface tests — DB-free (fake `db`, real express +
 * http.Server, following the pattern in scripts/test_feed_catalog_admin.ts).
 *
 * Covers:
 *   - GET /api/admin/config_activation: auth, deterministic ordering
 *     (activated_at DESC, activation_id DESC), pagination echo, response
 *     shape {schema_version, activations, returned_count, limit, offset,
 *     raw_values_in_output:false}.
 *   - GET /api/config (design Verifications 10 + 13): retains all existing
 *     fields and adds exactly one new boolean config_activation_degraded;
 *     that flag is served correctly BOTH when healthy AND when every
 *     DB-dependent field throws (the exact outage the flag exists to
 *     report) — degrading those specific fields to null/empty, never a 500.
 *
 * Run: `npx ts-node scripts/test_config_activation_endpoints.ts`
 */
import assert from 'assert'
import express from 'express'
import http from 'http'
import registerMonitorEndpoints from '../src/methods/monitor'
import registerConfigActivationAdminEndpoint from '../src/methods/config-activation-admin'
import {
  recordConfigActivation,
  resetConfigActivationStateForTests,
} from '../src/util/config-activation'
import { invalidatePublisherDidCache } from '../src/util/publisher-dids'
import { Config } from '../src/config'

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

type TableBehavior = { rows?: any[]; throws?: boolean }

/** Minimal chainable fake matching the subset of Kysely's query-builder API these handlers use. */
function makeFakeDb(tables: Record<string, TableBehavior>, calls: { orderBy: [string, string][] } = { orderBy: [] }): any {
  function chain(table: string): any {
    const behavior = tables[table] ?? { rows: [] }
    const builder: any = {
      select: () => builder,
      selectAll: () => builder,
      where: () => builder,
      orderBy: (col: string, dir: string) => {
        calls.orderBy.push([col, dir])
        return builder
      },
      limit: () => builder,
      offset: () => builder,
      async execute() {
        if (behavior.throws) throw new Error(`simulated failure for ${table}`)
        return behavior.rows ?? []
      },
      async executeTakeFirst() {
        if (behavior.throws) throw new Error(`simulated failure for ${table}`)
        return (behavior.rows ?? [])[0]
      },
    }
    return builder
  }
  return { selectFrom: chain }
}

type JsonResponse = { status: number; body: any }

async function requestJson(
  server: http.Server,
  path: string,
  headers: Record<string, string> = {},
): Promise<JsonResponse> {
  const address = server.address()
  assert(address && typeof address === 'object', 'server must listen on a port')
  return new Promise((resolve, reject) => {
    const req = http.request(
      { hostname: '127.0.0.1', port: (address as any).port, path, method: 'GET', headers },
      (res) => {
        let data = ''
        res.setEncoding('utf8')
        res.on('data', (chunk) => (data += chunk))
        res.on('end', () => resolve({ status: res.statusCode ?? 0, body: data ? JSON.parse(data) : null }))
      },
    )
    req.on('error', reject)
    req.end()
  })
}

async function withServer(
  db: any,
  cfg: Config,
  callback: (server: http.Server) => Promise<void>,
) {
  const app = express()
  const ctx = { db, cfg } as any
  const server = { xrpc: { router: app } } as any
  registerMonitorEndpoints(server, ctx)
  registerConfigActivationAdminEndpoint(server, ctx)
  const httpServer = app.listen(0, '127.0.0.1')
  await new Promise<void>((resolve) => httpServer.once('listening', resolve))
  try {
    await callback(httpServer)
  } finally {
    await new Promise<void>((resolve, reject) => httpServer.close((err) => (err ? reject(err) : resolve())))
  }
}

async function testConfigActivationAdminEndpoint() {
  process.env.FEEDGEN_ADMIN_API_KEY = 'ca-admin-key'
  const rows = [
    { activation_id: 3, activated_at: '2026-07-23T02:00:00Z', config_hash: 'h3', prev_config_hash: 'h2', config: { a: 1 }, reason: 'process_start' },
    { activation_id: 2, activated_at: '2026-07-23T01:00:00Z', config_hash: 'h2', prev_config_hash: 'h1', config: { a: 1 }, reason: 'process_start' },
  ]
  const calls: { orderBy: [string, string][] } = { orderBy: [] }
  const db = makeFakeDb({ 'feedgen_ops.config_activation': { rows } }, calls)

  await withServer(db, baseConfig(), async (server) => {
    const unauthorized = await requestJson(server, '/api/admin/config_activation')
    check(unauthorized.status === 401, 'config_activation admin: 401 without api-key')

    const ok = await requestJson(server, '/api/admin/config_activation?limit=10&offset=0', { 'api-key': 'ca-admin-key' })
    check(ok.status === 200, 'config_activation admin: 200 with api-key')
    check(ok.body.schema_version === 1, 'config_activation admin: schema_version')
    check(ok.body.returned_count === 2, 'config_activation admin: returned_count')
    check(ok.body.limit === 10, 'config_activation admin: limit echoed')
    check(ok.body.offset === 0, 'config_activation admin: offset echoed')
    check(ok.body.raw_values_in_output === false, 'config_activation admin: raw_values_in_output false')
    check(Array.isArray(ok.body.activations) && ok.body.activations.length === 2, 'config_activation admin: activations array')
    check(
      JSON.stringify(calls.orderBy) === JSON.stringify([['activated_at', 'desc'], ['activation_id', 'desc']]),
      'config_activation admin: deterministic ORDER BY (activated_at DESC, activation_id DESC)',
      JSON.stringify(calls.orderBy),
    )

    const badLimit = await requestJson(server, '/api/admin/config_activation?limit=0', { 'api-key': 'ca-admin-key' })
    check(badLimit.status === 400, 'config_activation admin: limit=0 rejected')

    const capped = await requestJson(server, '/api/admin/config_activation?limit=99999', { 'api-key': 'ca-admin-key' })
    check(capped.body.limit === 200, 'config_activation admin: limit capped at 200')
  })
}

async function testApiConfigDegradedFlagHealthy() {
  process.env.FEEDGEN_ADMIN_API_KEY = 'ca-admin-key-2'
  resetConfigActivationStateForTests()
  invalidatePublisherDidCache()
  const db = makeFakeDb({
    'feedgen_ops.feed_catalog': { rows: [{ publisher_did: 'did:plc:nl-catalog', enabled: true }] },
    subscriber: { rows: [{ count: 5 }] },
    follows: { rows: [{ count: 10 }] },
  })

  await withServer(db, baseConfig(), async (server) => {
    const res = await requestJson(server, '/api/config', { 'api-key': 'ca-admin-key-2' })
    check(res.status === 200, 'api/config healthy: 200')
    check(res.body.config_activation_degraded === false, 'api/config healthy: config_activation_degraded is false')
    check(res.body.service_did === 'did:web:feedgen.example.com', 'api/config healthy: existing field service_did unchanged')
    check(res.body.subscriber_count === 5, 'api/config healthy: subscriber_count from DB')
    check(Array.isArray(res.body.publisher_dids) && res.body.publisher_dids.length === 1, 'api/config healthy: publisher_dids from DB')
    check(Array.isArray(res.body.warnings), 'api/config healthy: warnings field present')
  })
}

async function testApiConfigDegradedFlagDuringDbOutage() {
  process.env.FEEDGEN_ADMIN_API_KEY = 'ca-admin-key-3'
  resetConfigActivationStateForTests()
  invalidatePublisherDidCache()

  // Flip the in-memory degraded flag the way it happens for real: a failed
  // recordConfigActivation with fast retries exhausted immediately.
  const failingHookDb: any = { transaction: () => ({ execute: async () => { throw new Error('db down') } }) }
  await recordConfigActivation(failingHookDb, baseConfig(), { fastRetries: 1, fastRetryBaseDelayMs: 1, backgroundRetryIntervalMs: 60_000 })

  const throwingDb = makeFakeDb({
    'feedgen_ops.feed_catalog': { throws: true },
    subscriber: { throws: true },
    follows: { throws: true },
  })

  await withServer(throwingDb, baseConfig(), async (server) => {
    const res = await requestJson(server, '/api/config', { 'api-key': 'ca-admin-key-3' })
    check(res.status === 200, 'api/config during DB outage: still 200, never 500')
    check(res.body.config_activation_degraded === true, 'api/config during DB outage: config_activation_degraded is true')
    check(res.body.subscriber_count === null, 'api/config during DB outage: subscriber_count degrades to null')
    check(Array.isArray(res.body.publisher_dids) && res.body.publisher_dids.length === 0, 'api/config during DB outage: publisher_dids degrades to empty array')
    check(res.body.service_did === 'did:web:feedgen.example.com', 'api/config during DB outage: pure-env fields still present')
  })

  resetConfigActivationStateForTests()
}

async function main() {
  await testConfigActivationAdminEndpoint()
  await testApiConfigDegradedFlagHealthy()
  await testApiConfigDegradedFlagDuringDbOutage()

  console.log(`config_activation endpoint tests: ${passed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
