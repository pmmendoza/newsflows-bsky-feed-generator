import express from 'express'
import { Kysely, PostgresDialect } from 'kysely'

const bufferModule = require('buffer')
bufferModule.SlowBuffer ??= bufferModule.Buffer

type Scenario = {
  contract: string
  expires: string
  numerator: number
  denominator: number
}

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

async function main() {
  const scenario: Scenario = {
    contract: 'newsflows-content-time/v2',
    expires: '2099-01-01T00:00:00Z',
    numerator: 80,
    denominator: 100,
  }
  const queries: string[] = []
  const client = {
    query: async (text: string) => {
      queries.push(text)
      if (text.includes('from "feedgen_ops"."feed_catalog"')) {
        return { rows: [{
          feed_id: 'newsflow-be-k',
          publisher_did: 'did:plc:be',
          publisher_time_clock: 'content_time_v1',
          content_time_cutover_min_valid_share: 0.8,
          content_time_contract_version: scenario.contract,
          publisher_time_transition_expires_at: scenario.expires,
          enabled: true,
        }] }
      }
      if (text.includes('FROM candidates')) {
        return { rows: [{
          numerator: scenario.numerator,
          denominator: scenario.denominator,
          source_invalid: scenario.denominator - scenario.numerator,
          legacy_unknown: 0,
          validator_version_mismatch: 0,
        }] }
      }
      if (text.includes('SELECT DISTINCT publisher_did')) {
        return { rows: [{ publisher_did: 'did:plc:be' }] }
      }
      if (text.includes('WITH base AS')) return { rows: [] }
      throw new Error(`unexpected query: ${text}`)
    },
    release() {},
  }
  const pool = { connect: async () => client, end: async () => {} }
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool: pool as any }) })
  const registerMonitorEndpoints = (await import('../src/methods/monitor')).default
  const app = express()
  registerMonitorEndpoints({ xrpc: { router: app } } as any, { db } as any)
  const server = app.listen(0, '127.0.0.1')
  await new Promise<void>((resolve) => server.once('listening', resolve))
  const address = server.address()
  check(address && typeof address === 'object', 'test server must listen')
  const base = `http://127.0.0.1:${address.port}/api/compliance/engagement`
  const headers = { 'api-key': 'contract-test-key' }
  const query = '?feed_id=newsflow-be-k&since=2026-08-01T00:00:00Z&until=2026-08-02T00:00:00Z&scope=publisher&types=like,comment'
  process.env.FEEDGEN_MONITOR_API_KEY = 'contract-test-key'
  try {
    let response = await fetch(base + query, { headers })
    let payload: any = await response.json()
    check(response.ok && payload.science_eligible === true, 'valid active contract must be science eligible')
    check(payload.time_clock === 'content_time_v1', 'response must use canonical clock enum')
    check(payload.validity.numerator === 80 && payload.validity.denominator === 100, 'validity counts must be explicit')

    const { invalidateActiveContentTimeContractCache } = await import('../src/util/content-time')
    invalidateActiveContentTimeContractCache()
    scenario.contract = 'newsflows-content-time/v3'
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(response.ok && payload.science_eligible === true, 'v3 active contract must be science eligible')
    check(payload.content_time_contract_version === 'newsflows-content-time/v3', 'v3 contract version reflected in response')

    invalidateActiveContentTimeContractCache()
    scenario.contract = 'unsupported/v2'
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(payload.science_eligible === false, 'unsupported contract must fail closed')
    check(payload.science_ineligible_reason === 'unresolved_contract_version', 'unresolved contract version raw-free reason reflected')

    invalidateActiveContentTimeContractCache()
    scenario.contract = 'newsflows-content-time/v2'
    scenario.expires = '2000-01-01T00:00:00Z'
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(payload.science_eligible === false, 'expired transition must fail closed')

    scenario.expires = '2099-01-01T00:00:00Z'
    scenario.numerator = 79
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(payload.science_eligible === false, 'below-threshold validity must fail closed')

    scenario.numerator = 0
    scenario.denominator = 0
    response = await fetch(base + query.replace('scope=publisher', 'scope=subscriber_on_publisher'), { headers })
    payload = await response.json()
    check(payload.science_eligible === true, 'an exact empty cohort must be science eligible')
    check(payload.validity.empty_population === true && payload.validity.observed_valid_share === null, 'empty cohort semantics must be explicit')

    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(payload.science_eligible === false && payload.validity.empty_population === false, 'empty publisher scope must remain fail closed')

    response = await fetch(base + '?since=2026-08-01T00:00:00Z&until=2026-08-02T00:00:00Z&scope=publisher&types=like,comment', { headers })
    payload = await response.json()
    check(payload.science_eligible === false && payload.time_clock === 'receipt_time', 'missing feed binding must stay transitional')

    check(queries.some((text) => text.includes("content_time_status = 'source_valid'") && text.includes('content_time_utc')), 'event SQL must enforce valid content time')
    const validityQuery = queries.find((text) => text.includes('FROM candidates'))
    check(validityQuery !== undefined && validityQuery.includes('e."indexedAt"') && validityQuery.includes('p."indexedAt"'), 'validity denominator must use bounded receipt time')
    check(!validityQuery.includes('"indexedAt")::timestamptz'), 'validity receipt-time bounds must not cast indexed columns')
    const receiptQuery = queries.find((text) => text.includes('e."createdAt" AS created_at'))
    check(receiptQuery !== undefined && receiptQuery.includes('p."createdAt" AS created_at'), 'transitional export must query receipt time')
    check(!receiptQuery.includes('"createdAt")::timestamptz'), 'receipt-time bounds must not cast indexed columns')
  } finally {
    delete process.env.FEEDGEN_MONITOR_API_KEY
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
    await db.destroy()
  }
  console.log('compliance engagement contract tests passed')
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
