import express from 'express'
import { Kysely, PostgresDialect } from 'kysely'

const bufferModule = require('buffer')
bufferModule.SlowBuffer ??= bufferModule.Buffer

type Scenario = {
  contract: string
  numerator: number
  denominator: number
  projected: number
  projectedV3Valid: number
  projectedV3Invalid: number
  semanticIncompatible: number
}

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

async function main() {
  const scenario: Scenario = {
    contract: 'newsflows-content-time/v2',
    numerator: 80,
    denominator: 100,
    projected: 0,
    projectedV3Valid: 2,
    projectedV3Invalid: 1,
    semanticIncompatible: 0,
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
          projected_v2_future: scenario.projected,
          projected_v3_to_v2_valid: scenario.projectedV3Valid,
          projected_v3_to_v2_invalid: scenario.projectedV3Invalid,
          semantic_incompatible: scenario.semanticIncompatible,
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
    check(payload.validity.projected_v3_to_v2_valid === 2 && payload.validity.projected_v3_to_v2_invalid === 1, 'v2 rollback projection evidence must be explicit and raw-free')
    check(!JSON.stringify(payload).includes('created_at_source_raw'), 'raw provenance must never leave the export')
    const v2EventQuery = queries.find((text) => text.includes('p0.* FROM post p0'))
    check(v2EventQuery !== undefined && v2EventQuery.includes('OFFSET 0'), 'v2 publisher comment query must narrow by publisher before contract filtering')

    const { invalidateActiveContentTimeContractCache } = await import('../src/util/content-time')
    invalidateActiveContentTimeContractCache()
    scenario.contract = 'newsflows-content-time/v3'
    const v3QueryStart = queries.length
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(response.ok && payload.science_eligible === true, 'v3 active contract must be science eligible')
    check(payload.content_time_contract_version === 'newsflows-content-time/v3', 'v3 contract version reflected in response')
    scenario.projected = 2
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(payload.science_eligible === true && payload.validity.projected_v2_future === 2, 'bounded v2 future engagement projection remains eligible and is counted')
    scenario.projected = 0
    scenario.semanticIncompatible = 1
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(payload.science_eligible === false && payload.science_ineligible_reason === 'semantic_incompatible', 'residual affected or unknown-version evidence fails closed')
    scenario.semanticIncompatible = 0

    invalidateActiveContentTimeContractCache()
    scenario.contract = 'unsupported/v2'
    response = await fetch(base + query, { headers })
    payload = await response.json()
    check(payload.science_eligible === false, 'unsupported contract must fail closed')
    check(payload.science_ineligible_reason === 'unresolved_contract_version', 'unresolved contract version raw-free reason reflected')

    invalidateActiveContentTimeContractCache()
    scenario.contract = 'newsflows-content-time/v2'
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
    check(!Object.prototype.hasOwnProperty.call(payload, 'publisher_time_transition_expires_at'), 'response must not advertise retired expiry authority')

    check(queries.some((text) => text.includes("content_time_status = 'source_valid'") && text.includes('content_time_utc')), 'event SQL must enforce valid content time')
    const v3EventQuery = queries.slice(v3QueryStart).find((text) => text.includes('p0.* FROM post p0'))
    check(v3EventQuery !== undefined && v3EventQuery.includes('OFFSET 0'), 'v3 publisher comment query must narrow by publisher before contract filtering')
    const validityQuery = queries.find((text) => text.includes('FROM candidates'))
    check(validityQuery !== undefined && validityQuery.includes('e."indexedAt"') && validityQuery.includes('p."indexedAt"'), 'validity denominator must use bounded receipt time')
    check(!validityQuery.includes('e."indexedAt")::timestamptz >=') && !validityQuery.includes('p."indexedAt")::timestamptz >='), 'validity receipt-time bounds must not cast indexed columns')
    check(validityQuery.includes('v3_to_v2_source_valid') && validityQuery.includes('v3_to_v2_source_invalid'), 'v2 validity SQL must report both projection outcomes')
    check(validityQuery.includes('created_at_source_raw') && validityQuery.includes("interval '1 millisecond'"), 'v2 projection must recompute the policy-bound future-skew limit from stored raw provenance and receipt time')
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
