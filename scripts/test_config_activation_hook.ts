/**
 * config_activation startup hook — retry / degraded-flag / read-only-skip
 * contract (design §2, Verifications 9 and 12), tested WITHOUT a real
 * Postgres.
 *
 * `recordConfigActivation(db, cfg)`'s only touch point on `db` is
 * `db.transaction().execute(callback)` (see util/config-activation.ts); a
 * fake `db` that programs `.execute()` to throw or resolve — without ever
 * invoking `callback` — exercises the real retry/backoff/degraded-flag/
 * background-retry orchestration end-to-end without needing Kysely's real
 * driver internals (which the advisory-lock `sql` call and the actual
 * insert/dedup logic inside that callback DO need — those are covered by
 * the disposable-Postgres rehearsal in scripts/test_config_activation_db.ts
 * instead, per the design's own split between pure-logic and DB-rehearsal
 * verifications).
 *
 * Run: `npx ts-node scripts/test_config_activation_hook.ts`
 */
import assert from 'assert'
import { Config } from '../src/config'
import {
  recordConfigActivation,
  isConfigActivationDegraded,
  resetConfigActivationStateForTests,
} from '../src/util/config-activation'

let failed = 0
let passed = 0

function check(cond: boolean, label: string) {
  if (cond) {
    passed++
  } else {
    failed++
    console.error(`FAIL: ${label}`)
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

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

/** A `db` whose only method the hook calls is `.transaction().execute(cb)`. */
function makeFakeDb(execute: () => Promise<unknown>, onTransactionCalled?: () => void): any {
  return {
    transaction() {
      onTransactionCalled?.()
      return { execute }
    },
  }
}

async function testReadOnlySkip() {
  resetConfigActivationStateForTests()
  let transactionCalled = false
  const db = makeFakeDb(
    async () => {
      throw new Error('should never be called under read-only mode')
    },
    () => {
      transactionCalled = true
    },
  )
  await recordConfigActivation(db, baseConfig({ readOnlyMode: true }))
  check(!transactionCalled, 'read-only mode: db.transaction() is never called')
  check(!isConfigActivationDegraded(), 'read-only mode: degraded flag stays false (nothing to degrade)')
}

async function testImmediateSuccess() {
  resetConfigActivationStateForTests()
  let calls = 0
  const db = makeFakeDb(async () => {
    calls++
    return 'inserted'
  })
  await recordConfigActivation(db, baseConfig(), { fastRetries: 3, fastRetryBaseDelayMs: 5 })
  check(calls === 1, 'immediate success: exactly one attempt made')
  check(!isConfigActivationDegraded(), 'immediate success: degraded flag stays false')
}

async function testFastRetriesExhaustedThenDegraded() {
  resetConfigActivationStateForTests()
  let calls = 0
  const db = makeFakeDb(async () => {
    calls++
    throw new Error('simulated DB outage')
  })
  await recordConfigActivation(db, baseConfig(), {
    fastRetries: 3,
    fastRetryBaseDelayMs: 5,
    fastRetryMaxDelayMs: 10,
    backgroundRetryIntervalMs: 60_000, // won't fire during this test
  })
  check(calls === 3, 'fast retries exhausted: exactly fastRetries attempts made')
  check(isConfigActivationDegraded(), 'fast retries exhausted: degraded flag is true')
  resetConfigActivationStateForTests() // stop the pending background timer
}

async function testBackgroundRetryRecovers() {
  resetConfigActivationStateForTests()
  let calls = 0
  const db = makeFakeDb(async () => {
    calls++
    if (calls <= 2) throw new Error('simulated DB outage')
    return 'inserted' // succeeds on the 3rd attempt (fast retries=2 exhausted, then background retry)
  })
  await recordConfigActivation(db, baseConfig(), {
    fastRetries: 2,
    fastRetryBaseDelayMs: 5,
    fastRetryMaxDelayMs: 10,
    backgroundRetryIntervalMs: 30,
  })
  check(calls === 2, 'background retry: fast retries exhausted before background kicks in')
  check(isConfigActivationDegraded(), 'background retry: degraded=true immediately after fast retries exhaust')

  // Give the background interval (30ms) time to fire and succeed.
  await sleep(200)
  check(calls === 3, 'background retry: background timer made exactly one more attempt')
  check(!isConfigActivationDegraded(), 'background retry: degraded clears to false once persisted')

  // Background timer must stop retrying once persisted (no more calls after a further wait).
  await sleep(100)
  check(calls === 3, 'background retry: timer stops firing once persisted (no extra calls)')
  resetConfigActivationStateForTests()
}

async function main() {
  await testReadOnlySkip()
  await testImmediateSuccess()
  await testFastRetriesExhaustedThenDegraded()
  await testBackgroundRetryRecovers()

  console.log(`config_activation hook tests: ${passed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
