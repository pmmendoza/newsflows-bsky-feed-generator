/**
 * Guarded no-store production relay connectivity smoke.
 *
 * This proves that the configured ATProto relay endpoint can deliver valid
 * `com.atproto.sync.subscribeRepos` frames to the local feedgen dependency
 * stack. It does not write to feedgen Postgres and skips unless explicitly
 * enabled.
 *
 * Run:
 *   FEEDGEN_FIREHOSE_RELAY_CONNECTIVITY=1 \
 *     npx ts-node scripts/test_firehose_relay_connectivity.ts
 */

import assert from 'assert'
import {
  runFirehoseRelayConnectivityProof,
} from '../src/rehearsal/firehose-relay-connectivity'

async function main() {
  if (process.env.FEEDGEN_FIREHOSE_RELAY_CONNECTIVITY !== '1') {
    console.log('SKIP: set FEEDGEN_FIREHOSE_RELAY_CONNECTIVITY=1')
    return
  }

  const result = await runFirehoseRelayConnectivityProof({
    serviceUrl:
      process.env.FEEDGEN_SUBSCRIPTION_ENDPOINT ?? 'wss://bsky.network',
    maxFrames: Number(process.env.FEEDGEN_FIREHOSE_RELAY_MAX_FRAMES ?? '3'),
    timeoutMs: Number(process.env.FEEDGEN_FIREHOSE_RELAY_TIMEOUT_MS ?? '15000'),
  })

  assert.equal(result.status, 'ok')
  assert.equal(result.store_mode, 'no_store')
  assert.ok(result.frame_count > 0)
  assert.ok(result.highest_seq > 0)
  assert.equal(result.raw_values_in_output, false)

  console.log(JSON.stringify(result, null, 2))
  console.log('OK: firehose relay connectivity smoke clean')
}

main().catch((err) => {
  console.error('firehose relay connectivity smoke failed:', err)
  process.exit(2)
})
