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
 *
 * Set FEEDGEN_FIREHOSE_RELAY_CURSOR_PROBE=1 to reconnect with the last observed
 * sequence as the cursor and prove the relay returns later sequence-bearing
 * frames without storing events.
 *
 * Set FEEDGEN_FIREHOSE_RELAY_MIN_DURATION_MS to keep the no-store relay
 * connection open until the frame floor and minimum duration are both reached.
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
    cursorProbe:
      process.env.FEEDGEN_FIREHOSE_RELAY_CURSOR_PROBE === '1',
    minDurationMs: Number(
      process.env.FEEDGEN_FIREHOSE_RELAY_MIN_DURATION_MS ?? '0',
    ),
  })

  assert.equal(result.status, 'ok')
  assert.equal(result.store_mode, 'no_store')
  assert.ok(result.frame_count > 0)
  assert.ok(result.highest_seq > 0)
  assert.equal(result.raw_values_in_output, false)
  const minDurationMs = Number(
    process.env.FEEDGEN_FIREHOSE_RELAY_MIN_DURATION_MS ?? '0',
  )
  if (minDurationMs > 0) {
    assert.equal(result.soak_status, 'ok')
    assert.ok(result.soak_observed_duration_ms >= minDurationMs)
    assert.ok(result.soak_frame_count >= result.frame_count)
  }
  if (process.env.FEEDGEN_FIREHOSE_RELAY_CURSOR_PROBE === '1') {
    assert.equal(result.cursor_probe_status, 'ok')
    assert.notEqual(result.cursor_probe_requested_cursor, null)
    assert.notEqual(result.cursor_probe_lowest_seq, null)
    assert.ok(result.cursor_probe_requested_cursor! > 0)
    assert.ok(
      result.cursor_probe_lowest_seq! >= result.cursor_probe_requested_cursor!,
    )
    assert.ok(['inclusive', 'after'].includes(result.cursor_probe_relation))
    assert.ok(result.cursor_probe_frame_count > 0)
  }

  console.log(JSON.stringify(result, null, 2))
  console.log('OK: firehose relay connectivity smoke clean')
}

main().catch((err) => {
  console.error('firehose relay connectivity smoke failed:', err)
  process.exit(2)
})
