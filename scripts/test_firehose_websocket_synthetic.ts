/**
 * Synthetic firehose WebSocket transport smoke test.
 *
 * This is a disposable-DB rehearsal proof. It starts a local XRPC stream
 * server, sends a real CAR-backed `com.atproto.sync.subscribeRepos#commit`
 * frame over WebSocket, and lets feedgen's FirehoseSubscription.run() consume
 * it through the normal subscription transport path.
 *
 * Run:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *   FEEDGEN_SYNTHETIC_FIREHOSE_WS_REHEARSAL=1 \
 *     npx ts-node scripts/test_firehose_websocket_synthetic.ts
 *
 * Set FEEDGEN_SYNTHETIC_FIREHOSE_WS_RECONNECT=1 to also prove a bounded
 * reconnect/resume pass against the synthetic stream.
 *
 * The test skips unless the base env vars are set, so normal execute smoke runs
 * do not accidentally write synthetic rows to an arbitrary database.
 */

import assert from 'assert'
import {
  runSyntheticFirehoseWebsocketRehearsal,
} from '../src/rehearsal/synthetic-firehose-websocket'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  const enabled = process.env.FEEDGEN_SYNTHETIC_FIREHOSE_WS_REHEARSAL === '1'

  if (!dsn || !enabled) {
    console.log(
      'SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_SYNTHETIC_FIREHOSE_WS_REHEARSAL=1',
    )
    return
  }

  const result = await runSyntheticFirehoseWebsocketRehearsal({
    connectionString: dsn,
    reconnect:
      process.env.FEEDGEN_SYNTHETIC_FIREHOSE_WS_RECONNECT === '1',
  })

  if (process.env.FEEDGEN_SYNTHETIC_FIREHOSE_WS_RECONNECT === '1') {
    assert.equal(result.connection_count, 2)
    assert.equal(result.reconnect_resume_cursor, 20)
    assert.equal(result.cursor, 40)
    assert.equal(result.post_count, 2)
    assert.equal(result.engagement_count, 2)
  }

  console.log(JSON.stringify(result, null, 2))
  console.log('OK: synthetic firehose WebSocket transport smoke clean')
}

main().catch((err) => {
  console.error('synthetic firehose WebSocket transport smoke failed:', err)
  process.exit(2)
})
