/**
 * Guarded live-relay-to-disposable-DB firehose rehearsal.
 *
 * This is a bounded rebuild proof only. It connects to the configured ATProto
 * relay, passes live commit frames through FirehoseSubscription.handleEvent(),
 * and writes only to FEEDGEN_TEST_DSN. It skips unless explicitly enabled.
 *
 * Run only against disposable Postgres:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *   FEEDGEN_FIREHOSE_LIVE_DB_REHEARSAL=1 \
 *     npx ts-node scripts/test_firehose_live_db_rehearsal.ts
 */

import assert from 'assert'
import {
  runFirehoseLiveDbRehearsal,
} from '../src/rehearsal/firehose-live-db-rehearsal'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  const enabled = process.env.FEEDGEN_FIREHOSE_LIVE_DB_REHEARSAL === '1'

  if (!dsn || !enabled) {
    console.log(
      'SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_FIREHOSE_LIVE_DB_REHEARSAL=1',
    )
    return
  }

  const result = await runFirehoseLiveDbRehearsal({
    connectionString: dsn,
    serviceUrl:
      process.env.FEEDGEN_SUBSCRIPTION_ENDPOINT ?? 'wss://bsky.network',
    minStoredRows: Number(
      process.env.FEEDGEN_FIREHOSE_LIVE_DB_MIN_STORED_ROWS ?? '1',
    ),
    maxFrames: Number(process.env.FEEDGEN_FIREHOSE_LIVE_DB_MAX_FRAMES ?? '200'),
    timeoutMs: Number(
      process.env.FEEDGEN_FIREHOSE_LIVE_DB_TIMEOUT_MS ?? '30000',
    ),
  })

  assert.equal(result.status, 'ok')
  assert.equal(result.store_mode, 'disposable_db')
  assert.equal(result.scoped_ingestion, 'false')
  assert.ok(result.frame_count > 0)
  assert.ok(result.commit_count > 0)
  assert.ok(result.stored_total >= result.min_stored_rows)
  assert.ok(result.post_count + result.engagement_count >= result.stored_total)
  assert.ok(result.cursor_persisted)
  assert.equal(result.raw_values_in_output, false)

  console.log(JSON.stringify(result, null, 2))
  console.log('OK: live firehose disposable DB rehearsal clean')
}

main().catch((err) => {
  console.error('live firehose disposable DB rehearsal failed:', err)
  process.exit(2)
})
