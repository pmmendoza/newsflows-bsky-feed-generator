/**
 * Synthetic firehose ingest smoke test.
 *
 * This is a disposable-DB rehearsal proof. It builds a real CAR-backed
 * `com.atproto.sync.subscribeRepos#commit`, passes it through feedgen's
 * FirehoseSubscription.handleEvent(), and reads back post + engagement rows.
 *
 * Run:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *   FEEDGEN_SYNTHETIC_FIREHOSE_REHEARSAL=1 \
 *     npx ts-node scripts/test_firehose_ingest_synthetic.ts
 *
 * Add `FEEDGEN_SYNTHETIC_FIREHOSE_SCOPED=1` to prove the same path with
 * scoped ingestion enabled and explicit allowlist fixtures.
 *
 * The test skips unless both env vars are set, so normal execute smoke runs do
 * not accidentally write synthetic rows to an arbitrary database.
 */

import {
  runSyntheticFirehoseIngestRehearsal,
} from '../src/rehearsal/synthetic-firehose-ingest'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  const enabled = process.env.FEEDGEN_SYNTHETIC_FIREHOSE_REHEARSAL === '1'
  const scoped = process.env.FEEDGEN_SYNTHETIC_FIREHOSE_SCOPED === '1'

  if (!dsn || !enabled) {
    console.log(
      'SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_SYNTHETIC_FIREHOSE_REHEARSAL=1',
    )
    return
  }

  const result = await runSyntheticFirehoseIngestRehearsal({
    connectionString: dsn,
    scopedIngestion: scoped,
  })

  console.log(JSON.stringify(result, null, 2))
  console.log('OK: synthetic firehose ingest smoke clean')
}

main().catch((err) => {
  console.error('synthetic firehose ingest smoke failed:', err)
  process.exit(2)
})
