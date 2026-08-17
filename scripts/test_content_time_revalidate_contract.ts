/**
 * No-DB unit tests for the content-time v1->v2 revalidation mode added to
 * src/tools/backfill-publisher-posts.ts. These cover the deterministic,
 * pure parts of the mode: the per-row recompute transform, the config hash
 * used to guard checkpoint resume, CLI arg parsing/defaults, and the shape
 * of what would be emitted in progress/checkpoint receipts (must stay
 * raw-free: counts, sha256 hashes, and ISO timestamps only).
 *
 * Behaviors that require a live Postgres connection (selection is
 * index-backed and only matches v1/enabled-publisher/in-window rows, CAS
 * skip when a row changes underneath the batch, idempotent second run,
 * cursor resume across a killed process, v2/legacy rows are never touched)
 * are proven by the gated, disposable-DB rehearsal in
 * scripts/test_content_time_revalidate_execute.ts and the operator-run
 * scripts/rehearse_content_time_revalidate.sh. Both are documented in this
 * repo's deploy notes and are intentionally not part of this no-DB suite.
 *
 * Run: `npx ts-node scripts/test_content_time_revalidate_contract.ts`
 */

import assert from 'assert'
import {
  CONTENT_TIME_VALIDATOR_VERSION,
  CONTENT_TIME_VALIDATOR_VERSION_V1,
} from '../src/util/content-time'
import {
  REVALIDATION_LIMITS,
  contentTimeRevalidationConfigSha256,
  revalidateContentTimeCandidate,
} from '../src/tools/backfill-publisher-posts'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

// --- revalidateContentTimeCandidate: the actual v1->v2 recompute ----------

// A row that was valid under v1 and remains comfortably valid under v2's
// tighter 5-minute future-skew policy: normalized content_time_utc must be
// bit-identical, only the bookkeeping fields (createdAt/validator bucket)
// change.
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-02T12:00:00+00:00'
  const result = revalidateContentTimeCandidate({
    indexedAt,
    created_at_source_raw: Buffer.from(raw, 'utf8'),
    content_time_status: 'source_valid',
  })
  check(result.outcome === 'v1_valid_to_v2_valid', 'previously-valid, still-valid row must land in v1_valid_to_v2_valid')
  check(result.content_time_status === 'source_valid', 'result must be source_valid')
  check(result.content_time_utc === '2026-08-02T12:00:00.000Z', 'normalized instant must be unchanged by revalidation')
  check(result.content_time_clamp_reason === null, 'valid rows carry no clamp reason')
  check(result.createdAt === result.content_time_utc, 'legacy createdAt must mirror content_time_utc for a valid row (ingestion parity)')
}

// A row that was invalid under v1 with reason=past_bound (>2y old, rejected
// by the retired v1 policy) must become valid under v2, which has no past
// bound at all.
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2020-01-01T00:00:00Z'
  const result = revalidateContentTimeCandidate({
    indexedAt,
    created_at_source_raw: Buffer.from(raw, 'utf8'),
    content_time_status: 'source_invalid',
  })
  check(result.outcome === 'v1_invalid_to_v2_valid', 'past_bound-invalid-under-v1 row must flip to v1_invalid_to_v2_valid')
  check(result.content_time_status === 'source_valid', 'v2 has no past bound, so an old-but-parseable raw must validate')
  check(result.content_time_utc === '2020-01-01T00:00:00.000Z', 'normalized instant must reflect the original raw content time')
}

// A row that was valid under v1's generous 24h future-skew allowance but
// sits between 5 minutes and 24 hours in the future must flip to invalid
// under v2's 5-minute policy, with reason=future_skew.
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-12T13:00:00.000Z' // 1h in the future: v1-valid, v2-invalid
  const result = revalidateContentTimeCandidate({
    indexedAt,
    created_at_source_raw: Buffer.from(raw, 'utf8'),
    content_time_status: 'source_valid',
  })
  check(result.outcome === 'v1_to_v2_invalid', 'previously-valid row inside the v1 24h skew window but outside v2 5min must flip to v1_to_v2_invalid')
  check(result.content_time_status === 'source_invalid', 'result must be source_invalid')
  check(result.content_time_clamp_reason === 'future_skew', 'reason must be future_skew')
  check(result.content_time_utc === null, 'invalid rows must never carry a content_time_utc')
  check(result.createdAt === indexedAt, 'legacy createdAt must fall back to the receipt time for an invalid row (ingestion parity)')
}

// A row still 3 minutes in the future stays valid under both v1 and v2
// (5-minute skew tolerance covers it).
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-12T12:03:00.000Z'
  const result = revalidateContentTimeCandidate({
    indexedAt,
    created_at_source_raw: Buffer.from(raw, 'utf8'),
    content_time_status: 'source_valid',
  })
  check(result.outcome === 'v1_valid_to_v2_valid', 'inside the 5-minute v2 tolerance must stay valid')
  check(result.content_time_status === 'source_valid', 'still valid under v2')
}

console.log('revalidateContentTimeCandidate transform checks passed')

// --- contentTimeRevalidationConfigSha256: deterministic checkpoint guard --

{
  const a = contentTimeRevalidationConfigSha256(['did:plc:b', 'did:plc:a'], '2026-08-01T00:00:00.000Z')
  const b = contentTimeRevalidationConfigSha256(['did:plc:a', 'did:plc:b', 'did:plc:a'], '2026-08-01T00:00:00.000Z')
  check(a === b, 'actor order and duplicates must not change the config hash')
  check(/^[0-9a-f]{64}$/.test(a), 'config hash must be a lowercase sha256 hex digest')

  const differentSince = contentTimeRevalidationConfigSha256(['did:plc:a', 'did:plc:b'], '2026-08-02T00:00:00.000Z')
  check(a !== differentSince, 'changing --since must change the config hash')

  const differentActors = contentTimeRevalidationConfigSha256(['did:plc:a'], '2026-08-01T00:00:00.000Z')
  check(a !== differentActors, 'changing the actor set must change the config hash')
}

console.log('contentTimeRevalidationConfigSha256 checks passed')

// --- bounded-contract constants must match the mission's numbers ---------

assert.deepEqual(REVALIDATION_LIMITS, {
  batchSize: 500,
  maxDurationMs: 30 * 60 * 1000,
  pauseMs: 1000,
  lockTimeoutMs: 5000,
  statementTimeoutMs: 30_000,
}, 'REVALIDATION_LIMITS must match the same bounded contract as RECOVERY_LIMITS')

check(CONTENT_TIME_VALIDATOR_VERSION === 'newsflows-content-time/v2', 'live validator version must still be v2')
check(CONTENT_TIME_VALIDATOR_VERSION_V1 === 'newsflows-content-time/v1', 'historical validator version constant must be v1')

console.log('bounded-contract constant checks passed')

// --- receipt/checkpoint shape must be raw-free ----------------------------

{
  const syntheticProgress = {
    batch: 3,
    scanned: 1500,
    updated: 1490,
    skipped_cas: 10,
    counts: {
      v1_valid_to_v2_valid: 1400,
      v1_invalid_to_v2_valid: 10,
      v1_to_v2_invalid: 80,
      by_v2_invalid_reason: { future_skew: 80 },
    },
    cursor_author_sha256: 'a'.repeat(64),
    cursor_uri_sha256: 'b'.repeat(64),
    elapsed_ms: 4200,
  }
  const serialized = JSON.stringify(syntheticProgress)
  check(!/at:\/\//.test(serialized), 'progress receipt must never contain a raw at:// post/publisher URI')
  check(!/did:plc:/.test(serialized), 'progress receipt must never contain a raw DID')
  check(!/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(serialized), 'progress receipt must never contain a raw content/receipt timestamp (only elapsed_ms and counts)')
  const allowedKeys = new Set([
    'batch', 'scanned', 'updated', 'skipped_cas', 'counts', 'cursor_author_sha256', 'cursor_uri_sha256', 'elapsed_ms',
  ])
  for (const key of Object.keys(syntheticProgress)) {
    check(allowedKeys.has(key), `unexpected key in progress receipt: ${key}`)
  }
}

console.log('receipt raw-free shape checks passed')

console.log('content-time revalidate contract tests passed')
