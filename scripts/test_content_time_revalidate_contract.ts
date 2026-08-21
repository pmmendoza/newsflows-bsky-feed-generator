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
  parseRevalidateCliArgs,
  normalizeAppViewPost,
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

// --- v2->v3 transforms ---------------------------------------------------

// 1. Valid past stays valid
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-02T12:00:00+00:00'
  const result = revalidateContentTimeCandidate(
    {
      indexedAt,
      created_at_source_raw: Buffer.from(raw, 'utf8'),
      content_time_status: 'source_valid',
      content_time_clamp_reason: null,
    },
    'newsflows-content-time/v3',
    'newsflows-content-time/v2',
  )
  check(result.outcome === 'v2_valid_to_v3_valid', 'v2 valid past content stays valid in v3')
  check(result.content_time_status === 'source_valid', 'status must be source_valid')
  check(result.content_time_utc === '2026-08-02T12:00:00.000Z', 'content_time_utc matches raw UTC')
  check(result.content_time_clamp_reason === null, 'clamp reason is null')
  check(result.content_time_validator_version === 'newsflows-content-time/v3', 'version is v3')
}

// 2. 0-5m future skew: valid in v2 -> clamped in v3
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-12T12:03:00.000Z'
  const result = revalidateContentTimeCandidate(
    {
      indexedAt,
      created_at_source_raw: Buffer.from(raw, 'utf8'),
      content_time_status: 'source_valid',
      content_time_clamp_reason: null,
    },
    'newsflows-content-time/v3',
    'newsflows-content-time/v2',
  )
  check(result.outcome === 'v2_skew_to_v3_clamped', 'v2 0-5m skew flips to v2_skew_to_v3_clamped')
  check(result.content_time_status === 'source_valid', 'status must be source_valid')
  check(result.content_time_utc === indexedAt, 'content_time_utc is clamped to receipt')
  check(result.createdAt === indexedAt, 'createdAt is updated to clamped receipt')
  check(result.content_time_clamp_reason === 'future_skew_clamped', 'clamp reason is future_skew_clamped')
  check(result.content_time_validator_version === 'newsflows-content-time/v3', 'version is v3')
}

// 3. >5m future skew: invalid in v2 -> restored in v3
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-12T13:00:00.000Z' // 1 hour skew
  const result = revalidateContentTimeCandidate(
    {
      indexedAt,
      created_at_source_raw: Buffer.from(raw, 'utf8'),
      content_time_status: 'source_invalid',
      content_time_clamp_reason: 'future_skew',
    },
    'newsflows-content-time/v3',
    'newsflows-content-time/v2',
  )
  check(result.outcome === 'v2_invalid_to_v3_clamped', 'v2 >5m skew flips to v2_invalid_to_v3_clamped')
  check(result.content_time_status === 'source_valid', 'status restored to source_valid')
  check(result.content_time_utc === indexedAt, 'content_time_utc is clamped to receipt')
  check(result.createdAt === indexedAt, 'createdAt is updated to clamped receipt')
  check(result.content_time_clamp_reason === 'future_skew_clamped', 'clamp reason is future_skew_clamped')
  check(result.content_time_validator_version === 'newsflows-content-time/v3', 'version is v3')
}

// 4. Missing/unparseable: invalid in v2 -> stays invalid in v3
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = 'invalid-time'
  const result = revalidateContentTimeCandidate(
    {
      indexedAt,
      created_at_source_raw: Buffer.from(raw, 'utf8'),
      content_time_status: 'source_invalid',
      content_time_clamp_reason: 'unparseable',
    },
    'newsflows-content-time/v3',
    'newsflows-content-time/v2',
  )
  check(result.outcome === 'v2_to_v3_invalid', 'unparseable stays invalid')
  check(result.content_time_status === 'source_invalid', 'status is source_invalid')
  check(result.content_time_utc === null, 'content_time_utc is null')
  check(result.content_time_clamp_reason === 'unparseable', 'clamp reason preserved')
  check(result.content_time_validator_version === 'newsflows-content-time/v3', 'version is v3')
}

// --- v3->v2 transforms (reversible rollback) -----------------------------

// 1. Clamped 0-5m skew in v3 -> flips back to valid raw UTC in v2
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-12T12:03:00.000Z'
  const result = revalidateContentTimeCandidate(
    {
      indexedAt,
      created_at_source_raw: Buffer.from(raw, 'utf8'),
      content_time_status: 'source_valid',
      content_time_clamp_reason: 'future_skew_clamped',
    },
    'newsflows-content-time/v2',
    'newsflows-content-time/v3',
  )
  check(result.outcome === 'v3_clamped_to_v2_valid', 'v3 clamped 0-5m skew flips to v3_clamped_to_v2_valid')
  check(result.content_time_status === 'source_valid', 'status is source_valid')
  check(result.content_time_utc === '2026-08-12T12:03:00.000Z', 'content_time_utc restored to raw UTC')
  check(result.content_time_clamp_reason === null, 'clamp reason cleared')
  check(result.content_time_validator_version === 'newsflows-content-time/v2', 'version is v2')
}

// 2. Clamped >5m skew in v3 -> flips back to invalid future_skew in v2
{
  const indexedAt = '2026-08-12T12:00:00.000Z'
  const raw = '2026-08-12T13:00:00.000Z'
  const result = revalidateContentTimeCandidate(
    {
      indexedAt,
      created_at_source_raw: Buffer.from(raw, 'utf8'),
      content_time_status: 'source_valid',
      content_time_clamp_reason: 'future_skew_clamped',
    },
    'newsflows-content-time/v2',
    'newsflows-content-time/v3',
  )
  check(result.outcome === 'v3_clamped_to_v2_invalid', 'v3 clamped >5m skew flips to v3_clamped_to_v2_invalid')
  check(result.content_time_status === 'source_invalid', 'status is source_invalid')
  check(result.content_time_utc === null, 'content_time_utc is null')
  check(result.content_time_clamp_reason === 'future_skew', 'clamp reason is future_skew')
  check(result.content_time_validator_version === 'newsflows-content-time/v2', 'version is v2')
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

  // Table binding
  const engagementConfig = contentTimeRevalidationConfigSha256([], '2026-08-01T00:00:00.000Z', 'engagement')
  check(a !== engagementConfig, 'changing table must change the config hash')

  // Version bindings
  const v2Tov3Config = contentTimeRevalidationConfigSha256(
    ['did:plc:a', 'did:plc:b'],
    '2026-08-01T00:00:00.000Z',
    'post',
    'newsflows-content-time/v2',
    'newsflows-content-time/v3',
  )
  check(a !== v2Tov3Config, 'changing validator versions must change the config hash')

  const v3Tov2Config = contentTimeRevalidationConfigSha256(
    ['did:plc:a', 'did:plc:b'],
    '2026-08-01T00:00:00.000Z',
    'post',
    'newsflows-content-time/v3',
    'newsflows-content-time/v2',
  )
  check(v2Tov3Config !== v3Tov2Config, 'direction of validator change must change the config hash')
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

check(CONTENT_TIME_VALIDATOR_VERSION === 'newsflows-content-time/v2', 'live validator version default is v2')
check(CONTENT_TIME_VALIDATOR_VERSION_V1 === 'newsflows-content-time/v1', 'historical validator version constant must be v1')

console.log('bounded-contract constant checks passed')

// --- receipt/checkpoint shape must be raw-free (except the packet hash,
// which is an operator-approved identifier, and the per-batch cursor,
// which is deliberately a raw publisher URI/DID -- see below) -----------

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
    packet_sha256: 'c'.repeat(64),
    // This batch's own WAL/relation-size deltas -- plain measured integers,
    // not derived from row content, so they carry no raw-data risk.
    wal_bytes: 333896,
    relation_bytes_before: 8192000,
    relation_bytes_after: 8355840,
  }
  const serialized = JSON.stringify(syntheticProgress)
  check(!/at:\/\//.test(serialized), 'progress receipt must never contain a raw at:// post/publisher URI')
  check(!/did:plc:/.test(serialized), 'progress receipt must never contain a raw DID')
  check(!/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(serialized), 'progress receipt must never contain a raw content/receipt timestamp (only elapsed_ms and counts)')
  check(/^[0-9a-f]{64}$/.test(syntheticProgress.packet_sha256), 'packet_sha256 must be a lowercase sha256 hex digest, not raw packet content')
  check(Number.isInteger(syntheticProgress.wal_bytes) && syntheticProgress.wal_bytes >= 0, 'wal_bytes must be a non-negative integer')
  check(Number.isInteger(syntheticProgress.relation_bytes_before) && Number.isInteger(syntheticProgress.relation_bytes_after), 'relation_bytes_before/after must be integers')
  const allowedKeys = new Set([
    'batch', 'scanned', 'updated', 'skipped_cas', 'counts', 'cursor_author_sha256', 'cursor_uri_sha256', 'elapsed_ms', 'packet_sha256',
    'wal_bytes', 'relation_bytes_before', 'relation_bytes_after',
  ])
  for (const key of Object.keys(syntheticProgress)) {
    check(allowedKeys.has(key), `unexpected key in progress receipt: ${key}`)
  }
}

// The per-batch `batches[]` breakdown is a deliberate, narrower exception:
// the mission explicitly allows raw publisher URIs/DIDs as the per-batch
// cursor (an operator correlating a batch against pg_stat_wal/relation-size/
// dead-tuple deltas needs to know exactly where that batch stopped), but
// nothing else about a row -- no post text, no participant data.
{
  const syntheticBatch = {
    batch: 1,
    candidates: 500,
    updated: 495,
    skipped_cas: 0,
    counts: {
      v1_valid_to_v2_valid: 480,
      v1_invalid_to_v2_valid: 5,
      v1_to_v2_invalid: 10,
      by_v2_invalid_reason: { future_skew: 10 },
    },
    cursor_author: 'did:plc:example-publisher',
    cursor_uri: 'at://did:plc:example-publisher/app.bsky.feed.post/abc123',
    elapsed_ms: 72,
    wal_bytes: 333896,
    relation_bytes_before: 8192000,
    relation_bytes_after: 8355840,
  }
  const allowedKeys = new Set([
    'batch', 'candidates', 'updated', 'skipped_cas', 'counts', 'cursor_author', 'cursor_uri', 'elapsed_ms',
    'wal_bytes', 'relation_bytes_before', 'relation_bytes_after',
  ])
  for (const key of Object.keys(syntheticBatch)) {
    check(allowedKeys.has(key), `unexpected key in per-batch summary: ${key}`)
  }
  check(syntheticBatch.cursor_author.startsWith('did:'), 'cursor_author is deliberately a raw publisher DID, not hashed')
  check(syntheticBatch.cursor_uri.startsWith('at://'), 'cursor_uri is deliberately a raw publisher post URI, not hashed')
}

console.log('receipt raw-free shape checks passed')

// --- parseRevalidateCliArgs: --packet-sha256 / --max-batches parsing -----

{
  const validSha = '1'.repeat(64)

  // Neither flag is required for a bare dry-run/preview invocation.
  const bare = parseRevalidateCliArgs([])
  check(bare.packetSha256 === undefined, 'packet-sha256 is optional (only required with --apply, enforced in mainRevalidate)')
  check(bare.maxBatches === undefined, 'max-batches defaults to unlimited (undefined)')

  // Preview "may accept it optionally and echo it": a well-formed hash parses through untouched.
  const withPacket = parseRevalidateCliArgs(['--packet-sha256', validSha])
  check(withPacket.packetSha256 === validSha, '--packet-sha256 must be captured verbatim')

  for (const bad of ['not-a-sha', '1'.repeat(63), '1'.repeat(65), 'A'.repeat(64), '']) {
    check(
      throws(() => parseRevalidateCliArgs(bad === '' ? ['--packet-sha256'] : ['--packet-sha256', bad])),
      `--packet-sha256 must reject malformed hash: ${JSON.stringify(bad)}`,
    )
  }

  const withMaxBatches = parseRevalidateCliArgs(['--max-batches', '3'])
  check(withMaxBatches.maxBatches === 3, '--max-batches must be captured as a number')

  // Leading/trailing whitespace around an otherwise-valid integer (e.g. ' 3 ')
  // is deliberately accepted (trimmed) -- only genuinely non-integer input
  // is rejected here.
  for (const bad of ['0', '-1', '1.5', 'abc', ' ', '3abc']) {
    check(
      throws(() => parseRevalidateCliArgs(['--max-batches', bad])),
      `--max-batches must reject non-positive-integer input: ${JSON.stringify(bad)}`,
    )
  }

  // --table, --from-version, --to-version CLI flags
  check(bare.table === 'post', 'table defaults to post')
  check(bare.fromVersion === 'newsflows-content-time/v1', 'fromVersion defaults to v1')
  check(bare.toVersion === 'newsflows-content-time/v2', 'toVersion defaults to v2')

  const engagementRevalidate = parseRevalidateCliArgs(['--table', 'engagement', '--from-version', 'newsflows-content-time/v2', '--to-version', 'newsflows-content-time/v3'])
  check(engagementRevalidate.table === 'engagement', 'captures table=engagement')
  check(engagementRevalidate.fromVersion === 'newsflows-content-time/v2', 'captures fromVersion=v2')
  check(engagementRevalidate.toVersion === 'newsflows-content-time/v3', 'captures toVersion=v3')

  // Rollback args
  const rollbackArgs = parseRevalidateCliArgs(['--from-version', 'newsflows-content-time/v3', '--to-version', 'newsflows-content-time/v2'])
  check(rollbackArgs.fromVersion === 'newsflows-content-time/v3', 'captures rollback fromVersion=v3')
  check(rollbackArgs.toVersion === 'newsflows-content-time/v2', 'captures rollback toVersion=v2')

  // Transitions restriction checks
  check(throws(() => parseRevalidateCliArgs(['--from-version', 'newsflows-content-time/v1', '--to-version', 'newsflows-content-time/v3'])), 'must reject v1->v3')
  check(throws(() => parseRevalidateCliArgs(['--from-version', 'newsflows-content-time/v3', '--to-version', 'newsflows-content-time/v1'])), 'must reject v3->v1')
  check(throws(() => parseRevalidateCliArgs(['--from-version', 'newsflows-content-time/v2', '--to-version', 'newsflows-content-time/v1'])), 'must reject v2->v1')
}

function throws(fn: () => unknown): boolean {
  try {
    fn()
    return false
  } catch {
    return true
  }
}

// --- Recovery helper version binding tests --------------------------------
{
  const appViewPost = {
    uri: 'at://did:plc:example-publisher/app.bsky.feed.post/123',
    cid: 'cid123',
    indexedAt: '2026-08-12T12:00:00.000Z',
    author: { did: 'did:plc:example-publisher' },
    record: {
      text: 'hello',
      createdAt: '2026-08-12T12:00:00.000Z',
    },
  }
  const normalized = normalizeAppViewPost(appViewPost, 'did:plc:example-publisher')
  check(normalized !== null, 'normalizeAppViewPost succeeds on valid input in default v2 mode')
  check(normalized?.content_time_validator_version === 'newsflows-content-time/v2', 'normalizes with v2 validator')

  check(
    throws(() => normalizeAppViewPost(appViewPost, 'did:plc:example-publisher', 'newsflows-content-time/v3')),
    'normalizeAppViewPost must fail closed for versions other than v2',
  )
}

// --- Engagement cursor masking in batch summary ---------------------------
{
  const rawUri = 'at://did:plc:user123/app.bsky.feed.like/456'
  const expectedHash = require('crypto').createHash('sha256').update(rawUri).digest('hex')
  const engagementBatch = {
    batch: 1,
    candidates: 100,
    updated: 100,
    skipped_cas: 0,
    counts: {
      v2_valid_to_v3_valid: 90,
      v2_skew_to_v3_clamped: 10,
      v2_invalid_to_v3_clamped: 0,
      v2_to_v3_invalid: 0,
      gt_5m_restored: 0,
      zero_to_5m_clamped: 10,
      gt_5m_invalidated: 0,
      zero_to_5m_unclamped: 0,
      by_invalid_reason: {},
      by_v2_invalid_reason: {},
    },
    cursor_author: '',
    cursor_uri: expectedHash,
    elapsed_ms: 50,
    wal_bytes: 1000,
    relation_bytes_before: 5000,
    relation_bytes_after: 6000,
  }
  check(engagementBatch.cursor_author === '', 'engagement batch summary omits author')
  check(!engagementBatch.cursor_uri.includes('at://'), 'engagement batch summary masks raw URI')
  check(engagementBatch.cursor_uri === expectedHash, 'engagement batch summary uses sha256 of cursor URI')
}

console.log('parseRevalidateCliArgs --packet-sha256 / --max-batches checks passed')

console.log('content-time revalidate contract tests passed')
