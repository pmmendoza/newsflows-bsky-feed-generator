import assert from 'assert'
import {
  CandidateRow,
  Loaded,
  parseArgs,
  rowMismatches,
  verify,
} from '../src/tools/verify-content-time-reconstruction'
import {
  CONTENT_TIME_VALIDATOR_VERSION_V3,
  validateContentTime,
} from '../src/util/content-time'

const SINCE = '2026-08-20T00:00:00.000Z'
const UNTIL = '2026-08-21T00:00:00.000Z'

const row = (
  uri: string,
  raw = '2026-08-20T10:00:00Z',
  indexedAt = '2026-08-20T10:01:00.000Z',
): CandidateRow => {
  const expected = validateContentTime(raw, indexedAt, CONTENT_TIME_VALIDATOR_VERSION_V3)
  return {
    uri,
    indexedAt,
    createdAt: expected.legacy_created_at,
    created_at_source_raw: expected.created_at_source_raw,
    content_time_utc: expected.content_time_utc,
    content_time_status: expected.content_time_status,
    content_time_clamp_reason: expected.content_time_clamp_reason,
    content_time_validator_version: expected.content_time_validator_version,
  }
}

const loaded = (rows: CandidateRow[], engagement: CandidateRow[] = []): Loaded => ({
  feeds: [{ feed_id: 'feed-a', publisher_did: 'did:example:publisher' }],
  posts: { 'feed-a': { denominator: rows.length, rows } },
  engagement: { denominator: engagement.length, rows: engagement },
})

async function main(): Promise<void> {
  assert.deepStrictEqual(
    parseArgs(['--since', '2026-08-20T01:00:00+01:00', '--until', UNTIL]),
    { since: SINCE, until: UNTIL },
  )
  const badArgs = [
    [],
    ['--since', SINCE],
    ['--since', SINCE, '--until'],
    ['--since', SINCE, '--until', UNTIL, '--extra', 'x'],
    ['positional', 'x', '--until', UNTIL],
    ['--since', SINCE, '--since', SINCE, '--until', UNTIL],
    ['--since', '2026-08-20T00:00:00', '--until', UNTIL],
    ['--since', '2026-02-30T00:00:00Z', '--until', UNTIL],
    ['--since', UNTIL, '--until', SINCE],
  ]
  badArgs.forEach((args) => assert.throws(() => parseArgs(args)))

  const valid = row('at://raw/secret-valid')
  const clamped = row(
    'at://raw/secret-clamped',
    '2026-08-20T11:00:00Z',
    '2026-08-20T10:00:00.000Z',
  )
  assert.deepStrictEqual(rowMismatches(valid), [])
  assert.deepStrictEqual(rowMismatches(clamped), [])
  assert.strictEqual(clamped.content_time_clamp_reason, 'future_skew_clamped')

  const clean = await verify(SINCE, UNTIL, async () => loaded([valid, clamped]))
  assert.strictEqual(clean.pass, true)
  assert.strictEqual(clean.feeds['feed-a'].denominator, 2)
  assert.deepStrictEqual(clean.engagement, {
    denominator: 0,
    sampled: 0,
    mismatches: 0,
    empty_population: true,
  })
  assert.strictEqual(clean.validator_version, CONTENT_TIME_VALIDATOR_VERSION_V3)
  const bounded = await verify(SINCE, UNTIL, async () => ({
    ...loaded([valid, clamped]),
    posts: { 'feed-a': { denominator: 100, rows: [valid, clamped] } },
  }))
  assert.strictEqual(bounded.feeds['feed-a'].denominator, 100)
  const oversized = Array.from({ length: 31 }, (_, i) => row(`at://raw/${i}`))
  assert.strictEqual((await verify(SINCE, UNTIL, async () => loaded(oversized))).pass, false)

  const mismatchCases: Array<[keyof CandidateRow, unknown, string]> = [
    ['content_time_utc', '2000-01-01T00:00:00.000Z', 'content_time_utc'],
    ['content_time_status', 'source_invalid', 'content_time_status'],
    ['content_time_clamp_reason', 'missing', 'content_time_clamp_reason'],
    ['content_time_validator_version', 'newsflows-content-time/v2', 'content_time_validator_version'],
    ['createdAt', '2000-01-01T00:00:00.000Z', 'createdAt'],
  ]
  for (const [key, value, field] of mismatchCases) {
    const changed = { ...valid, [key]: value } as CandidateRow
    assert.deepStrictEqual(rowMismatches(changed), [field])
  }
  const badRaw = { ...valid, created_at_source_raw: Buffer.from([0xff]) }
  assert(rowMismatches(badRaw).includes('created_at_source_raw'))
  assert.deepStrictEqual(rowMismatches({ ...valid, created_at_source_raw: null }).sort(), [
    'content_time_clamp_reason',
    'content_time_status',
    'content_time_utc',
    'content_time_validator_version',
    'createdAt',
    'created_at_source_raw',
  ].sort())

  const allBad = [
    ...mismatchCases.map(([key, value]) => ({ ...valid, uri: `${valid.uri}/${key}`, [key]: value } as CandidateRow)),
    { ...badRaw, uri: `${valid.uri}/raw` },
  ]
  const failed = await verify(SINCE, UNTIL, async () => loaded(allBad))
  assert.strictEqual(failed.pass, false)
  assert.strictEqual(failed.mismatch_rows, allBad.length)
  Object.values(failed.mismatches_by_field).forEach((count) => assert(count > 0))

  const zero = await verify(SINCE, UNTIL, async () => loaded([]))
  assert.deepStrictEqual(zero.errors, [{
    code: 'POPULATION_INVALID',
    message: 'verification population is invalid',
  }])
  const missingSample = await verify(SINCE, UNTIL, async () => ({
    ...loaded([valid]),
    posts: { 'feed-a': { denominator: 100, rows: [] } },
  }))
  assert.strictEqual(missingSample.pass, false)
  assert.strictEqual(missingSample.rows_checked, 0)

  const secret = 'SENTINEL_RAW_DB_ERROR'
  const queryFailure = await verify(SINCE, UNTIL, async () => { throw new Error(secret) })
  assert.deepStrictEqual(queryFailure.errors, [{
    code: 'QUERY_FAILED',
    message: 'verification query failed',
  }])
  assert(!JSON.stringify(queryFailure).includes(secret))

  const first = await verify(SINCE, UNTIL, async () => loaded([valid, clamped]))
  const second = await verify(SINCE, UNTIL, async () => loaded([clamped, valid]))
  assert.strictEqual(first.sample_sha256, second.sample_sha256)
  const output = JSON.stringify(first)
  assert(!output.includes(valid.uri))
  assert(!output.includes(valid.created_at_source_raw!.toString('utf8')))
  assert(!output.includes('did:example:publisher'))

  console.log('content-time reconstruction verifier checks passed')
}

void main()
