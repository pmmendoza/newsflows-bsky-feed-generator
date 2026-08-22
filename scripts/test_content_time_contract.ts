import { applyPublisherRecencyOrder, applyPublisherTimeFilter } from '../src/algos/publisher-time'
import {
  CONTENT_TIME_VALIDATOR_VERSION_V2,
  CONTENT_TIME_VALIDATOR_VERSION_V3,
  contentTimeSupportedSql,
  isV2FutureSemanticDeltaSql,
  revalidationSemanticDeltaSql,
  resolveActiveContentTimeVersionFromRows,
  validateContentTime,
} from '../src/util/content-time'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

function expectThrows(fn: () => void, messageMatch: string) {
  let threw = false
  try {
    fn()
  } catch (err: any) {
    threw = true
    check(
      err?.message?.includes(messageMatch),
      `expected error message containing "${messageMatch}", got "${err?.message}"`,
    )
  }
  check(threw, `expected function to throw error containing "${messageMatch}"`)
}

const receipt = '2026-08-12T12:00:00.000Z'

// --- v2 tests ---
const valid = validateContentTime('2026-08-02T12:00:00+00:00', receipt)
check(valid.content_time_status === 'source_valid', 'valid source must be accepted')
check(valid.content_time_utc === '2026-08-02T12:00:00.000Z', 'valid source must normalize to UTC')
check(valid.content_time_validator_version === CONTENT_TIME_VALIDATOR_VERSION_V2, 'new classifications must use validator v2')
check(valid.content_time_clamp_reason === null, 'valid source must have null clamp reason')

const toleratedSkew = validateContentTime('2026-08-12T12:05:00.000Z', receipt)
check(toleratedSkew.content_time_status === 'source_valid', 'five minutes of future skew must be accepted in v2')
const oldButParseable = validateContentTime('2020-01-01T00:00:00Z', receipt)
check(oldButParseable.content_time_status === 'source_valid', 'parseable old content must remain source-valid')

for (const [raw, reason] of [
  [undefined, 'missing'],
  ['not-a-time', 'unparseable'],
  ['2026-08-12T12:05:00.001Z', 'future_skew'],
] as const) {
  const result = validateContentTime(raw, receipt)
  check(result.content_time_status === 'source_invalid', `${reason} must be invalid`)
  check(result.content_time_clamp_reason === reason, `${reason} must be explicit`)
  check(result.content_time_utc === null, `${reason} must never become content time`)
  check(result.legacy_created_at === receipt, `${reason} may retain only the legacy receipt fallback`)
}

// --- v3 tests ---
const v3ValidPast = validateContentTime('2026-08-02T12:00:00+00:00', receipt, CONTENT_TIME_VALIDATOR_VERSION_V3)
check(v3ValidPast.content_time_status === 'source_valid', 'v3 valid past source must be accepted')
check(v3ValidPast.content_time_utc === '2026-08-02T12:00:00.000Z', 'v3 valid past source must normalize to UTC')
check(v3ValidPast.content_time_validator_version === CONTENT_TIME_VALIDATOR_VERSION_V3, 'v3 validator version must be recorded')
check(v3ValidPast.content_time_clamp_reason === null, 'v3 valid past source has null clamp reason')
check(v3ValidPast.created_at_source_raw.toString('utf8') === '2026-08-02T12:00:00+00:00', 'v3 preserves raw source buffer')

const v3ValidExact = validateContentTime(receipt, receipt, CONTENT_TIME_VALIDATOR_VERSION_V3)
check(v3ValidExact.content_time_status === 'source_valid', 'v3 exact receipt time is valid')
check(v3ValidExact.content_time_utc === receipt, 'v3 exact receipt matches receipt')
check(v3ValidExact.content_time_clamp_reason === null, 'v3 exact receipt has null clamp reason')

// +1ms future skew clamped
const v3Clamped1ms = validateContentTime('2026-08-12T12:00:00.001Z', receipt, CONTENT_TIME_VALIDATOR_VERSION_V3)
check(v3Clamped1ms.content_time_status === 'source_valid', 'v3 +1ms future skew must be source_valid')
check(v3Clamped1ms.content_time_utc === receipt, 'v3 +1ms future skew clamped to receipt time')
check(v3Clamped1ms.legacy_created_at === receipt, 'v3 legacy created at matches receipt')
check(v3Clamped1ms.content_time_clamp_reason === 'future_skew_clamped', 'v3 future skew reason is future_skew_clamped')
check(v3Clamped1ms.content_time_validator_version === CONTENT_TIME_VALIDATOR_VERSION_V3, 'v3 validator version recorded')
check(v3Clamped1ms.created_at_source_raw.toString('utf8') === '2026-08-12T12:00:00.001Z', 'v3 preserves raw source buffer')

// +1 hour future skew clamped
const v3Clamped1h = validateContentTime('2026-08-12T13:00:00.000Z', receipt, CONTENT_TIME_VALIDATOR_VERSION_V3)
check(v3Clamped1h.content_time_status === 'source_valid', 'v3 +1h future skew must be source_valid')
check(v3Clamped1h.content_time_utc === receipt, 'v3 +1h future skew clamped to receipt time')
check(v3Clamped1h.legacy_created_at === receipt, 'v3 +1h legacy created at matches receipt')
check(v3Clamped1h.content_time_clamp_reason === 'future_skew_clamped', 'v3 +1h reason is future_skew_clamped')
check(v3Clamped1h.created_at_source_raw.toString('utf8') === '2026-08-12T13:00:00.000Z', 'v3 preserves raw source buffer')

// Missing / unparseable in v3
for (const [raw, reason] of [
  [undefined, 'missing'],
  ['', 'missing'],
  ['invalid-iso', 'unparseable'],
] as const) {
  const result = validateContentTime(raw, receipt, CONTENT_TIME_VALIDATOR_VERSION_V3)
  check(result.content_time_status === 'source_invalid', `v3 ${reason} must be source_invalid`)
  check(result.content_time_clamp_reason === reason, `v3 ${reason} must record explicit clamp reason`)
  check(result.content_time_utc === null, `v3 ${reason} must have null content_time_utc`)
  check(result.legacy_created_at === receipt, `v3 ${reason} legacy created at fallback to receipt`)
  check(result.content_time_validator_version === CONTENT_TIME_VALIDATOR_VERSION_V3, 'v3 validator version recorded')
}

// Configured past bound in v3
const v3PastBound = validateContentTime('2026-08-01T00:00:00.000Z', receipt, CONTENT_TIME_VALIDATOR_VERSION_V3, {
  maxFutureSkewMs: 0,
  maxPastAgeMs: 24 * 60 * 60 * 1000,
})
check(v3PastBound.content_time_status === 'source_invalid', 'v3 past bound violation is source_invalid')
check(v3PastBound.content_time_clamp_reason === 'past_bound', 'v3 past bound violation reason is past_bound')

// Unknown validator version fails closed
expectThrows(
  () => validateContentTime('2026-08-02T12:00:00+00:00', receipt, 'newsflows-content-time/v4'),
  'unsupported content-time validator version',
)

// --- Catalog Resolver Tests ---
const v2Catalog = [
  { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V2 },
  { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V2 },
  { enabled: true, publisher_time_clock: 'receipt_time', content_time_contract_version: null },
  { enabled: false, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V3 },
]
check(resolveActiveContentTimeVersionFromRows(v2Catalog) === CONTENT_TIME_VALIDATOR_VERSION_V2, 'resolves v2 for enabled content_time_v1 feeds')

const v3Catalog = [
  { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V3 },
  { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V3 },
  { enabled: true, publisher_time_clock: 'receipt_time', content_time_contract_version: null },
]
check(resolveActiveContentTimeVersionFromRows(v3Catalog) === CONTENT_TIME_VALIDATOR_VERSION_V3, 'resolves v3 for enabled content_time_v1 feeds')

// Mixed versions fail closed
expectThrows(
  () => resolveActiveContentTimeVersionFromRows([
    { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V2 },
    { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V3 },
  ]),
  'mixed content-time contract versions',
)

// Missing contract version fails closed
expectThrows(
  () => resolveActiveContentTimeVersionFromRows([
    { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: null },
  ]),
  'missing content_time_contract_version',
)

// Unsupported contract version fails closed
expectThrows(
  () => resolveActiveContentTimeVersionFromRows([
    { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: 'newsflows-content-time/v1' },
  ]),
  'unsupported content-time contract version',
)

// No enabled content_time_v1 feeds fails closed
expectThrows(
  () => resolveActiveContentTimeVersionFromRows([
    { enabled: true, publisher_time_clock: 'receipt_time', content_time_contract_version: null },
    { enabled: false, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V2 },
  ]),
  'no enabled content_time_v1 feeds found',
)

// Default past bound behavior: no synthetic bounds in v2 and v3 without explicit policy
const pastDate2020 = '2020-01-01T00:00:00Z'
const v2Past = validateContentTime(pastDate2020, receipt, CONTENT_TIME_VALIDATOR_VERSION_V2)
check(v2Past.content_time_status === 'source_valid', 'v2 default accepts past content without synthetic bound')
const v3Past = validateContentTime(pastDate2020, receipt, CONTENT_TIME_VALIDATOR_VERSION_V3)
check(v3Past.content_time_status === 'source_valid', 'v3 default accepts past content without synthetic bound')

// Algo / Query tests
const calls: any[] = []
const query: any = {
  where: (...args: any[]) => { calls.push(['where', ...args]); return query },
  orderBy: (...args: any[]) => { calls.push(['orderBy', ...args]); return query },
}
applyPublisherRecencyOrder(
  applyPublisherTimeFilter(query, 'content_time_v1', '2026-08-02T00:00:00Z', CONTENT_TIME_VALIDATOR_VERSION_V3),
  'content_time_v1',
)
check(calls.length === 5, 'content eligibility plus deterministic ordering must be complete')
check(!calls.some((call) => call[0] === 'where' && call[1] === 'post.indexedAt'), 'content eligibility must not substitute receipt time')
const v2Predicate = JSON.stringify(contentTimeSupportedSql('post', CONTENT_TIME_VALIDATOR_VERSION_V2).toOperationNode())
const v3Predicate = JSON.stringify(contentTimeSupportedSql('post', CONTENT_TIME_VALIDATOR_VERSION_V3).toOperationNode())
const deltaPredicate = JSON.stringify(isV2FutureSemanticDeltaSql('post').toOperationNode())
check(v2Predicate.includes(CONTENT_TIME_VALIDATOR_VERSION_V2) && !v2Predicate.includes(CONTENT_TIME_VALIDATOR_VERSION_V3), 'v2 SQL predicate must require exact v2 provenance')
check(v3Predicate.includes(CONTENT_TIME_VALIDATOR_VERSION_V2) && v3Predicate.includes(CONTENT_TIME_VALIDATOR_VERSION_V3) && v3Predicate.includes('indexedAt'), 'v3 SQL predicate must accept v3 plus compatible v2 rows')
check(deltaPredicate.includes('future_skew') && deltaPredicate.includes('content_time_utc') && deltaPredicate.includes('indexedAt'), 'semantic-delta SQL predicate must cover both v2 future encodings')
check(!deltaPredicate.includes('timestamptz') && !deltaPredicate.includes('5 minutes'), 'semantic-delta SQL predicate must match the canonical-text partial-index predicate')
const reverseDeltaPredicate = JSON.stringify(revalidationSemanticDeltaSql('post', CONTENT_TIME_VALIDATOR_VERSION_V3, CONTENT_TIME_VALIDATOR_VERSION_V2).toOperationNode())
check(reverseDeltaPredicate.includes('future_skew_clamped') && !reverseDeltaPredicate.includes('source_valid'), 'rollback SQL predicate must select only clamped v3 rows')

;(async () => {
  // Generation-guard cache test
  const { invalidateActiveContentTimeContractCache, resolveActiveContentTimeContract } = await import('../src/util/content-time')
  invalidateActiveContentTimeContractCache()
  let queryResolveCount = 0
  const mockDb: any = {
    selectFrom: () => mockDb,
    select: () => mockDb,
    execute: async () => {
      queryResolveCount += 1
      return [
        { enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V2 },
      ]
    },
  }
  await resolveActiveContentTimeContract(mockDb)
  check((queryResolveCount as number) === 1, 'first resolve queries DB')
  await resolveActiveContentTimeContract(mockDb)
  check((queryResolveCount as number) === 1, 'second resolve hits cache')
  invalidateActiveContentTimeContractCache()
  await resolveActiveContentTimeContract(mockDb)
  check((queryResolveCount as number) === 2, 'resolve after invalidation queries DB again')

  invalidateActiveContentTimeContractCache()
  let releaseFirstQuery!: () => void
  const firstQueryBlocked = new Promise<void>((resolve) => { releaseFirstQuery = resolve })
  let raceQueryCount = 0
  const raceDb: any = {
    selectFrom: () => raceDb,
    select: () => raceDb,
    execute: async () => {
      raceQueryCount += 1
      if (raceQueryCount === 1) {
        await firstQueryBlocked
        return [{ enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V2 }]
      }
      return [{ enabled: true, publisher_time_clock: 'content_time_v1', content_time_contract_version: CONTENT_TIME_VALIDATOR_VERSION_V3 }]
    },
  }
  const inFlight = resolveActiveContentTimeContract(raceDb)
  await Promise.resolve()
  invalidateActiveContentTimeContractCache()
  releaseFirstQuery()
  check(await inFlight === CONTENT_TIME_VALIDATOR_VERSION_V3, 'in-flight pre-invalidation result must be retried')
  check(raceQueryCount === 2, 'in-flight invalidation performs one fresh query')

  console.log('content time contract tests passed')
})().catch((err) => {
  console.error(err)
  process.exit(1)
})
