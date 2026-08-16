import { applyPublisherRecencyOrder, applyPublisherTimeFilter } from '../src/algos/publisher-time'
import { validateContentTime } from '../src/util/content-time'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const receipt = '2026-08-12T12:00:00.000Z'
const valid = validateContentTime('2026-08-02T12:00:00+00:00', receipt)
check(valid.content_time_status === 'source_valid', 'valid source must be accepted')
check(valid.content_time_utc === '2026-08-02T12:00:00.000Z', 'valid source must normalize to UTC')
check(valid.content_time_validator_version === 'newsflows-content-time/v2', 'new classifications must use validator v2')

const toleratedSkew = validateContentTime('2026-08-12T12:05:00.000Z', receipt)
check(toleratedSkew.content_time_status === 'source_valid', 'five minutes of future skew must be accepted')
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

const calls: any[] = []
const query: any = {
  where: (...args: any[]) => { calls.push(['where', ...args]); return query },
  orderBy: (...args: any[]) => { calls.push(['orderBy', ...args]); return query },
}
applyPublisherRecencyOrder(
  applyPublisherTimeFilter(query, 'content_time_v1', '2026-08-02T00:00:00Z'),
  'content_time_v1',
)
check(calls.some((call) => call[1] === 'post.content_time_status' && call[2] === '=' && call[3] === 'source_valid'), 'content eligibility must require source_valid')
check(calls.length === 5, 'content eligibility plus deterministic ordering must be complete')
check(!calls.some((call) => call[0] === 'where' && call[1] === 'post.indexedAt'), 'content eligibility must not substitute receipt time')

console.log('content time contract tests passed')
