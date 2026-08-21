import {
  cutoffFromHours,
  parseStrictPositiveHours,
  resolvePublisherServingWindow,
} from '../src/algos/publisher-serving-window'
import { buildFeed } from '../src/algos/feed-builder'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

function withEnv(values: Record<string, string | undefined>, fn: () => void) {
  const before = Object.fromEntries(Object.keys(values).map((key) => [key, process.env[key]]))
  try {
    for (const [key, value] of Object.entries(values)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
    fn()
  } finally {
    for (const [key, value] of Object.entries(before)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  }
}

for (const raw of ['10junk', '1.5', '-1', '0', '', '8761']) {
  check(parseStrictPositiveHours(raw) === null, `strict parser must reject ${JSON.stringify(raw)}`)
}
check(parseStrictPositiveHours('240') === 240, 'strict parser must accept ten days')

withEnv({
  ENGAGEMENT_TIME_HOURS: '72',
  FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K: '168',
}, () => {
  const catalog = resolvePublisherServingWindow(10, 'study_default')
  check(catalog.effectiveHours === 240, 'materialized ten-day value must win')
  check(!catalog.compatibilityFallbackActive, 'catalog authority has no compatibility fallback')
  check(catalog.compatibilityEnvKey === null, 'catalog authority must not name an environment fallback')
})

const cutoff = cutoffFromHours(Date.parse('2026-08-12T00:00:00Z'), 240)
check(cutoff === '2026-08-02T00:00:00.000Z', 'ten-day cutoff must be exact')

let nextRequestId = 1
const insertBuilder: any = {
  values: () => insertBuilder,
  returning: () => insertBuilder,
  executeTakeFirstOrThrow: async () => ({ id: nextRequestId++ }),
  execute: async () => [],
}
const followsBuilder: any = {
  select: () => followsBuilder,
  where: () => followsBuilder,
  execute: async () => [{ follows: 'did:followed' }],
}
const db: any = {
  selectFrom: () => followsBuilder,
  insertInto: () => insertBuilder,
  transaction: () => ({ execute: async (fn: any) => fn(db) }),
}
const calls: Array<{ leg: string; cutoff: string; limit: number; reference: string }> = []
const post = (uri: string) => ({
  uri, cid: uri, indexedAt: '2026-08-12T00:00:00Z', createdAt: '2026-08-12T00:00:00Z',
  author: uri, text: '', rootUri: '', rootCid: '', link_uri: '', link_title: '',
  link_description: '', linkUrl: '', linkTitle: '', linkDescription: '',
})
const query = (leg: string, count: number) => (_db: any, cutoffIso: string, _follows: string[], _offset: number, limit: number, reference: string) => {
  calls.push({ leg, cutoff: cutoffIso, limit, reference })
  return { execute: async () => Array.from({ length: count }, (_, index) => post(`${leg}-${index}`)) }
}

async function run() {
  process.env.FEEDGEN_ARCHIVE_OUTBOX_ENABLED = 'true'
  process.env.ENGAGEMENT_TIME_HOURS = '72'
  const result = await buildFeed({
    shortname: 'newsflow-be-k',
    ctx: { db } as any,
    params: { limit: 9 } as any,
    requesterDid: 'did:requester',
    buildPublisherQuery: query('publisher', 3),
    buildFollowsQuery: query('follows', 6),
    publisherPostMaxAgeDays: 10,
    publisherPostMaxAgeSource: 'study_default',
    publisherTimeClock: 'content_time_v1',
  })
  const publisherHours = (Date.parse(calls[0].reference) - Date.parse(calls[0].cutoff)) / 3_600_000
  const followedHours = (Date.parse(calls[1].reference) - Date.parse(calls[1].cutoff)) / 3_600_000
  check(publisherHours === 240, 'publisher slice must use catalog ten-day window')
  check(followedHours === 72, 'followed slice must keep receipt-time window')
  check(calls[0].reference === calls[1].reference, 'both cutoffs must share one request reference')
  check(result.feed.map((item) => item.post).join(',') === 'publisher-0,follows-0,follows-1,publisher-1,follows-2,follows-3,publisher-2,follows-4,follows-5', '1:2 mix must remain unchanged')
  console.log('publisher window contract tests passed')
}

run().catch((error) => { console.error(error); process.exit(1) })
