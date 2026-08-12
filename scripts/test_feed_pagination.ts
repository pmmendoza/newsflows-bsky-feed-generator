import { feedPageOffsets } from '../src/algos/feed-builder'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

for (const limit of [1, 3, 5, 8, 12]) {
  const equalTimePublisher = Array.from({ length: 100 }, (_, id) => ({ id, time: '2026-08-12T00:00:00Z' }))
  const equalTimeFollows = Array.from({ length: 200 }, (_, id) => ({ id, time: '2026-08-12T00:00:00Z' }))
  let cursor: string | undefined
  const publisher: number[] = []
  const follows: number[] = []
  for (let page = 0; page < 3; page += 1) {
    const offsets = feedPageOffsets(limit, cursor)
    publisher.push(...equalTimePublisher.slice(offsets.publisherOffset, offsets.publisherOffset + offsets.publisherLimit).map((row) => row.id))
    follows.push(...equalTimeFollows.slice(offsets.followsOffset, offsets.followsOffset + offsets.followsLimit).map((row) => row.id))
    cursor = String(offsets.followsOffset + offsets.followsLimit)
  }
  check(publisher.every((value, index) => value === index), `publisher pages must not skip/duplicate at limit=${limit}`)
  check(follows.every((value, index) => value === index), `followed pages must not skip/duplicate at limit=${limit}`)
  check(follows.length === publisher.length * 2, `three-page capacity must preserve 1:2 at limit=${limit}`)
}

console.log('feed pagination tests passed')
