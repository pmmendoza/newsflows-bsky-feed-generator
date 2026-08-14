import { feedPageOffsets } from '../src/algos/feed-builder'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const activeFeeds = [
  ...['nl', 'fr', 'cz', 'ir'].flatMap((country) => [1, 2, 3].map((variant) => `newsflow-${country}-${variant}`)),
  'newsflow-be-k',
  'newsflow-be-m',
]

function legacyPageOffsets(limit: number, cursor?: string) {
  const offset = cursor ? parseInt(cursor, 10) : 0
  return {
    publisherLimit: Math.max(1, Math.floor(limit / 3)),
    followsLimit: Math.max(2, Math.floor(limit * 2 / 3)),
    publisherOffset: offset,
    followsOffset: offset,
  }
}

function threePages(limit: number, offsetsFor: typeof feedPageOffsets) {
  let cursor: string | undefined
  const ordered: string[] = []
  for (let page = 0; page < 3; page += 1) {
    const offsets = offsetsFor(limit, cursor)
    const publisher = Array.from({ length: offsets.publisherLimit }, (_, id) => `p${offsets.publisherOffset + id}`)
    const follows = Array.from({ length: offsets.followsLimit }, (_, id) => `f${offsets.followsOffset + id}`)
    for (let id = 0; id < Math.max(publisher.length, Math.ceil(follows.length / 2)); id += 1) {
      if (publisher[id]) ordered.push(publisher[id])
      ordered.push(...follows.slice(id * 2, id * 2 + 2))
    }
    cursor = String(offsets.followsOffset + offsets.followsLimit)
  }
  return ordered
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

const cursorShadowByLimit: Record<string, unknown> = {}
for (const feed of activeFeeds) {
  for (const limit of [1, 5, 15, 25, 30]) {
    const current = threePages(limit, feedPageOffsets)
    const legacy = threePages(limit, legacyPageOffsets)
    const currentSet = new Set(current)
    const legacySet = new Set(legacy)
    const currentOnly = [...currentSet].filter((uri) => !legacySet.has(uri)).length
    const legacyOnly = [...legacySet].filter((uri) => !currentSet.has(uri)).length
    const orderedChanges = current.filter((uri, position) => uri !== legacy[position]).length
    const metrics = {
      current_count: current.length,
      legacy_count: legacy.length,
      ordered_changes: orderedChanges,
      ordered_change_share: orderedChanges / current.length,
      current_only: currentOnly,
      legacy_only: legacyOnly,
      symmetric_difference: currentOnly + legacyOnly,
    }
    check(new Set(current).size === current.length, `${feed}: repaired cursor must not duplicate at limit=${limit}`)
    check(orderedChanges > 0, `${feed}: shadow must expose the legacy cursor delta at limit=${limit}`)
    if (limit === 5) {
      check(orderedChanges === 5 && current.length === 9, `${feed}: limit=5 ordered delta must remain 5/9`)
      check(currentOnly + legacyOnly === 7 && legacy.length === 12, `${feed}: limit=5 symmetric delta must remain 7/12`)
    } else {
      check(orderedChanges / current.length === 2 / 9, `${feed}: equal-cardinality ordered delta must remain 2/9 at limit=${limit}`)
      check(currentOnly / current.length === 1 / 9, `${feed}: one-way replacement must remain 1/9 at limit=${limit}`)
      check((currentOnly + legacyOnly) / current.length === 2 / 9, `${feed}: symmetric delta must remain 2/9 at limit=${limit}`)
    }
    const prior = cursorShadowByLimit[String(limit)]
    check(!prior || JSON.stringify(prior) === JSON.stringify(metrics), `${feed}: cursor shadow must be policy-independent at limit=${limit}`)
    cursorShadowByLimit[String(limit)] = metrics
  }
}

console.log(`feed pagination tests passed; cursor_shadow=${JSON.stringify({ feeds: activeFeeds, by_limit: cursorShadowByLimit })}`)
