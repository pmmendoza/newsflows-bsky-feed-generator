import {
  DISPATCH_CACHE_TTL_MS,
  invalidateDispatchCache,
  resolveDynamicHandler,
} from '../src/algos/catalog-dispatch'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

let row = {
  feed_id: 'be-k', rkey: 'newsflow-be-k', publisher_did: 'did:be',
  algo_policy_id: 'chronological', enabled: true,
  publisher_post_max_age_days: 7, publisher_post_max_age_source: 'feed_override',
  publisher_time_clock: 'receipt_time',
}
let reads = 0
const db: any = {
  selectFrom: () => {
    const query: any = {
      select: () => query,
      where: () => query,
      executeTakeFirst: async () => { reads += 1; return { ...row } },
    }
    return query
  },
}

async function run() {
  invalidateDispatchCache()
  const first = await resolveDynamicHandler(db, row.rkey, 1_000)
  check(first !== null && reads === 1, 'first resolution must read the catalog')

  row = { ...row, publisher_post_max_age_days: 10, publisher_post_max_age_source: 'study_default' }
  const cached = await resolveDynamicHandler(db, row.rkey, 1_001)
  check(cached === first && reads === 1, 'value remains cached before activation')

  // This is the same invalidation function called by the LISTEN/NOTIFY listener.
  invalidateDispatchCache(row.rkey)
  const notified = await resolveDynamicHandler(db, row.rkey, 1_002)
  check(notified !== null && notified !== first && Number(reads) === 2, 'notification invalidation must activate the new row')

  const ttl = await resolveDynamicHandler(db, row.rkey, 1_002 + DISPATCH_CACHE_TTL_MS + 1)
  check(ttl !== null && ttl !== notified && Number(reads) === 3, 'TTL expiry must activate without notification or restart')

  row = { ...row, algo_policy_id: 'ranker-priority' }
  invalidateDispatchCache(row.rkey)
  const transitionalRanker = await resolveDynamicHandler(db, row.rkey, 2_000)
  check(transitionalRanker !== null, 'legacy ranked row without new controls must retain 24h compatibility behavior')

  console.log('publisher window activation tests passed')
}

run().catch((error) => { console.error(error); process.exit(1) })
