import { PublisherTimeClock } from '../db/schema'
import { sql } from 'kysely'

export function applyPublisherTimeFilter(query: any, clock: PublisherTimeClock, cutoffIso: string) {
  if (clock === 'content_time_v1') {
    return query
      .where('post.content_time_status', '=', 'source_valid')
      .where(sql<boolean>`${sql.ref('post.content_time_utc')}::timestamptz >= ${cutoffIso}::timestamptz`)
  }
  return query.where('post.indexedAt', '>=', cutoffIso)
}

export function applyPublisherRecencyOrder(query: any, clock: PublisherTimeClock) {
  if (clock === 'content_time_v1') {
    return query
      .orderBy(sql`${sql.ref('post.content_time_utc')}::timestamptz`, 'desc')
      .orderBy('post.indexedAt', 'desc')
      .orderBy('post.uri', 'desc')
  }
  return query.orderBy('post.indexedAt', 'desc').orderBy('post.cid', 'desc')
}
