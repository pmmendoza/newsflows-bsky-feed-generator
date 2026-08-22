import { PublisherTimeClock } from '../db/schema'
import { sql } from 'kysely'
import { contentTimeSupportedSql } from '../util/content-time'

export function applyPublisherTimeFilter(query: any, clock: PublisherTimeClock, cutoffIso: string, contractVersion: string | null = null) {
  if (clock === 'content_time_v1') {
    return query
      .where(contentTimeSupportedSql('post', contractVersion))
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
