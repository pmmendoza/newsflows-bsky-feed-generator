import { FeedCatalog } from '../db/schema'

export function isSubscribableFeed(
  feed: Pick<FeedCatalog, 'enabled' | 'retired_at' | 'access_policy_id'>,
): boolean {
  return feed.enabled === true && !feed.retired_at && (
    feed.access_policy_id === 'subscriber-default' || feed.access_policy_id === 'study-only'
  )
}
