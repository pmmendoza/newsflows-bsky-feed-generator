/**
 * Unit tests for the publisher post backfill planner.
 *
 * Run: `npx ts-node scripts/test_publisher_post_backfill.ts`
 */

import assert from 'assert'
import {
  collectPublisherPosts,
  normalizeAppViewPost,
  type AuthorFeedPage,
} from '../src/tools/backfill-publisher-posts'

const publisherDid = 'did:plc:publisher'

const basePost = {
  uri: 'at://did:plc:publisher/app.bsky.feed.post/post1',
  cid: 'bafy-post1',
  author: { did: publisherDid },
  indexedAt: '2026-06-05T00:05:00.000Z',
  record: {
    text: 'hello\u0000 world',
    createdAt: '2026-06-05T00:00:00.000Z',
    embed: {
      external: {
        uri: 'https://example.com/story',
        title: 'Story\u0000 title',
        description: 'Story description',
      },
    },
  },
}

async function main() {
  const normalized = normalizeAppViewPost(basePost, publisherDid)
  assert.deepEqual(normalized, {
    uri: 'at://did:plc:publisher/app.bsky.feed.post/post1',
    cid: 'bafy-post1',
    author: publisherDid,
    indexedAt: '2026-06-05T00:05:00.000Z',
    createdAt: '2026-06-05T00:00:00.000Z',
    text: 'hello world',
    rootUri: '',
    rootCid: '',
    link_uri: 'https://example.com/story',
    link_title: 'Story title',
    link_description: 'Story description',
    linkUrl: 'https://example.com/story',
    linkTitle: 'Story title',
    linkDescription: 'Story description',
  })

  assert.equal(
    normalizeAppViewPost({ ...basePost, author: { did: 'did:plc:other' } }, publisherDid),
    null,
    'normalizer must reject rows for another author',
  )

  const pages: Record<string, AuthorFeedPage> = {
    first: {
      posts: [
        basePost,
        {
          ...basePost,
          uri: 'at://did:plc:publisher/app.bsky.feed.post/old',
          cid: 'bafy-old',
          record: { ...basePost.record, createdAt: '2026-06-01T00:00:00.000Z' },
        },
      ],
      cursor: 'second',
    },
    second: {
      posts: [
        {
          ...basePost,
          uri: 'at://did:plc:publisher/app.bsky.feed.post/post2',
          cid: 'bafy-post2',
          record: { ...basePost.record, createdAt: '2026-06-05T00:30:00.000Z' },
        },
      ],
    },
  }

  const collected = await collectPublisherPosts({
    actors: [publisherDid],
    since: new Date('2026-06-04T00:00:00.000Z'),
    until: new Date('2026-06-06T00:00:00.000Z'),
    fetchPage: async (_actor, cursor) => pages[cursor || 'first'],
  })

  assert.deepEqual(
    collected.posts.map((post) => post.uri).sort(),
    [
      'at://did:plc:publisher/app.bsky.feed.post/post1',
      'at://did:plc:publisher/app.bsky.feed.post/post2',
    ],
  )
  assert.equal(collected.scanned, 3)
  assert.equal(collected.skipped_out_of_window, 1)
  assert.deepEqual(collected.by_actor[publisherDid], {
    scanned: 3,
    candidate_posts: 2,
    skipped_out_of_window: 1,
    skipped_wrong_author: 0,
  })

  console.log('publisher post backfill tests passed')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
