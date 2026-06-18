import assert from 'assert'
import { BlockMap, blocksToCarFile } from '@atproto/repo'
import type { CID } from 'multiformats/cid'
import { createDb, migrateToLatest } from '../db'
import { ids, lexicons } from '../lexicon/lexicons'
import type {
  Commit,
  OutputSchema as RepoEvent,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscription } from '../subscription'

const SYNTHETIC_REPO = 'did:plc:syntheticfirehose'
const POST_RKEY = 'synthetic-post'
const LIKE_RKEY = 'synthetic-like'
const POST_TEXT = 'synthetic firehose post'

export type SyntheticFirehoseIngestResult = {
  status: 'ok'
  repo: string
  post_uri: string
  like_uri: string
  post_count: number
  engagement_count: number
  post_cid: string
  engagement_type: number
  car_block_count: number
  scoped_ingestion: string
}

type Options = {
  connectionString: string
}

type SyntheticCommitBundle = {
  event: RepoEvent
  postCid: string
  carBlockCount: number
}

export async function runSyntheticFirehoseIngestRehearsal(
  options: Options,
): Promise<SyntheticFirehoseIngestResult> {
  const previousScopedIngestion = process.env.FEEDGEN_SCOPED_INGESTION
  process.env.FEEDGEN_SCOPED_INGESTION = 'false'

  const db = createDb(options.connectionString)
  try {
    await migrateToLatest(db)

    const bundle = await buildSyntheticCommitEvent()
    const postUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.post/${POST_RKEY}`
    const likeUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.like/${LIKE_RKEY}`

    await db
      .deleteFrom('engagement')
      .where('uri', 'in', [likeUri, postUri])
      .execute()
    await db.deleteFrom('post').where('uri', '=', postUri).execute()

    await new FirehoseSubscription(
      db,
      'wss://synthetic-firehose.invalid',
    ).handleEvent(bundle.event)

    const posts = await db
      .selectFrom('post')
      .selectAll()
      .where('uri', '=', postUri)
      .execute()
    const engagements = await db
      .selectFrom('engagement')
      .selectAll()
      .where('uri', '=', likeUri)
      .execute()

    assert.equal(posts.length, 1)
    assert.equal(posts[0].uri, postUri)
    assert.equal(posts[0].cid, bundle.postCid)
    assert.equal(posts[0].author, SYNTHETIC_REPO)
    assert.equal(posts[0].text, POST_TEXT)
    assert.equal(posts[0].rootUri, '')
    assert.equal(posts[0].rootCid, '')
    assert.equal(posts[0].linkUrl, '')
    assert.equal(posts[0].linkTitle, '')
    assert.equal(posts[0].linkDescription, '')

    assert.equal(engagements.length, 1)
    assert.equal(engagements[0].uri, likeUri)
    assert.equal(engagements[0].subjectUri, postUri)
    assert.equal(engagements[0].subjectCid, bundle.postCid)
    assert.equal(engagements[0].type, 2)
    assert.equal(engagements[0].author, SYNTHETIC_REPO)

    return {
      status: 'ok',
      repo: SYNTHETIC_REPO,
      post_uri: postUri,
      like_uri: likeUri,
      post_count: posts.length,
      engagement_count: engagements.length,
      post_cid: posts[0].cid,
      engagement_type: engagements[0].type,
      car_block_count: bundle.carBlockCount,
      scoped_ingestion: process.env.FEEDGEN_SCOPED_INGESTION,
    }
  } finally {
    await db.destroy()
    if (previousScopedIngestion === undefined) {
      delete process.env.FEEDGEN_SCOPED_INGESTION
    } else {
      process.env.FEEDGEN_SCOPED_INGESTION = previousScopedIngestion
    }
  }
}

async function buildSyntheticCommitEvent(): Promise<SyntheticCommitBundle> {
  const now = new Date().toISOString()
  const blocks = new BlockMap()
  const postUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.post/${POST_RKEY}`

  const postCid = await blocks.add({
    $type: ids.AppBskyFeedPost,
    text: POST_TEXT,
    createdAt: now,
  })
  const likeCid = await blocks.add({
    $type: ids.AppBskyFeedLike,
    subject: { uri: postUri, cid: postCid.toString() },
    createdAt: now,
  })

  const event = {
    $type: 'com.atproto.sync.subscribeRepos#commit',
    seq: 1,
    repo: SYNTHETIC_REPO,
    rebase: false,
    tooBig: false,
    commit: postCid,
    prev: null,
    rev: '3syntheticfirehose',
    since: null,
    blocks: await blocksToCarFile(null, blocks),
    ops: [
      createOp(`${ids.AppBskyFeedPost}/${POST_RKEY}`, postCid),
      createOp(`${ids.AppBskyFeedLike}/${LIKE_RKEY}`, likeCid),
    ],
    blobs: [],
    time: now,
  } as Commit

  lexicons.assertValidXrpcMessage(ids.ComAtprotoSyncSubscribeRepos, event)
  return {
    event,
    postCid: postCid.toString(),
    carBlockCount: blocks.size,
  }
}

function createOp(path: string, cid: CID): Commit['ops'][number] {
  return {
    action: 'create',
    path,
    cid,
  }
}
