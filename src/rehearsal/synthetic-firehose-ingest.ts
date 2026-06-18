import assert from 'assert'
import { BlockMap, blocksToCarFile } from '@atproto/repo'
import { sql } from 'kysely'
import type { CID } from 'multiformats/cid'
import { createDb, migrateToLatest } from '../db'
import type { Database } from '../db'
import { ids, lexicons } from '../lexicon/lexicons'
import type {
  Commit,
  OutputSchema as RepoEvent,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscription } from '../subscription'
import { invalidatePublisherDidCache } from '../util/publisher-dids'

const SYNTHETIC_REPO = 'did:plc:syntheticfirehose'
const BLOCKED_REPO = 'did:plc:blockedfirehose'
const SYNTHETIC_SUBSCRIBER = 'did:plc:syntheticsubscriber'
const SYNTHETIC_PUBLISHER = 'did:plc:syntheticpublisher'
const POST_RKEY = 'synthetic-post'
const LIKE_RKEY = 'synthetic-like'
const BLOCKED_POST_RKEY = 'blocked-post'
const BLOCKED_LIKE_RKEY = 'blocked-like'
const POST_TEXT = 'synthetic firehose post'
const BLOCKED_POST_TEXT = 'blocked synthetic firehose post'

export type SyntheticFirehoseIngestResult = {
  status: 'ok'
  scope_mode: 'off' | 'allowlist'
  repo: string
  post_uri: string
  like_uri: string
  post_count: number
  engagement_count: number
  blocked_repo: string | null
  blocked_post_count: number | null
  blocked_engagement_count: number | null
  post_cid: string
  engagement_type: number
  car_block_count: number
  scoped_fixture_follows: number | null
  scoped_fixture_publishers: number | null
  scoped_ingestion: string
}

type Options = {
  connectionString: string
  scopedIngestion?: boolean
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
  const previousAllowlistRefresh = process.env.FEEDGEN_ALLOWLIST_REFRESH_MS
  const scopedIngestion = options.scopedIngestion === true
  process.env.FEEDGEN_SCOPED_INGESTION = scopedIngestion ? 'true' : 'false'
  process.env.FEEDGEN_ALLOWLIST_REFRESH_MS = '0'

  const db = createDb(options.connectionString)
  try {
    await migrateToLatest(db)

    if (scopedIngestion) {
      await seedScopedIngestionFixtures(db)
    }

    const bundle = await buildSyntheticCommitEvent({
      repo: SYNTHETIC_REPO,
      postRkey: POST_RKEY,
      likeRkey: LIKE_RKEY,
      text: POST_TEXT,
    })
    const blockedBundle = scopedIngestion
      ? await buildSyntheticCommitEvent({
          repo: BLOCKED_REPO,
          postRkey: BLOCKED_POST_RKEY,
          likeRkey: BLOCKED_LIKE_RKEY,
          text: BLOCKED_POST_TEXT,
        })
      : null
    const postUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.post/${POST_RKEY}`
    const likeUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.like/${LIKE_RKEY}`
    const blockedPostUri = `at://${BLOCKED_REPO}/app.bsky.feed.post/${BLOCKED_POST_RKEY}`
    const blockedLikeUri = `at://${BLOCKED_REPO}/app.bsky.feed.like/${BLOCKED_LIKE_RKEY}`

    await db
      .deleteFrom('engagement')
      .where('uri', 'in', [likeUri, postUri, blockedLikeUri, blockedPostUri])
      .execute()
    await db
      .deleteFrom('post')
      .where('uri', 'in', [postUri, blockedPostUri])
      .execute()

    await new FirehoseSubscription(
      db,
      'wss://synthetic-firehose.invalid',
    ).handleEvent(bundle.event)
    if (blockedBundle) {
      await new FirehoseSubscription(
        db,
        'wss://synthetic-firehose.invalid',
      ).handleEvent(blockedBundle.event)
    }

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
    const blockedPosts = await db
      .selectFrom('post')
      .selectAll()
      .where('uri', '=', blockedPostUri)
      .execute()
    const blockedEngagements = await db
      .selectFrom('engagement')
      .selectAll()
      .where('uri', '=', blockedLikeUri)
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
    if (scopedIngestion) {
      assert.equal(blockedPosts.length, 0)
      assert.equal(blockedEngagements.length, 0)
    }

    const scopedFixture = scopedIngestion
      ? await readScopedFixtureCounts(db)
      : { follows: null, publishers: null }

    return {
      status: 'ok',
      scope_mode: scopedIngestion ? 'allowlist' : 'off',
      repo: SYNTHETIC_REPO,
      post_uri: postUri,
      like_uri: likeUri,
      post_count: posts.length,
      engagement_count: engagements.length,
      blocked_repo: scopedIngestion ? BLOCKED_REPO : null,
      blocked_post_count: scopedIngestion ? blockedPosts.length : null,
      blocked_engagement_count: scopedIngestion ? blockedEngagements.length : null,
      post_cid: posts[0].cid,
      engagement_type: engagements[0].type,
      car_block_count: bundle.carBlockCount,
      scoped_fixture_follows: scopedFixture.follows,
      scoped_fixture_publishers: scopedFixture.publishers,
      scoped_ingestion: process.env.FEEDGEN_SCOPED_INGESTION,
    }
  } finally {
    await db.destroy()
    if (previousScopedIngestion === undefined) {
      delete process.env.FEEDGEN_SCOPED_INGESTION
    } else {
      process.env.FEEDGEN_SCOPED_INGESTION = previousScopedIngestion
    }
    if (previousAllowlistRefresh === undefined) {
      delete process.env.FEEDGEN_ALLOWLIST_REFRESH_MS
    } else {
      process.env.FEEDGEN_ALLOWLIST_REFRESH_MS = previousAllowlistRefresh
    }
  }
}

type SyntheticCommitSpec = {
  repo: string
  postRkey: string
  likeRkey: string
  text: string
}

async function buildSyntheticCommitEvent(
  spec: SyntheticCommitSpec,
): Promise<SyntheticCommitBundle> {
  const now = new Date().toISOString()
  const blocks = new BlockMap()
  const postUri = `at://${spec.repo}/app.bsky.feed.post/${spec.postRkey}`

  const postCid = await blocks.add({
    $type: ids.AppBskyFeedPost,
    text: spec.text,
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
    repo: spec.repo,
    rebase: false,
    tooBig: false,
    commit: postCid,
    prev: null,
    rev: '3syntheticfirehose',
    since: null,
    blocks: await blocksToCarFile(null, blocks),
    ops: [
      createOp(`${ids.AppBskyFeedPost}/${spec.postRkey}`, postCid),
      createOp(`${ids.AppBskyFeedLike}/${spec.likeRkey}`, likeCid),
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

async function seedScopedIngestionFixtures(db: Database): Promise<void> {
  invalidatePublisherDidCache()
  await sql`CREATE SCHEMA IF NOT EXISTS feedgen_ops`.execute(db)
  await sql`
    CREATE TABLE IF NOT EXISTS feedgen_ops.feed_catalog (
      feed_id text PRIMARY KEY,
      rkey text NOT NULL UNIQUE,
      display_name text NOT NULL,
      country text,
      publisher_did text,
      study_id text,
      algo_policy_id text NOT NULL,
      ranker_policy_id text,
      access_policy_id text NOT NULL,
      enabled boolean NOT NULL DEFAULT true,
      created_at timestamptz NOT NULL DEFAULT now(),
      retired_at timestamptz
    )
  `.execute(db)

  await db
    .insertInto('feedgen_ops.feed_catalog')
    .values({
      feed_id: `at://${SYNTHETIC_PUBLISHER}/app.bsky.feed.generator/synthetic-scope`,
      rkey: 'synthetic-scope',
      display_name: 'Synthetic Scope',
      country: 'ZZ',
      publisher_did: SYNTHETIC_PUBLISHER,
      study_id: null,
      algo_policy_id: 'chronological',
      ranker_policy_id: null,
      access_policy_id: 'subscriber-default',
      enabled: true,
      retired_at: null,
    })
    .onConflict((oc) =>
      oc.column('feed_id').doUpdateSet({
        publisher_did: SYNTHETIC_PUBLISHER,
        enabled: true,
        retired_at: null,
      }),
    )
    .execute()

  await db
    .insertInto('subscriber')
    .values({
      handle: 'synthetic-subscriber.test',
      did: SYNTHETIC_SUBSCRIBER,
    })
    .onConflict((oc) =>
      oc.column('did').doUpdateSet({ handle: 'synthetic-subscriber.test' }),
    )
    .execute()

  await db
    .insertInto('follows')
    .values({
      subject: SYNTHETIC_SUBSCRIBER,
      follows: SYNTHETIC_REPO,
    })
    .onConflict((oc) => oc.doNothing())
    .execute()
}

async function readScopedFixtureCounts(
  db: Database,
): Promise<{ follows: number; publishers: number }> {
  const follows = await db
    .selectFrom('follows')
    .select(({ fn }) => fn.countAll<number>().as('count'))
    .where('subject', '=', SYNTHETIC_SUBSCRIBER)
    .where('follows', '=', SYNTHETIC_REPO)
    .executeTakeFirstOrThrow()

  const publishers = await db
    .selectFrom('feedgen_ops.feed_catalog')
    .select(({ fn }) => fn.countAll<number>().as('count'))
    .where('publisher_did', '=', SYNTHETIC_PUBLISHER)
    .where('enabled', '=', true)
    .executeTakeFirstOrThrow()

  return {
    follows: Number(follows.count),
    publishers: Number(publishers.count),
  }
}
