import assert from 'assert'
import type { AddressInfo } from 'net'
import { XrpcStreamServer, MessageFrame } from '@atproto/xrpc-server'
import type {
  Commit,
  OutputSchema as RepoEvent,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { createDb, migrateToLatest } from '../db'
import { ids } from '../lexicon/lexicons'
import { FirehoseSubscription } from '../subscription'
import {
  buildSyntheticCommitEvent,
  LIKE_RKEY,
  POST_RKEY,
  POST_TEXT,
  SYNTHETIC_REPO,
} from './synthetic-firehose-ingest'

const CURSOR_SEQ = 20
const RECONNECT_CURSOR_SEQ = 40
const RECONNECT_POST_RKEY = 'synthetic-post-reconnect'
const RECONNECT_LIKE_RKEY = 'synthetic-like-reconnect'
const RECONNECT_POST_TEXT = 'synthetic firehose post after reconnect'

export type SyntheticFirehoseWebsocketResult = {
  status: 'ok'
  transport: 'xrpc_websocket'
  repo: string
  service_url: string
  request_url: string
  post_uri: string
  like_uri: string
  post_count: number
  engagement_count: number
  cursor_service: string
  cursor: number
  post_cid: string
  engagement_type: number
  car_block_count: number
  connection_count: number
  reconnect_resume_cursor: number | null
}

type Options = {
  connectionString: string
  reconnect?: boolean
}

export async function runSyntheticFirehoseWebsocketRehearsal(
  options: Options,
): Promise<SyntheticFirehoseWebsocketResult> {
  const db = createDb(options.connectionString)
  let server: XrpcStreamServer | null = null

  try {
    await migrateToLatest(db)

    const bundle = await buildSyntheticCommitEvent({
      repo: SYNTHETIC_REPO,
      postRkey: POST_RKEY,
      likeRkey: LIKE_RKEY,
      text: POST_TEXT,
      seq: CURSOR_SEQ,
    })

    const postUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.post/${POST_RKEY}`
    const likeUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.like/${LIKE_RKEY}`
    const reconnectBundle = options.reconnect
      ? await buildSyntheticCommitEvent({
          repo: SYNTHETIC_REPO,
          postRkey: RECONNECT_POST_RKEY,
          likeRkey: RECONNECT_LIKE_RKEY,
          text: RECONNECT_POST_TEXT,
          seq: RECONNECT_CURSOR_SEQ,
        })
      : null
    const reconnectPostUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.post/${RECONNECT_POST_RKEY}`
    const reconnectLikeUri = `at://${SYNTHETIC_REPO}/app.bsky.feed.like/${RECONNECT_LIKE_RKEY}`

    await db.deleteFrom('engagement').where('uri', '=', likeUri).execute()
    await db.deleteFrom('post').where('uri', '=', postUri).execute()
    await db
      .deleteFrom('engagement')
      .where('uri', '=', reconnectLikeUri)
      .execute()
    await db.deleteFrom('post').where('uri', '=', reconnectPostUri).execute()
    const requestUrls: string[] = []
    server = new XrpcStreamServer({
      host: '127.0.0.1',
      port: 0,
      async *handler(req) {
        requestUrls.push(req.url ?? '')
        const event =
          reconnectBundle && requestUrls.length > 1
            ? reconnectBundle.event
            : bundle.event
        yield new MessageFrame(stripType(event), { type: '#commit' })
      },
    })

    await waitForServer(server)
    const address = server.wss.address() as AddressInfo
    const serviceUrl = `ws://127.0.0.1:${address.port}`
    await db
      .deleteFrom('sub_state')
      .where('service', '=', serviceUrl)
      .execute()

    await withTimeout(
      new FirehoseSubscription(db, serviceUrl).run(10),
      5_000,
      'synthetic firehose WebSocket subscription did not finish',
    )
    if (options.reconnect) {
      await withTimeout(
        new FirehoseSubscription(db, serviceUrl).run(10),
        5_000,
        'synthetic firehose WebSocket reconnect subscription did not finish',
      )
    }

    const posts = await db
      .selectFrom('post')
      .selectAll()
      .where('uri', 'in', [postUri, reconnectPostUri])
      .execute()
    const engagements = await db
      .selectFrom('engagement')
      .selectAll()
      .where('uri', 'in', [likeUri, reconnectLikeUri])
      .execute()
    const cursor = await db
      .selectFrom('sub_state')
      .selectAll()
      .where('service', '=', serviceUrl)
      .executeTakeFirst()

    const postByUri = new Map(posts.map((post) => [post.uri, post]))
    const engagementByUri = new Map(engagements.map((eng) => [eng.uri, eng]))
    const post = postByUri.get(postUri)
    const engagement = engagementByUri.get(likeUri)

    assert.equal(posts.length, options.reconnect ? 2 : 1)
    assert.equal(post?.cid, bundle.postCid)
    assert.equal(post?.author, SYNTHETIC_REPO)
    assert.equal(post?.text, POST_TEXT)
    assert.equal(engagements.length, options.reconnect ? 2 : 1)
    assert.equal(engagement?.subjectUri, postUri)
    assert.equal(engagement?.subjectCid, bundle.postCid)
    assert.equal(engagement?.type, 2)
    assert.equal(
      Number(cursor?.cursor),
      options.reconnect ? RECONNECT_CURSOR_SEQ : CURSOR_SEQ,
    )
    assert.ok(
      requestUrls[0]?.startsWith(`/xrpc/${ids.ComAtprotoSyncSubscribeRepos}`),
    )
    if (options.reconnect) {
      const reconnectPost = postByUri.get(reconnectPostUri)
      const reconnectEngagement = engagementByUri.get(reconnectLikeUri)
      assert.equal(reconnectPost?.cid, reconnectBundle?.postCid)
      assert.equal(reconnectPost?.author, SYNTHETIC_REPO)
      assert.equal(reconnectPost?.text, RECONNECT_POST_TEXT)
      assert.equal(reconnectEngagement?.subjectUri, reconnectPostUri)
      assert.equal(reconnectEngagement?.subjectCid, reconnectBundle?.postCid)
      assert.equal(reconnectEngagement?.type, 2)
      assert.equal(readCursorFromRequestUrl(requestUrls[1]), CURSOR_SEQ)
    }

    return {
      status: 'ok',
      transport: 'xrpc_websocket',
      repo: SYNTHETIC_REPO,
      service_url: serviceUrl,
      request_url: requestUrls.join(','),
      post_uri: postUri,
      like_uri: likeUri,
      post_count: posts.length,
      engagement_count: engagements.length,
      cursor_service: serviceUrl,
      cursor: Number(cursor?.cursor),
      post_cid: post?.cid ?? '',
      engagement_type: engagement?.type ?? 0,
      car_block_count: bundle.carBlockCount,
      connection_count: requestUrls.length,
      reconnect_resume_cursor: options.reconnect
        ? readCursorFromRequestUrl(requestUrls[1])
        : null,
    }
  } finally {
    if (server) {
      await closeServer(server)
    }
    await db.destroy()
  }
}

function readCursorFromRequestUrl(requestUrl: string | undefined): number | null {
  if (!requestUrl) return null
  const parsed = new URL(requestUrl, 'ws://synthetic.local')
  const cursor = parsed.searchParams.get('cursor')
  return cursor ? Number(cursor) : null
}

function stripType(evt: RepoEvent): Omit<Commit, '$type'> {
  const { $type: _type, ...body } = evt as Commit
  return body
}

async function waitForServer(server: XrpcStreamServer): Promise<void> {
  if (server.wss.address()) return
  await new Promise<void>((resolve) => {
    server.wss.once('listening', () => resolve())
  })
}

async function closeServer(server: XrpcStreamServer): Promise<void> {
  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => resolve(), 1_000)
    for (const client of server.wss.clients) {
      client.terminate()
    }
    server.wss.close(() => {
      clearTimeout(timeout)
      resolve()
    })
  })
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  message: string,
): Promise<T> {
  let timeout: NodeJS.Timeout | null = null
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), timeoutMs)
      }),
    ])
  } finally {
    if (timeout) clearTimeout(timeout)
  }
}
