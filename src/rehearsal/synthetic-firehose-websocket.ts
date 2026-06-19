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
}

type Options = {
  connectionString: string
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

    await db.deleteFrom('engagement').where('uri', '=', likeUri).execute()
    await db.deleteFrom('post').where('uri', '=', postUri).execute()
    let requestUrl = ''
    server = new XrpcStreamServer({
      host: '127.0.0.1',
      port: 0,
      async *handler(req) {
        requestUrl = req.url ?? ''
        yield new MessageFrame(stripType(bundle.event), { type: '#commit' })
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
    const cursor = await db
      .selectFrom('sub_state')
      .selectAll()
      .where('service', '=', serviceUrl)
      .executeTakeFirst()

    assert.equal(posts.length, 1)
    assert.equal(posts[0].cid, bundle.postCid)
    assert.equal(posts[0].author, SYNTHETIC_REPO)
    assert.equal(posts[0].text, POST_TEXT)
    assert.equal(engagements.length, 1)
    assert.equal(engagements[0].subjectUri, postUri)
    assert.equal(engagements[0].subjectCid, bundle.postCid)
    assert.equal(engagements[0].type, 2)
    assert.equal(Number(cursor?.cursor), CURSOR_SEQ)
    assert.ok(requestUrl.startsWith(`/xrpc/${ids.ComAtprotoSyncSubscribeRepos}`))

    return {
      status: 'ok',
      transport: 'xrpc_websocket',
      repo: SYNTHETIC_REPO,
      service_url: serviceUrl,
      request_url: requestUrl,
      post_uri: postUri,
      like_uri: likeUri,
      post_count: posts.length,
      engagement_count: engagements.length,
      cursor_service: serviceUrl,
      cursor: Number(cursor?.cursor),
      post_cid: posts[0].cid,
      engagement_type: engagements[0].type,
      car_block_count: bundle.carBlockCount,
    }
  } finally {
    if (server) {
      await closeServer(server)
    }
    await db.destroy()
  }
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
    server.wss.close(() => resolve())
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
