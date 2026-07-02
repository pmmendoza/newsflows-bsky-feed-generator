import { Subscription } from '@atproto/xrpc-server'
import { cborToLexRecord, readCar } from '@atproto/repo'
import { BlobRef } from '@atproto/lexicon'
import { ids, lexicons } from '../lexicon/lexicons'
import { Record as PostRecord } from '../lexicon/types/app/bsky/feed/post'
import { Record as RepostRecord } from '../lexicon/types/app/bsky/feed/repost'
import { Record as LikeRecord } from '../lexicon/types/app/bsky/feed/like'
import { Record as FollowRecord } from '../lexicon/types/app/bsky/graph/follow'
import {
  Commit,
  OutputSchema as RepoEvent,
  isCommit,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { Database } from '../db'

export abstract class FirehoseSubscriptionBase {
  public sub: Subscription<RepoEvent>
  private hasLoggedStartCursor = false
  private hasPersistedCursor = false
  private abortController?: AbortController
  private idleWatchdog?: NodeJS.Timeout
  private reconnectTimer?: NodeJS.Timeout
  private lastProgressMs = Date.now()
  private runGeneration = 0
  private stopped = false
  private idleTimeoutMs: number

  constructor(
    public db: Database,
    public service: string,
    options: { idleTimeoutMs?: number } = {},
  ) {
    this.idleTimeoutMs = Math.max(0, options.idleTimeoutMs ?? 0)
    this.sub = this.createSubscription()
  }

  protected createSubscription(): Subscription<RepoEvent> {
    this.abortController = new AbortController()
    return new Subscription({
      service: this.service,
      method: ids.ComAtprotoSyncSubscribeRepos,
      getParams: () => this.getCursor(),
      signal: this.abortController.signal,
      validate: (value: unknown) => {
        try {
          return lexicons.assertValidXrpcMessage<RepoEvent>(
            ids.ComAtprotoSyncSubscribeRepos,
            value,
          )
        } catch (err) {
          console.error('repo subscription skipped invalid message', err)
        }
      },
    })
  }

  abstract handleEvent(evt: RepoEvent): Promise<void>

  async run(subscriptionReconnectDelay: number) {
    const generation = ++this.runGeneration
    this.stopped = false
    this.lastProgressMs = Date.now()
    this.startIdleWatchdog(subscriptionReconnectDelay)

    try {
      if (!this.hasLoggedStartCursor) {
        try {
          const start = await this.getCursor()
          if (typeof start.cursor === 'number') {
            console.log(
              `[${this.service}] Starting repo subscription from cursor ${start.cursor}`,
            )
          } else {
            console.log(
              `[${this.service}] No stored cursor; starting repo subscription without cursor`,
            )
          }
        } catch (err) {
          console.error(
            `[${this.service}] Failed to read stored cursor; starting without cursor`,
            err,
          )
        } finally {
          this.hasLoggedStartCursor = true
        }
      }

      for await (const evt of this.sub) {
        if (generation !== this.runGeneration || this.stopped) break
        this.lastProgressMs = Date.now()
        await this.handleEvent(evt)
        this.lastProgressMs = Date.now()
        // update stored cursor every 20 events or so
        if (isCommit(evt) && evt.seq % 20 === 0) {
          await this.updateCursor(evt.seq)
          this.lastProgressMs = Date.now()
        }
      }
    } catch (err) {
      if (this.stopped || generation !== this.runGeneration) return
      console.error('repo subscription errored', err)
      this.scheduleReconnect(subscriptionReconnectDelay, 'error')
    } finally {
      if (generation === this.runGeneration && !this.reconnectTimer) {
        this.stopIdleWatchdog()
      }
    }
  }

  stop() {
    this.stopped = true
    this.runGeneration += 1
    this.stopIdleWatchdog()
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = undefined
    }
    this.abortController?.abort()
  }

  private startIdleWatchdog(subscriptionReconnectDelay: number) {
    this.stopIdleWatchdog()
    if (this.idleTimeoutMs <= 0) return

    const intervalMs = Math.max(10, Math.min(this.idleTimeoutMs, 30_000))
    this.idleWatchdog = setInterval(() => {
      const idleForMs = Date.now() - this.lastProgressMs
      if (idleForMs < this.idleTimeoutMs) return

      this.scheduleReconnect(
        subscriptionReconnectDelay,
        `idle for ${idleForMs}ms (limit ${this.idleTimeoutMs}ms)`,
      )
    }, intervalMs)
  }

  private stopIdleWatchdog() {
    if (!this.idleWatchdog) return
    clearInterval(this.idleWatchdog)
    this.idleWatchdog = undefined
  }

  private scheduleReconnect(subscriptionReconnectDelay: number, reason: string) {
    if (this.stopped || this.reconnectTimer) return

    console.error(`[${this.service}] Reconnecting repo subscription: ${reason}`)
    this.runGeneration += 1
    this.hasLoggedStartCursor = false
    this.stopIdleWatchdog()
    this.abortController?.abort()
    this.sub = this.createSubscription()
    this.lastProgressMs = Date.now()

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined
      void this.run(subscriptionReconnectDelay)
    }, subscriptionReconnectDelay)
  }

  async updateCursor(cursor: number) {
    const bigintCursor = BigInt(cursor)
    await this.db
      .insertInto('sub_state')
      .values({ service: this.service, cursor: bigintCursor })
      .onConflict((oc) => oc.column('service').doUpdateSet({ cursor: bigintCursor }))
      .execute()

    if (!this.hasPersistedCursor) {
      this.hasPersistedCursor = true
      console.log(`[${this.service}] Persisted cursor ${cursor}`)
    } else if (cursor % 10000 === 0) {
      console.log(`[${this.service}] Persisted cursor ${cursor}`)
    }
  }

  async getCursor(): Promise<{ cursor?: number }> {
    const res = await this.db
      .selectFrom('sub_state')
      .selectAll()
      .where('service', '=', this.service)
      .executeTakeFirst()
    
    return res ? { cursor: Number(res.cursor) } : {}
  }
}

export const getOpsByType = async (evt: Commit): Promise<OperationsByType> => {
  const car = await readCar(evt.blocks)
  const opsByType: OperationsByType = {
    posts: { creates: [], deletes: [] },
    reposts: { creates: [], deletes: [] },
    likes: { creates: [], deletes: [] },
    follows: { creates: [], deletes: [] },
  }

  for (const op of evt.ops) {
    const uri = `at://${evt.repo}/${op.path}`
    const [collection] = op.path.split('/')

    if (op.action === 'update') continue // updates not supported yet

    if (op.action === 'create') {
      if (!op.cid) continue
      const recordBytes = car.blocks.get(op.cid)
      if (!recordBytes) continue
      const record = cborToLexRecord(recordBytes)
      const create = { uri, cid: op.cid.toString(), author: evt.repo }
      if (collection === ids.AppBskyFeedPost && isPost(record)) {
        opsByType.posts.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyFeedRepost && isRepost(record)) {
        opsByType.reposts.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyFeedLike && isLike(record)) {
        opsByType.likes.creates.push({ record, ...create })
      } else if (collection === ids.AppBskyGraphFollow && isFollow(record)) {
        opsByType.follows.creates.push({ record, ...create })
      }
    }

    if (op.action === 'delete') {
      if (collection === ids.AppBskyFeedPost) {
        opsByType.posts.deletes.push({ uri })
      } else if (collection === ids.AppBskyFeedRepost) {
        opsByType.reposts.deletes.push({ uri })
      } else if (collection === ids.AppBskyFeedLike) {
        opsByType.likes.deletes.push({ uri })
      } else if (collection === ids.AppBskyGraphFollow) {
        opsByType.follows.deletes.push({ uri })
      }
    }
  }

  return opsByType
}

type OperationsByType = {
  posts: Operations<PostRecord>
  reposts: Operations<RepostRecord>
  likes: Operations<LikeRecord>
  follows: Operations<FollowRecord>
}

type Operations<T = Record<string, unknown>> = {
  creates: CreateOp<T>[]
  deletes: DeleteOp[]
}

type CreateOp<T> = {
  uri: string
  cid: string
  author: string
  record: T
}

type DeleteOp = {
  uri: string
}

export const isPost = (obj: unknown): obj is PostRecord => {
  return isType(obj, ids.AppBskyFeedPost)
}

export const isRepost = (obj: unknown): obj is RepostRecord => {
  return isType(obj, ids.AppBskyFeedRepost)
}

export const isLike = (obj: unknown): obj is LikeRecord => {
  return isType(obj, ids.AppBskyFeedLike)
}

export const isFollow = (obj: unknown): obj is FollowRecord => {
  return isType(obj, ids.AppBskyGraphFollow)
}

const isType = (obj: unknown, nsid: string) => {
  try {
    lexicons.assertValidRecord(nsid, fixBlobRefs(obj))
    return true
  } catch (err) {
    return false
  }
}

// @TODO right now record validation fails on BlobRefs
// simply because multiple packages have their own copy
// of the BlobRef class, causing instanceof checks to fail.
// This is a temporary solution.
const fixBlobRefs = (obj: unknown): unknown => {
  if (Array.isArray(obj)) {
    return obj.map(fixBlobRefs)
  }
  if (obj && typeof obj === 'object') {
    if (obj.constructor.name === 'BlobRef') {
      const blob = obj as BlobRef
      return new BlobRef(blob.ref, blob.mimeType, blob.size, blob.original)
    }
    return Object.entries(obj).reduce((acc, [key, val]) => {
      return Object.assign(acc, { [key]: fixBlobRefs(val) })
    }, {} as Record<string, unknown>)
  }
  return obj
}
