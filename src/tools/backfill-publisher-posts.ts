import dotenv from 'dotenv'
import { createDb } from '../db'

type AppViewPost = {
  uri?: string
  cid?: string
  author?: { did?: string }
  indexedAt?: string
  record?: {
    text?: string
    createdAt?: string
    reply?: {
      root?: { uri?: string; cid?: string }
    }
    embed?: any
  }
}

export type AuthorFeedPage = {
  posts: AppViewPost[]
  cursor?: string
}

export type BackfillPostRow = {
  uri: string
  cid: string
  indexedAt: string
  createdAt: string
  author: string
  text: string
  rootUri: string
  rootCid: string
  linkUrl: string
  linkTitle: string
  linkDescription: string
}

export type BackfillPlan = {
  posts: BackfillPostRow[]
  scanned: number
  skipped_out_of_window: number
  skipped_wrong_author: number
  by_actor: Record<string, {
    scanned: number
    candidate_posts: number
    skipped_out_of_window: number
    skipped_wrong_author: number
  }>
}

type FetchPage = (actor: string, cursor?: string) => Promise<AuthorFeedPage>

type CollectOptions = {
  actors: string[]
  since: Date
  until: Date
  fetchPage: FetchPage
  maxPagesPerActor?: number
}

type CliOptions = {
  actors: string[]
  since: Date
  until: Date
  apply: boolean
  json: boolean
  apiBase: string
  maxPagesPerActor: number
  dbUrl: string
}

function sanitizeForPostgres(text: string | null | undefined): string {
  if (text === null || text === undefined) return ''
  return text.replace(/\0/g, '')
}

function clampCreatedAt(raw: string | undefined | null, indexedAt: string): string {
  if (!raw) return indexedAt
  const d = new Date(raw)
  const i = new Date(indexedAt)
  if (isNaN(d.getTime()) || isNaN(i.getTime())) return indexedAt
  if (d.getTime() > i.getTime() + 86_400_000) return indexedAt
  if (d.getTime() < i.getTime() - 2 * 365 * 86_400_000) return indexedAt
  return raw
}

function externalEmbed(embed: any): { uri?: string; title?: string; description?: string } | null {
  if (embed?.external && typeof embed.external.uri === 'string') return embed.external
  return null
}

function parseDate(value: string | undefined): Date | null {
  if (!value) return null
  const parsed = new Date(value)
  return isNaN(parsed.getTime()) ? null : parsed
}

export function normalizeAppViewPost(
  post: AppViewPost,
  expectedAuthorDid: string,
): BackfillPostRow | null {
  if (!post.uri || !post.cid) return null
  if (post.author?.did !== expectedAuthorDid) return null

  const indexedAt = post.indexedAt || new Date().toISOString()
  const record = post.record || {}
  const embed = externalEmbed(record.embed)

  return {
    uri: post.uri,
    cid: post.cid,
    indexedAt,
    createdAt: clampCreatedAt(record.createdAt, indexedAt),
    author: expectedAuthorDid,
    text: sanitizeForPostgres(record.text),
    rootUri: record.reply?.root?.uri || '',
    rootCid: record.reply?.root?.cid || '',
    linkUrl: embed?.uri || '',
    linkTitle: sanitizeForPostgres(embed?.title),
    linkDescription: sanitizeForPostgres(embed?.description),
  }
}

export async function collectPublisherPosts(options: CollectOptions): Promise<BackfillPlan> {
  const maxPagesPerActor = options.maxPagesPerActor ?? 50
  const posts = new Map<string, BackfillPostRow>()
  let scanned = 0
  let skippedOutOfWindow = 0
  let skippedWrongAuthor = 0
  const byActor: BackfillPlan['by_actor'] = {}

  for (const actor of options.actors) {
    byActor[actor] = {
      scanned: 0,
      candidate_posts: 0,
      skipped_out_of_window: 0,
      skipped_wrong_author: 0,
    }
    let cursor: string | undefined
    for (let page = 0; page < maxPagesPerActor; page += 1) {
      const authorPage = await options.fetchPage(actor, cursor)
      if (!authorPage.posts.length) break

      for (const post of authorPage.posts) {
        scanned += 1
        byActor[actor].scanned += 1
        const normalized = normalizeAppViewPost(post, actor)
        if (!normalized) {
          skippedWrongAuthor += 1
          byActor[actor].skipped_wrong_author += 1
          continue
        }
        const created = parseDate(normalized.createdAt)
        if (!created || created < options.since || created >= options.until) {
          skippedOutOfWindow += 1
          byActor[actor].skipped_out_of_window += 1
          continue
        }
        posts.set(normalized.uri, normalized)
        byActor[actor].candidate_posts += 1
      }

      if (!authorPage.cursor) break
      cursor = authorPage.cursor
    }
  }

  return {
    posts: [...posts.values()],
    scanned,
    skipped_out_of_window: skippedOutOfWindow,
    skipped_wrong_author: skippedWrongAuthor,
    by_actor: byActor,
  }
}

function splitCsv(value: string | undefined): string[] {
  return (value || '')
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
}

function requireValue(value: string | undefined, message: string): string {
  if (!value) throw new Error(message)
  return value
}

function dbUrlFromEnv(): string {
  if (process.env.FEEDGEN_POSTGRES_URL) return process.env.FEEDGEN_POSTGRES_URL

  const host = process.env.FEEDGEN_DB_HOST || 'feedgen-db'
  const port = process.env.FEEDGEN_DB_PORT || '5432'
  const user = process.env.FEEDGEN_DB_USER || 'feedgen'
  const password = process.env.FEEDGEN_DB_PASSWORD || process.env.FEEDGEN_DBPASSWORD || 'feedgen'
  const database =
    process.env.FEEDGEN_DB_DATABASE ||
    process.env.FEEDGEN_DB_BASE ||
    process.env.FEEDGEN_DBBASE ||
    'feedgen-db'

  return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${database}`
}

function parseCliArgs(argv: string[]): CliOptions {
  const flags = new Map<string, string[]>()
  let apply = false
  let json = false

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--apply') {
      apply = true
      continue
    }
    if (arg === '--dry-run') {
      apply = false
      continue
    }
    if (arg === '--json') {
      json = true
      continue
    }
    if (!arg.startsWith('--')) throw new Error(`Unexpected positional argument: ${arg}`)
    const value = argv[i + 1]
    if (!value || value.startsWith('--')) throw new Error(`Missing value for ${arg}`)
    i += 1
    const key = arg.slice(2)
    flags.set(key, [...(flags.get(key) || []), value])
  }

  const actors = [
    ...splitCsv(flags.get('actors')?.join(',')),
    ...splitCsv(flags.get('dids')?.join(',')),
  ]
  if (!actors.length) throw new Error('Pass --actors or --dids with comma-separated publisher DIDs')

  const since = new Date(requireValue(flags.get('since')?.at(-1), 'Pass --since ISO timestamp'))
  const until = new Date(requireValue(flags.get('until')?.at(-1), 'Pass --until ISO timestamp'))
  if (isNaN(since.getTime())) throw new Error('--since must be an ISO timestamp')
  if (isNaN(until.getTime())) throw new Error('--until must be an ISO timestamp')
  if (until <= since) throw new Error('--until must be after --since')

  return {
    actors,
    since,
    until,
    apply,
    json,
    apiBase: flags.get('api-base')?.at(-1) || 'https://public.api.bsky.app',
    maxPagesPerActor: Number.parseInt(flags.get('max-pages-per-actor')?.at(-1) || '50', 10),
    dbUrl: flags.get('db-url')?.at(-1) || dbUrlFromEnv(),
  }
}

async function fetchAuthorFeedPage(
  apiBase: string,
  actor: string,
  cursor?: string,
): Promise<AuthorFeedPage> {
  const url = new URL('/xrpc/app.bsky.feed.getAuthorFeed', apiBase)
  url.searchParams.set('actor', actor)
  url.searchParams.set('limit', '100')
  url.searchParams.set('filter', 'posts_no_replies')
  if (cursor) url.searchParams.set('cursor', cursor)

  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`AppView request failed for ${actor}: ${response.status} ${response.statusText}`)
  }
  const body: any = await response.json()
  return {
    posts: (body.feed || []).map((item: any) => item.post).filter(Boolean),
    cursor: typeof body.cursor === 'string' ? body.cursor : undefined,
  }
}

async function insertPosts(dbUrl: string, posts: BackfillPostRow[]): Promise<number> {
  if (!posts.length) return 0
  const db = createDb(dbUrl)
  try {
    let inserted = 0
    for (let i = 0; i < posts.length; i += 1000) {
      const batch = posts.slice(i, i + 1000)
      const result: any = await db
        .insertInto('post')
        .values(batch)
        .onConflict((oc) => oc.column('uri').doNothing())
        .executeTakeFirst()
      inserted += Number(result?.numInsertedOrUpdatedRows ?? batch.length)
    }
    return inserted
  } finally {
    await db.destroy()
  }
}

async function main() {
  dotenv.config()
  const options = parseCliArgs(process.argv.slice(2))
  const plan = await collectPublisherPosts({
    actors: options.actors,
    since: options.since,
    until: options.until,
    maxPagesPerActor: options.maxPagesPerActor,
    fetchPage: (actor, cursor) => fetchAuthorFeedPage(options.apiBase, actor, cursor),
  })

  const inserted = options.apply ? await insertPosts(options.dbUrl, plan.posts) : 0
  const summary = {
    mode: options.apply ? 'apply' : 'dry-run',
    actors: options.actors,
    since: options.since.toISOString(),
    until: options.until.toISOString(),
    scanned: plan.scanned,
    candidate_posts: plan.posts.length,
    skipped_out_of_window: plan.skipped_out_of_window,
    skipped_wrong_author: plan.skipped_wrong_author,
    by_actor: plan.by_actor,
    inserted,
  }

  if (options.json) {
    console.log(JSON.stringify(summary, null, 2))
  } else {
    console.log(
      `${summary.mode}: scanned=${summary.scanned} candidates=${summary.candidate_posts} inserted=${summary.inserted}`,
    )
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message || err)
    process.exit(1)
  })
}
