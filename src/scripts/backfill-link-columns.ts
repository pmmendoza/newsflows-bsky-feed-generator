import dotenv from 'dotenv'
import { sql } from 'kysely'
import { createDb, Database } from '../db'

export type LinkBackfillTarget = 'post' | 'archive'

export type LinkBackfillOptions = {
  target: LinkBackfillTarget
  batchSize: number
  afterUri?: string
  afterCid?: string
  maxBatches?: number
  pauseMs?: number
  verifyOnly?: boolean
  onProgress?: (progress: LinkBackfillProgress) => void
}

export type LinkBackfillProgress = {
  target: LinkBackfillTarget
  batch: number
  scanned: number
  updated: number
  cursor_uri: string
  cursor_cid?: string
  conflicts: 0
}

export type LinkBackfillResult = LinkBackfillProgress & {
  complete: boolean
  global_zero_mismatch: boolean
}

type PostRow = {
  uri: string
  link_uri: string
  link_title: string
  link_description: string
  linkUrl: string
  linkTitle: string
  linkDescription: string
}

type ArchiveRow = {
  post_uri: string
  cid: string
  link_uri: string | null
  link_url: string | null
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

function postConflict(row: PostRow): string | null {
  for (const [canonical, legacy, name] of [
    [row.link_uri, row.linkUrl, 'link_uri/linkUrl'],
    [row.link_title, row.linkTitle, 'link_title/linkTitle'],
    [row.link_description, row.linkDescription, 'link_description/linkDescription'],
  ]) {
    if (canonical !== legacy && canonical !== '') return name
  }
  return null
}

function postNeedsBackfill(row: PostRow): boolean {
  return (
    (row.link_uri === '' && row.linkUrl !== '') ||
    (row.link_title === '' && row.linkTitle !== '') ||
    (row.link_description === '' && row.linkDescription !== '')
  )
}

async function runPostBatch(
  db: Database,
  cursor: string,
  batchSize: number,
  verifyOnly: boolean,
) {
  return db.transaction().execute(async (trx) => {
    const rows = (await sql<PostRow>`
      SELECT uri, link_uri, link_title, link_description,
             "linkUrl", "linkTitle", "linkDescription"
      FROM public.post
      WHERE uri > ${cursor}
      ORDER BY uri
      LIMIT ${batchSize}
      ${verifyOnly ? sql`` : sql`FOR UPDATE`}
    `.execute(trx)).rows

    const conflict = rows.find((row) => postConflict(row))
    if (conflict) {
      throw new Error(
        `link-column conflict at post uri=${conflict.uri} field=${postConflict(conflict)}`,
      )
    }

    if (verifyOnly) {
      const mismatch = rows.find(postNeedsBackfill)
      if (mismatch) {
        throw new Error(`zero-mismatch gate failed at post uri=${mismatch.uri}`)
      }
      return { rows, updated: 0 }
    }

    const uris = rows.filter(postNeedsBackfill).map((row) => row.uri)
    if (!uris.length) return { rows, updated: 0 }

    const updated = await sql<{ uri: string }>`
      UPDATE public.post
      SET link_uri = CASE WHEN link_uri = '' THEN "linkUrl" ELSE link_uri END,
          link_title = CASE WHEN link_title = '' THEN "linkTitle" ELSE link_title END,
          link_description = CASE
            WHEN link_description = '' THEN "linkDescription"
            ELSE link_description
          END
      WHERE uri = ANY(${uris}::varchar[])
        AND (
          (link_uri = '' AND "linkUrl" <> '') OR
          (link_title = '' AND "linkTitle" <> '') OR
          (link_description = '' AND "linkDescription" <> '')
        )
      RETURNING uri
    `.execute(trx)

    const mismatch = await sql<{ uri: string }>`
      SELECT uri
      FROM public.post
      WHERE uri = ANY(${uris}::varchar[])
        AND (
          link_uri IS DISTINCT FROM "linkUrl" OR
          link_title IS DISTINCT FROM "linkTitle" OR
          link_description IS DISTINCT FROM "linkDescription"
        )
      LIMIT 1
    `.execute(trx)
    if (mismatch.rows.length) {
      throw new Error(`post batch verification failed at uri=${mismatch.rows[0].uri}`)
    }

    return { rows, updated: updated.rows.length }
  })
}

async function runArchiveBatch(
  db: Database,
  cursorUri: string,
  cursorCid: string,
  batchSize: number,
  verifyOnly: boolean,
) {
  return db.transaction().execute(async (trx) => {
    const rows = (await sql<ArchiveRow>`
      SELECT post_uri, cid, link_uri, link_url
      FROM research_archive.post_snapshot
      WHERE (post_uri, cid) > (${cursorUri}, ${cursorCid})
      ORDER BY post_uri, cid
      LIMIT ${batchSize}
      ${verifyOnly ? sql`` : sql`FOR UPDATE`}
    `.execute(trx)).rows

    const conflict = rows.find(
      (row) => row.link_uri !== row.link_url && row.link_uri !== null,
    )
    if (conflict) {
      throw new Error(
        `link-column conflict at archive post_uri=${conflict.post_uri} cid=${conflict.cid}`,
      )
    }

    if (verifyOnly) {
      const mismatch = rows.find((row) => row.link_uri !== row.link_url)
      if (mismatch) {
        throw new Error(
          `zero-mismatch gate failed at archive post_uri=${mismatch.post_uri} cid=${mismatch.cid}`,
        )
      }
      return { rows, updated: 0 }
    }

    const candidates = rows.filter(
      (row) => row.link_uri === null && row.link_url !== null,
    )
    if (!candidates.length) return { rows, updated: 0 }

    const postUris = candidates.map((row) => row.post_uri)
    const cids = candidates.map((row) => row.cid)
    const updated = await sql<{ post_uri: string }>`
      UPDATE research_archive.post_snapshot AS target
      SET link_uri = target.link_url
      FROM unnest(${postUris}::text[], ${cids}::text[]) AS batch(post_uri, cid)
      WHERE target.post_uri = batch.post_uri
        AND target.cid = batch.cid
        AND target.link_uri IS NULL
        AND target.link_url IS NOT NULL
      RETURNING target.post_uri
    `.execute(trx)

    const mismatch = await sql<{ post_uri: string }>`
      SELECT target.post_uri
      FROM research_archive.post_snapshot AS target
      JOIN unnest(${postUris}::text[], ${cids}::text[]) AS batch(post_uri, cid)
        ON target.post_uri = batch.post_uri AND target.cid = batch.cid
      WHERE target.link_uri IS DISTINCT FROM target.link_url
      LIMIT 1
    `.execute(trx)
    if (mismatch.rows.length) {
      throw new Error(
        `archive batch verification failed at post_uri=${mismatch.rows[0].post_uri}`,
      )
    }

    return { rows, updated: updated.rows.length }
  })
}

export async function runLinkColumnBackfill(
  db: Database,
  options: LinkBackfillOptions,
): Promise<LinkBackfillResult> {
  if (!Number.isInteger(options.batchSize) || options.batchSize < 1 || options.batchSize > 100_000) {
    throw new Error('batchSize must be an integer from 1 to 100000')
  }
  if (options.target === 'archive' && Boolean(options.afterUri) !== Boolean(options.afterCid)) {
    throw new Error('archive resume requires both afterUri and afterCid')
  }

  const startUri = options.afterUri ?? ''
  const startCid = options.afterCid ?? ''
  let cursorUri = startUri
  let cursorCid = startCid
  let scanned = 0
  let updated = 0
  let batch = 0
  let complete = false

  while (options.maxBatches === undefined || batch < options.maxBatches) {
    const result = options.target === 'post'
      ? await runPostBatch(db, cursorUri, options.batchSize, options.verifyOnly === true)
      : await runArchiveBatch(
          db,
          cursorUri,
          cursorCid,
          options.batchSize,
          options.verifyOnly === true,
        )

    if (!result.rows.length) {
      complete = true
      break
    }

    batch += 1
    scanned += result.rows.length
    updated += result.updated
    const last = result.rows[result.rows.length - 1]
    cursorUri = options.target === 'post'
      ? (last as PostRow).uri
      : (last as ArchiveRow).post_uri
    cursorCid = options.target === 'archive' ? (last as ArchiveRow).cid : ''

    const progress: LinkBackfillProgress = {
      target: options.target,
      batch,
      scanned,
      updated,
      cursor_uri: cursorUri,
      ...(options.target === 'archive' ? { cursor_cid: cursorCid } : {}),
      conflicts: 0,
    }
    ;(options.onProgress ?? ((value) => console.log(JSON.stringify(value))))(progress)

    if (result.rows.length < options.batchSize) {
      complete = true
      break
    }
    if ((options.pauseMs ?? 0) > 0) await sleep(options.pauseMs ?? 0)
  }

  return {
    target: options.target,
    batch,
    scanned,
    updated,
    cursor_uri: cursorUri,
    ...(options.target === 'archive' ? { cursor_cid: cursorCid } : {}),
    conflicts: 0,
    complete,
    global_zero_mismatch:
      options.verifyOnly === true && complete && startUri === '' && startCid === '',
  }
}

function valueAfter(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name)
  return index === -1 ? undefined : argv[index + 1]
}

function positiveInt(value: string | undefined, name: string): number | undefined {
  if (value === undefined) return undefined
  const parsed = Number.parseInt(value, 10)
  if (!Number.isInteger(parsed) || parsed < 1) throw new Error(`${name} must be a positive integer`)
  return parsed
}

async function main() {
  dotenv.config()
  const argv = process.argv.slice(2)
  const target = valueAfter(argv, '--target') as LinkBackfillTarget | undefined
  if (target !== 'post' && target !== 'archive') {
    throw new Error('--target must be post or archive')
  }
  const batchSize = positiveInt(valueAfter(argv, '--batch-size'), '--batch-size')
  if (!batchSize) throw new Error('--batch-size is required')
  const dsn = process.env.FEEDGEN_POSTGRES_URL
  if (!dsn) throw new Error('FEEDGEN_POSTGRES_URL is required')

  const db = createDb(dsn)
  try {
    const result = await runLinkColumnBackfill(db, {
      target,
      batchSize,
      afterUri: valueAfter(argv, '--after-uri'),
      afterCid: valueAfter(argv, '--after-cid'),
      maxBatches: positiveInt(valueAfter(argv, '--max-batches'), '--max-batches'),
      pauseMs: positiveInt(valueAfter(argv, '--pause-ms'), '--pause-ms') ?? 0,
      verifyOnly: argv.includes('--verify-only'),
    })
    console.log(JSON.stringify({ status: result.complete ? 'complete' : 'paused', ...result }))
    if (!result.global_zero_mismatch) {
      console.log('global zero-mismatch gate remains required: rerun --verify-only from the beginning')
    }
  } finally {
    await db.destroy()
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error)
    process.exit(1)
  })
}
