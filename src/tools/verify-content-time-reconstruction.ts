import crypto from 'crypto'
import { sql } from 'kysely'
import { createDb } from '../db'
import type { Database } from '../db'
import {
  CONTENT_TIME_VALIDATOR_VERSION_V3,
  validateContentTime,
} from '../util/content-time'

const VERSION = CONTENT_TIME_VALIDATOR_VERSION_V3
const LIMIT = 30
const FIELDS = [
  'created_at_source_raw',
  'content_time_utc',
  'content_time_status',
  'content_time_clamp_reason',
  'content_time_validator_version',
  'createdAt',
] as const

type Field = typeof FIELDS[number]

export type CandidateRow = {
  uri: string
  indexedAt: string
  createdAt: string
  created_at_source_raw: Buffer | null
  content_time_utc: string | null
  content_time_status: string | null
  content_time_clamp_reason: string | null
  content_time_validator_version: string | null
  population_count?: string | number
}

export type Feed = { feed_id: string; publisher_did: string }
export type Population = { denominator: number; rows: CandidateRow[] }
export type Loaded = {
  feeds: Feed[]
  posts: Record<string, Population>
  engagement: Population
}
export type Loader = (since: string, until: string) => Promise<Loaded>

type ScopeResult = { denominator: number; sampled: number; mismatches: number }
type Result = {
  pass: boolean
  validator_version: string
  since: string
  until: string
  sample_limit: number
  feeds: Record<string, ScopeResult>
  engagement: ScopeResult & { empty_population: boolean }
  rows_checked: number
  mismatch_rows: number
  mismatches_by_field: Record<Field, number>
  sample_sha256: string
  errors: Array<{ code: string; message: string }>
}

const digest = (value: string | Buffer) =>
  crypto.createHash('sha256').update(value).digest('hex')

const strictBound = (value: string, flag: string): string => {
  const match = value.match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?(Z|[+-]\d{2}:?\d{2})$/,
  )
  if (!match) {
    throw new Error(`${flag} requires an ISO timestamp with an explicit timezone`)
  }
  const [, year, month, day, hour, minute, second = '0', fraction = '0'] = match
  const parts = [year, month, day, hour, minute, second, fraction.padEnd(3, '0')].map(Number)
  const local = new Date(Date.UTC(parts[0], parts[1] - 1, parts[2], parts[3], parts[4], parts[5], parts[6]))
  const validParts = [
    local.getUTCFullYear(), local.getUTCMonth() + 1, local.getUTCDate(),
    local.getUTCHours(), local.getUTCMinutes(), local.getUTCSeconds(), local.getUTCMilliseconds(),
  ]
  if (parts.some((part, index) => part !== validParts[index])) throw new Error(`${flag} is invalid`)
  const time = Date.parse(value)
  if (!Number.isFinite(time)) throw new Error(`${flag} is invalid`)
  return new Date(time).toISOString()
}

export function parseArgs(argv: string[]): { since: string; until: string } {
  const values: Record<string, string> = {}
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i]
    const value = argv[i + 1]
    if ((flag !== '--since' && flag !== '--until') || !value || value.startsWith('--')) {
      throw new Error('only complete --since VALUE and --until VALUE arguments are accepted')
    }
    if (values[flag]) throw new Error(`${flag} may be supplied once`)
    values[flag] = value
  }
  if (!values['--since'] || !values['--until']) {
    throw new Error('--since and --until are required')
  }
  const since = strictBound(values['--since'], '--since')
  const until = strictBound(values['--until'], '--until')
  if (since >= until) throw new Error('--since must be earlier than --until')
  return { since, until }
}

const denominator = (rows: CandidateRow[], scope: string): number => {
  if (!rows.length) return 0
  const values = new Set(rows.map((row) => Number(row.population_count)))
  const [value] = values
  if (values.size !== 1 || !Number.isSafeInteger(value) || value < rows.length) {
    throw new Error(`invalid ${scope} population count`)
  }
  return value
}

const rowFingerprint = (row: CandidateRow): string => digest(JSON.stringify({
  uri: digest(row.uri),
  indexedAt: digest(row.indexedAt),
  createdAt: digest(row.createdAt),
  raw: row.created_at_source_raw === null ? null : digest(row.created_at_source_raw),
  utc: row.content_time_utc,
  status: row.content_time_status,
  reason: row.content_time_clamp_reason,
  version: row.content_time_validator_version,
}))

export function rowMismatches(row: CandidateRow): Field[] {
  if (row.created_at_source_raw === null) return [...FIELDS]
  const raw = row.created_at_source_raw.toString('utf8')
  let expected: ReturnType<typeof validateContentTime>
  try {
    expected = validateContentTime(raw, row.indexedAt, VERSION)
  } catch {
    return [...FIELDS]
  }
  const mismatches: Field[] = []
  if (!row.created_at_source_raw.equals(expected.created_at_source_raw)) mismatches.push('created_at_source_raw')
  if (row.content_time_utc !== expected.content_time_utc) mismatches.push('content_time_utc')
  if (row.content_time_status !== expected.content_time_status) mismatches.push('content_time_status')
  if (row.content_time_clamp_reason !== expected.content_time_clamp_reason) mismatches.push('content_time_clamp_reason')
  if (row.content_time_validator_version !== expected.content_time_validator_version) mismatches.push('content_time_validator_version')
  if (row.createdAt !== expected.legacy_created_at) mismatches.push('createdAt')
  return mismatches
}

const emptyResult = (since: string, until: string): Result => ({
  pass: false,
  validator_version: VERSION,
  since,
  until,
  sample_limit: LIMIT,
  feeds: {},
  engagement: { denominator: 0, sampled: 0, mismatches: 0, empty_population: true },
  rows_checked: 0,
  mismatch_rows: 0,
  mismatches_by_field: Object.fromEntries(FIELDS.map((field) => [field, 0])) as Record<Field, number>,
  sample_sha256: digest(''),
  errors: [],
})

export async function verify(since: string, until: string, load: Loader): Promise<Result> {
  const result = emptyResult(since, until)
  let data: Loaded
  try {
    data = await load(since, until)
  } catch {
    result.errors.push({ code: 'QUERY_FAILED', message: 'verification query failed' })
    return result
  }
  if (!data.feeds.length) {
    result.errors.push({ code: 'CATALOG_EMPTY', message: 'no enabled v3 content-time feeds' })
    return result
  }

  const fingerprints: string[] = []
  const reduce = (population: Population, scope: string): ScopeResult => {
    if (population.rows.length > LIMIT) throw new Error(`oversized ${scope} sample`)
    if (
      (population.denominator > 0 && population.rows.length === 0) ||
      population.denominator < population.rows.length ||
      !Number.isSafeInteger(population.denominator)
    ) {
      throw new Error(`invalid ${scope} population count`)
    }
    let mismatchRows = 0
    for (const row of population.rows) {
      const fields = rowMismatches(row)
      result.rows_checked += 1
      fingerprints.push(`${scope}:${rowFingerprint(row)}`)
      if (fields.length) {
        mismatchRows += 1
        result.mismatch_rows += 1
        fields.forEach((field) => { result.mismatches_by_field[field] += 1 })
      }
    }
    return { denominator: population.denominator, sampled: population.rows.length, mismatches: mismatchRows }
  }

  try {
    for (const feed of data.feeds) {
      const population = data.posts[feed.feed_id]
      if (!population || population.denominator === 0) throw new Error('required feed population is empty')
      result.feeds[feed.feed_id] = reduce(population, `post:${feed.feed_id}`)
    }
    const engagement = reduce(data.engagement, 'engagement')
    result.engagement = { ...engagement, empty_population: engagement.denominator === 0 }
  } catch {
    result.errors.push({ code: 'POPULATION_INVALID', message: 'verification population is invalid' })
    return result
  }
  result.sample_sha256 = digest(fingerprints.sort().join('\n'))
  result.pass = result.mismatch_rows === 0
  return result
}

type CatalogRow = {
  feed_id: string
  publisher_did: string | null
  content_time_contract_version: string | null
}

const sampled = async (
  query: Promise<{ rows: CandidateRow[] }>,
  scope: string,
): Promise<Population> => {
  const rows = (await query).rows
  return { denominator: denominator(rows, scope), rows }
}

export function databaseLoader(db: Database): Loader {
  return async (since, until) => {
    const catalog = (await sql<CatalogRow>`
      SELECT feed_id, publisher_did, content_time_contract_version
      FROM feedgen_ops.feed_catalog
      WHERE enabled IS TRUE AND publisher_time_clock = 'content_time_v1'
      ORDER BY feed_id
    `.execute(db)).rows
    if (!catalog.length || catalog.some((row) =>
      !row.feed_id || !row.publisher_did || row.content_time_contract_version !== VERSION
    )) throw new Error('catalog incoherent')
    const feeds = catalog.map((row) => ({ feed_id: row.feed_id, publisher_did: row.publisher_did! }))
    if (new Set(feeds.map((feed) => feed.feed_id)).size !== feeds.length) throw new Error('duplicate feed')

    const posts: Record<string, Population> = {}
    for (const feed of feeds) {
      posts[feed.feed_id] = await sampled(sql<CandidateRow>`
        WITH candidates AS (
          SELECT p.*, count(*) OVER () AS population_count
          FROM public.post p
          WHERE p.author = ${feed.publisher_did}
            AND p."indexedAt" >= ${since} AND p."indexedAt" < ${until}
        )
        SELECT uri, "indexedAt", "createdAt", created_at_source_raw,
               content_time_utc, content_time_status, content_time_clamp_reason,
               content_time_validator_version, population_count
        FROM candidates ORDER BY md5(uri), uri LIMIT ${LIMIT}
      `.execute(db), `post:${feed.feed_id}`)
    }

    const dids = [...new Set(feeds.map((feed) => feed.publisher_did))].sort()
    const engagement = await sampled(sql<CandidateRow>`
      WITH candidates AS (
        SELECT e.uri, e."indexedAt", e."createdAt", e.created_at_source_raw,
               e.content_time_utc, e.content_time_status, e.content_time_clamp_reason,
               e.content_time_validator_version
        FROM public.engagement e JOIN public.subscriber s ON s.did = e.author
        WHERE e."indexedAt" >= ${since} AND e."indexedAt" < ${until}
          AND e.type IN (1, 2, 3) AND split_part(e."subjectUri", '/', 3) = ANY(${dids}::text[])
        UNION ALL
        SELECT p.uri, p."indexedAt", p."createdAt", p.created_at_source_raw,
               p.content_time_utc, p.content_time_status, p.content_time_clamp_reason,
               p.content_time_validator_version
        FROM public.post p JOIN public.subscriber s ON s.did = p.author
        WHERE p."rootUri" <> '' AND p."indexedAt" >= ${since} AND p."indexedAt" < ${until}
          AND split_part(p."rootUri", '/', 3) = ANY(${dids}::text[])
      ), dedup AS (SELECT DISTINCT ON (uri) * FROM candidates ORDER BY uri),
      counted AS (SELECT *, count(*) OVER () AS population_count FROM dedup)
      SELECT * FROM counted ORDER BY md5(uri), uri LIMIT ${LIMIT}
    `.execute(db), 'engagement')
    return { feeds, posts, engagement }
  }
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  let result: Result
  try {
    const { since, until } = parseArgs(argv)
    const connection = process.env.FEEDGEN_POSTGRES_URL
    if (!connection) throw new Error('FEEDGEN_POSTGRES_URL is required')
    const db = createDb(connection)
    try {
      result = await verify(since, until, databaseLoader(db))
    } finally {
      await db.destroy()
    }
  } catch {
    result = emptyResult('', '')
    result.errors.push({ code: 'INPUT_INVALID', message: 'arguments or environment are invalid' })
  }
  console.log(JSON.stringify(result))
  if (!result.pass) process.exitCode = 1
}

if (require.main === module) void main()
