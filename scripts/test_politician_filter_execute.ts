/**
 * Executing test for the BE politician-or-party filter against real Postgres.
 *
 * Guards BLOCKER 1 (node-postgres last-column-wins): the eligibility LEFT JOIN
 * adds a `pe.uri` column, and on fail-open rows (no eligibility row) pe.uri is
 * NULL. A bare `select *` would let that NULL clobber post.uri. The SQL-shape
 * tests can't see this — only executing against real pg proves the returned
 * row's `uri` is the post's uri, not null.
 *
 * Everything runs inside a transaction that is rolled back, so it leaves no
 * residue (temp posts, eligibility rows, and any created schema/table vanish).
 * SKIPs cleanly when FEEDGEN_TEST_DSN is unset.
 *
 * Run:
 *   FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
 *     npx ts-node scripts/test_politician_filter_execute.ts
 */
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import { publisherQueryChronological } from '../src/algos/policies/chronological'

const TEST_DID = `did:plc:pf-exec-${Date.now()}`
const OLD_TIME_LIMIT = '2000-01-01T00:00:00.000Z'
const ROLLBACK = Symbol('rollback')

function post(uri: string, indexedAt: string) {
  return {
    uri,
    cid: `cid-${uri}`,
    indexedAt,
    createdAt: indexedAt,
    author: TEST_DID,
    text: 't',
    rootUri: uri,
    rootCid: `cid-${uri}`,
    link_uri: '',
    link_title: '',
    link_description: '',
    linkUrl: '',
    linkTitle: '',
    linkDescription: '',
  }
}

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn) {
    console.log('SKIP: set FEEDGEN_TEST_DSN to a reachable Postgres to enable')
    return
  }
  process.env.FEEDGEN_BE_POLITICIAN_FILTER = 'true'

  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
  })

  let failed = 0
  const assert = (cond: boolean, label: string, detail?: string) => {
    if (cond) console.log(`  ✓ ${label}`)
    else {
      failed++
      console.log(`  ✗ ${label}${detail ? ` — ${detail}` : ''}`)
    }
  }

  const uriKeep = `at://${TEST_DID}/app.bsky.feed.post/keep` // no eligibility row → fail-open
  const uriDrop = `at://${TEST_DID}/app.bsky.feed.post/drop` // eligible=false → excluded
  const uriPass = `at://${TEST_DID}/app.bsky.feed.post/pass` // eligible=true  → included

  try {
    await db.transaction().execute(async (trx) => {
      // Full deployed (migration-028) shape so inserts satisfy every NOT NULL
      // column. IF NOT EXISTS no-ops against the real table; when absent we
      // recreate it. All of this is rolled back at the end of the transaction.
      await sql`create schema if not exists ranker_prod`.execute(trx)
      await sql`create table if not exists ranker_prod.post_political_eligibility (
        uri text primary key,
        eligible boolean not null,
        has_politician boolean not null,
        has_party boolean not null,
        party_ids jsonb,
        direct_party_ids jsonb not null default '[]',
        inferred_party_ids jsonb not null default '[]',
        reference_version text not null,
        status text not null default 'ok',
        updated_at text
      )`.execute(trx)

      await trx
        .insertInto('post')
        .values([
          post(uriKeep, '2026-06-26T03:00:00.000Z'),
          post(uriDrop, '2026-06-26T02:00:00.000Z'),
          post(uriPass, '2026-06-26T01:00:00.000Z'),
        ])
        .execute()
      await trx
        .insertInto('ranker_prod.post_political_eligibility' as any)
        .values([
          // NOT NULL cols without a DB default (has_politician, has_party,
          // reference_version) must be supplied; defaulted cols are omitted.
          { uri: uriDrop, eligible: false, has_politician: false, has_party: false, reference_version: 'test', party_ids: null, updated_at: '2026-06-26T00:00:00.000Z' },
          { uri: uriPass, eligible: true, has_politician: true, has_party: false, reference_version: 'test', party_ids: null, updated_at: '2026-06-26T00:00:00.000Z' },
        ] as any)
        .execute()

      const rows = await publisherQueryChronological(
        trx as any,
        OLD_TIME_LIMIT,
        [],
        0,
        10,
        TEST_DID,
        'newsflow-be-k',
      ).execute()

      const uris = rows.map((r: any) => r.uri)
      // BLOCKER 1: fail-open row's uri must be the post uri, NOT null.
      assert(uris.includes(uriKeep), 'fail-open post kept', JSON.stringify(uris))
      assert(!uris.includes(null as any), 'no NULL uri clobbered onto a row', JSON.stringify(uris))
      const keepRow = rows.find((r: any) => r.uri === uriKeep)
      assert(!!keepRow && keepRow.uri === uriKeep, 'kept row.uri === post uri (not null)')
      // OR semantics: eligible=true kept, eligible=false excluded.
      assert(uris.includes(uriPass), 'eligible=true post included')
      assert(!uris.includes(uriDrop), 'eligible=false post excluded')

      throw ROLLBACK // discard everything
    })
  } catch (err) {
    if (err !== ROLLBACK) {
      failed++
      console.log(`  ✗ execute FAILED: ${err instanceof Error ? err.message : String(err)}`)
    }
  } finally {
    await db.destroy()
  }

  console.log(`Summary: ${failed === 0 ? 'passed' : `${failed} failed`}`)
  if (failed > 0) process.exit(1)
}

main()
