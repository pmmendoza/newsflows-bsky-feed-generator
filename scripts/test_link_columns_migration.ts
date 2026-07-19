/**
 * Destructive disposable-DB rehearsal for migration 005.
 *
 * Run only against a throwaway database:
 *   FEEDGEN_TEST_DSN=postgresql://.../throwaway \
 *   FEEDGEN_LINK_COLUMNS_REHEARSAL=1 \
 *     npx ts-node scripts/test_link_columns_migration.ts
 */
import assert from 'assert'
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import { migrationProvider } from '../src/db/migrations'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_LINK_COLUMNS_REHEARSAL !== '1') {
    console.log('SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_LINK_COLUMNS_REHEARSAL=1')
    return
  }

  const db = new Kysely<unknown>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
  })

  try {
    await sql`DROP SCHEMA IF EXISTS research_archive CASCADE`.execute(db)
    await sql`DROP SCHEMA IF EXISTS public CASCADE`.execute(db)
    await sql`CREATE SCHEMA public`.execute(db)

    const migrations = await migrationProvider.getMigrations()
    await migrations['001'].up(db)

    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text,
        "rootUri", "rootCid", "linkUrl", "linkTitle", "linkDescription"
      ) VALUES (
        'at://legacy', 'cid-legacy', '2026-07-19T00:00:00Z',
        '2026-07-19T00:00:00Z', 'did:legacy', 'post', '', '',
        'https://example.com/legacy', 'Legacy title', 'Legacy description'
      )
    `.execute(db)

    await sql`CREATE SCHEMA research_archive`.execute(db)
    await sql`
      CREATE TABLE research_archive.post_snapshot (
        post_uri text NOT NULL,
        cid text NOT NULL,
        link_url text,
        link_title text,
        link_description text,
        PRIMARY KEY (post_uri, cid)
      )
    `.execute(db)
    await sql`
      INSERT INTO research_archive.post_snapshot
        (post_uri, cid, link_url, link_title, link_description)
      VALUES
        ('at://legacy', 'cid-legacy', 'https://example.com/archive', 'Archive title', 'Archive description')
    `.execute(db)

    // A partially introduced conflicting canonical value must never be overwritten.
    await sql`
      ALTER TABLE public.post
        ADD COLUMN link_uri varchar,
        ADD COLUMN link_title varchar,
        ADD COLUMN link_description varchar
    `.execute(db)
    await sql`UPDATE public.post SET link_uri = 'conflict'`.execute(db)
    await assert.rejects(
      migrations['005_canonical_link_columns'].up(db),
      /canonical and legacy public\.post values differ/,
    )
    await sql`UPDATE public.post SET link_uri = "linkUrl"`.execute(db)

    await migrations['005_canonical_link_columns'].up(db)
    await migrations['005_canonical_link_columns'].up(db) // idempotency

    const publicRows = await sql<{
      link_uri: string
      link_title: string
      link_description: string
      linkUrl: string
      linkTitle: string
      linkDescription: string
    }>`
      SELECT link_uri, link_title, link_description,
             "linkUrl", "linkTitle", "linkDescription"
      FROM public.post WHERE uri = 'at://legacy'
    `.execute(db)
    assert.deepEqual(publicRows.rows[0], {
      link_uri: 'https://example.com/legacy',
      link_title: 'Legacy title',
      link_description: 'Legacy description',
      linkUrl: 'https://example.com/legacy',
      linkTitle: 'Legacy title',
      linkDescription: 'Legacy description',
    })

    const archiveRows = await sql<{ link_uri: string; link_url: string }>`
      SELECT link_uri, link_url
      FROM research_archive.post_snapshot
      WHERE post_uri = 'at://legacy' AND cid = 'cid-legacy'
    `.execute(db)
    assert.deepEqual(archiveRows.rows[0], {
      link_uri: 'https://example.com/archive',
      link_url: 'https://example.com/archive',
    })

    // The rolling-deploy trigger keeps a still-running old writer safe.
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text,
        "rootUri", "rootCid", "linkUrl", "linkTitle", "linkDescription"
      ) VALUES (
        'at://old-writer', 'cid-old', '2026-07-19T00:00:00Z',
        '2026-07-19T00:00:00Z', 'did:old', 'post', '', '',
        'old-uri', 'old-title', 'old-description'
      )
    `.execute(db)
    const oldWriter = await sql<{ link_uri: string; link_title: string }>`
      SELECT link_uri, link_title FROM public.post WHERE uri = 'at://old-writer'
    `.execute(db)
    assert.deepEqual(oldWriter.rows[0], { link_uri: 'old-uri', link_title: 'old-title' })

    await assert.rejects(
      sql`
        INSERT INTO public.post (
          uri, cid, "indexedAt", "createdAt", author, text,
          "rootUri", "rootCid", link_uri, link_title, link_description,
          "linkUrl", "linkTitle", "linkDescription"
        ) VALUES (
          'at://conflict', 'cid-conflict', '2026-07-19T00:00:00Z',
          '2026-07-19T00:00:00Z', 'did:conflict', 'post', '', '',
          'canonical', '', '', 'legacy', '', ''
        )
      `.execute(db),
      /conflicting link_uri\/linkUrl values/,
    )

    await sql`
      INSERT INTO research_archive.post_snapshot
        (post_uri, cid, link_url, link_title, link_description)
      VALUES ('at://old-archive-writer', 'cid-old', 'old-archive-uri', '', '')
    `.execute(db)
    const oldArchiveWriter = await sql<{ link_uri: string }>`
      SELECT link_uri FROM research_archive.post_snapshot
      WHERE post_uri = 'at://old-archive-writer'
    `.execute(db)
    assert.equal(oldArchiveWriter.rows[0].link_uri, 'old-archive-uri')

    console.log('canonical link column migration rehearsal passed')
  } finally {
    await db.destroy()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
