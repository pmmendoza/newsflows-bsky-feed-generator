/**
 * Destructive disposable-DB rehearsal for migration 005 + bounded backfill.
 *
 * Run only against a throwaway database:
 *   FEEDGEN_TEST_DSN=postgresql://.../throwaway \
 *   FEEDGEN_LINK_COLUMNS_REHEARSAL=1 \
 *     npx ts-node scripts/test_link_columns_migration.ts
 */
import assert from 'assert'
import fs from 'fs'
import path from 'path'
import { Kysely, PostgresDialect, sql } from 'kysely'
import { Pool } from 'pg'
import { archiveHasCanonicalLinkUri } from '../src/archive-worker'
import { migrationProvider } from '../src/db/migrations'
import { runLinkColumnBackfill } from '../src/scripts/backfill-link-columns'

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_LINK_COLUMNS_REHEARSAL !== '1') {
    console.log('SKIP: set FEEDGEN_TEST_DSN and FEEDGEN_LINK_COLUMNS_REHEARSAL=1')
    return
  }

  const db = new Kysely<any>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString: dsn }) }),
  })

  try {
    await sql`DROP SCHEMA IF EXISTS research_archive CASCADE`.execute(db)
    await sql`DROP SCHEMA IF EXISTS public CASCADE`.execute(db)
    await sql`CREATE SCHEMA public`.execute(db)

    const migrations = await migrationProvider.getMigrations()
    await migrations['001'].up(db)

    for (let index = 1; index <= 5; index += 1) {
      await sql`
        INSERT INTO public.post (
          uri, cid, "indexedAt", "createdAt", author, text,
          "rootUri", "rootCid", "linkUrl", "linkTitle", "linkDescription"
        ) VALUES (
          ${`at://legacy-${index}`}, ${`cid-${index}`}, '2026-07-19T00:00:00Z',
          '2026-07-19T00:00:00Z', 'did:legacy', 'post', '', '',
          ${`https://example.com/${index}`}, ${`Legacy title ${index}`},
          ${`Legacy description ${index}`}
        )
      `.execute(db)
    }

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
        ('at://archive-1', 'cid-1', 'https://example.com/archive-1', '', ''),
        ('at://archive-2', 'cid-2', 'https://example.com/archive-2', '', '')
    `.execute(db)
    assert.equal(await archiveHasCanonicalLinkUri(db), false)

    const migrationSource = fs.readFileSync(
      path.join(__dirname, '../src/db/migrations.ts'),
      'utf8',
    ).split("migrations['005_canonical_link_columns'] =", 2)[1]
    assert(migrationSource.includes("ADD COLUMN IF NOT EXISTS link_uri varchar NOT NULL DEFAULT ''"))
    assert(!migrationSource.includes('UPDATE public.post'))
    assert(!migrationSource.includes('UPDATE research_archive.post_snapshot'))
    assert(!migrationSource.includes('VALIDATE CONSTRAINT'))
    assert(!migrationSource.includes('ALTER COLUMN link_uri SET NOT NULL'))

    await migrations['005_canonical_link_columns'].up(db)
    await migrations['005_canonical_link_columns'].up(db) // idempotency
    assert.equal(await archiveHasCanonicalLinkUri(db), true)

    // Existing heap rows expose the catalog default without being updated.
    const expanded = await sql<{
      link_uri: string
      link_title: string
      link_description: string
      linkUrl: string
      has_missing: boolean
    }>`
      SELECT p.link_uri, p.link_title, p.link_description, p."linkUrl",
             a.atthasmissing AS has_missing
      FROM public.post AS p
      CROSS JOIN pg_attribute AS a
      WHERE p.uri = 'at://legacy-1'
        AND a.attrelid = 'public.post'::regclass
        AND a.attname = 'link_uri'
    `.execute(db)
    assert.deepEqual(expanded.rows[0], {
      link_uri: '',
      link_title: '',
      link_description: '',
      linkUrl: 'https://example.com/1',
      has_missing: true,
    })
    const archiveExpanded = await sql<{ link_uri: string | null; link_url: string }>`
      SELECT link_uri, link_url FROM research_archive.post_snapshot
      WHERE post_uri = 'at://archive-1'
    `.execute(db)
    assert.deepEqual(archiveExpanded.rows[0], {
      link_uri: null,
      link_url: 'https://example.com/archive-1',
    })

    // Old and new writers are synchronized after the metadata-only expand.
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
    await sql`
      INSERT INTO public.post (
        uri, cid, "indexedAt", "createdAt", author, text,
        "rootUri", "rootCid", link_uri, link_title, link_description,
        "linkUrl", "linkTitle", "linkDescription"
      ) VALUES (
        'at://new-writer', 'cid-new', '2026-07-19T00:00:00Z',
        '2026-07-19T00:00:00Z', 'did:new', 'post', '', '',
        'new-uri', 'new-title', 'new-description',
        'new-uri', 'new-title', 'new-description'
      )
    `.execute(db)
    const writerParity = await sql<{ mismatches: string }>`
      SELECT count(*)::text AS mismatches FROM public.post
      WHERE uri IN ('at://old-writer', 'at://new-writer')
        AND (
          link_uri IS DISTINCT FROM "linkUrl" OR
          link_title IS DISTINCT FROM "linkTitle" OR
          link_description IS DISTINCT FROM "linkDescription"
        )
    `.execute(db)
    assert.equal(writerParity.rows[0].mismatches, '0')

    // One bounded batch pauses with a stable cursor; resuming finishes the suffix.
    const first = await runLinkColumnBackfill(db, {
      target: 'post',
      batchSize: 2,
      maxBatches: 1,
      onProgress: () => undefined,
    })
    assert.equal(first.complete, false)
    assert.equal(first.scanned, 2)
    assert.equal(first.global_zero_mismatch, false)

    const resumed = await runLinkColumnBackfill(db, {
      target: 'post',
      batchSize: 2,
      afterUri: first.cursor_uri,
      onProgress: () => undefined,
    })
    assert.equal(resumed.complete, true)
    assert.equal(resumed.global_zero_mismatch, false)

    const zeroGate = await runLinkColumnBackfill(db, {
      target: 'post',
      batchSize: 3,
      verifyOnly: true,
      onProgress: () => undefined,
    })
    assert.equal(zeroGate.global_zero_mismatch, true)

    const archiveBackfill = await runLinkColumnBackfill(db, {
      target: 'archive',
      batchSize: 1,
      onProgress: () => undefined,
    })
    assert.equal(archiveBackfill.updated, 2)
    assert.equal(archiveBackfill.global_zero_mismatch, false)
    const archiveZeroGate = await runLinkColumnBackfill(db, {
      target: 'archive',
      batchSize: 1,
      verifyOnly: true,
      onProgress: () => undefined,
    })
    assert.equal(archiveZeroGate.global_zero_mismatch, true)

    // Simulate out-of-band corruption; the owner command must stop, not overwrite.
    await sql`SET session_replication_role = replica`.execute(db)
    await sql`
      UPDATE public.post
      SET link_uri = 'conflict'
      WHERE uri = 'at://legacy-1'
    `.execute(db)
    await sql`SET session_replication_role = origin`.execute(db)
    await assert.rejects(
      runLinkColumnBackfill(db, {
        target: 'post',
        batchSize: 10,
        onProgress: () => undefined,
      }),
      /link-column conflict at post uri=at:\/\/legacy-1 field=link_uri\/linkUrl/,
    )

    console.log('rolling canonical link column migration/backfill rehearsal passed')
  } finally {
    await db.destroy()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
