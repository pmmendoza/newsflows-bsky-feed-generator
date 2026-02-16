import { Kysely, Migration, MigrationProvider, sql } from 'kysely'

const migrations: Record<string, Migration> = {}

export const migrationProvider: MigrationProvider = {
  async getMigrations() {
    return migrations
  },
}

migrations['001'] = {
  async up(db: Kysely<unknown>) {
    await db.schema
      .createTable('post')
      .addColumn('uri', 'varchar', (col) => col.primaryKey())
      .addColumn('cid', 'varchar', (col) => col.notNull())
      .addColumn('indexedAt', 'varchar', (col) => col.notNull())
      .addColumn('createdAt', 'varchar', (col) => col.notNull())
      .addColumn('author', 'varchar', (col) => col.notNull())
      .addColumn('text', 'text', (col) => col.notNull())
      .addColumn('rootUri', 'varchar', (col) => col.notNull())
      .addColumn('rootCid', 'varchar', (col) => col.notNull())
      .addColumn('linkUrl', 'varchar', (col) => col.notNull())
      .addColumn('linkTitle', 'varchar', (col) => col.notNull())
      .addColumn('linkDescription', 'varchar', (col) => col.notNull())
      .addColumn('priority', 'integer')
      .addColumn('likes_count', 'integer', (col) => col.defaultTo(0))
      .addColumn('repost_count', 'integer', (col) => col.defaultTo(0))
      .addColumn('comments_count', 'integer', (col) => col.defaultTo(0))
      .execute()
    await db.schema
      .createTable('engagement')
      .addColumn('uri', 'varchar', (col) => col.primaryKey())
      .addColumn('cid', 'varchar', (col) => col.notNull())
      .addColumn('subjectUri', 'varchar', (col) => col.notNull())
      .addColumn('subjectCid', 'varchar', (col) => col.notNull())
      .addColumn('type', 'integer', (col) => col.notNull())
      .addColumn('indexedAt', 'varchar', (col) => col.notNull())
      .addColumn('createdAt', 'varchar', (col) => col.notNull())
      .addColumn('author', 'varchar', (col) => col.notNull())
      .execute()
    await db.schema
      .createTable('follows')
      .addColumn('subject', 'varchar', (col) => col.notNull())
      .addColumn('follows', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('follows_pk', ['subject', 'follows']) // Composite primary key
      .execute()
    await db.schema
      .createTable('sub_state')
      .addColumn('service', 'varchar', (col) => col.primaryKey())
      .addColumn('cursor', 'bigint', (col) => col.notNull())
      .execute()
    await db.schema
      .createTable('subscriber')
      .addColumn('handle', 'varchar', (col) => col.notNull())
      .addColumn('did', 'varchar', (col) => col.primaryKey())
      .execute()

    // indexes should bring performance
    await db.schema
      .createIndex('post_author_index')
      .on('post')
      .column('author')
      .execute()

    await db.schema
      .createIndex('follows_subject_index')
      .on('follows')
      .column('subject')
      .execute()
    
    await db.schema
      .createIndex('engagement_subject_uri_index')
      .on('engagement')
      .column('subjectUri')
      .execute()

    await db.schema
      .createTable('request_log')
      .addColumn('id', 'serial', (col) => col.primaryKey())
      .addColumn('algo', 'varchar', (col) => col.notNull())
      .addColumn('requester_did', 'varchar', (col) => col.notNull())
      .addColumn('timestamp', 'varchar', (col) => col.notNull())
      .execute()

    await db.schema
      .createTable('request_posts')
      .addColumn('position', 'integer', (col) => col.notNull())
      .addColumn('request_id', 'integer', (col) => col.notNull().references('request_log.id'))
      .addColumn('post_uri', 'varchar', (col) => col.notNull())
      .addPrimaryKeyConstraint('request_posts_pk', ['request_id', 'post_uri'])
      .execute()

  },
  async down(db: Kysely<unknown>) {
    await db.schema.dropTable('post').execute()
    await db.schema.dropTable('sub_state').execute()
  },
}

migrations['002'] = {
  async up(db: Kysely<unknown>) {
    await db.schema
      .alterTable('post')
      .addColumn('quote_count', 'integer', (col) => col.defaultTo(0))
      .execute()
  },
  async down(db: Kysely<unknown>) {
    await db.schema
      .alterTable('post')
      .dropColumn('quote_count')
      .execute()
  },
}

migrations['003'] = {
  async up(db: Kysely<unknown>) {
    // Store timestamps as proper timestamptz for reliable filtering/indexing.
    // Guarded for idempotency in case schema was patched before deploy.
    await sql`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'request_log'
            AND column_name = 'timestamp'
            AND data_type = 'character varying'
        ) THEN
          ALTER TABLE request_log
          ALTER COLUMN timestamp TYPE timestamptz
          USING NULLIF(timestamp, '')::timestamptz;
        END IF;
      END $$;
    `.execute(db)

    // Add request-shape/result metadata so empty pages can be diagnosed from DB alone.
    await sql`
      ALTER TABLE request_log
      ADD COLUMN IF NOT EXISTS cursor_in varchar,
      ADD COLUMN IF NOT EXISTS cursor_out varchar,
      ADD COLUMN IF NOT EXISTS requested_limit integer,
      ADD COLUMN IF NOT EXISTS publisher_count integer,
      ADD COLUMN IF NOT EXISTS follows_count integer,
      ADD COLUMN IF NOT EXISTS result_count integer
    `.execute(db)

    // Backfill total result counts for historical rows.
    await sql`
      UPDATE request_log
      SET result_count = 0
      WHERE result_count IS NULL
    `.execute(db)
    await sql`
      UPDATE request_log rl
      SET result_count = agg.cnt
      FROM (
        SELECT request_id, COUNT(*)::int AS cnt
        FROM request_posts
        GROUP BY request_id
      ) AS agg
      WHERE rl.id = agg.request_id
    `.execute(db)

    await sql`
      CREATE INDEX IF NOT EXISTS request_log_requester_timestamp_index
      ON request_log (requester_did, timestamp)
    `.execute(db)
    await sql`
      CREATE INDEX IF NOT EXISTS request_log_algo_timestamp_index
      ON request_log (algo, timestamp)
    `.execute(db)
  },
  async down(db: Kysely<unknown>) {
    await db.schema
      .dropIndex('request_log_requester_timestamp_index')
      .ifExists()
      .execute()
    await db.schema
      .dropIndex('request_log_algo_timestamp_index')
      .ifExists()
      .execute()
    await db.schema
      .alterTable('request_log')
      .dropColumn('cursor_in')
      .dropColumn('cursor_out')
      .dropColumn('requested_limit')
      .dropColumn('publisher_count')
      .dropColumn('follows_count')
      .dropColumn('result_count')
      .execute()
    await sql`
      ALTER TABLE request_log
      ALTER COLUMN timestamp TYPE varchar
      USING to_char(timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    `.execute(db)
  },
}
