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

migrations['004_exact_feed_subscriptions'] = {
  async up(db: Kysely<unknown>) {
    await sql`
      DO $$
      DECLARE
        access_scope_exists boolean;
      BEGIN
        SELECT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'subscriber'
            AND column_name = 'access_scope'
        ) INTO access_scope_exists;
        IF NOT access_scope_exists THEN
          ALTER TABLE subscriber
          ADD COLUMN access_scope varchar NOT NULL DEFAULT 'omni';
          ALTER TABLE subscriber ADD CONSTRAINT subscriber_access_scope_check
          CHECK (access_scope IN ('omni', 'assigned', 'none'));
          COMMENT ON COLUMN subscriber.access_scope IS
            'feedgen:migration:004_exact_feed_subscriptions';
        ELSIF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'subscriber'
            AND column_name = 'access_scope'
            AND data_type = 'character varying'
            AND is_nullable = 'NO'
            AND column_default LIKE '%omni%'
        ) OR NOT EXISTS (
          SELECT 1 FROM pg_constraint
          WHERE conrelid = 'subscriber'::regclass
            AND conname = 'subscriber_access_scope_check'
            AND contype = 'c'
            AND convalidated
            AND pg_get_constraintdef(oid) LIKE '%access_scope%'
            AND pg_get_constraintdef(oid) LIKE '%omni%'
            AND pg_get_constraintdef(oid) LIKE '%assigned%'
            AND pg_get_constraintdef(oid) LIKE '%none%'
        ) THEN
          RAISE EXCEPTION '004_exact_feed_subscriptions schema mismatch: subscriber.access_scope must be varchar NOT NULL DEFAULT omni with subscriber_access_scope_check';
        END IF;
      END $$
    `.execute(db)

    await sql`
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'feedgen_ops') THEN
          CREATE SCHEMA feedgen_ops;
        END IF;
        IF to_regclass('feedgen_ops.subscriber_feed_assignment') IS NULL THEN
          CREATE TABLE feedgen_ops.subscriber_feed_assignment (
            assignment_id bigserial PRIMARY KEY,
            feed_id varchar NOT NULL,
            did varchar NOT NULL REFERENCES subscriber(did) ON DELETE CASCADE,
            active_from timestamptz NOT NULL DEFAULT now(),
            active_until timestamptz,
            source varchar,
            status varchar NOT NULL DEFAULT 'active',
            CONSTRAINT subscriber_feed_assignment_interval_check
              CHECK (active_until IS NULL OR active_until > active_from),
            CONSTRAINT subscriber_feed_assignment_status_check
              CHECK (
                (active_until IS NULL AND status = 'active') OR
                (active_until IS NOT NULL AND status IN ('removed', 'replaced', 'omni'))
              )
          );
          CREATE UNIQUE INDEX subscriber_feed_assignment_active_uq
            ON feedgen_ops.subscriber_feed_assignment (feed_id, did)
            WHERE active_until IS NULL;
          CREATE INDEX subscriber_feed_assignment_did_active_idx
            ON feedgen_ops.subscriber_feed_assignment (did)
            WHERE active_until IS NULL;
          COMMENT ON TABLE feedgen_ops.subscriber_feed_assignment IS
            'feedgen:migration:004_exact_feed_subscriptions';
        ELSIF NOT (
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'feedgen_ops' AND table_name = 'subscriber_feed_assignment'
              AND column_name = 'assignment_id' AND data_type = 'bigint'
              AND is_nullable = 'NO' AND column_default IS NOT NULL
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'feedgen_ops' AND table_name = 'subscriber_feed_assignment'
              AND column_name = 'feed_id' AND data_type = 'character varying' AND is_nullable = 'NO'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'feedgen_ops' AND table_name = 'subscriber_feed_assignment'
              AND column_name = 'did' AND data_type = 'character varying' AND is_nullable = 'NO'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'feedgen_ops' AND table_name = 'subscriber_feed_assignment'
              AND column_name = 'active_from' AND data_type = 'timestamp with time zone'
              AND is_nullable = 'NO' AND column_default IS NOT NULL
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'feedgen_ops' AND table_name = 'subscriber_feed_assignment'
              AND column_name = 'active_until' AND data_type = 'timestamp with time zone' AND is_nullable = 'YES'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'feedgen_ops' AND table_name = 'subscriber_feed_assignment'
              AND column_name = 'source' AND data_type = 'character varying' AND is_nullable = 'YES'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'feedgen_ops' AND table_name = 'subscriber_feed_assignment'
              AND column_name = 'status' AND data_type = 'character varying'
              AND is_nullable = 'NO' AND column_default LIKE '%active%'
          ) AND EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'feedgen_ops.subscriber_feed_assignment'::regclass
              AND conname = 'subscriber_feed_assignment_pkey'
              AND contype = 'p'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%(assignment_id)%'
          ) AND EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'feedgen_ops.subscriber_feed_assignment'::regclass
              AND conname = 'subscriber_feed_assignment_did_fkey'
              AND contype = 'f'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%FOREIGN KEY (did)%'
              AND pg_get_constraintdef(oid) LIKE '%REFERENCES subscriber(did) ON DELETE CASCADE%'
          ) AND EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'feedgen_ops.subscriber_feed_assignment'::regclass
              AND conname = 'subscriber_feed_assignment_interval_check'
              AND contype = 'c'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%active_until IS NULL%'
              AND pg_get_constraintdef(oid) LIKE '%active_until > active_from%'
          ) AND EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'feedgen_ops.subscriber_feed_assignment'::regclass
              AND conname = 'subscriber_feed_assignment_status_check'
              AND contype = 'c'
              AND convalidated
              AND pg_get_constraintdef(oid) LIKE '%active_until IS NULL%'
              AND pg_get_constraintdef(oid) LIKE '%status%active%'
              AND pg_get_constraintdef(oid) LIKE '%removed%'
              AND pg_get_constraintdef(oid) LIKE '%replaced%'
              AND pg_get_constraintdef(oid) LIKE '%omni%'
          ) AND EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'feedgen_ops' AND tablename = 'subscriber_feed_assignment'
              AND indexname = 'subscriber_feed_assignment_active_uq'
              AND indexdef LIKE 'CREATE UNIQUE INDEX%'
              AND indexdef LIKE '%(feed_id, did)%'
              AND indexdef LIKE '%WHERE (active_until IS NULL)%'
          ) AND EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'feedgen_ops' AND tablename = 'subscriber_feed_assignment'
              AND indexname = 'subscriber_feed_assignment_did_active_idx'
              AND indexdef LIKE '%(did)%'
              AND indexdef LIKE '%WHERE (active_until IS NULL)%'
          )
        ) THEN
          RAISE EXCEPTION '004_exact_feed_subscriptions schema mismatch: feedgen_ops.subscriber_feed_assignment is missing required columns, constraints, or indexes';
        END IF;
      END $$
    `.execute(db)
  },
  async down(db: Kysely<unknown>) {
    await sql`
      DO $$
      BEGIN
        IF to_regclass('feedgen_ops.subscriber_feed_assignment') IS NOT NULL
          AND obj_description(
            'feedgen_ops.subscriber_feed_assignment'::regclass,
            'pg_class'
          ) = 'feedgen:migration:004_exact_feed_subscriptions'
        THEN
          DROP TABLE feedgen_ops.subscriber_feed_assignment;
        END IF;
      END $$
    `.execute(db)
    await sql`
      DO $$
      DECLARE
        access_scope_attnum integer;
      BEGIN
        SELECT attnum INTO access_scope_attnum
        FROM pg_attribute
        WHERE attrelid = 'subscriber'::regclass
          AND attname = 'access_scope'
          AND NOT attisdropped;
        IF access_scope_attnum IS NOT NULL
          AND col_description('subscriber'::regclass, access_scope_attnum)
            = 'feedgen:migration:004_exact_feed_subscriptions'
        THEN
          ALTER TABLE subscriber DROP COLUMN access_scope;
        END IF;
      END $$
    `.execute(db)
  },
}
