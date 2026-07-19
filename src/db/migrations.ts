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

migrations['005_canonical_link_columns'] = {
  async up(db: Kysely<unknown>) {
    // Kysely runs migrations in a transaction. Keep this transaction-local so
    // a busy table fails quickly and the pooled session reverts automatically.
    // This covers both ALTER TABLE and the later CREATE TRIGGER statements.
    await sql`SET LOCAL lock_timeout = '2s'`.execute(db)

    await sql`
      DO $migration$
      BEGIN
        IF to_regclass('public.post') IS NULL THEN
          RAISE EXCEPTION '005_canonical_link_columns requires public.post';
        END IF;

        IF NOT (
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'post'
              AND column_name = 'linkUrl' AND data_type = 'character varying'
              AND is_nullable = 'NO'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'post'
              AND column_name = 'linkTitle' AND data_type = 'character varying'
              AND is_nullable = 'NO'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'post'
              AND column_name = 'linkDescription' AND data_type = 'character varying'
              AND is_nullable = 'NO'
          )
        ) THEN
          RAISE EXCEPTION '005_canonical_link_columns schema mismatch: legacy public.post link columns must be varchar NOT NULL';
        END IF;

        -- PostgreSQL 11+ stores a constant default in the catalog for existing
        -- rows. On production PostgreSQL 17 this is metadata-only: no table
        -- rewrite, backfill, NOT NULL scan, or constraint validation occurs.
        ALTER TABLE public.post
          ADD COLUMN IF NOT EXISTS link_uri varchar NOT NULL DEFAULT '',
          ADD COLUMN IF NOT EXISTS link_title varchar NOT NULL DEFAULT '',
          ADD COLUMN IF NOT EXISTS link_description varchar NOT NULL DEFAULT '';

        IF NOT (
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'post'
              AND column_name = 'link_uri' AND data_type = 'character varying'
              AND is_nullable = 'NO' AND column_default IS NOT NULL
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'post'
              AND column_name = 'link_title' AND data_type = 'character varying'
              AND is_nullable = 'NO' AND column_default IS NOT NULL
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'post'
              AND column_name = 'link_description' AND data_type = 'character varying'
              AND is_nullable = 'NO' AND column_default IS NOT NULL
          )
        ) THEN
          RAISE EXCEPTION '005_canonical_link_columns schema mismatch: canonical public.post link columns must be varchar NOT NULL with constant defaults';
        END IF;
      END
      $migration$;
    `.execute(db)

    // Keep old containers safe between the expand migration and app rollout,
    // while application writers also dual-write the two names.
    await sql`
      CREATE OR REPLACE FUNCTION public.feedgen_sync_post_link_columns()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $function$
      BEGIN
        IF NEW.link_uri = '' AND NEW."linkUrl" <> '' THEN
          NEW.link_uri := NEW."linkUrl";
        ELSIF NEW."linkUrl" = '' AND NEW.link_uri <> '' THEN
          NEW."linkUrl" := NEW.link_uri;
        ELSIF NEW.link_uri IS DISTINCT FROM NEW."linkUrl" THEN
          RAISE EXCEPTION 'conflicting link_uri/linkUrl values for post %', NEW.uri;
        END IF;

        IF NEW.link_title = '' AND NEW."linkTitle" <> '' THEN
          NEW.link_title := NEW."linkTitle";
        ELSIF NEW."linkTitle" = '' AND NEW.link_title <> '' THEN
          NEW."linkTitle" := NEW.link_title;
        ELSIF NEW.link_title IS DISTINCT FROM NEW."linkTitle" THEN
          RAISE EXCEPTION 'conflicting link_title/linkTitle values for post %', NEW.uri;
        END IF;

        IF NEW.link_description = '' AND NEW."linkDescription" <> '' THEN
          NEW.link_description := NEW."linkDescription";
        ELSIF NEW."linkDescription" = '' AND NEW.link_description <> '' THEN
          NEW."linkDescription" := NEW.link_description;
        ELSIF NEW.link_description IS DISTINCT FROM NEW."linkDescription" THEN
          RAISE EXCEPTION 'conflicting link_description/linkDescription values for post %', NEW.uri;
        END IF;

        RETURN NEW;
      END
      $function$;
    `.execute(db)

    await sql`
      DO $migration$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_trigger
          WHERE tgrelid = 'public.post'::regclass
            AND tgname = 'feedgen_sync_post_link_columns_trigger'
            AND NOT tgisinternal
        ) THEN
          CREATE TRIGGER feedgen_sync_post_link_columns_trigger
          BEFORE INSERT OR UPDATE OF
            "linkUrl", "linkTitle", "linkDescription",
            link_uri, link_title, link_description
          ON public.post
          FOR EACH ROW
          EXECUTE FUNCTION public.feedgen_sync_post_link_columns();
        ELSIF NOT EXISTS (
          SELECT 1 FROM pg_trigger
          WHERE tgrelid = 'public.post'::regclass
            AND tgname = 'feedgen_sync_post_link_columns_trigger'
            AND tgfoid = 'public.feedgen_sync_post_link_columns()'::regprocedure
            AND NOT tgisinternal
        ) THEN
          RAISE EXCEPTION '005_canonical_link_columns schema mismatch: feedgen_sync_post_link_columns_trigger has an unexpected function';
        END IF;

        COMMENT ON COLUMN public.post.link_uri IS
          'Canonical posted external-card URI; expand stage default is empty until bounded owner backfill';
        COMMENT ON COLUMN public.post.link_title IS
          'Canonical external-card title; expand stage default is empty until bounded owner backfill';
        COMMENT ON COLUMN public.post.link_description IS
          'Canonical external-card description; expand stage default is empty until bounded owner backfill';
        COMMENT ON COLUMN public.post."linkUrl" IS
          'Deprecated compatibility mirror of link_uri; remove only at the gated contract stage';
        COMMENT ON COLUMN public.post."linkTitle" IS
          'Deprecated compatibility mirror of link_title; remove only at the gated contract stage';
        COMMENT ON COLUMN public.post."linkDescription" IS
          'Deprecated compatibility mirror of link_description; remove only at the gated contract stage';
      END
      $migration$;
    `.execute(db)

    await sql`
      DO $migration$
      BEGIN
        IF to_regclass('research_archive.post_snapshot') IS NULL THEN
          RETURN;
        END IF;

        IF NOT (
          EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'research_archive' AND table_name = 'post_snapshot'
              AND column_name = 'link_url' AND data_type = 'text'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'research_archive' AND table_name = 'post_snapshot'
              AND column_name = 'link_title' AND data_type = 'text'
          ) AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'research_archive' AND table_name = 'post_snapshot'
              AND column_name = 'link_description' AND data_type = 'text'
          )
        ) THEN
          RAISE EXCEPTION '005_canonical_link_columns schema mismatch: research_archive.post_snapshot legacy link columns must be text';
        END IF;

        ALTER TABLE research_archive.post_snapshot
          ADD COLUMN IF NOT EXISTS link_uri text;

        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'research_archive' AND table_name = 'post_snapshot'
            AND column_name = 'link_uri' AND data_type = 'text'
        ) THEN
          RAISE EXCEPTION '005_canonical_link_columns schema mismatch: research_archive.post_snapshot.link_uri must be text';
        END IF;

        EXECUTE $function$
          CREATE OR REPLACE FUNCTION research_archive.feedgen_sync_post_snapshot_link_uri()
          RETURNS trigger
          LANGUAGE plpgsql
          AS $body$
          BEGIN
            IF NEW.link_uri IS NULL THEN
              NEW.link_uri := NEW.link_url;
            ELSIF NEW.link_url IS NULL THEN
              NEW.link_url := NEW.link_uri;
            ELSIF NEW.link_uri IS DISTINCT FROM NEW.link_url THEN
              RAISE EXCEPTION 'conflicting link_uri/link_url values for archived post %', NEW.post_uri;
            END IF;
            RETURN NEW;
          END
          $body$
        $function$;

        IF NOT EXISTS (
          SELECT 1 FROM pg_trigger
          WHERE tgrelid = 'research_archive.post_snapshot'::regclass
            AND tgname = 'feedgen_sync_post_snapshot_link_uri_trigger'
            AND NOT tgisinternal
        ) THEN
          CREATE TRIGGER feedgen_sync_post_snapshot_link_uri_trigger
          BEFORE INSERT OR UPDATE OF link_uri, link_url
          ON research_archive.post_snapshot
          FOR EACH ROW
          EXECUTE FUNCTION research_archive.feedgen_sync_post_snapshot_link_uri();
        ELSIF NOT EXISTS (
          SELECT 1 FROM pg_trigger
          WHERE tgrelid = 'research_archive.post_snapshot'::regclass
            AND tgname = 'feedgen_sync_post_snapshot_link_uri_trigger'
            AND tgfoid = 'research_archive.feedgen_sync_post_snapshot_link_uri()'::regprocedure
            AND NOT tgisinternal
        ) THEN
          RAISE EXCEPTION '005_canonical_link_columns schema mismatch: feedgen_sync_post_snapshot_link_uri_trigger has an unexpected function';
        END IF;

        COMMENT ON COLUMN research_archive.post_snapshot.link_uri IS
          'Canonical posted external-card URI; expand stage remains nullable until bounded owner backfill';
        COMMENT ON COLUMN research_archive.post_snapshot.link_url IS
          'Deprecated compatibility mirror of link_uri; remove only at the gated contract stage';
      END
      $migration$;
// BE-VLG condition IDs are intentionally semantic inside the estate while
// their Bluesky-facing rkeys remain neutral.  This migration is the sole
// primary-key rename path; routine catalog administration cannot edit feed_id.
migrations['006_semantic_be_feed_ids'] = {
  async up(db: Kysely<unknown>) {
    await sql`
      DO $$
      DECLARE
        old_count integer;
        new_count integer;
      BEGIN
        IF to_regclass('feedgen_ops.feed_catalog') IS NULL THEN
          RETURN;
        END IF;

        SELECT count(*) INTO old_count
        FROM feedgen_ops.feed_catalog
        WHERE feed_id IN ('newsflow-be-k', 'newsflow-be-m');
        SELECT count(*) INTO new_count
        FROM feedgen_ops.feed_catalog
        WHERE feed_id IN ('be-k-conventional', 'be-m-party-diversity');

        IF old_count = 0 THEN
          RETURN;
        ELSIF old_count <> 2 OR new_count <> 0 THEN
          RAISE EXCEPTION '005_semantic_be_feed_ids requires exactly the two legacy BE rows and no semantic replacements';
        END IF;

        IF to_regclass('ranker_prod.feed_current_priority') IS NOT NULL THEN
          UPDATE ranker_prod.feed_current_priority
          SET feed_id = CASE feed_id
            WHEN 'newsflow-be-k' THEN 'be-k-conventional'
            WHEN 'newsflow-be-m' THEN 'be-m-party-diversity'
          END
          WHERE feed_id IN ('newsflow-be-k', 'newsflow-be-m');
        END IF;

        UPDATE feedgen_ops.subscriber_feed_assignment
        SET feed_id = CASE feed_id
          WHEN 'newsflow-be-k' THEN 'be-k-conventional'
          WHEN 'newsflow-be-m' THEN 'be-m-party-diversity'
        END
        WHERE feed_id IN ('newsflow-be-k', 'newsflow-be-m');

        UPDATE feedgen_ops.feed_catalog
        SET feed_id = CASE feed_id
          WHEN 'newsflow-be-k' THEN 'be-k-conventional'
          WHEN 'newsflow-be-m' THEN 'be-m-party-diversity'
        END
        WHERE feed_id IN ('newsflow-be-k', 'newsflow-be-m');
      END $$;
    `.execute(db)
  },
  async down(db: Kysely<unknown>) {
    await sql`
      DO $$
      DECLARE
        old_count integer;
        new_count integer;
      BEGIN
        IF to_regclass('feedgen_ops.feed_catalog') IS NULL THEN
          RETURN;
        END IF;

        SELECT count(*) INTO old_count
        FROM feedgen_ops.feed_catalog
        WHERE feed_id IN ('newsflow-be-k', 'newsflow-be-m');
        SELECT count(*) INTO new_count
        FROM feedgen_ops.feed_catalog
        WHERE feed_id IN ('be-k-conventional', 'be-m-party-diversity');

        IF new_count = 0 THEN
          RETURN;
        ELSIF new_count <> 2 OR old_count <> 0 THEN
          RAISE EXCEPTION '005_semantic_be_feed_ids rollback requires exactly the two semantic BE rows and no legacy replacements';
        END IF;

        IF to_regclass('ranker_prod.feed_current_priority') IS NOT NULL THEN
          UPDATE ranker_prod.feed_current_priority
          SET feed_id = CASE feed_id
            WHEN 'be-k-conventional' THEN 'newsflow-be-k'
            WHEN 'be-m-party-diversity' THEN 'newsflow-be-m'
          END
          WHERE feed_id IN ('be-k-conventional', 'be-m-party-diversity');
        END IF;

        UPDATE feedgen_ops.subscriber_feed_assignment
        SET feed_id = CASE feed_id
          WHEN 'be-k-conventional' THEN 'newsflow-be-k'
          WHEN 'be-m-party-diversity' THEN 'newsflow-be-m'
        END
        WHERE feed_id IN ('be-k-conventional', 'be-m-party-diversity');

        UPDATE feedgen_ops.feed_catalog
        SET feed_id = CASE feed_id
          WHEN 'be-k-conventional' THEN 'newsflow-be-k'
          WHEN 'be-m-party-diversity' THEN 'newsflow-be-m'
        END
        WHERE feed_id IN ('be-k-conventional', 'be-m-party-diversity');
      END $$;
    `.execute(db)
  },
}
