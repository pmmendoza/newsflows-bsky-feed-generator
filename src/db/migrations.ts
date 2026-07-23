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
    `.execute(db)
  },
}

// 006_semantic_be_feed_ids INTENTIONALLY DROPPED (2026-07-22) — do not re-add.
// It renamed BE score-row feed_ids newsflow-be-k/-m -> be-k-conventional/
// be-m-party-diversity in place. It was never applied (live ledger stops at
// 005). Production uses neutral public rkeys (newsflow-be-*) uniformly across
// all countries and the ranker actively writes them; applying this rename
// would split-brain against the ranker's live writes. The estate-semantic vs
// participant-neutral naming split is handled the right way by the feed<->
// profile decoupling (T-D: a separate profile_id, feed_id stays neutral), which
// supersedes this in-place rename. Rationale:
// dev/ontology/2026-07-22_be_score_feed_id_resolution.md.

// WebUI subscriber-state foundation (INFRA-WEB-024/026/030/032). Ten known
// special-group DIDs, seeded then hard-asserted (RT-6): a drifted production
// roster must fail the migration loudly rather than silently mis-seed.
const PUBLISHER_DIDS = [
  'did:plc:toz4no26o2x4vsbum7cp4bxp',
  'did:plc:kzmukwaf72iwepygposicgt3',
  'did:plc:cegiy4pfghh4rjs7ks7pbnkm',
  'did:plc:vzmnljt7otfbbgrmachtefxh',
  'did:plc:tlmi333azel2jcornp2qeolm',
]
const TESTING_DIDS = [
  'did:plc:weksrderzzdyxdh26pu5jyqo',
  'did:plc:u7d6u2a5wu7dbjp6wruttlrv',
]
const RESEARCHER_DIDS = [
  'did:plc:3vomhawgkjhtvw4euuxbll3r',
  'did:plc:df5sxbescomzxz7fwovti4vd',
  'did:plc:upgwmkhteysqu2n7mar2w4rk',
]

migrations['007_subscriber_state_and_kind'] = {
  async up(db: Kysely<unknown>) {
    // --- first_subscribed_at / scope_changed_at (additive, nullable, NO
    // column DEFAULT — RT-2. Stamped explicitly by the mutation write-path in
    // exact-subscription.ts, never by a DB default, so CSV/backfill writers
    // that don't set them stay honestly NULL.) ---
    await sql`
      ALTER TABLE subscriber
      ADD COLUMN IF NOT EXISTS first_subscribed_at timestamptz,
      ADD COLUMN IF NOT EXISTS scope_changed_at timestamptz
    `.execute(db)
    await sql`
      COMMENT ON COLUMN subscriber.first_subscribed_at IS 'feedgen:migration:007_subscriber_state_and_kind';
      COMMENT ON COLUMN subscriber.scope_changed_at IS 'feedgen:migration:007_subscriber_state_and_kind';
    `.execute(db)

    // Backfill from feedgen_ops.subscriber_feed_assignment where rows exist;
    // subscribers with no assignment rows (the omni majority) stay NULL —
    // do not stamp them with the migration time.
    await sql`
      DO $$
      BEGIN
        IF to_regclass('feedgen_ops.subscriber_feed_assignment') IS NOT NULL THEN
          UPDATE subscriber s
          SET first_subscribed_at = agg.first_at,
              scope_changed_at = agg.last_at
          FROM (
            SELECT did,
                   MIN(active_from) AS first_at,
                   GREATEST(MAX(active_from), MAX(active_until)) AS last_at
            FROM feedgen_ops.subscriber_feed_assignment
            GROUP BY did
          ) agg
          WHERE s.did = agg.did;
        END IF;
      END $$;
    `.execute(db)

    // --- kind (special-group membership; owner-owned runtime state) ---
    await sql`
      ALTER TABLE subscriber
      ADD COLUMN IF NOT EXISTS kind varchar NOT NULL DEFAULT 'participant'
    `.execute(db)
    await sql`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint
          WHERE conrelid = 'subscriber'::regclass AND conname = 'subscriber_kind_check'
        ) THEN
          ALTER TABLE subscriber ADD CONSTRAINT subscriber_kind_check
            CHECK (kind IN ('participant', 'publisher', 'testing', 'researcher'));
        END IF;
      END $$;
    `.execute(db)
    await sql`COMMENT ON COLUMN subscriber.kind IS 'feedgen:migration:007_subscriber_state_and_kind'`.execute(db)

    // Seed the known special-group DIDs, then hard-fail if the production
    // roster drifted (RT-6): exactly 10 rows must be updated across the three
    // groups, and all 10 must already be access_scope='omni'. A totally
    // absent roster (0 updated — every fresh/dev/test DB, which never has
    // these production subscriber rows) is a clean skip, matching the
    // skip-if-absent idiom migration 006 already uses in this file: only a
    // PARTIAL match (>0 and <10) or a non-omni seeded row is drift worth
    // failing loudly for. Production always has all 10 rows before this
    // migration runs.
    const publisherResult = await sql`
      UPDATE subscriber SET kind = 'publisher' WHERE did IN (${sql.join(PUBLISHER_DIDS)})
    `.execute(db)
    const testingResult = await sql`
      UPDATE subscriber SET kind = 'testing' WHERE did IN (${sql.join(TESTING_DIDS)})
    `.execute(db)
    const researcherResult = await sql`
      UPDATE subscriber SET kind = 'researcher' WHERE did IN (${sql.join(RESEARCHER_DIDS)})
    `.execute(db)
    const publisherCount = Number(publisherResult.numAffectedRows ?? 0)
    const testingCount = Number(testingResult.numAffectedRows ?? 0)
    const researcherCount = Number(researcherResult.numAffectedRows ?? 0)
    const totalSeeded = publisherCount + testingCount + researcherCount
    if (totalSeeded !== 0) {
      if (totalSeeded !== 10) {
        throw new Error(
          '007_subscriber_state_and_kind seed mismatch: expected exactly 10 subscriber rows updated ' +
          `(publisher=${publisherCount}, testing=${testingCount}, researcher=${researcherCount}), got ${totalSeeded}`,
        )
      }
      const allSeededDids = [...PUBLISHER_DIDS, ...TESTING_DIDS, ...RESEARCHER_DIDS]
      const omniCheck = await sql<{ count: string }>`
        SELECT count(*) AS count FROM subscriber
        WHERE did IN (${sql.join(allSeededDids)}) AND access_scope = 'omni'
      `.execute(db)
      const omniCount = Number(omniCheck.rows[0]?.count ?? 0)
      if (omniCount !== 10) {
        throw new Error(
          '007_subscriber_state_and_kind seed mismatch: expected all 10 seeded kind DIDs to be ' +
          `access_scope='omni', got ${omniCount}`,
        )
      }
    }

    // --- subscriber_handle_history (append-only rename transition log) ---
    await sql`
      CREATE TABLE IF NOT EXISTS subscriber_handle_history (
        id bigserial PRIMARY KEY,
        did varchar NOT NULL REFERENCES subscriber(did) ON DELETE CASCADE,
        old_handle varchar NOT NULL,
        new_handle varchar NOT NULL,
        observed_at timestamptz NOT NULL DEFAULT now(),
        source varchar
      )
    `.execute(db)
    await sql`
      CREATE INDEX IF NOT EXISTS subscriber_handle_history_did_idx
      ON subscriber_handle_history (did, observed_at DESC)
    `.execute(db)
    await sql`
      COMMENT ON TABLE subscriber_handle_history IS 'feedgen:migration:007_subscriber_state_and_kind'
    `.execute(db)
  },
  async down(db: Kysely<unknown>) {
    await sql`
      DO $$
      BEGIN
        IF to_regclass('subscriber_handle_history') IS NOT NULL
          AND obj_description('subscriber_handle_history'::regclass, 'pg_class')
            = 'feedgen:migration:007_subscriber_state_and_kind'
        THEN
          DROP TABLE subscriber_handle_history;
        END IF;
      END $$;
    `.execute(db)
    await sql`
      ALTER TABLE subscriber
      DROP COLUMN IF EXISTS kind,
      DROP COLUMN IF EXISTS first_subscribed_at,
      DROP COLUMN IF EXISTS scope_changed_at
    `.execute(db)
  },
}

migrations['008_feed_catalog_ranker_score_source'] = {
  // D1.2 (TARGET_STATE DEC-MOD-051 / ontology T-D score-storage decoupling).
  // Add the feedgen-owned RUNTIME column recording which ranker profile a feed
  // currently serves. NULL => serve by the feed's own rkey (self), preserving
  // today's behavior (the `?? rkey` opt-in the read path adopts in D1.4). This
  // migration ONLY adds the nullable column; the serving read path is unchanged
  // here (still joins ranker_prod.feed_current_priority on feed_id).
  async up(db: Kysely<unknown>) {
    await sql`
      DO $$
      BEGIN
        IF to_regclass('feedgen_ops.feed_catalog') IS NULL THEN
          RAISE EXCEPTION '008_feed_catalog_ranker_score_source: feedgen_ops.feed_catalog is missing';
        END IF;
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'feedgen_ops' AND table_name = 'feed_catalog'
            AND column_name = 'ranker_score_source'
        ) THEN
          ALTER TABLE feedgen_ops.feed_catalog
            ADD COLUMN ranker_score_source text DEFAULT NULL;
          COMMENT ON COLUMN feedgen_ops.feed_catalog.ranker_score_source IS
            'feedgen:migration:008_feed_catalog_ranker_score_source';
        END IF;
      END $$
    `.execute(db)
  },
  async down(db: Kysely<unknown>) {
    await sql`
      ALTER TABLE feedgen_ops.feed_catalog
      DROP COLUMN IF EXISTS ranker_score_source
    `.execute(db)
  },
}

migrations['009_feed_catalog_history'] = {
  async up(db: Kysely<unknown>) {
    await sql`
      CREATE TABLE feedgen_ops.feed_catalog_history (
        feed_id text NOT NULL,
        rkey text NOT NULL,
        revision integer NOT NULL,
        changed_at timestamptz NOT NULL DEFAULT now(),
        actor text NOT NULL,
        source text NOT NULL,
        before_row jsonb,
        after_row jsonb NOT NULL,
        changed_fields jsonb NOT NULL,
        feed_code_hash_before text,
        feed_code_hash_after text,
        ranker_code_hash_before text,
        ranker_code_hash_after text,
        PRIMARY KEY (feed_id, revision)
      )
    `.execute(db)
    await sql`
      CREATE INDEX feed_catalog_history_feed_changed_at_idx
      ON feedgen_ops.feed_catalog_history (feed_id, changed_at DESC)
    `.execute(db)
    await sql`
      REVOKE UPDATE, DELETE ON feedgen_ops.feed_catalog_history FROM PUBLIC
    `.execute(db)
    await sql`
      CREATE FUNCTION feedgen_ops.reject_feed_catalog_history_mutation()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $$
      BEGIN
        RAISE EXCEPTION 'feedgen_ops.feed_catalog_history is append-only';
      END
      $$
    `.execute(db)
    await sql`
      CREATE TRIGGER feed_catalog_history_append_only
      BEFORE UPDATE OR DELETE ON feedgen_ops.feed_catalog_history
      FOR EACH ROW
      EXECUTE FUNCTION feedgen_ops.reject_feed_catalog_history_mutation()
    `.execute(db)
  },
  async down(db: Kysely<unknown>) {
    await sql`
      DROP TABLE feedgen_ops.feed_catalog_history
    `.execute(db)
    await sql`
      DROP FUNCTION feedgen_ops.reject_feed_catalog_history_mutation()
    `.execute(db)
  },
}

migrations['010_config_activation'] = {
  // Append-only under normal roles, mirroring + hardening migration 009
  // (feed_catalog_history): same REVOKE + BEFORE UPDATE OR DELETE reject
  // trigger, PLUS a BEFORE TRUNCATE FOR EACH STATEMENT reject trigger (009
  // did not close the TRUNCATE gap; this migration does). Both triggers
  // share one function since it unconditionally RAISE EXCEPTIONs regardless
  // of TG_OP. This is normal-role integrity only — a superuser can still
  // DISABLE TRIGGER / DROP; that is accepted (superuser tamper-evidence is
  // explicitly out of scope per the design doc).
  async up(db: Kysely<unknown>) {
    await sql`
      CREATE TABLE feedgen_ops.config_activation (
        activation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        activated_at timestamptz NOT NULL DEFAULT now(),
        build_sha text,
        image_id text,
        feed_code_hash text,
        ranker_code_hash text,
        config jsonb NOT NULL,
        config_hash text NOT NULL,
        prev_config_hash text,
        reason text NOT NULL DEFAULT 'process_start'
      )
    `.execute(db)
    await sql`
      CREATE INDEX config_activation_activated_at_idx
      ON feedgen_ops.config_activation (activated_at DESC, activation_id DESC)
    `.execute(db)
    await sql`
      REVOKE UPDATE, DELETE, TRUNCATE ON feedgen_ops.config_activation FROM PUBLIC
    `.execute(db)
    await sql`
      CREATE FUNCTION feedgen_ops.reject_config_activation_mutation()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $$
      BEGIN
        RAISE EXCEPTION 'feedgen_ops.config_activation is append-only';
      END
      $$
    `.execute(db)
    await sql`
      CREATE TRIGGER config_activation_append_only
      BEFORE UPDATE OR DELETE ON feedgen_ops.config_activation
      FOR EACH ROW
      EXECUTE FUNCTION feedgen_ops.reject_config_activation_mutation()
    `.execute(db)
    await sql`
      CREATE TRIGGER config_activation_reject_truncate
      BEFORE TRUNCATE ON feedgen_ops.config_activation
      FOR EACH STATEMENT
      EXECUTE FUNCTION feedgen_ops.reject_config_activation_mutation()
    `.execute(db)
  },
  async down(db: Kysely<unknown>) {
    await sql`
      DROP TABLE feedgen_ops.config_activation
    `.execute(db)
    await sql`
      DROP FUNCTION feedgen_ops.reject_config_activation_mutation()
    `.execute(db)
  },
}
