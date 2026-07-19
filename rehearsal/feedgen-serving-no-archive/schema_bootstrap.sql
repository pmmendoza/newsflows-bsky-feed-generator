-- Minimal feedgen-serving-no-archive schema for disposable loopback rebuild
-- rehearsal only. This is not a production migration.

DO $$
BEGIN
  IF current_setting('newsflows.allow_rehearsal_schema_bootstrap', true) IS DISTINCT FROM 'true' THEN
    RAISE EXCEPTION 'Refusing rehearsal schema bootstrap without newsflows.allow_rehearsal_schema_bootstrap=true';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS feedgen_ops;

CREATE TABLE IF NOT EXISTS feedgen_ops.study_catalog (
  study_id text PRIMARY KEY,
  name text NOT NULL,
  starts_at timestamptz,
  ends_at timestamptz,
  status text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS feedgen_ops.feed_catalog (
  feed_id text PRIMARY KEY,
  rkey text NOT NULL UNIQUE,
  display_name text NOT NULL,
  country text,
  publisher_did text,
  study_id text REFERENCES feedgen_ops.study_catalog(study_id),
  algo_policy_id text NOT NULL CHECK (
    algo_policy_id IN ('chronological', 'ranker-priority', 'engagement-sorted')
  ),
  ranker_policy_id text,
  access_policy_id text NOT NULL CHECK (
    access_policy_id IN ('subscriber-default', 'study-only', 'disabled')
  ),
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  retired_at timestamptz
);

CREATE TABLE IF NOT EXISTS public.subscriber (
  handle varchar NOT NULL,
  did varchar PRIMARY KEY,
  access_scope varchar NOT NULL DEFAULT 'omni' CHECK (
    access_scope IN ('omni', 'assigned', 'none')
  )
);

CREATE TABLE IF NOT EXISTS feedgen_ops.subscriber_feed_assignment (
  assignment_id bigserial PRIMARY KEY,
  feed_id varchar NOT NULL,
  did varchar NOT NULL REFERENCES public.subscriber(did) ON DELETE CASCADE,
  active_from timestamptz NOT NULL DEFAULT now(),
  active_until timestamptz,
  source varchar,
  status varchar NOT NULL DEFAULT 'active',
  CHECK (active_until IS NULL OR active_until > active_from),
  CHECK (
    (active_until IS NULL AND status = 'active') OR
    (active_until IS NOT NULL AND status IN ('removed', 'replaced', 'omni'))
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS subscriber_feed_assignment_active_uq
  ON feedgen_ops.subscriber_feed_assignment(feed_id, did)
  WHERE active_until IS NULL;

CREATE TABLE IF NOT EXISTS public.follows (
  subject varchar NOT NULL,
  follows varchar NOT NULL,
  PRIMARY KEY (subject, follows)
);

CREATE TABLE IF NOT EXISTS public.post (
  uri varchar PRIMARY KEY,
  cid varchar NOT NULL,
  "indexedAt" varchar NOT NULL,
  "createdAt" varchar NOT NULL,
  author varchar NOT NULL,
  text text NOT NULL,
  "rootUri" varchar NOT NULL,
  "rootCid" varchar NOT NULL,
  link_uri varchar NOT NULL,
  link_title varchar NOT NULL,
  link_description varchar NOT NULL,
  "linkUrl" varchar NOT NULL,
  "linkTitle" varchar NOT NULL,
  "linkDescription" varchar NOT NULL,
  priority integer,
  likes_count integer DEFAULT 0,
  repost_count integer DEFAULT 0,
  comments_count integer DEFAULT 0,
  quote_count integer DEFAULT 0,
  CONSTRAINT post_link_columns_match_check CHECK (
    link_uri IS NOT DISTINCT FROM "linkUrl"
    AND link_title IS NOT DISTINCT FROM "linkTitle"
    AND link_description IS NOT DISTINCT FROM "linkDescription"
  )
);

CREATE INDEX IF NOT EXISTS post_author_index ON public.post(author);
CREATE INDEX IF NOT EXISTS follows_subject_index ON public.follows(subject);

CREATE TABLE IF NOT EXISTS public.request_log (
  id serial PRIMARY KEY,
  algo varchar NOT NULL,
  requester_did varchar NOT NULL,
  timestamp timestamptz NOT NULL,
  cursor_in varchar,
  cursor_out varchar,
  requested_limit integer,
  publisher_count integer,
  follows_count integer,
  result_count integer
);

CREATE INDEX IF NOT EXISTS request_log_requester_timestamp_index
  ON public.request_log(requester_did, timestamp);
CREATE INDEX IF NOT EXISTS request_log_algo_timestamp_index
  ON public.request_log(algo, timestamp);

CREATE TABLE IF NOT EXISTS public.request_posts (
  position integer NOT NULL,
  request_id integer NOT NULL REFERENCES public.request_log(id),
  post_uri varchar NOT NULL,
  PRIMARY KEY (request_id, post_uri)
);
