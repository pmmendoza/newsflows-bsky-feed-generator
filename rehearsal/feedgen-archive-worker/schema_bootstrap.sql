-- Minimal archive-worker schema for disposable rebuild rehearsal only.
-- This is not a production migration.
--
-- The table shapes mirror the live worker-required archive surfaces:
-- feedgen_ops.archive_outbox, feedgen_ops.archive_outbox_dlq, and the
-- research_archive request/post/capture/served surfaces. Broader production
-- research tables are outside this archive-worker runtime profile.

DO $$
BEGIN
  IF current_setting('newsflows.allow_archive_rehearsal_schema_bootstrap', true) IS DISTINCT FROM 'true' THEN
    RAISE EXCEPTION 'Refusing archive rehearsal schema bootstrap without newsflows.allow_archive_rehearsal_schema_bootstrap=true';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS feedgen_ops;
CREATE SCHEMA IF NOT EXISTS research_archive;

CREATE TABLE IF NOT EXISTS feedgen_ops.archive_outbox (
  outbox_id bigserial PRIMARY KEY,
  request_id integer NOT NULL,
  "position" integer NOT NULL,
  feed_id text,
  study_id text,
  requester_did text,
  requested_at timestamptz NOT NULL,
  post_uri text,
  post_cid text,
  payload_json jsonb NOT NULL,
  payload_schema_version integer NOT NULL DEFAULT 1,
  enqueued_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0,
  last_attempt_at timestamptz,
  last_error text,
  UNIQUE (request_id, "position")
);

CREATE INDEX IF NOT EXISTS archive_outbox_enqueued_idx
  ON feedgen_ops.archive_outbox(enqueued_at);
CREATE INDEX IF NOT EXISTS archive_outbox_post_idx
  ON feedgen_ops.archive_outbox(post_uri);
CREATE INDEX IF NOT EXISTS archive_outbox_request_idx
  ON feedgen_ops.archive_outbox(request_id);

CREATE TABLE IF NOT EXISTS feedgen_ops.archive_outbox_dlq (
  outbox_id bigint PRIMARY KEY,
  request_id integer NOT NULL,
  "position" integer NOT NULL,
  feed_id text,
  study_id text,
  requested_at timestamptz,
  post_uri text,
  post_cid text,
  payload_json jsonb NOT NULL,
  payload_schema_version integer NOT NULL DEFAULT 1,
  failed_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL,
  last_error text NOT NULL
);

CREATE INDEX IF NOT EXISTS archive_outbox_dlq_failed_idx
  ON feedgen_ops.archive_outbox_dlq(failed_at);
CREATE INDEX IF NOT EXISTS archive_outbox_dlq_post_idx
  ON feedgen_ops.archive_outbox_dlq(post_uri);

CREATE TABLE IF NOT EXISTS research_archive.request_event (
  request_id integer PRIMARY KEY,
  feed_id text,
  study_id text,
  requester_ref text,
  requester_did_hash text,
  requested_at timestamptz NOT NULL,
  cursor_in text,
  cursor_out text,
  requested_limit integer,
  result_count integer,
  feedgen_build_sha text,
  algo_policy_id text,
  ranker_run_id text,
  captured_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS request_event_did_hash_time_idx
  ON research_archive.request_event(requester_did_hash, requested_at DESC);
CREATE INDEX IF NOT EXISTS request_event_feed_time_idx
  ON research_archive.request_event(feed_id, requested_at DESC);
CREATE INDEX IF NOT EXISTS request_event_study_time_idx
  ON research_archive.request_event(study_id, requested_at DESC);

CREATE TABLE IF NOT EXISTS research_archive.post_snapshot (
  post_uri text NOT NULL,
  cid text NOT NULL,
  author_did text,
  created_at timestamptz,
  indexed_at timestamptz,
  created_at_raw text,
  indexed_at_raw text,
  text text,
  root_uri text,
  root_cid text,
  link_url text,
  link_title text,
  link_description text,
  raw_record_json jsonb,
  first_seen_at timestamptz,
  first_captured_from text NOT NULL,
  captured_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (post_uri, cid)
);

CREATE INDEX IF NOT EXISTS post_snapshot_author_created_idx
  ON research_archive.post_snapshot(author_did, created_at DESC);
CREATE INDEX IF NOT EXISTS post_snapshot_captured_brin_idx
  ON research_archive.post_snapshot USING brin(captured_at);
CREATE INDEX IF NOT EXISTS post_snapshot_raw_record_gin_idx
  ON research_archive.post_snapshot USING gin(raw_record_json);

CREATE TABLE IF NOT EXISTS research_archive.post_snapshot_capture_source (
  post_uri text NOT NULL,
  cid text NOT NULL,
  captured_from text NOT NULL,
  first_captured_at timestamptz NOT NULL DEFAULT now(),
  last_captured_at timestamptz NOT NULL DEFAULT now(),
  observation_count bigint NOT NULL DEFAULT 1,
  PRIMARY KEY (post_uri, cid, captured_from),
  FOREIGN KEY (post_uri, cid)
    REFERENCES research_archive.post_snapshot(post_uri, cid)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS post_snapshot_capture_source_source_idx
  ON research_archive.post_snapshot_capture_source(captured_from, last_captured_at DESC);

CREATE TABLE IF NOT EXISTS research_archive.served_post_event (
  request_id integer NOT NULL,
  "position" integer NOT NULL,
  feed_id text,
  study_id text,
  post_uri text NOT NULL,
  post_cid text,
  likes_count integer,
  repost_count integer,
  comments_count integer,
  quote_count integer,
  selection_reason_json jsonb,
  ranker_run_id text,
  payload_status text NOT NULL DEFAULT 'present'
    CHECK (payload_status IN ('present', 'tombstone_orphan', 'tombstone_redacted')),
  captured_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (request_id, "position"),
  CHECK (
    (payload_status = 'present' AND post_cid IS NOT NULL)
    OR (payload_status <> 'present' AND post_cid IS NULL)
  ),
  FOREIGN KEY (post_uri, post_cid)
    REFERENCES research_archive.post_snapshot(post_uri, cid)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS served_post_event_captured_brin_idx
  ON research_archive.served_post_event USING brin(captured_at);
CREATE INDEX IF NOT EXISTS served_post_event_feed_time_idx
  ON research_archive.served_post_event(feed_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS served_post_event_post_idx
  ON research_archive.served_post_event(post_uri);
CREATE INDEX IF NOT EXISTS served_post_event_ranker_idx
  ON research_archive.served_post_event(ranker_run_id);
CREATE INDEX IF NOT EXISTS served_post_event_study_time_idx
  ON research_archive.served_post_event(study_id, captured_at DESC);
