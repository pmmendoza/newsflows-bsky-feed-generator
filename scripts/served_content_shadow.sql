-- Post-cutover served-order shadow for ranker-priority feeds under publisher_time_clock=content_time_v1 (read-only, raw-free).
-- For each target feed: take the latest served request (request_log), its publisher-slot rows (request_posts joined to post by
-- the feed's publisher DID), and check (a) every served publisher row is content-time valid v2 and inside the content window,
-- (b) the served publisher rows appear in the same relative order as the content shadow (score desc, content_time desc,
-- indexedAt desc, uri desc) -> zero inversions, (c) the served request carried the new clock/revision.
-- Params via psql -v: rkeys='newsflow-nl-2,newsflow-fr-2,newsflow-cz-2,newsflow-ir-2'
\set ON_ERROR_STOP on
\pset format unaligned
\pset fieldsep '\t'
\pset footer off
SET statement_timeout = '60s';
SET default_transaction_read_only = on;
\echo [served_content_shadow]
WITH target AS (
  SELECT rkey, publisher_did, publisher_post_max_age_days AS horizon_days,
         COALESCE(ranker_score_max_age_hours, 24) AS score_hours, catalog_revision, publisher_time_clock
  FROM feedgen_ops.feed_catalog
  WHERE enabled AND rkey = ANY(string_to_array(:'rkeys', ','))
), latest AS (
  SELECT DISTINCT ON (rl.algo) rl.algo AS rkey, rl.id AS request_id, rl.timestamp AS ts,
         rl.publisher_time_clock AS served_clock, rl.feed_catalog_revision AS served_revision, rl.request_reference_time AS ref
  FROM request_log rl JOIN target t ON t.rkey = rl.algo
  ORDER BY rl.algo, rl.timestamp DESC
), served AS (
  SELECT l.rkey, rp.position, rp.post_uri AS uri, p.content_time_status, p.content_time_validator_version,
         p.content_time_utc, p."indexedAt", COALESCE(f.score, -1.0) AS score, (f.score IS NOT NULL) AS scored
  FROM latest l
  JOIN target t ON t.rkey = l.rkey
  JOIN request_posts rp ON rp.request_id = l.request_id
  JOIN post p ON p.uri = rp.post_uri AND p.author = t.publisher_did
  LEFT JOIN ranker_prod.feed_current_priority f
    ON f.post_uri = p.uri AND f.profile_id = t.rkey
   AND f.updated_at >= l.ref::timestamptz - make_interval(hours => t.score_hours)
), ordered AS (
  SELECT s.*, row_number() OVER (PARTITION BY s.rkey ORDER BY s.position) AS served_rank,
         row_number() OVER (PARTITION BY s.rkey ORDER BY s.score DESC, s.content_time_utc::timestamptz DESC, s."indexedAt" DESC, s.uri DESC) AS shadow_rank
  FROM served s
)
SELECT t.rkey, t.publisher_time_clock AS catalog_clock, t.catalog_revision,
       l.served_clock, l.served_revision, l.ts AS served_at,
       (SELECT count(*) FROM served s WHERE s.rkey = t.rkey) AS served_publisher_rows,
       (SELECT count(*) FROM served s WHERE s.rkey = t.rkey AND NOT (s.content_time_status = 'source_valid' AND s.content_time_validator_version = 'newsflows-content-time/v2')) AS served_not_valid_v2,
       (SELECT count(*) FROM served s WHERE s.rkey = t.rkey AND s.content_time_utc::timestamptz < l.ref::timestamptz - make_interval(days => t.horizon_days)) AS served_outside_content_window,
       (SELECT count(*) FROM ordered o WHERE o.rkey = t.rkey AND o.served_rank <> o.shadow_rank) AS order_inversions,
       (SELECT count(*) FROM served s WHERE s.rkey = t.rkey AND s.scored) AS served_scored,
       (SELECT encode(digest(COALESCE(string_agg(o.uri, ',' ORDER BY o.served_rank), ''), 'sha256'), 'hex') FROM ordered o WHERE o.rkey = t.rkey) AS served_sha256
FROM target t LEFT JOIN latest l ON l.rkey = t.rkey
ORDER BY t.rkey;
