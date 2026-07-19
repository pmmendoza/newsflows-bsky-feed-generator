-- Synthetic archive-worker seed data for disposable rebuild rehearsal only.
-- This is not production data and must not be run without the guard setting.

DO $$
BEGIN
  IF current_setting('newsflows.allow_archive_rehearsal_seed', true) IS DISTINCT FROM 'true' THEN
    RAISE EXCEPTION 'Refusing archive rehearsal seed without newsflows.allow_archive_rehearsal_seed=true';
  END IF;
END $$;

INSERT INTO feedgen_ops.archive_outbox (
  request_id,
  "position",
  feed_id,
  study_id,
  requester_did,
  requested_at,
  post_uri,
  post_cid,
  payload_json,
  payload_schema_version
) VALUES (
  1001,
  1,
  'newsflow-nl-1',
  'rehearsal-study',
  'did:plc:synthetic-requester',
  '2026-06-20T12:00:00Z',
  'at://did:plc:synthetic-publisher/app.bsky.feed.post/post1',
  'bafySyntheticCidPost1',
  $$
  {
    "schema_version": 1,
    "captured_from": "served",
    "request": {
      "request_id": 1001,
      "position": 1,
      "feed_id": "newsflow-nl-1",
      "study_id": "rehearsal-study",
      "requester_did": "did:plc:synthetic-requester",
      "requested_at": "2026-06-20T12:00:00Z",
      "cursor_in": null,
      "cursor_out": "3",
      "requested_limit": 3,
      "result_count": 1,
      "feedgen_build_sha": "rehearsal",
      "algo_policy_id": "chronological",
      "ranker_run_id": null
    },
    "post": {
      "uri": "at://did:plc:synthetic-publisher/app.bsky.feed.post/post1",
      "cid": "bafySyntheticCidPost1",
      "author": "did:plc:synthetic-publisher",
      "createdAt": "2026-06-20T11:59:00.000Z",
      "indexedAt": "2026-06-20T12:00:01.000Z",
      "text": "synthetic archive worker rehearsal post",
      "rootUri": "",
      "rootCid": "",
      "link_uri": "",
      "link_title": "",
      "link_description": "",
      "linkUrl": "",
      "linkTitle": "",
      "linkDescription": "",
      "likes_count": 2,
      "repost_count": 1,
      "comments_count": 0,
      "quote_count": 0
    }
  }
  $$::jsonb,
  1
)
ON CONFLICT (request_id, "position") DO NOTHING;

INSERT INTO feedgen_ops.archive_outbox (
  request_id,
  "position",
  feed_id,
  study_id,
  requester_did,
  requested_at,
  post_uri,
  post_cid,
  payload_json,
  payload_schema_version
) VALUES (
  1002,
  0,
  'newsflow-nl-1',
  'rehearsal-study',
  'did:plc:synthetic-requester',
  '2026-06-20T12:05:00Z',
  null,
  null,
  $$
  {
    "schema_version": 1,
    "captured_from": "served",
    "request": {
      "request_id": 1002,
      "position": 0,
      "feed_id": "newsflow-nl-1",
      "study_id": "rehearsal-study",
      "requester_did": "did:plc:synthetic-requester",
      "requested_at": "2026-06-20T12:05:00Z",
      "cursor_in": "3",
      "cursor_out": "3",
      "requested_limit": 3,
      "result_count": 0,
      "feedgen_build_sha": "rehearsal",
      "algo_policy_id": "chronological",
      "ranker_run_id": null
    }
  }
  $$::jsonb,
  1
)
ON CONFLICT (request_id, "position") DO NOTHING;
