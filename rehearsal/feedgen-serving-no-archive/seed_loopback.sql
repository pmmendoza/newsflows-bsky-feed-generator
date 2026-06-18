-- Synthetic seed data for the feedgen-serving-no-archive disposable loopback
-- profile. This file is not a production data migration.

DO $$
BEGIN
  IF current_setting('newsflows.allow_rehearsal_seed', true) IS DISTINCT FROM 'true' THEN
    RAISE EXCEPTION 'Refusing rehearsal seed without newsflows.allow_rehearsal_seed=true';
  END IF;
END $$;

INSERT INTO feedgen_ops.feed_catalog (
  feed_id,
  rkey,
  display_name,
  country,
  publisher_did,
  study_id,
  algo_policy_id,
  ranker_policy_id,
  access_policy_id,
  enabled
)
VALUES (
  'at://did:plc:loopback-publisher/app.bsky.feed.generator/newsflow-nl-1',
  'newsflow-nl-1',
  'Loopback NL 1',
  'NL',
  'did:plc:loopback-publisher',
  NULL,
  'chronological',
  NULL,
  'subscriber-default',
  true
)
ON CONFLICT (feed_id) DO UPDATE
SET
  rkey = EXCLUDED.rkey,
  display_name = EXCLUDED.display_name,
  country = EXCLUDED.country,
  publisher_did = EXCLUDED.publisher_did,
  study_id = EXCLUDED.study_id,
  algo_policy_id = EXCLUDED.algo_policy_id,
  ranker_policy_id = EXCLUDED.ranker_policy_id,
  access_policy_id = EXCLUDED.access_policy_id,
  enabled = EXCLUDED.enabled,
  retired_at = NULL;

INSERT INTO public.subscriber(handle, did)
VALUES ('loopback-requester.test', 'did:plc:loopback-requester')
ON CONFLICT (did) DO UPDATE SET handle = EXCLUDED.handle;

INSERT INTO public.follows(subject, follows)
VALUES
  ('did:plc:loopback-requester', 'did:plc:loopback-follow-a'),
  ('did:plc:loopback-requester', 'did:plc:loopback-follow-b')
ON CONFLICT (subject, follows) DO NOTHING;

INSERT INTO public.post (
  uri,
  cid,
  "indexedAt",
  "createdAt",
  author,
  text,
  "rootUri",
  "rootCid",
  "linkUrl",
  "linkTitle",
  "linkDescription",
  likes_count,
  repost_count,
  comments_count,
  quote_count
)
VALUES
  (
    'at://did:plc:loopback-publisher/app.bsky.feed.post/pub1',
    'bafyloopbackpub1',
    to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'did:plc:loopback-publisher',
    'publisher post',
    'at://did:plc:loopback-publisher/app.bsky.feed.post/pub1',
    'bafyloopbackpub1',
    '',
    '',
    '',
    0,
    0,
    0,
    0
  ),
  (
    'at://did:plc:loopback-follow-a/app.bsky.feed.post/f1',
    'bafyloopbackf1',
    to_char((now() - interval '1 minute') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    to_char((now() - interval '1 minute') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'did:plc:loopback-follow-a',
    'followed post 1',
    'at://did:plc:loopback-follow-a/app.bsky.feed.post/f1',
    'bafyloopbackf1',
    '',
    '',
    '',
    0,
    0,
    0,
    0
  ),
  (
    'at://did:plc:loopback-follow-b/app.bsky.feed.post/f2',
    'bafyloopbackf2',
    to_char((now() - interval '2 minutes') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    to_char((now() - interval '2 minutes') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'did:plc:loopback-follow-b',
    'followed post 2',
    'at://did:plc:loopback-follow-b/app.bsky.feed.post/f2',
    'bafyloopbackf2',
    '',
    '',
    '',
    0,
    0,
    0,
    0
  )
ON CONFLICT (uri) DO UPDATE
SET
  cid = EXCLUDED.cid,
  "indexedAt" = EXCLUDED."indexedAt",
  "createdAt" = EXCLUDED."createdAt",
  author = EXCLUDED.author,
  text = EXCLUDED.text,
  "rootUri" = EXCLUDED."rootUri",
  "rootCid" = EXCLUDED."rootCid",
  "linkUrl" = EXCLUDED."linkUrl",
  "linkTitle" = EXCLUDED."linkTitle",
  "linkDescription" = EXCLUDED."linkDescription",
  likes_count = EXCLUDED.likes_count,
  repost_count = EXCLUDED.repost_count,
  comments_count = EXCLUDED.comments_count,
  quote_count = EXCLUDED.quote_count;
