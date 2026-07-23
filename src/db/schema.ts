export type DatabaseSchema = {
  post: Post
  engagement: Engagement
  follows: Follows
  sub_state: SubState
  subscriber: Subscriber
  subscriber_handle_history: SubscriberHandleHistory
  request_log: RequestLog
  request_posts: RequestPosts
  'feedgen_ops.archive_outbox': ArchiveOutbox
  'feedgen_ops.archive_outbox_dlq': ArchiveOutboxDlq
  'feedgen_ops.feed_catalog': FeedCatalog
  'feedgen_ops.study_catalog': StudyCatalog
  'feedgen_ops.study_registry': StudyRegistry
  'feedgen_ops.subscriber_feed_assignment': SubscriberFeedAssignment
  'ranker_prod.feed_current_priority': FeedCurrentPriority
  'ranker_prod.post_politician': PostPolitician
  'ranker_prod.post_political_eligibility': PostPoliticalEligibility
  'research_archive.post_snapshot': PostSnapshot
  'research_archive.post_snapshot_capture_source': PostSnapshotCaptureSource
  'research_archive.request_event': RequestEvent
  'research_archive.served_post_event': ServedPostEvent
}

export type Post = {
  uri: string
  cid: string
  indexedAt: string
  createdAt: string
  author: string
  text: string
  rootUri: string
  rootCid: string
  link_uri: string
  link_title: string
  link_description: string
  // Deprecated compatibility columns; remove only in the gated contract stage.
  linkUrl: string
  linkTitle: string
  linkDescription: string
  likes_count?: number
  repost_count?: number
  comments_count?: number
  quote_count?: number
}

export type Engagement = {
  uri: string
  cid: string
  subjectUri: string
  subjectCid: string
  type: number
  indexedAt: string
  createdAt: string
  author: string
}

export type Follows = {
  subject: string
  follows: string
}

export type SubState = {
  service: string
  cursor: bigint
}

export type SubscriberKind = 'participant' | 'publisher' | 'testing' | 'researcher'

export type Subscriber = {
  handle: string
  did: string
  access_scope?: 'omni' | 'assigned' | 'none'
  // Additive INFRA-WEB-024/030 columns (migration 007). No column DEFAULT on
  // the two timestamps (RT-2) — stamped explicitly by exact-subscription.ts.
  first_subscribed_at?: string | Date | null
  scope_changed_at?: string | Date | null
  kind?: SubscriberKind
}

// Append-only handle-rename transition log (INFRA-WEB-032, migration 007).
export type SubscriberHandleHistory = {
  id?: number | string
  did: string
  old_handle: string
  new_handle: string
  observed_at?: string | Date
  source?: string | null
}

export type RequestLog = {
  id?: number
  algo: string
  requester_did: string
  timestamp: string | Date
  cursor_in?: string | null
  cursor_out?: string | null
  requested_limit?: number | null
  publisher_count?: number | null
  follows_count?: number | null
  result_count?: number | null
}

export type RequestPosts = {
  position: number
  request_id: number
  post_uri: string
}

export type ArchiveOutbox = {
  outbox_id?: number | string
  request_id: number
  position: number
  feed_id?: string | null
  study_id?: string | null
  requester_did?: string | null
  requested_at: string | Date
  // Null for empty-result request rows (position = 0); non-null for
  // per-served-post rows (position > 0). See migration 002.
  post_uri?: string | null
  post_cid?: string | null
  payload_json: any
  payload_schema_version?: number
  enqueued_at?: string | Date
  attempts?: number
  last_attempt_at?: string | Date | null
  last_error?: string | null
}

export type ArchiveOutboxDlq = {
  outbox_id: number | string
  request_id: number
  position: number
  feed_id?: string | null
  study_id?: string | null
  requested_at?: string | Date | null
  // Nullable per migration 002 to mirror archive_outbox.post_uri.
  post_uri?: string | null
  post_cid?: string | null
  payload_json: any
  payload_schema_version?: number
  failed_at?: string | Date
  attempts: number
  last_error: string
}

export type PostSnapshot = {
  post_uri: string
  cid: string
  author_did?: string | null
  created_at?: string | Date | null
  indexed_at?: string | Date | null
  created_at_raw?: string | null
  indexed_at_raw?: string | null
  text?: string | null
  root_uri?: string | null
  root_cid?: string | null
  link_uri?: string | null
  // Deprecated compatibility column; remove only in the gated contract stage.
  link_url?: string | null
  link_title?: string | null
  link_description?: string | null
  raw_record_json?: any
  first_seen_at?: string | Date | null
  first_captured_from: string
  captured_at?: string | Date
}

export type PostSnapshotCaptureSource = {
  post_uri: string
  cid: string
  captured_from: string
  first_captured_at?: string | Date
  last_captured_at?: string | Date
  observation_count?: number | string
}

export type RequestEvent = {
  request_id: number
  feed_id?: string | null
  study_id?: string | null
  requester_ref?: string | null
  requester_did_hash?: string | null
  requested_at: string | Date
  cursor_in?: string | null
  cursor_out?: string | null
  requested_limit?: number | null
  result_count?: number | null
  feedgen_build_sha?: string | null
  algo_policy_id?: string | null
  ranker_run_id?: string | null
  captured_at?: string | Date
}

// Sprint 6 Lane A — feedgen_ops.feed_catalog. One row per active or
// retired feed. Mirrors migration 003. Used by describeFeedGenerator
// (Sprint 6 Lane D) to enumerate enabled feeds from the database
// rather than the static handler registry.
export type FeedCatalog = {
  feed_id: string
  rkey: string
  display_name: string
  country?: string | null
  publisher_did?: string | null
  study_id?: string | null
  algo_policy_id: string
  ranker_policy_id?: string | null
  access_policy_id: string
  // Migration 008: the profile a feed currently serves scores from.
  // NULL ⇒ serve its own rkey (see score-source-cache / D1.4).
  ranker_score_source?: string | null
  enabled: boolean
  created_at?: string | Date
  retired_at?: string | Date | null
}

export type StudyCatalog = {
  study_id: string
  name: string
  starts_at?: string | Date | null
  ends_at?: string | Date | null
  status: string
  created_at?: string | Date
}

// Sprint 6 Lane A — feedgen_ops.study_registry. Governance layer on
// top of subscriber. A DID without a row is "in scope" by default;
// only an explicit ':stop_tracking' status terminates engagement
// archive enqueue (per Sprint 6 Lane D contract).
export type StudyRegistry = {
  study_id: string
  did: string
  active_from: string | Date
  active_until?: string | Date | null
  source?: string | null
  status: string
}

export type SubscriberFeedAssignment = {
  assignment_id?: number | string
  feed_id: string
  did: string
  active_from: string | Date
  active_until?: string | Date | null
  source?: string | null
  status: 'active' | 'removed' | 'replaced' | 'omni'
}

// ranker_prod.feed_current_priority. Current per-(feed, post) score surface,
// maintained by the ranker via upserts from each ranker_run. Variant-2 feed
// handlers join this table and order by score.
export type FeedCurrentPriority = {
  feed_id: string
  // Score-storage decoupling (T-D): the profile these scores belong to.
  // Backfilled to equal feed_id in production; the D1.4 read path joins on it.
  profile_id?: string | null
  post_uri: string
  score?: number | null
  run_id: string
  updated_at?: string | Date
}

export type PostPolitician = {
  post_uri: string
  has_politician: boolean
  matched_entities: any
  classifier_version: string
  roster_version: string
  classified_at: string | Date
}

// BSR-owned politician-or-party eligibility surface (mission D4 fixed seam:
// uri TEXT PK, eligible BOOLEAN NOT NULL, party_ids JSONB, updated_at TEXT).
// Feedgen consumes only `uri` + `eligible`; `party_ids` is BSR/analysis party
// evidence (nullable — a politician-only post may resolve no party) and is not
// read by the serving filter. Any further columns are BSR-owner-defined and
// not modelled here (feedgen never reads them).
export type PostPoliticalEligibility = {
  uri: string
  eligible: boolean
  party_ids: string[] | null
  updated_at: string
}

export type ServedPostEvent = {
  request_id: number
  position: number
  feed_id?: string | null
  study_id?: string | null
  post_uri: string
  post_cid?: string | null
  likes_count?: number | null
  repost_count?: number | null
  comments_count?: number | null
  quote_count?: number | null
  selection_reason_json?: any
  ranker_run_id?: string | null
  payload_status?: string
  captured_at?: string | Date
}
