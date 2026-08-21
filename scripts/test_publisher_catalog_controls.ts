import {
  buildFeedCatalogApplyResult,
  buildFeedCatalogDryRun,
  currentValueMismatches,
  feedCatalogShowPayload,
  validateUpdate,
} from '../src/methods/feed-catalog-admin'
import { FeedCatalog } from '../src/db/schema'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const current: FeedCatalog = {
  feed_id: 'be-k', rkey: 'newsflow-be-k', display_name: 'BE K', country: 'BE',
  publisher_did: 'did:be', study_id: 'be-study', algo_policy_id: 'ranker-priority',
  ranker_policy_id: 'be-k', access_policy_id: 'study-only', enabled: true,
  ranker_score_max_age_hours: 24,
  ranker_score_max_age_source: 'study_default',
  ranker_min_score_backed_share: 0.8,
  ranker_min_score_backed_source: 'study_default',
  publisher_post_max_age_days: 7,
  publisher_post_max_age_source: 'feed_override',
  publisher_time_clock: 'receipt_time',
}

for (const value of ['10', '10junk', 1.5, 0, 366]) {
  const parsed = validateUpdate({ rkey: current.rkey, publisher_post_max_age_days: value })
  check(!parsed.ok, `catalog validation must reject ${JSON.stringify(value)}`)
}

const parsed = validateUpdate({
  rkey: current.rkey,
  publisher_post_max_age_days: 10,
  publisher_post_max_age_source: 'study_default',
  publisher_time_clock: 'content_time_v1',
  content_time_cutover_min_valid_share: 0.99,
  content_time_contract_version: 'newsflows-content-time/v2',
  if_current: {
    publisher_post_max_age_days: 7,
    publisher_post_max_age_source: 'feed_override',
    publisher_time_clock: 'receipt_time',
  },
})
check(parsed.ok, 'valid resolved update must parse')
const dryRun = buildFeedCatalogDryRun(current, parsed.row, { studyExists: true })
check(dryRun.status === 'dry-run' && dryRun.change_count === 5, 'dry-run must expose all materialized changes')
check(dryRun.rollback.fields.publisher_post_max_age_days === 7, 'rollback packet must preserve prior value')
check(currentValueMismatches(current, parsed.row.ifCurrent).length === 0, 'matching CAS must pass')
check(currentValueMismatches({ ...current, publisher_post_max_age_days: 8 }, parsed.row.ifCurrent).length === 1, 'stale CAS must fail')

const after = { ...current, ...parsed.row.patch }
const applied = buildFeedCatalogApplyResult(current, after, dryRun, true)
check(applied.readback.publisher_post_max_age_days === 10, 'apply readback must expose value')
check(applied.readback.publisher_post_max_age_source === 'study_default', 'apply readback must expose provenance')
check(!applied.readback.publisher_serving_window.compatibility_fallback_active, 'catalog value must disable fallback')
const rankerCompatibility = feedCatalogShowPayload({
  ...current,
  ranker_score_max_age_hours: null,
  ranker_score_max_age_source: null,
})
check(rankerCompatibility.ranker_score_freshness.effective_hours === 24, 'legacy ranked row keeps 24h score freshness')
check(rankerCompatibility.ranker_score_freshness.compatibility_fallback_active, 'readback labels score compatibility fallback')

const rollbackParsed = validateUpdate({
  rkey: current.rkey,
  ...dryRun.rollback.fields,
  if_current: {
    publisher_post_max_age_days: 10,
    publisher_post_max_age_source: 'study_default',
    publisher_time_clock: 'content_time_v1',
    content_time_cutover_min_valid_share: 0.99,
    content_time_contract_version: 'newsflows-content-time/v2',
  },
})
check(rollbackParsed.ok, 'rollback update must parse through the same CAS path')
const rollbackDryRun = buildFeedCatalogDryRun(after, rollbackParsed.row, { studyExists: true })
check(rollbackDryRun.status === 'dry-run' && rollbackDryRun.proposed.publisher_post_max_age_days === 7, 'rollback dry-run must restore prior value')
const retiredExpiry = validateUpdate({
  rkey: current.rkey,
  publisher_time_transition_expires_at: 'not-a-timestamp',
})
check(!retiredExpiry.ok, 'retired expiry field must not be writable')
check(!Object.prototype.hasOwnProperty.call(applied.readback, 'publisher_time_transition_expires_at'), 'readback must not advertise retired expiry authority')

let missingWindowFailedClosed = false
try {
  feedCatalogShowPayload({
    ...current,
    publisher_post_max_age_days: null,
    publisher_post_max_age_source: null,
  })
} catch {
  missingWindowFailedClosed = true
}
check(missingWindowFailedClosed, 'readback must fail closed without a catalog publisher window')

console.log('publisher catalog control tests passed')
