/**
 * Feed catalog admin read/dry-run helper tests.
 *
 * This is intentionally DB-free: M4a needs the response contract and
 * validation semantics pinned down before bskyops starts using the live
 * feedgen admin surface.
 */

import {
  buildFeedCatalogApplyBlocked,
  buildFeedCatalogApplyConflict,
  buildFeedCatalogApplyResult,
  buildFeedCatalogDryRun,
  currentValueMismatches,
  feedCatalogListPayload,
  feedCatalogNotFoundPayload,
  feedCatalogShowPayload,
  parseSubscribableFilter,
  readCatalogRowFromDb,
  default as registerFeedCatalogAdminEndpoint,
  validateUpdate,
} from '../src/methods/feed-catalog-admin'
import { FeedCatalog, FeedCatalogHistory } from '../src/db/schema'
import express from 'express'
import http from 'http'

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

function assertEqual<T>(actual: T, expected: T, message: string) {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`)
  }
}

function assertJsonEqual(actual: unknown, expected: unknown, message: string) {
  assertEqual(JSON.stringify(actual), JSON.stringify(expected), message)
}

function validUpdate(body: Record<string, unknown>) {
  const result = validateUpdate(body)
  assert(result.ok, `expected valid update, got ${result.ok ? '' : result.error}`)
  return result.row
}

const baseFeed: FeedCatalog = {
  feed_id: 'feed-nl-1',
  rkey: 'newsflow-nl-1',
  display_name: 'Newsflow NL 1',
  country: 'NL',
  publisher_did: 'did:plc:nlbot',
  study_id: 'newsflows-main',
  algo_policy_id: 'chronological',
  ranker_policy_id: null,
  publisher_post_max_age_days: 3,
  publisher_post_max_age_source: 'study_default',
  publisher_time_clock: 'receipt_time',
  access_policy_id: 'study-only',
  enabled: true,
  created_at: '2026-05-01T00:00:00Z',
  retired_at: null,
}

const otherFeed: FeedCatalog = {
  ...baseFeed,
  feed_id: 'feed-nl-2',
  rkey: 'newsflow-nl-2',
  display_name: 'Newsflow NL 2',
  algo_policy_id: 'ranker-priority',
}

function testListPayload() {
  const payload = feedCatalogListPayload([otherFeed, baseFeed])
  assertEqual(payload.schema_version, 1, 'list schema_version')
  assertEqual(payload.feed_count, 2, 'list feed_count')
  assertEqual(payload.raw_values_in_output, false, 'list raw_values_in_output')
  assertEqual(payload.feeds[0].rkey, 'newsflow-nl-2', 'list preserves given row order')
  assertEqual(payload.feeds[1].operator_status, 'active', 'list item operator_status')
  assertEqual(payload.feeds[1].published.status, 'unknown', 'list published placeholder')
}

function testSubscribableListPayload() {
  const payload = feedCatalogListPayload([
    baseFeed,
    { ...otherFeed, enabled: false },
    { ...otherFeed, rkey: 'newsflow-nl-3', retired_at: '2026-07-01T00:00:00Z' },
    { ...otherFeed, rkey: 'newsflow-nl-4', access_policy_id: 'disabled' },
    { ...otherFeed, rkey: 'newsflow-nl-5', access_policy_id: 'unknown-policy' },
  ], true)
  assertEqual(payload.feed_count, 1, 'subscribable list feed_count')
  assertEqual(payload.feeds[0].rkey, 'newsflow-nl-1', 'subscribable list active feed')
  assertEqual(payload.subscribable_only, true, 'subscribable list marker')
}

function testSubscribableFilterParsing() {
  assertEqual(parseSubscribableFilter(undefined), false, 'missing filter')
  assertEqual(parseSubscribableFilter('true'), true, 'true filter')
  assertEqual(parseSubscribableFilter('false'), false, 'false filter')
  let rejected = false
  try {
    parseSubscribableFilter('yes')
  } catch {
    rejected = true
  }
  assert(rejected, 'invalid subscribable filter should be rejected')
}

function testShowPayload() {
  const payload = feedCatalogShowPayload(baseFeed)
  assertEqual(payload.schema_version, 1, 'show schema_version')
  assertEqual(payload.rkey, 'newsflow-nl-1', 'show rkey')
  assertEqual(payload.enabled, true, 'show enabled')
  assertEqual(payload.operator_status, 'active', 'show operator_status')
  assertEqual(payload.raw_values_in_output, false, 'show raw_values_in_output')
}

function testMissingFeedPayload() {
  const payload = feedCatalogNotFoundPayload('missing-feed')
  assertEqual(payload.error, 'rkey=missing-feed not found', 'missing feed message')
}

function testInvalidAccessPolicy() {
  const result = validateUpdate({
    rkey: 'newsflow-nl-1',
    access_policy_id: 'not-a-policy',
  })
  assert(!result.ok, 'invalid access policy should fail')
  assert(
    result.error.includes('access_policy_id must be one of'),
    'invalid access policy error should explain allowed values',
  )
}

function testInvalidUpdateOp() {
  const result = validateUpdate({
    op: 'insert',
    rkey: 'newsflow-nl-1',
    enabled: false,
  })
  assert(!result.ok, 'invalid update op should fail')
  assertEqual(result.error, "op must be 'update' when provided", 'invalid update op error')
}

function testValidIfCurrent() {
  const update = validUpdate({
    rkey: 'newsflow-nl-1',
    enabled: false,
    if_current: {
      enabled: true,
      access_policy_id: 'study-only',
      study_id: 'newsflows-main',
      retired_at: null,
    },
  })
  assertEqual(update.ifCurrent?.enabled, true, 'if_current enabled')
  assertEqual(update.ifCurrent?.study_id, 'newsflows-main', 'if_current study_id')
}

function testValidPolicyFieldUpdate() {
  const update = validUpdate({
    rkey: 'newsflow-nl-1',
    display_name: 'NEWSFLOWS NL - Test',
    publisher_did: 'did:plc:newpublisher',
    algo_policy_id: 'engagement-sorted',
    ranker_policy_id: null,
    if_current: {
      display_name: 'Newsflow NL 1',
      publisher_did: 'did:plc:nlbot',
      algo_policy_id: 'chronological',
      ranker_policy_id: null,
    },
  })
  assertEqual(update.patch.display_name, 'NEWSFLOWS NL - Test', 'patch display_name')
  assertEqual(update.patch.publisher_did, 'did:plc:newpublisher', 'patch publisher_did')
  assertEqual(update.patch.algo_policy_id, 'engagement-sorted', 'patch algo_policy_id')
  assertEqual(update.patch.ranker_policy_id, null, 'patch ranker_policy_id')
  assertEqual(update.ifCurrent?.display_name, 'Newsflow NL 1', 'if_current display_name')
  assertEqual(update.ifCurrent?.algo_policy_id, 'chronological', 'if_current algo_policy_id')
}

function testContentTimeWithoutExpiryIsPermanent() {
  // FT-FU-6: an absent transition expiry means the content-time arrangement is
  // permanent -- the target state -- so the merged row must pass the age rules.
  const live: FeedCatalog = {
    ...baseFeed,
    publisher_time_clock: 'content_time_v1',
    publisher_time_transition_expires_at: '2026-09-01T18:00:00Z',
    content_time_cutover_min_valid_share: 0.8,
    content_time_contract_version: 'newsflows-content-time/v2',
  }
  const dryRun = buildFeedCatalogDryRun(
    live,
    validUpdate({ rkey: 'newsflow-nl-1', publisher_time_transition_expires_at: null }),
    { studyExists: true },
  )
  assertEqual(dryRun.proposed.publisher_time_transition_expires_at, null, 'expiry cleared in proposal')
  assertJsonEqual(
    dryRun.blockers.filter((b: { code: string }) => b.code === 'invalid-publisher-age'),
    [],
    'clearing the expiry must not be blocked',
  )
}

function testContentTimeStillRequiresFloorAndContractVersion() {
  // Only the deadline became optional. The floor and contract version are what keep
  // the clock auditable, so clearing either must still be blocked.
  const live: FeedCatalog = {
    ...baseFeed,
    publisher_time_clock: 'content_time_v1',
    publisher_time_transition_expires_at: '2026-09-01T18:00:00Z',
    content_time_cutover_min_valid_share: 0.8,
    content_time_contract_version: 'newsflows-content-time/v2',
  }
  for (const patch of [
    { rkey: 'newsflow-nl-1', content_time_cutover_min_valid_share: null },
    { rkey: 'newsflow-nl-1', content_time_contract_version: null },
  ]) {
    const dryRun = buildFeedCatalogDryRun(live, validUpdate(patch), { studyExists: true })
    assert(
      dryRun.blockers.some((b: { code: string }) => b.code === 'invalid-publisher-age'),
      `clearing ${Object.keys(patch)[1]} on a content-time feed must be blocked`,
    )
  }
}

function testMalformedExpiryStillRejected() {
  // Absent is permanent, but corrupt is still corrupt. (Note: the patch validator is
  // shallow by design; the age rules run on the merged row, so assert them there.)
  const live: FeedCatalog = {
    ...baseFeed,
    publisher_time_clock: 'content_time_v1',
    publisher_time_transition_expires_at: '2026-09-01T18:00:00Z',
    content_time_cutover_min_valid_share: 0.8,
    content_time_contract_version: 'newsflows-content-time/v2',
  }
  const dryRun = buildFeedCatalogDryRun(
    live,
    validUpdate({ rkey: 'newsflow-nl-1', publisher_time_transition_expires_at: 'not-a-timestamp' }),
    { studyExists: true },
  )
  assert(
    dryRun.blockers.some((b: { code: string }) => b.code === 'invalid-publisher-age'),
    'a malformed expiry must still be blocked',
  )
}

function testContentTimeAcceptsV2AndV3Only() {
  // Update to v2 is valid
  const v2Update = validateUpdate({
    rkey: 'newsflow-nl-1',
    content_time_contract_version: 'newsflows-content-time/v2',
  })
  assert(v2Update.ok, 'v2 contract version update must be accepted')

  // Update to v3 is valid
  const v3Update = validateUpdate({
    rkey: 'newsflow-nl-1',
    content_time_contract_version: 'newsflows-content-time/v3',
  })
  assert(v3Update.ok, 'v3 contract version update must be accepted')

  // Update to null is valid (when disabling or switching clock)
  const nullUpdate = validateUpdate({
    rkey: 'newsflow-nl-1',
    content_time_contract_version: null,
  })
  assert(nullUpdate.ok, 'null contract version update must be accepted by validateUpdate')

  // Rejects invalid version strings
  for (const bad of ['newsflows-content-time/v1', 'newsflows-content-time/v4', 'invalid-version']) {
    const badUpdate = validateUpdate({
      rkey: 'newsflow-nl-1',
      content_time_contract_version: bad,
    })
    assert(!badUpdate.ok, `validateUpdate must reject ${bad}`)
    assertEqual(
      badUpdate.error,
      'content_time_contract_version must be newsflows-content-time/v2 or newsflows-content-time/v3, or null',
      'invalid version error message',
    )
  }

  // Dry-run checking for v3 enabled content-time feed
  const liveV2: FeedCatalog = {
    ...baseFeed,
    publisher_time_clock: 'content_time_v1',
    content_time_cutover_min_valid_share: 0.8,
    content_time_contract_version: 'newsflows-content-time/v2',
  }
  const dryRunV3 = buildFeedCatalogDryRun(
    liveV2,
    validUpdate({ rkey: 'newsflow-nl-1', content_time_contract_version: 'newsflows-content-time/v3' }),
    { studyExists: true },
  )
  assertJsonEqual(
    dryRunV3.blockers.filter((b: { code: string }) => b.code === 'invalid-publisher-age'),
    [],
    'v3 update must have no invalid-publisher-age blockers',
  )
}

function testRankerPriorityRequiresRankerPolicy() {
  const result = validateUpdate({
    rkey: 'newsflow-nl-1',
    algo_policy_id: 'ranker-priority',
    ranker_policy_id: null,
  })
  assert(!result.ok, 'ranker-priority without ranker_policy_id should fail')
  assertEqual(
    result.error,
    'ranker_policy_id required when algo_policy_id=ranker-priority',
    'ranker-priority ranker policy error',
  )
}

function testNonRankerPolicyRequiresNullRankerPolicy() {
  const result = validateUpdate({
    rkey: 'newsflow-nl-1',
    algo_policy_id: 'chronological',
    ranker_policy_id: 'news-cluster-engagement',
  })
  assert(!result.ok, 'chronological with ranker_policy_id should fail')
  assertEqual(
    result.error,
    'ranker_policy_id must be null when algo_policy_id is chronological or engagement-sorted',
    'non-ranker ranker policy error',
  )
}

function testRankerScoreSourceUpdate() {
  // (a) set to a string and to null are both accepted + applied.
  const toProfile = validUpdate({ rkey: 'newsflow-nl-1', ranker_score_source: 'nl-shared' })
  assertEqual(toProfile.patch.ranker_score_source, 'nl-shared', 'patch ranker_score_source string')
  const toNull = validUpdate({
    rkey: 'newsflow-nl-1',
    ranker_score_source: null,
    if_current: { ranker_score_source: 'nl-shared' },
  })
  assertEqual(toNull.patch.ranker_score_source, null, 'patch ranker_score_source null')

  const servingSelf: FeedCatalog = { ...baseFeed, ranker_score_source: null }
  const dryRun = buildFeedCatalogDryRun(
    servingSelf,
    validUpdate({ rkey: 'newsflow-nl-1', ranker_score_source: 'nl-shared' }),
    { studyExists: true },
  )
  assertEqual(dryRun.status, 'dry-run', 'score-source diff status')
  assertEqual(dryRun.current.ranker_score_source, null, 'score-source diff current')
  assertEqual(dryRun.proposed.ranker_score_source, 'nl-shared', 'score-source diff proposed')
  assertEqual(dryRun.rollback.fields.ranker_score_source, null, 'score-source diff rollback')
  const after = { ...servingSelf, ranker_score_source: 'nl-shared' }
  const applied = buildFeedCatalogApplyResult(servingSelf, after, dryRun, true)
  assertEqual(applied.after.ranker_score_source, 'nl-shared', 'score-source apply after')
  assertEqual(applied.readback.ranker_score_source, 'nl-shared', 'score-source apply readback')
}

function testRankerScoreSourceCas() {
  // (b) CAS: an if_current.ranker_score_source that doesn't match the live
  // value rejects the switch; a match passes.
  const live: FeedCatalog = { ...baseFeed, ranker_score_source: 'nl-shared' }
  const update = validUpdate({
    rkey: 'newsflow-nl-1',
    ranker_score_source: 'nl-ideology',
    if_current: { ranker_score_source: 'nl-shared' },
  })
  assert(
    currentValueMismatches(live, update.ifCurrent).length === 0,
    'matching if_current.ranker_score_source should pass CAS',
  )
  const drifted: FeedCatalog = { ...baseFeed, ranker_score_source: 'nl-other' }
  const mismatches = currentValueMismatches(drifted, update.ifCurrent)
  assertEqual(mismatches.length, 1, 'mismatched if_current.ranker_score_source should reject CAS')
  assertEqual(mismatches[0].field, 'ranker_score_source', 'CAS mismatch field')
  assertEqual(mismatches[0].expected, 'nl-shared', 'CAS mismatch expected')
  assertEqual(mismatches[0].actual, 'nl-other', 'CAS mismatch actual')
}

function testRankerScoreSourceRejectsNonString() {
  // (c) non-string / non-null values are rejected in patch and if_current.
  const badPatch = validateUpdate({ rkey: 'newsflow-nl-1', ranker_score_source: 42 })
  assert(!badPatch.ok, 'numeric ranker_score_source should fail')
  assertEqual(badPatch.error, 'ranker_score_source must be string or null', 'ranker_score_source patch error')
  const badIfCurrent = validateUpdate({
    rkey: 'newsflow-nl-1',
    enabled: false,
    if_current: { ranker_score_source: 42 },
  })
  assert(!badIfCurrent.ok, 'numeric if_current.ranker_score_source should fail')
  assertEqual(
    badIfCurrent.error,
    'if_current.ranker_score_source must be string or null',
    'ranker_score_source if_current error',
  )
}

function testInvalidIfCurrentField() {
  const result = validateUpdate({
    rkey: 'newsflow-nl-1',
    enabled: false,
    if_current: {
      not_a_field: true,
    },
  })
  assert(!result.ok, 'invalid if_current field should fail')
  assert(
    result.error.includes('if_current contains unsupported field'),
    'invalid if_current field should explain unsupported field',
  )
}

function testNoOpDryRun() {
  const dryRun = buildFeedCatalogDryRun(
    baseFeed,
    validUpdate({ rkey: 'newsflow-nl-1', enabled: true }),
    { studyExists: true },
  )
  assertEqual(dryRun.status, 'no-op', 'no-op status')
  assertEqual(dryRun.dry_run, true, 'no-op dry_run flag')
  assertEqual(dryRun.would_write, false, 'no-op would_write flag')
  assertEqual(dryRun.change_count, 0, 'no-op change_count')
  assertEqual(dryRun.raw_values_in_output, false, 'no-op raw_values_in_output')
}

function testRealDiffDryRun() {
  const dryRun = buildFeedCatalogDryRun(
    baseFeed,
    validUpdate({
      rkey: 'newsflow-nl-1',
      enabled: false,
      access_policy_id: 'disabled',
      retired_at: '2026-05-10T00:00:00Z',
    }),
    { studyExists: true },
  )
  assertEqual(dryRun.status, 'dry-run', 'real diff status')
  assertEqual(dryRun.change_count, 3, 'real diff change_count')
  assertEqual(dryRun.current.enabled, true, 'real diff current enabled')
  assertEqual(dryRun.proposed.enabled, false, 'real diff proposed enabled')
  assertEqual(dryRun.current_status, 'active', 'real diff current status')
  assertEqual(dryRun.proposed_status, 'retired', 'real diff proposed status')
  assertEqual(dryRun.rollback.fields.enabled, true, 'real diff rollback enabled')
  assert(
    dryRun.warnings.some((warning: any) => warning.code === 'retirement-semantics-review'),
    'real diff should warn about retirement semantics',
  )
}

function testPolicyDiffDryRun() {
  const dryRun = buildFeedCatalogDryRun(
    baseFeed,
    validUpdate({
      rkey: 'newsflow-nl-1',
      display_name: 'NEWSFLOWS NL - Test',
      publisher_did: 'did:plc:newpublisher',
      algo_policy_id: 'engagement-sorted',
      ranker_policy_id: null,
    }),
    { studyExists: true },
  )
  assertEqual(dryRun.status, 'dry-run', 'policy diff status')
  assertEqual(dryRun.change_count, 3, 'policy diff change_count')
  assertEqual(dryRun.current.display_name, 'Newsflow NL 1', 'policy diff current display_name')
  assertEqual(dryRun.proposed.display_name, 'NEWSFLOWS NL - Test', 'policy diff proposed display_name')
  assertEqual(dryRun.current.algo_policy_id, 'chronological', 'policy diff current algo')
  assertEqual(dryRun.proposed.algo_policy_id, 'engagement-sorted', 'policy diff proposed algo')
  assertEqual(dryRun.rollback.fields.algo_policy_id, 'chronological', 'policy diff rollback algo')
}

function testBlockedDryRun() {
  const dryRun = buildFeedCatalogDryRun(
    baseFeed,
    validUpdate({
      rkey: 'newsflow-nl-1',
      study_id: null,
    }),
    { studyExists: undefined },
  )
  assertEqual(dryRun.status, 'blocked', 'blocked status')
  assert(
    dryRun.blockers.some((blocker: any) => blocker.code === 'study-id-required'),
    'blocked run should require study_id for study-only access',
  )
}

function testApplyResultPayload() {
  const dryRun = buildFeedCatalogDryRun(
    baseFeed,
    validUpdate({ rkey: 'newsflow-nl-1', enabled: false }),
    { studyExists: true },
  )
  const afterFeed = { ...baseFeed, enabled: false }
  const result = buildFeedCatalogApplyResult(baseFeed, afterFeed, dryRun, true)
  assertEqual(result.mode, 'apply', 'apply result mode')
  assertEqual(result.status, 'applied', 'apply result status')
  assertEqual(result.applied, true, 'apply result applied')
  assertEqual(result.wrote, true, 'apply result wrote')
  assertEqual(result.before.enabled, true, 'apply result before enabled')
  assertEqual(result.after.enabled, false, 'apply result after enabled')
  assertEqual(result.readback.enabled, false, 'apply result readback enabled')
  assertEqual(result.rollback.fields.enabled, true, 'apply result rollback enabled')
  assertEqual(result.raw_values_in_output, false, 'apply result raw_values_in_output')
}

function testApplyConflictPayload() {
  const update = validUpdate({
    rkey: 'newsflow-nl-1',
    enabled: false,
    if_current: { enabled: true },
  })
  const dryRun = buildFeedCatalogDryRun(baseFeed, update, { studyExists: true })
  const currentAfterConcurrentChange = { ...baseFeed, enabled: false }
  const mismatches = currentValueMismatches(
    currentAfterConcurrentChange,
    update.ifCurrent,
  )
  const conflict = buildFeedCatalogApplyConflict(dryRun, mismatches)
  assertEqual(conflict.mode, 'apply', 'conflict mode')
  assertEqual(conflict.status, 'conflict', 'conflict status')
  assertEqual(conflict.applied, false, 'conflict applied')
  assert(
    conflict.blockers.some((blocker: any) => blocker.code === 'stale-current-values'),
    'conflict should include stale-current-values blocker',
  )
}

function testApplyBlockedPayload() {
  const dryRun = buildFeedCatalogDryRun(
    baseFeed,
    validUpdate({ rkey: 'newsflow-nl-1', study_id: null }),
    { studyExists: undefined },
  )
  const blocked = buildFeedCatalogApplyBlocked(dryRun, {
    code: 'dry-run-blocked',
    message: 'apply refused because feedgen dry-run has blockers',
  })
  assertEqual(blocked.mode, 'apply', 'blocked apply mode')
  assertEqual(blocked.status, 'blocked', 'blocked apply status')
  assertEqual(blocked.applied, false, 'blocked apply applied')
  assert(
    blocked.blockers.some((blocker: any) => blocker.code === 'dry-run-blocked'),
    'blocked apply should include dry-run-blocked blocker',
  )
}

// Records whether the query issued a FOR UPDATE lock. Any builder method
// returns the same stub so the chain works regardless of call order.
function fakeCatalogDb(row: FeedCatalog | undefined) {
  const calls = { forUpdate: 0 }
  const qb: any = {
    selectFrom: () => qb,
    select: () => qb,
    where: () => qb,
    forUpdate: () => {
      calls.forUpdate += 1
      return qb
    },
    executeTakeFirst: async () => row,
  }
  return { db: qb, calls }
}

async function testUpdatePathLocksRowGetPathDoesNot() {
  // The update transaction's CAS read must lock the row (FOR UPDATE) so two
  // concurrent switches serialize; the GET/dry-run read must stay unlocked.
  const locked = fakeCatalogDb(baseFeed)
  await readCatalogRowFromDb(locked.db, 'newsflow-nl-1', true)
  assertEqual(locked.calls.forUpdate, 1, 'update-path read must lock the row (FOR UPDATE)')

  const unlocked = fakeCatalogDb(baseFeed)
  await readCatalogRowFromDb(unlocked.db, 'newsflow-nl-1')
  assertEqual(unlocked.calls.forUpdate, 0, 'GET/dry-run read must NOT lock the row')
}

function fakeHistoryDb(
  initialFeed: FeedCatalog,
  initialHistory: FeedCatalogHistory[] = [],
) {
  let feed = { ...initialFeed }
  const history = [...initialHistory]

  function selectFrom(table: string) {
    let whereColumn = ''
    let whereValue: unknown
    let limitValue: number | undefined
    let offsetValue = 0
    const order: Array<{ column: string; direction: string }> = []
    const query: any = {
      select: () => query,
      selectAll: () => query,
      where: (column: string, _operator: string, value: unknown) => {
        whereColumn = column
        whereValue = value
        return query
      },
      orderBy: (column: string, direction: string) => {
        order.push({ column, direction })
        return query
      },
      limit: (value: number) => {
        limitValue = value
        return query
      },
      offset: (value: number) => {
        offsetValue = value
        return query
      },
      forUpdate: () => query,
      executeTakeFirst: async () => {
        if (table === 'feedgen_ops.feed_catalog') {
          return whereColumn !== 'rkey' || whereValue === feed.rkey
            ? { ...feed }
            : undefined
        }
        if (table === 'feedgen_ops.feed_catalog_history') {
          const rows = history
            .filter((row) => whereColumn !== 'feed_id' || row.feed_id === whereValue)
            .sort((a, b) => b.revision - a.revision)
          const row = rows[0]
          return row
            ? {
              revision: row.revision,
              feed_code_hash_after: row.feed_code_hash_after,
            }
            : undefined
        }
        return undefined
      },
      execute: async () => {
        if (table !== 'feedgen_ops.feed_catalog_history') return []
        const rows = history
          .filter((row) => whereColumn !== 'feed_id' || row.feed_id === whereValue)
          .sort((a, b) => {
            for (const item of order) {
              const aValue = a[item.column as keyof FeedCatalogHistory]
              const bValue = b[item.column as keyof FeedCatalogHistory]
              if (aValue === bValue) continue
              const comparison = String(aValue) < String(bValue) ? -1 : 1
              return item.direction === 'desc' ? -comparison : comparison
            }
            return 0
          })
        return rows.slice(offsetValue, limitValue === undefined ? undefined : offsetValue + limitValue)
      },
    }
    return query
  }

  function insertInto(table: string) {
    let values: any
    const decodeJsonExpression = (value: any) => {
      const node = value?.toOperationNode?.()
      const serialized = node?.parameters?.[0]?.value
      return typeof serialized === 'string' ? JSON.parse(serialized) : value
    }
    const query: any = {
      values: (next: any) => {
        values = next
        return query
      },
      execute: async () => {
        if (table === 'feedgen_ops.feed_catalog_history') {
          history.push({
            ...values,
            before_row: values.before_row == null ? null : decodeJsonExpression(values.before_row),
            after_row: decodeJsonExpression(values.after_row),
            changed_fields: decodeJsonExpression(values.changed_fields),
            changed_at: `2026-07-23T00:00:${String(values.revision).padStart(2, '0')}Z`,
          })
        } else if (table === 'feedgen_ops.feed_catalog') {
          feed = { ...values }
        }
        return []
      },
    }
    return query
  }

  function updateTable(table: string) {
    let patch: Partial<FeedCatalog> = {}
    const query: any = {
      set: (next: Partial<FeedCatalog>) => {
        patch = next
        return query
      },
      where: () => query,
      executeTakeFirst: async () => {
        if (table === 'feedgen_ops.feed_catalog') feed = { ...feed, ...patch }
        return { numUpdatedRows: 1 }
      },
    }
    return query
  }

  const db: any = {
    selectFrom,
    insertInto,
    updateTable,
    getExecutor: () => ({
      transformQuery: (node: any) => node,
      compileQuery: (node: any) => ({ sql: '', parameters: [] }),
      executeQuery: async () => ({ rows: [] }),
    }),
    transaction: () => ({
      execute: async (callback: (trx: any) => Promise<unknown>) => callback(db),
    }),
  }
  return { db, history }
}

type JsonResponse = {
  status: number
  body: any
}

async function requestJson(
  server: http.Server,
  path: string,
  method: 'GET' | 'POST',
  headers: Record<string, string> = {},
  body?: unknown,
): Promise<JsonResponse> {
  const address = server.address()
  assert(address && typeof address === 'object', 'server must listen on a port')
  const payload = body === undefined ? undefined : JSON.stringify(body)

  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1',
      port: address.port,
      path,
      method,
      headers: {
        ...headers,
        ...(payload === undefined
          ? {}
          : {
            'content-type': 'application/json',
            'content-length': Buffer.byteLength(payload).toString(),
          }),
      },
    }, (res) => {
      let data = ''
      res.setEncoding('utf8')
      res.on('data', (chunk) => {
        data += chunk
      })
      res.on('end', () => {
        resolve({
          status: res.statusCode ?? 0,
          body: data ? JSON.parse(data) : null,
        })
      })
    })
    req.on('error', reject)
    if (payload !== undefined) req.write(payload)
    req.end()
  })
}

async function withFeedCatalogServer(
  db: any,
  callback: (server: http.Server) => Promise<void>,
) {
  const app = express()
  app.use(express.json())
  registerFeedCatalogAdminEndpoint(
    { xrpc: { router: app } } as any,
    { db } as any,
  )
  const server = app.listen(0, '127.0.0.1')
  await new Promise<void>((resolve) => server.once('listening', resolve))
  try {
    await callback(server)
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((err) => (err ? reject(err) : resolve())),
    )
  }
}

async function testAdminUpdateWritesOneHistoryRowPerRevision() {
  const testFeed: FeedCatalog = {
    ...baseFeed,
    study_id: null,
    access_policy_id: 'subscriber-default',
  }
  const fake = fakeHistoryDb(testFeed)
  process.env.FEEDGEN_ADMIN_API_KEY = 'history-admin-key'
  process.env.FEEDGEN_FEED_CODE_HASH = 'feed-code-a'

  await withFeedCatalogServer(fake.db, async (server) => {
    const first = await requestJson(
      server,
      '/api/admin/feed_catalog',
      'POST',
      {
        'api-key': 'history-admin-key',
        'x-feedgen-actor': 'operator@example.test',
        'x-feedgen-source': 'console',
      },
      {
        op: 'update',
        rkey: testFeed.rkey,
        display_name: 'Newsflow NL One',
      },
    )
    assertEqual(first.status, 200, 'first update status')
    assertEqual(fake.history.length, 1, 'first update writes exactly one history row')
    assertEqual(fake.history[0].revision, 1, 'first history revision')
    assertEqual(fake.history[0].actor, 'operator@example.test', 'history actor')
    assertEqual(fake.history[0].source, 'console', 'history source')
    assertEqual(fake.history[0].before_row?.display_name, testFeed.display_name, 'history before row')
    assertEqual(fake.history[0].after_row.display_name, 'Newsflow NL One', 'history after row')
    assertJsonEqual(fake.history[0].changed_fields, [{
      field: 'display_name',
      current: testFeed.display_name,
      proposed: 'Newsflow NL One',
    }], 'history changed_fields')
    assertEqual(fake.history[0].feed_code_hash_before, 'feed-code-a', 'first feed hash before')
    assertEqual(fake.history[0].feed_code_hash_after, 'feed-code-a', 'first feed hash after')
    assertEqual(fake.history[0].ranker_code_hash_before, null, 'ranker hash before placeholder')
    assertEqual(fake.history[0].ranker_code_hash_after, null, 'ranker hash after placeholder')

    process.env.FEEDGEN_FEED_CODE_HASH = 'feed-code-b'
    const second = await requestJson(
      server,
      '/api/admin/feed_catalog',
      'POST',
      { 'api-key': 'history-admin-key' },
      {
        op: 'update',
        rkey: testFeed.rkey,
        enabled: false,
      },
    )
    assertEqual(second.status, 200, 'second update status')
    assertEqual(fake.history.length, 2, 'second update writes exactly one more history row')
    assertEqual(fake.history[1].revision, 2, 'history revision is monotonic')
    assertEqual(fake.history[1].before_row?.enabled, true, 'second history before row')
    assertEqual(fake.history[1].after_row.enabled, false, 'second history after row')
    assertEqual(fake.history[1].actor, 'api-key', 'default history actor')
    assertEqual(fake.history[1].source, 'direct-api', 'default history source')
    assertEqual(fake.history[1].feed_code_hash_before, 'feed-code-a', 'prior after hash becomes before')
    assertEqual(fake.history[1].feed_code_hash_after, 'feed-code-b', 'new feed hash after')
  })
}

async function testHistoryReadEndpointAuthPaginationAndNewestFirst() {
  const history = [1, 2, 3].map((revision): FeedCatalogHistory => ({
    feed_id: baseFeed.feed_id,
    rkey: baseFeed.rkey,
    revision,
    changed_at: `2026-07-23T00:00:0${revision}Z`,
    actor: 'api-key',
    source: 'direct-api',
    before_row: revision === 1 ? null : baseFeed,
    after_row: baseFeed,
    changed_fields: [],
    feed_code_hash_before: null,
    feed_code_hash_after: null,
    ranker_code_hash_before: null,
    ranker_code_hash_after: null,
  }))
  const fake = fakeHistoryDb(baseFeed, history)
  process.env.FEEDGEN_READ_API_KEY = 'history-read-key'

  await withFeedCatalogServer(fake.db, async (server) => {
    const unauthorized = await requestJson(
      server,
      `/api/admin/feed_catalog/${baseFeed.rkey}/history`,
      'GET',
    )
    assertEqual(unauthorized.status, 401, 'history endpoint requires auth')

    const firstPage = await requestJson(
      server,
      `/api/admin/feed_catalog/${baseFeed.rkey}/history?limit=2&offset=0`,
      'GET',
      { 'api-key': 'history-read-key' },
    )
    assertEqual(firstPage.status, 200, 'history first page status')
    assertEqual(firstPage.body.raw_values_in_output, false, 'history raw-free marker')
    assertJsonEqual(
      firstPage.body.history.map((row: FeedCatalogHistory) => row.revision),
      [3, 2],
      'history is newest-first',
    )

    const secondPage = await requestJson(
      server,
      `/api/admin/feed_catalog/${baseFeed.rkey}/history?limit=2&offset=2`,
      'GET',
      { 'api-key': 'history-read-key' },
    )
    assertJsonEqual(
      secondPage.body.history.map((row: FeedCatalogHistory) => row.revision),
      [1],
      'history offset pagination',
    )
  })
}

async function testBulkRequiresCasAndUniqueRows() {
  const fake = fakeHistoryDb({ ...baseFeed, study_id: null, access_policy_id: 'subscriber-default' })
  process.env.FEEDGEN_ADMIN_API_KEY = 'bulk-admin-key'
  await withFeedCatalogServer(fake.db, async (server) => {
    const missingCas = await requestJson(server, '/api/admin/feed_catalog/bulk', 'POST', { 'api-key': 'bulk-admin-key' }, {
      updates: [{ op: 'update', rkey: baseFeed.rkey, enabled: false }],
    })
    assertEqual(missingCas.status, 400, 'bulk rows require CAS')
    const duplicate = await requestJson(server, '/api/admin/feed_catalog/bulk', 'POST', { 'api-key': 'bulk-admin-key' }, {
      updates: [
        { op: 'update', rkey: baseFeed.rkey, enabled: false, if_current: { enabled: true } },
        { op: 'update', rkey: baseFeed.rkey, enabled: false, if_current: { enabled: true } },
      ],
    })
    assertEqual(duplicate.status, 400, 'bulk rows require unique rkeys')
  })
}

async function testRejectedCoherenceRollsBackRowAndHistory() {
  const initialV2Feed1: FeedCatalog = {
    ...baseFeed,
    rkey: 'newsflow-nl-1',
    study_id: null,
    access_policy_id: 'subscriber-default',
    publisher_time_clock: 'content_time_v1',
    content_time_contract_version: 'newsflows-content-time/v2',
    content_time_cutover_min_valid_share: 0.8,
  }
  const initialV2Feed2: FeedCatalog = {
    ...baseFeed,
    feed_id: 'feed-nl-2',
    rkey: 'newsflow-nl-2',
    study_id: null,
    access_policy_id: 'subscriber-default',
    publisher_time_clock: 'content_time_v1',
    content_time_contract_version: 'newsflows-content-time/v2',
    content_time_cutover_min_valid_share: 0.8,
  }

  let feeds: Record<string, FeedCatalog> = {
    'newsflow-nl-1': { ...initialV2Feed1 },
    'newsflow-nl-2': { ...initialV2Feed2 },
  }
  const history: FeedCatalogHistory[] = []

  let inTransaction = false
  let snapshotFeeds: Record<string, FeedCatalog> | null = null
  let snapshotHistory: FeedCatalogHistory[] | null = null

  const mockDb: any = {
    selectFrom: (table: string) => {
      let whereCol = ''
      let whereVal: any
      const q: any = {
        select: () => q,
        selectAll: () => q,
        where: (col: string, op: string, val: any) => {
          whereCol = col
          whereVal = val
          return q
        },
        orderBy: () => q,
        limit: () => q,
        forUpdate: () => q,
        executeTakeFirst: async () => {
          if (table === 'feedgen_ops.feed_catalog') {
            if (whereCol === 'rkey' && typeof whereVal === 'string') {
              return feeds[whereVal]
            }
            return Object.values(feeds)[0]
          }
          return undefined
        },
        execute: async () => {
          if (table === 'feedgen_ops.feed_catalog') {
            return Object.values(feeds)
          }
          return history
        },
      }
      return q
    },
    updateTable: (table: string) => {
      let patch: any = {}
      let targetRkey = ''
      const q: any = {
        set: (p: any) => { patch = p; return q },
        where: (col: string, op: string, val: string) => { targetRkey = val; return q },
        executeTakeFirst: async () => {
          if (feeds[targetRkey]) {
            feeds[targetRkey] = { ...feeds[targetRkey], ...patch }
            return { numUpdatedRows: 1 }
          }
          return { numUpdatedRows: 0 }
        },
      }
      return q
    },
    insertInto: (table: string) => {
      let values: any
      const q: any = {
        values: (v: any) => { values = v; return q },
        execute: async () => {
          if (table === 'feedgen_ops.feed_catalog_history') {
            history.push(values)
          }
          return []
        },
      }
      return q
    },
    getExecutor: () => ({
      transformQuery: (node: any) => node,
      compileQuery: (node: any) => ({ sql: '', parameters: [] }),
      executeQuery: async () => ({ rows: [] }),
    }),
    transaction: () => ({
      execute: async (callback: (trx: any) => Promise<unknown>) => {
        inTransaction = true
        snapshotFeeds = JSON.parse(JSON.stringify(feeds))
        snapshotHistory = JSON.parse(JSON.stringify(history))
        try {
          const res = await callback(mockDb)
          inTransaction = false
          return res
        } catch (err) {
          // Transaction abort / rollback!
          feeds = snapshotFeeds!
          history.length = 0
          history.push(...snapshotHistory!)
          inTransaction = false
          throw err
        }
      },
    }),
  }

  process.env.FEEDGEN_ADMIN_API_KEY = 'coherence-test-key'
  await withFeedCatalogServer(mockDb, async (server) => {
    // 1. Incoherent single update (newsflow-nl-1 to v3 while newsflow-nl-2 remains v2)
    const singleUpdateRes = await requestJson(
      server,
      '/api/admin/feed_catalog',
      'POST',
      { 'api-key': 'coherence-test-key' },
      {
        op: 'update',
        rkey: 'newsflow-nl-1',
        content_time_contract_version: 'newsflows-content-time/v3',
        if_current: { content_time_contract_version: 'newsflows-content-time/v2' },
      },
    )
    assertEqual(singleUpdateRes.status, 409, 'incoherent single update must be 409')
    assertEqual(feeds['newsflow-nl-1'].content_time_contract_version, 'newsflows-content-time/v2', 'row must be rolled back on coherence failure')
    assertEqual(history.length, 0, 'no history must be committed on coherence rollback')

    // 2. Coherent bulk update (updating both feeds to v3 atomically)
    const bulkRes = await requestJson(
      server,
      '/api/admin/feed_catalog/bulk',
      'POST',
      { 'api-key': 'coherence-test-key' },
      {
        updates: [
          {
            op: 'update',
            rkey: 'newsflow-nl-1',
            content_time_contract_version: 'newsflows-content-time/v3',
            if_current: { content_time_contract_version: 'newsflows-content-time/v2' },
          },
          {
            op: 'update',
            rkey: 'newsflow-nl-2',
            content_time_contract_version: 'newsflows-content-time/v3',
            if_current: { content_time_contract_version: 'newsflows-content-time/v2' },
          },
        ],
      },
    )
    assertEqual(bulkRes.status, 200, 'coherent bulk update must succeed')
    assertEqual(feeds['newsflow-nl-1'].content_time_contract_version, 'newsflows-content-time/v3', 'feed1 updated to v3')
    assertEqual(feeds['newsflow-nl-2'].content_time_contract_version, 'newsflows-content-time/v3', 'feed2 updated to v3')
    assertEqual(history.length, 2, '2 history rows committed on successful bulk update')
  })
}

const tests = [
  testBulkRequiresCasAndUniqueRows,
  testRejectedCoherenceRollsBackRowAndHistory,
  testAdminUpdateWritesOneHistoryRowPerRevision,
  testHistoryReadEndpointAuthPaginationAndNewestFirst,
  testUpdatePathLocksRowGetPathDoesNot,
  testListPayload,
  testSubscribableListPayload,
  testSubscribableFilterParsing,
  testShowPayload,
  testMissingFeedPayload,
  testInvalidAccessPolicy,
  testInvalidUpdateOp,
  testValidIfCurrent,
  testValidPolicyFieldUpdate,
  testContentTimeWithoutExpiryIsPermanent,
  testContentTimeStillRequiresFloorAndContractVersion,
  testMalformedExpiryStillRejected,
  testContentTimeAcceptsV2AndV3Only,
  testRankerPriorityRequiresRankerPolicy,
  testNonRankerPolicyRequiresNullRankerPolicy,
  testRankerScoreSourceUpdate,
  testRankerScoreSourceCas,
  testRankerScoreSourceRejectsNonString,
  testInvalidIfCurrentField,
  testNoOpDryRun,
  testRealDiffDryRun,
  testPolicyDiffDryRun,
  testBlockedDryRun,
  testApplyResultPayload,
  testApplyConflictPayload,
  testApplyBlockedPayload,
]

;(async () => {
  for (const test of tests) {
    await test()
    console.log(`✓ ${test.name}`)
  }
  console.log(`feed catalog admin helper tests passed (${tests.length})`)
})().catch((err) => {
  console.error(err)
  process.exit(1)
})
