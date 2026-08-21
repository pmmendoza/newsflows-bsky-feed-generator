import fs from 'fs'
import path from 'path'
import { assessEngagementScienceEligibility } from '../src/util/engagement-time-contract'
import { deriveEngagementRefreshPlan } from '../src/util/engagement-updater'

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

const reference = Date.parse('2026-08-14T12:00:00Z')
const plan = deriveEngagementRefreshPlan([
  { rkey: 'receipt', publisher_did: 'did:r', publisher_post_max_age_days: 3, publisher_time_clock: 'receipt_time' },
  { rkey: 'content', publisher_did: 'did:c', publisher_post_max_age_days: 10, publisher_time_clock: 'content_time_v1' },
], reference)
check(plan.receiptDays === 3, 'receipt scan must cover receipt-clock consumers only')
check(plan.receiptCutoff === '2026-08-11T12:00:00.000Z', 'receipt cutoff must use receipt-clock catalog horizons')
check(plan.contentDays === 10, 'content scan must cover all content-clock consumers')
check(plan.contentCutoff === '2026-08-04T12:00:00.000Z', 'content cutoff must use content time')
check(plan.receiptPublisherRows.length === 1, 'receipt publishers must not leak into content scan')
check(plan.contentPublisherRows.length === 1, 'content publishers must not leak into receipt publisher scan')

let failedClosed = false
try {
  deriveEngagementRefreshPlan([
    { rkey: 'broken', publisher_did: 'did:x', publisher_post_max_age_days: null, publisher_time_clock: 'content_time_v1' },
  ], reference)
} catch { failedClosed = true }
check(failedClosed, 'an unresolved catalog consumer must fail closed')

const activeContract = {
  expectedContractVersion: 'newsflows-content-time/v2',
  transitionExpiresAt: '2026-08-15T00:00:00Z',
  referenceMs: reference,
  allowEmptyPopulation: false,
}
const eligible = assessEngagementScienceEligibility({
  ...activeContract,
  contentTime: true,
  explicitBounds: true,
  contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8,
  numerator: 80,
  denominator: 100,
})
check(eligible.scienceEligible && eligible.observedValidShare === 0.8, 'declared threshold must be inclusive')
check(!assessEngagementScienceEligibility({
  ...activeContract,
  contentTime: true, explicitBounds: true, contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8, numerator: 79, denominator: 100,
}).scienceEligible, 'below-threshold validity must fail closed')
check(!assessEngagementScienceEligibility({
  ...activeContract,
  contentTime: false, explicitBounds: true, contractVersion: null,
  minimumValidShare: 0.8, numerator: 100, denominator: 100,
}).scienceEligible, 'receipt-time exports must be science-ineligible')
check(!assessEngagementScienceEligibility({
  ...activeContract,
  contentTime: true, explicitBounds: false, contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8, numerator: 100, denominator: 100,
}).scienceEligible, 'implicit bounds must be science-ineligible')
const empty = assessEngagementScienceEligibility({
  ...activeContract,
  allowEmptyPopulation: true,
  contentTime: true, explicitBounds: true, contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8, numerator: 0, denominator: 0,
})
check(empty.scienceEligible && empty.emptyPopulation && empty.observedValidShare === null, 'an exact empty cohort must be explicitly neutral')
check(!assessEngagementScienceEligibility({
  ...activeContract, contentTime: true, explicitBounds: true, contractVersion: 'unsupported/v2',
  minimumValidShare: 0.8, numerator: 100, denominator: 100,
}).scienceEligible, 'unsupported contract versions must fail closed')
check(!assessEngagementScienceEligibility({
  ...activeContract, transitionExpiresAt: '2026-08-14T12:00:00Z',
  contentTime: true, explicitBounds: true, contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8, numerator: 100, denominator: 100,
}).scienceEligible, 'expired transitions must fail closed')

// FT-FU-6: once the transition is adopted permanently the catalog carries NO expiry.
// That must stay science-eligible -- otherwise the ranker's compliance-export validator
// fails closed and the ranking cycle dies (observed in production 2026-08-20T14:12Z).
check(assessEngagementScienceEligibility({
  ...activeContract, transitionExpiresAt: null,
  contentTime: true, explicitBounds: true, contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8, numerator: 100, denominator: 100,
}).scienceEligible, 'an absent transition expiry means permanent and must stay science-eligible')
check(!assessEngagementScienceEligibility({
  ...activeContract, transitionExpiresAt: 'not-a-timestamp',
  contentTime: true, explicitBounds: true, contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8, numerator: 100, denominator: 100,
}).scienceEligible, 'a corrupt expiry must still fail closed, not read as permanent')

// v3 contract eligibility
const activeContractV3 = {
  expectedContractVersion: 'newsflows-content-time/v3',
  transitionExpiresAt: null,
  referenceMs: reference,
  allowEmptyPopulation: false,
}
const eligibleV3 = assessEngagementScienceEligibility({
  ...activeContractV3,
  contentTime: true,
  explicitBounds: true,
  contractVersion: 'newsflows-content-time/v3',
  minimumValidShare: 0.8,
  numerator: 80,
  denominator: 100,
})
check(eligibleV3.scienceEligible && eligibleV3.observedValidShare === 0.8, 'v3 active contract with matching v3 version is science eligible')

check(!assessEngagementScienceEligibility({
  ...activeContractV3,
  contentTime: true,
  explicitBounds: true,
  contractVersion: 'newsflows-content-time/v2',
  minimumValidShare: 0.8,
  numerator: 100,
  denominator: 100,
}).scienceEligible, 'v2 version when active catalog is v3 must fail closed')

const monitorSource = fs.readFileSync(path.resolve(__dirname, '../src/methods/monitor.ts'), 'utf8')
check(monitorSource.includes(".where('feed_id', '=', feedId)"), 'science export must select its clock from the feed catalog')
check(monitorSource.includes('? [feedClock.publisher_did]'), 'feed-scoped export must use the selected publisher population')
check(monitorSource.includes("content_time_status = 'source_valid'"), 'content-time science export must exclude invalid events')
check(monitorSource.includes('denominator_clock'), 'science export must declare its bounded validity denominator')

console.log('engagement time contract tests passed')
