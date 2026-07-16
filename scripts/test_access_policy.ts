import {
  evaluateAccessPolicy,
  invalidatePolicyCache,
  type FeedCatalogPolicyRow,
} from '../src/util/access-policy'

type State = {
  catalog: FeedCatalogPolicyRow | null
  scope?: 'omni' | 'assigned' | 'none'
  assignment?: boolean
  study?: boolean
  fail?: 'catalog' | 'subscriber' | 'assignment' | 'study'
}

function fakeDb(state: State): any {
  return {
    selectFrom(table: string) {
      const target = table === 'feedgen_ops.feed_catalog'
        ? 'catalog'
        : table === 'subscriber'
        ? 'subscriber'
        : table === 'feedgen_ops.subscriber_feed_assignment'
        ? 'assignment'
        : 'study'
      return {
        select() { return this },
        where() { return this },
        async executeTakeFirst() {
          if (state.fail === target) throw new Error(`simulated ${target} failure`)
          if (target === 'catalog') return state.catalog ?? undefined
          if (target === 'subscriber') return state.scope ? { access_scope: state.scope } : undefined
          if (target === 'assignment') return state.assignment ? { did: 'did:plc:user' } : undefined
          return state.study ? { did: 'did:plc:user' } : undefined
        },
        async execute() {
          if (state.fail === 'subscriber' || state.fail === 'assignment' || state.fail === 'study') {
            throw new Error(`simulated ${state.fail} failure`)
          }
          return state.scope ? [{
            did: 'did:plc:user',
            access_scope: state.scope,
            has_active_assignment: Boolean(state.assignment),
            active_study: Boolean(state.study),
          }] : []
        },
      }
    },
  }
}

const subscriberDefault: FeedCatalogPolicyRow = {
  feed_id: 'feed-be-1',
  access_policy_id: 'subscriber-default',
  study_id: 'study-be',
  enabled: true,
  retired_at: null,
}
const studyOnly: FeedCatalogPolicyRow = { ...subscriberDefault, access_policy_id: 'study-only' }

let passed = 0
async function check(label: string, state: State, allowed: boolean, reason: string) {
  invalidatePolicyCache()
  const verdict = await evaluateAccessPolicy(fakeDb(state), 'newsflow-be-1', 'did:plc:user')
  if (verdict.allowed !== allowed || !verdict.reason.includes(reason)) {
    throw new Error(`${label}: got ${JSON.stringify(verdict)}`)
  }
  passed++
  console.log(`✓ ${label}`)
}

async function main() {
  await check('omni permits subscriber-default', { catalog: subscriberDefault, scope: 'omni' }, true, 'subscriber-default')
  await check('exact assignment permits subscriber-default', { catalog: subscriberDefault, scope: 'assigned', assignment: true }, true, 'subscriber-default')
  await check('sibling feed is denied', { catalog: subscriberDefault, scope: 'assigned', assignment: false }, false, 'not-assigned')
  await check('none scope is denied', { catalog: subscriberDefault, scope: 'none' }, false, 'not-assigned')
  await check('study-only requires assignment and lifecycle', { catalog: studyOnly, scope: 'assigned', assignment: true, study: true }, true, 'study-only:study-be')
  await check('study-only denies missing lifecycle', { catalog: studyOnly, scope: 'assigned', assignment: true }, false, 'not-active-or-assigned')
  await check('study-only denies missing assignment', { catalog: studyOnly, scope: 'assigned', study: true }, false, 'not-active-or-assigned')
  await check('study-only rejects missing study id', { catalog: { ...studyOnly, study_id: null }, scope: 'omni', study: true }, false, 'misconfigured')
  await check('disabled policy denies omni', { catalog: { ...subscriberDefault, access_policy_id: 'disabled' }, scope: 'omni' }, false, 'disabled')
  await check('disabled feed is denied', { catalog: { ...subscriberDefault, enabled: false }, scope: 'omni' }, false, 'feed-disabled')
  await check('retired feed is denied', { catalog: { ...subscriberDefault, retired_at: new Date() }, scope: 'omni' }, false, 'feed-disabled')
  await check('missing catalog fails closed', { catalog: null, scope: 'omni' }, false, 'no-catalog-row')
  await check('catalog read failure fails closed', { catalog: null, scope: 'omni', fail: 'catalog' }, false, 'no-catalog-row')
  await check('assignment read failure fails closed', { catalog: subscriberDefault, scope: 'assigned', fail: 'assignment' }, false, 'access-state-read-failed')
  await check('unknown policy fails closed', { catalog: { ...subscriberDefault, access_policy_id: 'open' }, scope: 'omni' }, false, 'unknown-policy')
  console.log(`Summary: ${passed} passed`)
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
