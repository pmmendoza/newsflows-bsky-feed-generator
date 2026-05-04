/**
 * Sprint 6 Lane D / TASK-039.03 unit test for the access-policy
 * dispatcher (`src/util/access-policy.ts`).
 *
 * Each case constructs a minimal fake `Database` whose query-builder
 * returns a controlled result for the three tables the dispatcher
 * reads: `feedgen_ops.feed_catalog`, `subscriber`, and
 * `feedgen_ops.study_registry`. Then it asserts the verdict.
 *
 * Why a fake DB and not Kysely DummyDriver: the dispatcher uses
 * `executeTakeFirst()` which requires a real-shaped result, not an
 * SQL string compile. Mocking the chain with a fake is more direct.
 *
 * Run: `npx ts-node scripts/test_access_policy.ts`
 * Exits non-zero on failure.
 */

import {
  evaluateAccessPolicy,
  invalidatePolicyCache,
  type FeedCatalogPolicyRow,
} from '../src/util/access-policy'

let failed = 0
let passed = 0

function assert(cond: boolean, label: string, detail?: string) {
  if (cond) {
    passed++
    console.log(`  ✓ ${label}`)
  } else {
    failed++
    console.log(`  ✗ ${label}${detail ? ` — ${detail}` : ''}`)
  }
}

// ---------------------------------------------------------------------
// Fake Database. Each test sets `feedCatalogRow`, `subscriberHit`,
// `studyRegistryHit` then invokes evaluateAccessPolicy.
// ---------------------------------------------------------------------

type FakeState = {
  feedCatalogRow: FeedCatalogPolicyRow | null
  subscriberHit: boolean
  studyRegistryHit: boolean
  catalogReadShouldFail: boolean
}

function makeFakeDb(state: FakeState): any {
  return {
    selectFrom(table: string) {
      const target =
        table === 'feedgen_ops.feed_catalog'
          ? 'catalog'
          : table === 'subscriber'
          ? 'subscriber'
          : table === 'feedgen_ops.study_registry'
          ? 'study_registry'
          : 'unknown'
      return {
        select() {
          return this
        },
        where() {
          return this
        },
        async executeTakeFirst() {
          if (target === 'catalog') {
            if (state.catalogReadShouldFail) throw new Error('simulated catalog failure')
            if (state.feedCatalogRow === null) return undefined
            return state.feedCatalogRow
          }
          if (target === 'subscriber') {
            return state.subscriberHit ? { did: 'x' } : undefined
          }
          if (target === 'study_registry') {
            return state.studyRegistryHit ? { did: 'x' } : undefined
          }
          return undefined
        },
      }
    },
  }
}

async function runCase(label: string, state: FakeState, did: string, rkey: string) {
  invalidatePolicyCache() // each case starts cold
  const db = makeFakeDb(state)
  const verdict = await evaluateAccessPolicy(db as any, rkey, did)
  console.log(`Case: ${label}`)
  console.log(`  → allowed=${verdict.allowed} reason=${verdict.reason}`)
  return verdict
}

// ---------------------------------------------------------------------
// Cases.
// ---------------------------------------------------------------------

;(async () => {
  // 1. subscriber-default + DID is in subscriber → allowed
  {
    const v = await runCase(
      'subscriber-default + subscriber=hit → allowed',
      {
        feedCatalogRow: {
          access_policy_id: 'subscriber-default',
          study_id: null,
          enabled: true,
        },
        subscriberHit: true,
        studyRegistryHit: false,
        catalogReadShouldFail: false,
      },
      'did:plc:user',
      'newsflow-nl-2',
    )
    assert(v.allowed === true, '  allowed')
    assert(v.reason === 'subscriber-default', '  reason matches')
  }

  // 2. subscriber-default + DID NOT in subscriber → denied
  {
    const v = await runCase(
      'subscriber-default + subscriber=miss → denied',
      {
        feedCatalogRow: {
          access_policy_id: 'subscriber-default',
          study_id: null,
          enabled: true,
        },
        subscriberHit: false,
        studyRegistryHit: false,
        catalogReadShouldFail: false,
      },
      'did:plc:rando',
      'newsflow-nl-2',
    )
    assert(v.allowed === false, '  denied')
    assert(v.reason.startsWith('subscriber-default:not-subscriber'), '  reason matches')
  }

  // 3. study-only + active study_registry hit → allowed
  {
    const v = await runCase(
      'study-only + study_registry=hit → allowed',
      {
        feedCatalogRow: {
          access_policy_id: 'study-only',
          study_id: 'bsky-elif-2026-be',
          enabled: true,
        },
        subscriberHit: true,
        studyRegistryHit: true,
        catalogReadShouldFail: false,
      },
      'did:plc:participant',
      'newsflow-be-2',
    )
    assert(v.allowed === true, '  allowed')
    assert(v.reason === 'study-only:bsky-elif-2026-be', '  reason includes study_id')
  }

  // 4. study-only + study_registry MISS → denied
  {
    const v = await runCase(
      'study-only + study_registry=miss → denied',
      {
        feedCatalogRow: {
          access_policy_id: 'study-only',
          study_id: 'bsky-elif-2026-be',
          enabled: true,
        },
        subscriberHit: true, // even though they're a subscriber
        studyRegistryHit: false,
        catalogReadShouldFail: false,
      },
      'did:plc:not-in-be',
      'newsflow-be-2',
    )
    assert(v.allowed === false, '  denied (subscriber alone insufficient for study-only)')
    assert(v.reason.endsWith(':not-active'), '  reason indicates study inactive')
  }

  // 5. study-only with NULL study_id → fail-closed
  {
    const v = await runCase(
      'study-only + study_id=NULL → fail-closed',
      {
        feedCatalogRow: {
          access_policy_id: 'study-only',
          study_id: null,
          enabled: true,
        },
        subscriberHit: true,
        studyRegistryHit: true,
        catalogReadShouldFail: false,
      },
      'did:plc:user',
      'newsflow-misconfigured',
    )
    assert(v.allowed === false, '  denied')
    assert(v.reason === 'study-only:misconfigured', '  reason flags misconfiguration')
  }

  // 6. disabled → empty feed
  {
    const v = await runCase(
      'disabled → empty',
      {
        feedCatalogRow: {
          access_policy_id: 'disabled',
          study_id: null,
          enabled: true,
        },
        subscriberHit: true,
        studyRegistryHit: true,
        catalogReadShouldFail: false,
      },
      'did:plc:user',
      'newsflow-retired',
    )
    assert(v.allowed === false, '  denied')
    assert(v.reason === 'disabled', '  reason')
  }

  // 7. enabled=false on the catalog row → empty
  {
    const v = await runCase(
      'enabled=false → empty',
      {
        feedCatalogRow: {
          access_policy_id: 'subscriber-default',
          study_id: null,
          enabled: false,
        },
        subscriberHit: true,
        studyRegistryHit: false,
        catalogReadShouldFail: false,
      },
      'did:plc:user',
      'newsflow-soft-disabled',
    )
    assert(v.allowed === false, '  denied')
    assert(v.reason === 'feed-disabled', '  reason')
  }

  // 8. Unknown access_policy_id → fail-closed
  {
    const v = await runCase(
      'unknown access_policy_id → fail-closed',
      {
        feedCatalogRow: {
          access_policy_id: 'open', // hypothetical relaxed CHECK
          study_id: null,
          enabled: true,
        },
        subscriberHit: true,
        studyRegistryHit: false,
        catalogReadShouldFail: false,
      },
      'did:plc:user',
      'newsflow-future',
    )
    assert(v.allowed === false, '  denied (unknown policy)')
    assert(v.reason === 'unknown-policy', '  reason')
  }

  // 9. No catalog row at all → fallback-to-legacy (allowed=true)
  {
    const v = await runCase(
      'no-catalog-row → legacy fallback',
      {
        feedCatalogRow: null,
        subscriberHit: false,
        studyRegistryHit: false,
        catalogReadShouldFail: false,
      },
      'did:plc:user',
      'newsflow-uncatalogued',
    )
    assert(v.allowed === true, '  allowed (back-compat)')
    assert(v.reason.startsWith('no-catalog-row'), '  reason flags fallback')
  }

  // 10. Catalog read FAILS → fallback-to-legacy
  {
    const v = await runCase(
      'catalog-read fails → legacy fallback',
      {
        feedCatalogRow: null,
        subscriberHit: false,
        studyRegistryHit: false,
        catalogReadShouldFail: true,
      },
      'did:plc:user',
      'newsflow-nl-2',
    )
    assert(v.allowed === true, '  allowed (read-error fallback)')
    assert(v.reason.startsWith('no-catalog-row'), '  reason flags fallback')
  }

  console.log()
  console.log(`Summary: ${passed} passed, ${failed} failed`)
  if (failed > 0) process.exit(1)
})()
