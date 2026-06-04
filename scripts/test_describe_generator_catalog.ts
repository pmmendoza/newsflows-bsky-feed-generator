/**
 * Regression tests for catalog-driven describeFeedGenerator row selection.
 *
 * Run: `npx ts-node scripts/test_describe_generator_catalog.ts`
 */

import assert from 'assert'
import {
  describeRkeysFromCatalogRows,
  shouldUseCatalogForDescribe,
} from '../src/methods/describe-generator'

function testUsesCatalogWhenStaticRegistryIsEmpty() {
  assert.equal(
    shouldUseCatalogForDescribe([], {}),
    true,
    'describe must read catalog when the static registry is retired',
  )
}

function testEnvFlagStillEnablesCatalogWhenStaticRegistryExists() {
  assert.equal(
    shouldUseCatalogForDescribe(['newsflow-nl-1'], {
      FEEDGEN_DESCRIBE_FROM_CATALOG: 'true',
    }),
    true,
    'explicit env flag should keep catalog mode enabled',
  )
}

function testStaticFallbackCanStillBeUsedWhenStaticRegistryExists() {
  assert.equal(
    shouldUseCatalogForDescribe(['newsflow-nl-1'], {
      FEEDGEN_DESCRIBE_FROM_CATALOG: 'false',
    }),
    false,
    'static fallback is only valid while a non-empty static registry exists',
  )
}

function testCatalogRowsUseDynamicPolicySupportInsteadOfStaticRegistry() {
  const result = describeRkeysFromCatalogRows([
    { rkey: 'newsflow-nl-1', enabled: true, algo_policy_id: 'chronological' },
    { rkey: 'newsflow-be-2', enabled: true, algo_policy_id: 'ranker-priority' },
    { rkey: 'newsflow-fr-3', enabled: true, algo_policy_id: 'engagement-sorted' },
    { rkey: 'newsflow-ir-4', enabled: true, algo_policy_id: 'hybrid' },
    { rkey: 'newsflow-disabled', enabled: false, algo_policy_id: 'chronological' },
    { rkey: '', enabled: true, algo_policy_id: 'chronological' },
  ])

  assert.deepEqual(
    result.rkeys,
    ['newsflow-nl-1', 'newsflow-be-2', 'newsflow-fr-3'],
    'describe should include enabled rows with dynamically supported policies',
  )
  assert.deepEqual(
    result.unsupportedPolicyRkeys,
    ['newsflow-ir-4'],
    'unsupported policies should be omitted and reported',
  )
}

function main() {
  testUsesCatalogWhenStaticRegistryIsEmpty()
  testEnvFlagStillEnablesCatalogWhenStaticRegistryExists()
  testStaticFallbackCanStillBeUsedWhenStaticRegistryExists()
  testCatalogRowsUseDynamicPolicySupportInsteadOfStaticRegistry()
  console.log('describe generator catalog tests passed')
}

main()
