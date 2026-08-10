/**
 * Unit tests for the per-feed publisher serving-window override
 * (resolveServingTimeHours / servingTimeHourOverrides in algos/feed-builder).
 *
 * Contract under test:
 *   - FEEDGEN_SERVING_TIME_HOURS_<RKEY> overrides the publisher serving window
 *     for that ONE feed (rkey -> env suffix via rkeyToEnvSuffix);
 *   - unset / empty / non-numeric / non-positive override => fall back to the
 *     global resolveEngagementTimeHours() (byte-for-byte prior behavior);
 *   - an override for one feed never leaks to another feed;
 *   - servingTimeHourOverrides() reports exactly the valid overrides, so the
 *     config-activation manifest records what serving uses.
 *
 * Run: `npx ts-node scripts/test_serving_window.ts`
 */

import assert from 'assert'
import {
  resolveServingTimeHours,
  servingTimeHourOverrides,
} from '../src/algos/feed-builder'

async function withEnv<T>(env: Record<string, string | undefined>, fn: () => T): Promise<T> {
  const saved: Record<string, string | undefined> = {}
  for (const [key, value] of Object.entries(env)) {
    saved[key] = process.env[key]
    if (value === undefined) delete process.env[key]
    else process.env[key] = value
  }
  try {
    return fn()
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  }
}

async function main() {
  // 1) valid override applies to the mapped feed; a different feed falls back.
  await withEnv(
    {
      ENGAGEMENT_TIME_HOURS: '72',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K: '168',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_M: undefined,
    },
    () => {
      assert.equal(resolveServingTimeHours('newsflow-be-k'), 168, 'be-k uses its override')
      assert.equal(resolveServingTimeHours('newsflow-be-m'), 72, 'be-m (unset) falls back to global 72')
      assert.equal(resolveServingTimeHours('newsflow-nl-2'), 72, 'unrelated feed falls back to global 72')
    },
  )

  // 2) fallbacks: unset, empty, non-numeric, zero, negative -> global default.
  await withEnv(
    {
      ENGAGEMENT_TIME_HOURS: '72',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K: '',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_M: 'abc',
    },
    () => {
      assert.equal(resolveServingTimeHours('newsflow-be-k'), 72, 'empty override falls back')
      assert.equal(resolveServingTimeHours('newsflow-be-m'), 72, 'non-numeric override falls back')
    },
  )
  await withEnv(
    {
      ENGAGEMENT_TIME_HOURS: '72',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K: '0',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_M: '-5',
    },
    () => {
      assert.equal(resolveServingTimeHours('newsflow-be-k'), 72, 'zero override falls back')
      assert.equal(resolveServingTimeHours('newsflow-be-m'), 72, 'negative override falls back')
    },
  )

  // 3) override tracks the GLOBAL default when the global itself is customised.
  await withEnv(
    { ENGAGEMENT_TIME_HOURS: '48', FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K: undefined },
    () => {
      assert.equal(resolveServingTimeHours('newsflow-be-k'), 48, 'fallback tracks the global engagement window')
    },
  )

  // 4) servingTimeHourOverrides() reports valid entries and drops junk.
  await withEnv(
    {
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K: '168',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_M: '156',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_JUNK: 'abc',
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_ZERO: '0',
    },
    () => {
      const o = servingTimeHourOverrides()
      assert.equal(o['NEWSFLOW_BE_K'], 168, 'reports be-k override')
      assert.equal(o['NEWSFLOW_BE_M'], 156, 'reports be-m override')
      assert.ok(!('NEWSFLOW_BE_JUNK' in o), 'drops non-numeric override')
      assert.ok(!('NEWSFLOW_BE_ZERO' in o), 'drops non-positive override')
    },
  )

  // 5) no overrides set -> the two test feeds are absent from the map.
  await withEnv(
    {
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K: undefined,
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_M: undefined,
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_JUNK: undefined,
      FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_ZERO: undefined,
    },
    () => {
      const o = servingTimeHourOverrides()
      assert.ok(!('NEWSFLOW_BE_K' in o) && !('NEWSFLOW_BE_M' in o), 'no overrides -> feeds absent')
    },
  )

  console.log('serving-window override tests passed')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
