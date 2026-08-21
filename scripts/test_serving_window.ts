import assert from 'assert'
import { resolvePublisherServingWindow } from '../src/algos/publisher-serving-window'

process.env.ENGAGEMENT_TIME_HOURS = '48'
process.env.FEEDGEN_SERVING_TIME_HOURS_NEWSFLOW_BE_K = '168'

const window = resolvePublisherServingWindow(10, 'study_default')
assert.equal(window.effectiveHours, 240, 'catalog days are the only publisher-window authority')
assert.equal(window.source, 'study_default')
assert.equal(window.compatibilityFallbackActive, false)
assert.equal(window.compatibilityEnvKey, null)

for (const [days, source] of [
  [null, null],
  [0, 'study_default'],
  [366, 'study_default'],
  [10, null],
  [10, 'environment'],
] as const) {
  assert.throws(
    () => resolvePublisherServingWindow(days, source as any),
    /requires valid catalog days and provenance/,
  )
}

console.log('catalog-only serving-window tests passed')
