// Pure-logic unit checks for the atomic desired-state parser (no DB).
import { parseDesiredState, SubscriptionError } from '../src/util/exact-subscription'

function fails(s: any, code: string) {
  try { parseDesiredState(s); throw new Error('expected failure for ' + JSON.stringify(s)) }
  catch (e) {
    if (!(e instanceof SubscriptionError) || e.code !== code) {
      throw new Error(`wrong error for ${JSON.stringify(s)}: ${e}`)
    }
  }
}

const omni = parseDesiredState({ state: { scope: 'omni' } })
if (omni.scope !== 'omni' || omni.feeds.length !== 0) throw new Error('omni')
const none = parseDesiredState({ state: { scope: 'none' } })
if (none.scope !== 'none' || none.feeds.length !== 0) throw new Error('none')
const asg = parseDesiredState({ state: { scope: 'assigned', feeds: ['b', 'a', 'a'] } })
if (asg.scope !== 'assigned' || asg.feeds.join(',') !== 'a,b') throw new Error('dedupe')

fails({}, 'invalid_state')
fails({ state: { scope: 'bogus' } }, 'invalid_state')
fails({ state: { scope: 'assigned', feeds: [] } }, 'invalid_state')
fails({ state: { scope: 'omni', feeds: ['x'] } }, 'invalid_state')
fails({ state: { scope: 'none', feeds: ['x'] } }, 'invalid_state')
// Strict validation (red-team MED 5): non-array feeds, blank/non-string
// entries, and conflicting legacy verbs must all be rejected, not coerced.
fails({ state: { scope: 'omni', feeds: 'newsflow-nl-1' } }, 'invalid_state')
fails({ state: { scope: 'assigned', feeds: ['a', ''] } }, 'invalid_state')
fails({ state: { scope: 'assigned', feeds: ['a', 3] } }, 'invalid_state')
fails({ state: { scope: 'omni' }, mode: 'add' }, 'invalid_state')
fails({ state: { scope: 'omni' }, feed: 'x' }, 'invalid_state')

console.log('test_subscription_state_unit OK')
