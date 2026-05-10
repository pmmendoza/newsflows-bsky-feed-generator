import { selectDescribeRkeysFromCatalogRows } from '../src/methods/describe-generator'

function assert(condition: unknown, message: string): void {
  if (!condition) {
    throw new Error(message)
  }
}

const selection = selectDescribeRkeysFromCatalogRows([
  { rkey: 'newsflow-nl-1', algo_policy_id: 'chronological', enabled: true },
  { rkey: 'newsflow-fr-2', algo_policy_id: 'ranker-priority', enabled: true },
  { rkey: 'newsflow-cz-3', algo_policy_id: 'engagement-sorted', enabled: true },
  { rkey: 'newsflow-ir-4', algo_policy_id: 'chronological', enabled: false },
  { rkey: 'newsflow-hybrid-1', algo_policy_id: 'hybrid', enabled: true },
  { rkey: 'newsflow-missing-policy', algo_policy_id: null, enabled: true },
])

assert(
  selection.rkeys.join(',') === 'newsflow-nl-1,newsflow-fr-2,newsflow-cz-3',
  `expected enabled supported catalog rkeys, got ${selection.rkeys.join(',')}`,
)
assert(
  !selection.rkeys.includes('newsflow-ir-4'),
  'disabled catalog rows must not be described',
)
assert(
  selection.unsupportedRkeys.join(',') === 'newsflow-hybrid-1,newsflow-missing-policy',
  `expected unsupported policy rows to fail closed, got ${selection.unsupportedRkeys.join(',')}`,
)
assert(selection.rowCount === 6, `expected rowCount=6, got ${selection.rowCount}`)

console.log('describe-generator catalog selection tests passed')
