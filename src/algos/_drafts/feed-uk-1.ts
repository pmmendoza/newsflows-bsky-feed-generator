import { makeHandler } from './make-handler'

// max 15 chars
export const shortname = 'newsflow-uk-1'

// Sprint 11 / Task 5 — collapsed to a shim. Behaviour identical to
// the previous per-feed handler; SQL still produced from the policies
// in src/algos/policies/.
export const handler = makeHandler({
  shortname,
  policy: 'chronological',
  publisherEnv: 'NEWSBOT_UK_DID',
})
