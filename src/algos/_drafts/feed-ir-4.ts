import { makeHandler } from './make-handler'

// max 15 chars
export const shortname = 'newsflow-ir-4'

// Sprint 5 fourth-feed (Ireland actor-diversity proof-of-concept).
// Will be retired after validation per operator decision 2026-05-04.// Sprint 11 / Task 5 — collapsed to a shim. Behaviour identical to
// the previous per-feed handler; SQL still produced from the policies
// in src/algos/policies/.
export const handler = makeHandler({
  shortname,
  policy: 'ranker-priority',
  publisherEnv: 'NEWSBOT_IR_DID',
})
