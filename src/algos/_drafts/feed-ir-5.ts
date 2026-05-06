import { makeHandler } from './make-handler'

// max 15 chars
export const shortname = 'newsflow-ir-5'

// IR-5 — reverse-chronological test feed (created 2026-05-05 to
// validate the end-to-end "add a new feed" workflow). Disposable;
// can be retired by setting feed_catalog.enabled=false and moving
// this shim to _drafts/.
export const handler = makeHandler({
  shortname,
  policy: 'chronological',
  publisherEnv: 'NEWSBOT_IR_DID',
})
