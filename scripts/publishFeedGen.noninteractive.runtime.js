// Minimal non-interactive publish — used once to land newsflow-ir-4.
// Compiled-out variant of scripts/publishFeedGen.noninteractive.ts.
// All inputs come from env vars; no prompts; no values printed.

const { AtpAgent, AppBskyFeedDefs } = require('@atproto/api')

const handle = process.env.PUBLISH_FEED_HANDLE
const password = process.env.PUBLISH_FEED_PASSWORD
const recordName = process.env.PUBLISH_FEED_RKEY
const displayName = process.env.PUBLISH_FEED_DISPLAY_NAME
const description = process.env.PUBLISH_FEED_DESCRIPTION || ''
const service = process.env.PUBLISH_FEED_SERVICE || 'https://bsky.social'

if (!handle || !password || !recordName || !displayName) {
  console.error(
    'Missing one of PUBLISH_FEED_HANDLE / PUBLISH_FEED_PASSWORD / PUBLISH_FEED_RKEY / PUBLISH_FEED_DISPLAY_NAME',
  )
  process.exit(2)
}

const feedGenDid =
  process.env.FEEDGEN_SERVICE_DID ||
  (process.env.FEEDGEN_HOSTNAME ? `did:web:${process.env.FEEDGEN_HOSTNAME}` : null)
if (!feedGenDid) {
  console.error('FEEDGEN_SERVICE_DID or FEEDGEN_HOSTNAME must be set')
  process.exit(2)
}

;(async () => {
  const agent = new AtpAgent({ service })
  await agent.login({ identifier: handle, password })

  await agent.api.com.atproto.repo.putRecord({
    repo: agent.session.did,
    collection: 'app.bsky.feed.generator',
    rkey: recordName,
    record: {
      did: feedGenDid,
      displayName,
      description,
      createdAt: new Date().toISOString(),
      contentMode: AppBskyFeedDefs.CONTENTMODEUNSPECIFIED,
    },
  })

  console.log(
    `published at://${agent.session.did}/app.bsky.feed.generator/${recordName}`,
  )
})().catch((err) => {
  console.error('publish failed:', err.message || err)
  process.exit(1)
})
