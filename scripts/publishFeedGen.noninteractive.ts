/**
 * Non-interactive variant of publishFeedGen.ts.
 *
 * Reads all required values from env vars instead of inquirer prompts so
 * the publish step can run from a deploy script, CI, or SSH session
 * without a TTY.
 *
 * Why: the interactive script blocks SSH-based workflows and is the
 * single most-skipped step when adding a new feed (Sprint 5 IR-4 went
 * 36 h invisible after this step was missed). Documenting + automating
 * it reduces the gap.
 *
 * Required env vars:
 *   PUBLISH_FEED_HANDLE      e.g. news-flows-ir.bsky.social
 *   PUBLISH_FEED_PASSWORD    the bot's app password (NOT main password;
 *                            generate at https://bsky.app/settings/app-passwords)
 *   PUBLISH_FEED_RKEY        e.g. newsflow-ir-4 (must match feed_catalog.rkey
 *                            and the entry in src/algos/index.ts)
 *   PUBLISH_FEED_DISPLAY_NAME    e.g. newsinfusion-4
 *
 * Optional env vars:
 *   PUBLISH_FEED_DESCRIPTION     short user-facing string
 *   PUBLISH_FEED_AVATAR          local path to a PNG/JPG (skip = no avatar)
 *   PUBLISH_FEED_SERVICE         default https://bsky.social
 *   PUBLISH_FEED_VIDEO_ONLY      "true" sets content mode to video
 *
 * Plus the existing FEEDGEN_SERVICE_DID or FEEDGEN_HOSTNAME (read by the
 * existing publishFeedGen.ts as well).
 *
 * Usage:
 *   PUBLISH_FEED_HANDLE=news-flows-ir.bsky.social \
 *   PUBLISH_FEED_PASSWORD='xxxx-xxxx-xxxx-xxxx' \
 *   PUBLISH_FEED_RKEY=newsflow-ir-4 \
 *   PUBLISH_FEED_DISPLAY_NAME=newsinfusion-4 \
 *   PUBLISH_FEED_DESCRIPTION='Irish news, actor-diversity reranker (PoC)' \
 *     npx ts-node scripts/publishFeedGen.noninteractive.ts
 *
 * After running, verify the record landed in the publisher's repo:
 *   curl -sS "https://public.api.bsky.app/xrpc/com.atproto.repo.listRecords?\
 *   repo=<publisher_did>&collection=app.bsky.feed.generator&limit=20"
 */

import dotenv from 'dotenv'
import { AtpAgent, BlobRef, AppBskyFeedDefs } from '@atproto/api'
import fs from 'fs/promises'
import { ids } from '../src/lexicon/lexicons'

function require_env(name: string): string {
  const v = process.env[name]
  if (!v) {
    throw new Error(`${name} env var is required`)
  }
  return v
}

const run = async () => {
  dotenv.config()

  if (!process.env.FEEDGEN_SERVICE_DID && !process.env.FEEDGEN_HOSTNAME) {
    throw new Error(
      'FEEDGEN_SERVICE_DID or FEEDGEN_HOSTNAME must be set in env (.env or shell)',
    )
  }

  const handle = require_env('PUBLISH_FEED_HANDLE')
  const password = require_env('PUBLISH_FEED_PASSWORD')
  const recordName = require_env('PUBLISH_FEED_RKEY')
  const displayName = require_env('PUBLISH_FEED_DISPLAY_NAME')
  const description = process.env.PUBLISH_FEED_DESCRIPTION ?? ''
  const avatar = process.env.PUBLISH_FEED_AVATAR ?? ''
  const service = process.env.PUBLISH_FEED_SERVICE ?? 'https://bsky.social'
  const videoOnly = (process.env.PUBLISH_FEED_VIDEO_ONLY ?? 'false').toLowerCase() === 'true'

  const feedGenDid =
    process.env.FEEDGEN_SERVICE_DID ?? `did:web:${process.env.FEEDGEN_HOSTNAME}`

  const agent = new AtpAgent({ service })
  await agent.login({ identifier: handle, password })

  let avatarRef: BlobRef | undefined
  if (avatar) {
    let encoding: string
    if (avatar.endsWith('png')) {
      encoding = 'image/png'
    } else if (avatar.endsWith('jpg') || avatar.endsWith('jpeg')) {
      encoding = 'image/jpeg'
    } else {
      throw new Error('expected png or jpeg avatar')
    }
    const img = await fs.readFile(avatar)
    const blobRes = await agent.api.com.atproto.repo.uploadBlob(img, {
      encoding,
    })
    avatarRef = blobRes.data.blob
  }

  await agent.api.com.atproto.repo.putRecord({
    repo: agent.session?.did ?? '',
    collection: ids.AppBskyFeedGenerator,
    rkey: recordName,
    record: {
      did: feedGenDid,
      displayName,
      description,
      avatar: avatarRef,
      createdAt: new Date().toISOString(),
      contentMode: videoOnly
        ? AppBskyFeedDefs.CONTENTMODEVIDEO
        : AppBskyFeedDefs.CONTENTMODEUNSPECIFIED,
    },
  })

  console.log(
    `published at://${agent.session?.did}/${ids.AppBskyFeedGenerator}/${recordName}`,
  )
  console.log('All done 🎉')
}

run().catch((err) => {
  console.error('publish failed:', err.message ?? err)
  process.exit(1)
})
