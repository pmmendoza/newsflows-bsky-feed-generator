import assert from 'assert'
import express from 'express'
import { Kysely, PostgresDialect } from 'kysely'

const bufferModule = require('buffer')
bufferModule.SlowBuffer ??= bufferModule.Buffer

async function main() {
  const queries: string[] = []
  const client = {
    query: async (text: string) => {
      queries.push(text)
      if (text.includes('from "feedgen_ops"."feed_catalog"')) {
        return { rows: [{ publisher_did: 'did:plc:publisher', enabled: true }] }
      }
      if (text.includes('WITH served AS MATERIALIZED')) {
        return {
          rows: [{
            type: 'original_post',
            event_uri: 'at://did:plc:subscriber/app.bsky.feed.post/post1',
            target_uri: 'at://did:plc:subscriber/app.bsky.feed.post/post1',
            author_did: 'did:plc:subscriber',
            created_at: '2026-08-21T00:00:00.000Z',
            indexed_at: '2026-08-21T00:00:00.000Z',
            publisher_target: false,
            served_to_subscriber: false,
          }],
        }
      }
      throw new Error(`unexpected query: ${text}`)
    },
    release() {},
  }
  const pool = { connect: async () => client, end: async () => {} }
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool: pool as any }) })
  const registerMonitorEndpoints = (await import('../src/methods/monitor')).default
  const app = express()
  process.env.FEEDGEN_MONITOR_API_KEY = 'contract-test-key'
  registerMonitorEndpoints({ xrpc: { router: app } } as any, { db } as any)
  const server = app.listen(0, '127.0.0.1')
  await new Promise<void>((resolve) => server.once('listening', resolve))
  const address = server.address()
  assert(address && typeof address === 'object', 'test server must listen')
  const base = `http://127.0.0.1:${address.port}/api/compliance/activity`

  try {
    let response = await fetch(`${base}?types=original_post`)
    assert.strictEqual(response.status, 401, 'activity endpoint must require an API key')

    response = await fetch(`${base}?scope=all_tracked&types=original_post&days=1`, {
      headers: { 'api-key': 'contract-test-key' },
    })
    assert.strictEqual(response.status, 400, 'original_post must require one subscriber DID')

    response = await fetch(
      `${base}?scope=publisher_posts&subscriber_did=did:plc:subscriber&types=original_post&include_retrievals=false&days=1`,
      { headers: { 'api-key': 'contract-test-key' } },
    )
    assert.strictEqual(response.status, 400, 'original_post must require scope=all_tracked')

    response = await fetch(
      `${base}?scope=all_tracked&subscriber_did=did:plc:subscriber&types=original_post&include_retrievals=false&limit=1&days=1`,
      { headers: { 'api-key': 'contract-test-key' } },
    )
    const payload: any = await response.json()
    assert.strictEqual(response.status, 200, 'original_post must be an accepted activity type')
    assert.deepStrictEqual(payload.types, ['original_post'])
    assert.strictEqual(payload.engagements[0]?.type, 'original_post')
    const activityQuery = queries.find((text) => text.includes("p.\"rootUri\" = ''"))
    assert(activityQuery, 'original posts must be selected as root posts')
    assert(activityQuery.includes('p.author ='), 'original posts must be restricted to the requested subscriber')
    assert(
      activityQuery.includes('NOT EXISTS') &&
        activityQuery.includes('e_quote.uri = p.uri') &&
        activityQuery.includes('e_quote.type = 3'),
      'quote posts must be excluded from original_post results',
    )

    response = await fetch(
      `${base}?scope=all_tracked&subscriber_did=did:plc:subscriber&types=original_post&days=8`,
      { headers: { 'api-key': 'contract-test-key' } },
    )
    assert.strictEqual(response.status, 400, 'activity window must remain bounded to seven days')

    response = await fetch(
      `${base}?scope=all_tracked&subscriber_did=did:plc:subscriber&types=original_post&limit=1001`,
      { headers: { 'api-key': 'contract-test-key' } },
    )
    assert.strictEqual(response.status, 400, 'activity result count must remain bounded to 1000')
  } finally {
    delete process.env.FEEDGEN_MONITOR_API_KEY
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
    await db.destroy()
  }

  console.log('compliance activity contract tests passed')
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
