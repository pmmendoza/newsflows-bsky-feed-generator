/**
 * Regression test for firehose cursor persistence ordering.
 *
 * Run: `npx ts-node scripts/test_firehose_cursor_await.ts`
 */

import assert from 'assert'
import { FirehoseSubscriptionBase } from '../src/util/subscription'
import type { OutputSchema as RepoEvent } from '../src/lexicon/types/com/atproto/sync/subscribeRepos'

const commitEvent = {
  $type: 'com.atproto.sync.subscribeRepos#commit',
  seq: 20,
  repo: 'did:plc:test',
  rebase: false,
  tooBig: false,
  commit: {} as any,
  prev: null,
  rev: 'rev',
  since: null,
  blocks: new Uint8Array(),
  ops: [],
  blobs: [],
  time: '2026-06-05T00:00:00.000Z',
} as RepoEvent

class OrderedFirehose extends FirehoseSubscriptionBase {
  events: string[] = []

  constructor() {
    super({} as any, 'test-firehose')
    this.sub = {
      async *[Symbol.asyncIterator]() {
        yield commitEvent
      },
    } as any
  }

  async getCursor(): Promise<{ cursor?: number }> {
    return {}
  }

  async updateCursor(cursor: number): Promise<void> {
    this.events.push(`cursor:${cursor}`)
  }

  async handleEvent(): Promise<void> {
    this.events.push('handle:start')
    await new Promise((resolve) => setTimeout(resolve, 1))
    this.events.push('handle:end')
  }
}

async function main() {
  const firehose = new OrderedFirehose()
  await firehose.run(0)

  assert.deepEqual(firehose.events, [
    'handle:start',
    'handle:end',
    'cursor:20',
  ])

  console.log('firehose cursor await test passed')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
