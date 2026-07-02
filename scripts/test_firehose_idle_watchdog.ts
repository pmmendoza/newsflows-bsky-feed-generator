/**
 * Regression test for firehose no-progress watchdog reconnects.
 *
 * Run: `npx ts-node scripts/test_firehose_idle_watchdog.ts`
 */

import assert from 'assert'
import { FirehoseSubscriptionBase } from '../src/util/subscription'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

class IdleFirehose extends FirehoseSubscriptionBase {
  cursorReads = 0

  constructor() {
    super({} as any, 'test-firehose', { idleTimeoutMs: 15 })
  }

  protected createSubscription() {
    return {
      async *[Symbol.asyncIterator]() {
        await new Promise(() => {
          /* never resolves */
        })
      },
    } as any
  }

  async getCursor(): Promise<{ cursor?: number }> {
    this.cursorReads += 1
    return {}
  }

  async updateCursor(): Promise<void> {
    throw new Error('idle subscription should not persist cursors')
  }

  async handleEvent(): Promise<void> {
    throw new Error('idle subscription should not handle events')
  }
}

async function main() {
  const firehose = new IdleFirehose()
  void firehose.run(1)

  await sleep(80)

  assert.ok(
    firehose.cursorReads >= 2,
    `expected idle watchdog to reconnect and reread cursor, got ${firehose.cursorReads}`,
  )

  firehose.stop()
  console.log('firehose idle watchdog test passed')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
