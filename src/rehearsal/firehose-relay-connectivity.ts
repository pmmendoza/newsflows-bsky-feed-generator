import assert from 'assert'
import { Subscription } from '@atproto/xrpc-server'
import { ids, lexicons } from '../lexicon/lexicons'
import {
  OutputSchema as RepoEvent,
  isCommit,
  isHandle,
  isInfo,
  isMigrate,
  isTombstone,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'

type Options = {
  serviceUrl: string
  maxFrames: number
  timeoutMs: number
}

export type FirehoseRelayConnectivityResult = {
  status: 'ok'
  transport: 'xrpc_websocket'
  store_mode: 'no_store'
  service_host: string
  frame_count: number
  commit_count: number
  handle_count: number
  migrate_count: number
  tombstone_count: number
  info_count: number
  other_count: number
  lowest_seq: number
  highest_seq: number
  timeout_ms: number
  max_frames: number
  raw_values_in_output: false
}

export async function runFirehoseRelayConnectivityProof(
  options: Options,
): Promise<FirehoseRelayConnectivityResult> {
  assert.ok(options.maxFrames > 0, 'maxFrames must be positive')
  assert.ok(options.timeoutMs > 0, 'timeoutMs must be positive')

  const abort = new AbortController()
  const counts = {
    commit: 0,
    handle: 0,
    migrate: 0,
    tombstone: 0,
    info: 0,
    other: 0,
  }
  const seqs: number[] = []
  const serviceHost = new URL(options.serviceUrl).host

  const timeout = setTimeout(() => {
    abort.abort(new Error('relay connectivity timeout'))
  }, options.timeoutMs)

  const sub = new Subscription<RepoEvent>({
    service: options.serviceUrl,
    method: ids.ComAtprotoSyncSubscribeRepos,
    signal: abort.signal,
    validate: (value: unknown) => {
      try {
        return lexicons.assertValidXrpcMessage<RepoEvent>(
          ids.ComAtprotoSyncSubscribeRepos,
          value,
        )
      } catch (err) {
        console.error('relay connectivity skipped invalid message', err)
      }
    },
  })

  try {
    try {
      for await (const event of sub) {
        const seq = readSeq(event)
        if (typeof seq === 'number') seqs.push(seq)

        if (isCommit(event)) counts.commit += 1
        else if (isHandle(event)) counts.handle += 1
        else if (isMigrate(event)) counts.migrate += 1
        else if (isTombstone(event)) counts.tombstone += 1
        else if (isInfo(event)) counts.info += 1
        else counts.other += 1

        if (frameCount(counts) >= options.maxFrames) {
          abort.abort(new Error('relay connectivity frame limit reached'))
        }
      }
    } catch (err) {
      if (
        !(err instanceof Error) ||
        (err.message !== 'relay connectivity frame limit reached' &&
          err.message !== 'relay connectivity timeout')
      ) {
        throw err
      }
    }

    const totalFrames = frameCount(counts)
    assert.ok(totalFrames > 0, 'relay produced no valid frames')
    assert.ok(seqs.length > 0, 'relay produced no sequence-bearing frames')

    return {
      status: 'ok',
      transport: 'xrpc_websocket',
      store_mode: 'no_store',
      service_host: serviceHost,
      frame_count: totalFrames,
      commit_count: counts.commit,
      handle_count: counts.handle,
      migrate_count: counts.migrate,
      tombstone_count: counts.tombstone,
      info_count: counts.info,
      other_count: counts.other,
      lowest_seq: Math.min(...seqs),
      highest_seq: Math.max(...seqs),
      timeout_ms: options.timeoutMs,
      max_frames: options.maxFrames,
      raw_values_in_output: false,
    }
  } finally {
    clearTimeout(timeout)
  }
}

function readSeq(event: RepoEvent): number | null {
  const seq = (event as { seq?: unknown }).seq
  return typeof seq === 'number' ? seq : null
}

function frameCount(counts: Record<string, number>): number {
  return Object.values(counts).reduce((sum, value) => sum + value, 0)
}
