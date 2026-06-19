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
  cursorProbe?: boolean
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
  cursor_probe_status: 'ok' | 'skipped'
  cursor_probe_requested_cursor: number | null
  cursor_probe_frame_count: number
  cursor_probe_lowest_seq: number | null
  cursor_probe_highest_seq: number | null
  cursor_probe_relation: 'inclusive' | 'after' | 'skipped'
  raw_values_in_output: false
}

export async function runFirehoseRelayConnectivityProof(
  options: Options,
): Promise<FirehoseRelayConnectivityResult> {
  assert.ok(options.maxFrames > 0, 'maxFrames must be positive')
  assert.ok(options.timeoutMs > 0, 'timeoutMs must be positive')

  const serviceHost = new URL(options.serviceUrl).host
  const baseline = await collectRelayFrames({
    serviceUrl: options.serviceUrl,
    maxFrames: options.maxFrames,
    timeoutMs: options.timeoutMs,
  })
  let cursorProbe: RelayFrameCollection | null = null
  if (options.cursorProbe) {
    cursorProbe = await collectRelayFrames({
      serviceUrl: options.serviceUrl,
      maxFrames: 1,
      timeoutMs: options.timeoutMs,
      cursor: baseline.highestSeq,
    })
    assert.ok(
      cursorProbe.lowestSeq >= baseline.highestSeq,
      'relay cursor probe returned a sequence before the requested cursor',
    )
  }

  return {
    status: 'ok',
    transport: 'xrpc_websocket',
    store_mode: 'no_store',
    service_host: serviceHost,
    frame_count: baseline.totalFrames,
    commit_count: baseline.counts.commit,
    handle_count: baseline.counts.handle,
    migrate_count: baseline.counts.migrate,
    tombstone_count: baseline.counts.tombstone,
    info_count: baseline.counts.info,
    other_count: baseline.counts.other,
    lowest_seq: baseline.lowestSeq,
    highest_seq: baseline.highestSeq,
    timeout_ms: options.timeoutMs,
    max_frames: options.maxFrames,
    cursor_probe_status: cursorProbe ? 'ok' : 'skipped',
    cursor_probe_requested_cursor: cursorProbe ? baseline.highestSeq : null,
    cursor_probe_frame_count: cursorProbe?.totalFrames ?? 0,
    cursor_probe_lowest_seq: cursorProbe?.lowestSeq ?? null,
    cursor_probe_highest_seq: cursorProbe?.highestSeq ?? null,
    cursor_probe_relation: cursorProbe
      ? cursorProbe.lowestSeq === baseline.highestSeq
        ? 'inclusive'
        : 'after'
      : 'skipped',
    raw_values_in_output: false,
  }
}

type RelayFrameCollection = {
  counts: {
    commit: number
    handle: number
    migrate: number
    tombstone: number
    info: number
    other: number
  }
  totalFrames: number
  lowestSeq: number
  highestSeq: number
}

async function collectRelayFrames(options: {
  serviceUrl: string
  maxFrames: number
  timeoutMs: number
  cursor?: number
}): Promise<RelayFrameCollection> {
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

  const timeout = setTimeout(() => {
    abort.abort(new Error('relay connectivity timeout'))
  }, options.timeoutMs)

  const sub = new Subscription<RepoEvent>({
    service: options.serviceUrl,
    method: ids.ComAtprotoSyncSubscribeRepos,
    signal: abort.signal,
    getParams: () =>
      typeof options.cursor === 'number'
        ? { cursor: options.cursor }
        : undefined,
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
      counts,
      totalFrames,
      lowestSeq: Math.min(...seqs),
      highestSeq: Math.max(...seqs),
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
