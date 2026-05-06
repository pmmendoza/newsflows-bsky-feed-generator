/**
 * Postgres LISTEN/NOTIFY listener for `feedgen_ops.feed_catalog`
 * mutations. Replaces the implicit 5-minute LRU TTL freshness model
 * (see `access-policy.ts`) with event-driven cache invalidation.
 *
 * Plan: BSKY/dev_feeds/blueskyranker_v2/dev/storage/plan_storage_refactor/plan_feed_catalog_listen_notify.md
 * Migration: BSKY/dev_feeds/blueskyranker_v2/dev/storage/migrations/015_feed_catalog_notify.sql
 *
 * Lifecycle:
 *   - Started from `server.start()` after migrations applied.
 *   - Holds ONE dedicated long-lived pg connection (LISTEN holds a
 *     session). Reconnects with backoff on failure.
 *   - Failure to start does NOT crash the server — the 5-minute TTL
 *     in the access-policy LRU remains as a safety net.
 *
 * Activation: `FEEDGEN_CATALOG_LISTEN_NOTIFY=true`. Default off until
 * the first prod soak completes.
 */

import { Client } from 'pg'
import { invalidatePolicyCache } from './access-policy'
import { invalidateDispatchCache } from '../algos/catalog-dispatch'

const RECONNECT_DELAY_MS = 5000
const CHANNEL = 'feed_catalog_changed'

type ListenerHandle = {
  client: Client
  shutdown: () => Promise<void>
}

let activeHandle: ListenerHandle | null = null
let stopRequested = false

/**
 * Start the LISTEN/NOTIFY loop. Idempotent — if already running,
 * returns the existing handle.
 */
export async function startFeedCatalogListener(
  connectionString: string,
): Promise<void> {
  if (activeHandle) {
    return
  }
  if (process.env.FEEDGEN_CATALOG_LISTEN_NOTIFY !== 'true') {
    console.log(
      `[${new Date().toISOString()}] - catalog-listener: disabled (FEEDGEN_CATALOG_LISTEN_NOTIFY != 'true'); 5-min TTL remains in effect`,
    )
    return
  }
  stopRequested = false
  await connectAndListen(connectionString)
}

async function connectAndListen(connectionString: string): Promise<void> {
  if (stopRequested) return
  const client = new Client({ connectionString })
  try {
    await client.connect()
    await client.query(`LISTEN ${CHANNEL}`)
    console.log(
      `[${new Date().toISOString()}] - catalog-listener: subscribed to ${CHANNEL}`,
    )

    client.on('notification', (msg) => {
      try {
        const payload = msg.payload ? JSON.parse(msg.payload) : null
        if (payload?.rkey) {
          invalidatePolicyCache(String(payload.rkey))
          // Sprint 14 / T2 Phase 1 — dispatch cache invalidates on
          // the same NOTIFY trigger as policy cache.
          invalidateDispatchCache(String(payload.rkey))
          console.log(
            `[${new Date().toISOString()}] - catalog-listener: invalidated rkey=${payload.rkey} op=${payload.op}`,
          )
        } else {
          invalidatePolicyCache()
          invalidateDispatchCache()
          console.log(
            `[${new Date().toISOString()}] - catalog-listener: payload missing rkey; cleared full cache`,
          )
        }
      } catch (err) {
        invalidatePolicyCache()
        invalidateDispatchCache()
        console.warn(
          `[${new Date().toISOString()}] - catalog-listener: payload parse failed; cleared full cache. error=${
            err instanceof Error ? err.message : String(err)
          }`,
        )
      }
    })

    const handleError = (err: Error) => {
      console.error(
        `[${new Date().toISOString()}] - catalog-listener: client error; will reconnect. error=${err.message}`,
      )
      invalidatePolicyCache()
      invalidateDispatchCache()
      cleanup()
      if (!stopRequested) {
        setTimeout(() => {
          connectAndListen(connectionString).catch((e) => {
            console.error(
              `[${new Date().toISOString()}] - catalog-listener: reconnect attempt failed. error=${
                e instanceof Error ? e.message : String(e)
              }`,
            )
          })
        }, RECONNECT_DELAY_MS)
      }
    }
    client.on('error', handleError)
    client.on('end', () => {
      if (!stopRequested) {
        handleError(new Error('connection ended unexpectedly'))
      }
    })

    activeHandle = {
      client,
      shutdown: async () => {
        stopRequested = true
        try {
          await client.query(`UNLISTEN ${CHANNEL}`)
        } catch {
          // best effort
        }
        await client.end().catch(() => {
          /* best effort */
        })
      },
    }
  } catch (err) {
    cleanup()
    console.warn(
      `[${new Date().toISOString()}] - catalog-listener: startup failed; falling back to TTL-only invalidation. error=${
        err instanceof Error ? err.message : String(err)
      }`,
    )
    if (!stopRequested) {
      setTimeout(() => {
        connectAndListen(connectionString).catch((e) => {
          console.error(
            `[${new Date().toISOString()}] - catalog-listener: retry after startup failure also failed. error=${
              e instanceof Error ? e.message : String(e)
            }`,
          )
        })
      }, RECONNECT_DELAY_MS)
    }
  }
}

function cleanup() {
  activeHandle = null
}

/**
 * Shut the listener down (used by FeedGenerator.stop()). Best-effort.
 */
export async function stopFeedCatalogListener(): Promise<void> {
  stopRequested = true
  if (activeHandle) {
    await activeHandle.shutdown()
    cleanup()
  }
}
