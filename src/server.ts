import http from 'http'
import events from 'events'
import express from 'express'
import { DidResolver, MemoryCache } from '@atproto/identity'
import { createServer } from './lexicon'
import feedGeneration from './methods/feed-generation'
import describeGenerator from './methods/describe-generator'
import registerSubscribeEndpoint from './methods/subscribe'
import registerPrioritizeEndpoint from './methods/prioritize-posts'
import registerMonitorEndpoints from './methods/monitor'
import registerUpdaterEndpoints from './methods/updater'
import registerStudyEndpoints from './methods/study'
import registerFeedCatalogAdminEndpoint from './methods/feed-catalog-admin'
import registerSubscriberAdminEndpoints from './methods/subscriber-admin'
import { importSubscribersFromCSV } from './util/import-subscribers'
import { createDb, Database, migrateToLatest, getPendingMigrations } from './db'
import { FirehoseSubscription } from './subscription'
import { AppContext, Config } from './config'
import wellKnown from './well-known'
import { setupFollowsUpdateScheduler, setupEngagmentUpdateScheduler, setupDailyFullSyncScheduler, setupRetentionScheduler, setupScoreSourceCacheScheduler, stopAllSchedulers, followsUpdateIntervalMs, retentionIntervalMs } from './util/scheduled-updater'
import { startFeedCatalogListener, stopFeedCatalogListener } from './util/catalog-listener'
import { isRetentionSchedulerEnabledStrict } from './util/retention'
import { recordConfigActivation as persistConfigActivation } from './util/config-activation'
import registerConfigActivationAdminEndpoint from './methods/config-activation-admin'

export class FeedGenerator {
  public app: express.Application
  public server?: http.Server
  public db: Database
  public legacyDb?: Database
  public firehose: FirehoseSubscription
  public cfg: Config
  private followsUpdateTimer?: NodeJS.Timeout
  private engagementUpdateTimer?: NodeJS.Timeout

  constructor(
    app: express.Application,
    db: Database,
    legacyDb: Database | undefined,
    firehose: FirehoseSubscription,
    cfg: Config,
  ) {
    this.app = app
    this.db = db
    this.legacyDb = legacyDb
    this.firehose = firehose
    this.cfg = cfg
  }

  static create(cfg: Config) {
    const app = express()

    // Add JSON body parser middleware
    app.use(express.json({ limit: '10mb' })) // Adjust limit as needed
    app.use(express.urlencoded({ extended: true, limit: '10mb' }))

    // comply with CORS policy
    app.use((req, res, next) => {
      res.header('Access-Control-Allow-Origin', '*')
      res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, api-key')
      res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
      if (req.method === 'OPTIONS') {
        res.sendStatus(200)
      } else {
        next()
      }
    })

    const db = createDb(
      cfg.postgresUrl ||
        `postgres://${cfg.pgUser}:${cfg.pgPassword}@${cfg.pgHost}:${cfg.pgPort}/${cfg.pgDatabase}`,
    )
    const legacyDb = cfg.legacyPostgresUrl || cfg.legacyPgHost
      ? createDb(
          cfg.legacyPostgresUrl ||
            `postgres://${cfg.legacyPgUser ?? 'feedgen'}:${cfg.legacyPgPassword ?? 'feedgen'}@${cfg.legacyPgHost ?? 'localhost'}:${cfg.legacyPgPort ?? 5432}/${cfg.legacyPgDatabase ?? 'feedgen-db'}`,
        )
      : undefined
    const firehose = new FirehoseSubscription(db, cfg.subscriptionEndpoint, {
      idleTimeoutMs: cfg.subscriptionIdleTimeoutMs,
    })

    const didCache = new MemoryCache()
    const didResolver = new DidResolver({
      plcUrl: 'https://plc.directory',
      didCache,
    })

    const server = createServer({
      validateResponse: true,
      payload: {
        jsonLimit: 100 * 1024, // 100kb
        textLimit: 100 * 1024, // 100kb
        blobLimit: 5 * 1024 * 1024, // 5mb
      },
    })
    const ctx: AppContext = {
      db,
      legacyDb,
      didResolver,
      cfg,
    }
    feedGeneration(server, ctx)
    describeGenerator(server, ctx)
    app.use(server.xrpc.router)
    app.use(wellKnown(ctx))

    // register api endpoints
    registerSubscribeEndpoint(server, ctx)
    registerPrioritizeEndpoint(server, ctx)
    registerMonitorEndpoints(server, ctx)
    registerUpdaterEndpoints(server, ctx)
    registerStudyEndpoints(server, ctx)
    registerFeedCatalogAdminEndpoint(server, ctx)
    registerSubscriberAdminEndpoints(server, ctx)
    registerConfigActivationAdminEndpoint(server, ctx)

    return new FeedGenerator(app, db, legacyDb, firehose, cfg)
  }

  async start(): Promise<http.Server> {
    if (!this.cfg.readOnlyMode) {
      if (this.cfg.autoMigrate) {
        await migrateToLatest(this.db)
      } else {
        // Do NOT silently apply migrations on startup: a bad migration would
        // brick the serving process. Fail fast if any are pending so they are
        // applied explicitly (yarn db:migrate) before the new image serves.
        const pending = await getPendingMigrations(this.db)
        if (pending.length > 0) {
          throw new Error(
            `${pending.length} pending DB migration(s) [${pending.join(', ')}]. ` +
              `Run 'yarn db:migrate' before starting, or set FEEDGEN_AUTO_MIGRATE=true ` +
              `to apply on startup (dev/first-run only).`,
          )
        }
      }
      this.firehose.run(this.cfg.subscriptionReconnectDelay)
    }

    this.server = this.app.listen(this.cfg.port, this.cfg.listenhost)
    await events.once(this.server, 'listening')

    if (this.cfg.readOnlyMode) {
      console.log(
        `[${new Date().toISOString()}] - Read-only mode enabled: skipped migrations, firehose, subscriber import, and schedulers`,
      )
      return this.server
    }

    // Import subscribers at startup
    try {
      await importSubscribersFromCSV(this.db)
    } catch (err) {
      console.error('Failed to import subscribers:', err)
    }

    // Set up the scheduler to update follows
    // Run once every hour by default (or override with env var) for incremental updates
    const updateInterval = followsUpdateIntervalMs()
    console.log(`[${new Date().toISOString()}] - Setting up follows updater to run every ${updateInterval / 1000} seconds`)
    this.followsUpdateTimer = setupFollowsUpdateScheduler(this.db, updateInterval)
    this.engagementUpdateTimer = setupEngagmentUpdateScheduler(this.db, updateInterval)

    // D1.4: keep the ranker score-source map warm for the sync read-path lookup.
    setupScoreSourceCacheScheduler(this.db)

    // Set up daily full sync at 4:00 AM to remove unfollowed accounts
    setupDailyFullSyncScheduler(this.db)

    // Optional retention (TTL deletes) to bound storage growth
    if (isRetentionSchedulerEnabledStrict()) {
      const retentionInterval = retentionIntervalMs()
      console.log(`[${new Date().toISOString()}] - Retention enabled; running every ${Math.round(retentionInterval / 1000 / 60)} minutes`)
      setupRetentionScheduler(this.db, retentionInterval)
    }

    // Catalog cache invalidation via Postgres LISTEN/NOTIFY (Q4 #6).
    // Gated on FEEDGEN_CATALOG_LISTEN_NOTIFY=true; safe no-op otherwise.
    const connectionString =
      this.cfg.postgresUrl ||
      `postgres://${this.cfg.pgUser}:${this.cfg.pgPassword}@${this.cfg.pgHost}:${this.cfg.pgPort}/${this.cfg.pgDatabase}`
    startFeedCatalogListener(connectionString).catch((err) => {
      console.error(
        `[${new Date().toISOString()}] - catalog-listener: bootstrap raised; TTL fallback remains in effect. error=${
          err instanceof Error ? err.message : String(err)
        }`,
      )
    })

    return this.server
  }

  /**
   * Record this process's resolved behavior config as a
   * `feedgen_ops.config_activation` row. Called from index.ts AFTER
   * `await server.start()` so it never delays serving; `ctx` is not
   * returned from `create()`, so this reaches the DB/cfg it needs via
   * `this.db` / `this.cfg` (both already assigned by the constructor).
   * Fail-open: never throws — see util/config-activation.ts.
   */
  async recordConfigActivation(): Promise<void> {
    await persistConfigActivation(this.db, this.cfg)
  }

  async stop(): Promise<void> {
    // Stop the scheduler
    stopAllSchedulers()

    // Best-effort: stop the catalog LISTEN/NOTIFY connection.
    await stopFeedCatalogListener().catch(() => {
      /* best effort */
    })

    if (this.db) {
      await this.db.destroy()
    }

    if (this.legacyDb) {
      await this.legacyDb.destroy()
    }

    // Close the server if it's running
    if (this.server) {
      await new Promise<void>((resolve, reject) => {
        this.server?.close((err) => {
          if (err) reject(err)
          else resolve()
        })
      })
    }
  }
}

export default FeedGenerator
