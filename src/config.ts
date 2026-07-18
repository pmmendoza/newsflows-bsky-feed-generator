import { Database } from './db'
import { DidResolver } from '@atproto/identity'

export type AppContext = {
  db: Database
  legacyDb?: Database
  didResolver: DidResolver
  cfg: Config
}

export type Config = {
  port: number
  listenhost: string
  hostname: string
  postgresUrl?: string
  pgHost?: string
  pgPort?: number
  pgUser?: string
  pgPassword?: string
  pgDatabase?: string
  legacyPostgresUrl?: string
  legacyPgHost?: string
  legacyPgPort?: number
  legacyPgUser?: string
  legacyPgPassword?: string
  legacyPgDatabase?: string
  subscriptionEndpoint: string
  serviceDid: string
  publisherDid: string
  subscriptionReconnectDelay: number
  subscriptionIdleTimeoutMs: number
  // Canary testing mode: prevents mutating operations against production DB.
  readOnlyMode?: boolean
  // When true, apply pending DB migrations automatically on startup (dev/first-run
  // convenience). Default false: production startup fails fast if migrations are
  // pending, so migrations are applied explicitly (yarn db:migrate) before serving.
  autoMigrate?: boolean
}
