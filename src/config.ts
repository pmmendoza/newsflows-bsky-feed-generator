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
}
