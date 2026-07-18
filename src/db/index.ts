import { Pool } from 'pg'
import { Kysely, Migrator, PostgresDialect } from 'kysely'
import { DatabaseSchema } from './schema'
import { migrationProvider } from './migrations'

export const createDb = (connectionString: string): Database => {
  return new Kysely<DatabaseSchema>({
    dialect: new PostgresDialect({
      pool: new Pool({
        connectionString,
        max: 30,
        connectionTimeoutMillis: 5000,
        idleTimeoutMillis: 30000,
        statement_timeout: 30000,
      }),
    }),
  })
}

export const migrateToLatest = async (db: Database) => {
  const migrator = new Migrator({ db, provider: migrationProvider })
  const { error } = await migrator.migrateToLatest()
  if (error) throw error
}

// Names of migrations that exist in the provider but have not been executed
// against this database yet. Read-only: does not apply anything.
export const getPendingMigrations = async (db: Database): Promise<string[]> => {
  const migrator = new Migrator({ db, provider: migrationProvider })
  const migrations = await migrator.getMigrations()
  return migrations.filter((m) => !m.executedAt).map((m) => m.name)
}

export type Database = Kysely<DatabaseSchema>