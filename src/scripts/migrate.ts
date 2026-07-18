import dotenv from 'dotenv'
import { createDb, migrateToLatest } from '../db'

// Apply pending DB migrations, then exit. Run as an explicit, gated pre-serve
// step (see docs/runbooks/feedgen_production_deploy_runbook.md) instead of
// relying on startup auto-migration.
const connectionString = (): string =>
  process.env.FEEDGEN_POSTGRES_URL ||
  `postgres://${process.env.FEEDGEN_DB_USER ?? 'feedgen'}:${
    process.env.FEEDGEN_DB_PASSWORD ?? 'feedgen'
  }@${process.env.FEEDGEN_DB_HOST ?? 'localhost'}:${
    process.env.FEEDGEN_DB_PORT ?? 5432
  }/${process.env.FEEDGEN_DB_DATABASE ?? 'feedgen-db'}`

const run = async () => {
  dotenv.config()
  const db = createDb(connectionString())
  try {
    await migrateToLatest(db)
    console.log(
      `[${new Date().toISOString()}] - feedgen DB migrations applied (up to date)`,
    )
  } finally {
    await db.destroy()
  }
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})
