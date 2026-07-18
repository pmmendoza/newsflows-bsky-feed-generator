import dotenv from 'dotenv'
import { createDb, getPendingMigrations } from '../db'

// Read-only: report pending migrations without applying anything. Use before a
// deploy to review what `yarn db:migrate` would apply. Exits 0 either way.
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
    const pending = await getPendingMigrations(db)
    if (pending.length === 0) {
      console.log('no pending migrations')
    } else {
      console.log(`${pending.length} pending migration(s):`)
      for (const name of pending) console.log(`  - ${name}`)
    }
  } finally {
    await db.destroy()
  }
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})
