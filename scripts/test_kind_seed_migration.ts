// RT-6: verify migration 007's kind-seed assertion — exactly 10 rows across
// publisher/testing/researcher must be updated, and all 10 must already be
// access_scope='omni', or the migration must fail loudly. Also verifies the
// fresh/dev/test skip-if-absent case (0 seeded) is a clean no-op, matching
// the migration-006 idiom already used in this codebase.
import assert from 'assert'
import { Migrator, sql } from 'kysely'
import { createDb, migrateToLatest } from '../src/db'
import { migrationProvider } from '../src/db/migrations'

const PUBLISHER_DIDS = [
  'did:plc:toz4no26o2x4vsbum7cp4bxp',
  'did:plc:kzmukwaf72iwepygposicgt3',
  'did:plc:cegiy4pfghh4rjs7ks7pbnkm',
  'did:plc:vzmnljt7otfbbgrmachtefxh',
  'did:plc:tlmi333azel2jcornp2qeolm',
]
const TESTING_DIDS = ['did:plc:weksrderzzdyxdh26pu5jyqo', 'did:plc:u7d6u2a5wu7dbjp6wruttlrv']
const RESEARCHER_DIDS = [
  'did:plc:3vomhawgkjhtvw4euuxbll3r',
  'did:plc:df5sxbescomzxz7fwovti4vd',
  'did:plc:upgwmkhteysqu2n7mar2w4rk',
]
const ALL_DIDS = [...PUBLISHER_DIDS, ...TESTING_DIDS, ...RESEARCHER_DIDS]

async function main() {
  const dsn = process.env.FEEDGEN_TEST_DSN
  if (!dsn || process.env.FEEDGEN_SUBSCRIPTION_TEST_CONFIRM !== 'disposable') {
    throw new Error('requires FEEDGEN_TEST_DSN and FEEDGEN_SUBSCRIPTION_TEST_CONFIRM=disposable')
  }
  const db = createDb(dsn)
  const migrator = new Migrator({ db, provider: migrationProvider })
  try {
    await migrateToLatest(db)

    const rollbackTo007 = async () => {
      const result = await migrator.migrateTo('006_semantic_be_feed_ids')
      if (result.error) throw result.error
    }
    const insertFixtureDids = async (dids: string[], accessScope: string) => {
      for (const did of dids) {
        await db
          .insertInto('subscriber')
          .values({ handle: `${did}.fixture`, did, access_scope: accessScope as any })
          .onConflict((oc) => oc.column('did').doUpdateSet({ access_scope: accessScope as any }))
          .execute()
      }
    }
    const deleteFixtureDids = async () => {
      await db.deleteFrom('subscriber').where('did', 'in', ALL_DIDS).execute()
    }

    // ---- Scenario A: happy path — all 10 present, all omni. ----
    await deleteFixtureDids()
    await rollbackTo007()
    await insertFixtureDids(ALL_DIDS, 'omni')
    await migrateToLatest(db)
    const kinds = await db
      .selectFrom('subscriber')
      .select(['did', 'kind'])
      .where('did', 'in', ALL_DIDS)
      .execute()
    for (const did of PUBLISHER_DIDS) {
      assert(kinds.find((k) => k.did === did)?.kind === 'publisher', `A: ${did} must be seeded publisher`)
    }
    for (const did of TESTING_DIDS) {
      assert(kinds.find((k) => k.did === did)?.kind === 'testing', `A: ${did} must be seeded testing`)
    }
    for (const did of RESEARCHER_DIDS) {
      assert(kinds.find((k) => k.did === did)?.kind === 'researcher', `A: ${did} must be seeded researcher`)
    }
    console.log('A: happy-path seed OK')

    // ---- Scenario B: partial roster (9 of 10) — must hard-fail. ----
    await deleteFixtureDids()
    await rollbackTo007()
    await insertFixtureDids(ALL_DIDS.slice(0, 9), 'omni')
    let partialError: unknown
    try {
      await migrateToLatest(db)
    } catch (error) {
      partialError = error
    }
    assert(partialError instanceof Error, 'B: partial roster must fail the migration')
    assert(
      (partialError as Error).message.includes('seed mismatch'),
      'B: partial roster failure must be the actionable seed-mismatch error',
    )
    console.log('B: partial-roster hard-fail OK')

    // ---- Scenario C: all 10 present but one is not omni — must hard-fail. ----
    await deleteFixtureDids()
    await rollbackTo007()
    await insertFixtureDids(ALL_DIDS, 'omni')
    await db.updateTable('subscriber').set({ access_scope: 'none' as any }).where('did', '=', ALL_DIDS[0]).execute()
    let nonOmniError: unknown
    try {
      await migrateToLatest(db)
    } catch (error) {
      nonOmniError = error
    }
    assert(nonOmniError instanceof Error, 'C: a non-omni seeded DID must fail the migration')
    assert(
      (nonOmniError as Error).message.includes("access_scope='omni'"),
      'C: non-omni failure must be the actionable omni-mismatch error',
    )
    console.log('C: non-omni hard-fail OK')

    // ---- Scenario D: none of the 10 present (fresh/dev/test DB) — clean skip. ----
    await deleteFixtureDids()
    await rollbackTo007()
    await migrateToLatest(db)
    console.log('D: absent-roster clean skip OK')

    // Leave the DB migrated and free of fixture rows for any subsequent script.
    await deleteFixtureDids()
    await rollbackTo007()
    await migrateToLatest(db)

    console.log('test_kind_seed_migration OK')
  } finally {
    await db.destroy()
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
