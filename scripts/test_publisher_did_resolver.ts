/**
 * Unit tests for the catalog-backed publisher DID resolver.
 *
 * Run: `npx ts-node scripts/test_publisher_did_resolver.ts`
 */

import assert from 'assert'
import {
  getPublisherDidsFromEnv,
  invalidatePublisherDidCache,
  resolvePublisherDidInfo,
} from '../src/util/publisher-dids'

type CatalogRow = {
  publisher_did?: string | null
  enabled?: boolean
}

function makeFakeDb(rows: CatalogRow[], fail = false): any {
  return {
    selectFrom(table: string) {
      assert.equal(table, 'feedgen_ops.feed_catalog')
      return {
        select() {
          return this
        },
        where() {
          return this
        },
        async execute() {
          if (fail) throw new Error('simulated catalog failure')
          return rows
        },
      }
    },
  }
}

async function withEnv<T>(env: Record<string, string | undefined>, fn: () => Promise<T>): Promise<T> {
  const saved: Record<string, string | undefined> = {}
  for (const [key, value] of Object.entries(env)) {
    saved[key] = process.env[key]
    if (value === undefined) delete process.env[key]
    else process.env[key] = value
  }
  try {
    return await fn()
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  }
}

async function main() {
  await withEnv(
    {
      NEWSBOT_NL_DID: 'did:plc:nl-env',
      NEWSBOT_BE_DID: 'did:plc:be-env',
      NEWSBOT_EMPTY_DID: '',
    },
    async () => {
      assert.deepEqual(
        getPublisherDidsFromEnv().sort(),
        ['did:plc:be-env', 'did:plc:nl-env'],
        'env resolver should return non-empty NEWSBOT_*_DID values',
      )

      invalidatePublisherDidCache()
      const catalog = await resolvePublisherDidInfo(
        makeFakeDb([
          { publisher_did: 'did:plc:nl-catalog', enabled: true },
          { publisher_did: 'did:plc:nl-catalog', enabled: true },
          { publisher_did: 'did:plc:be-catalog', enabled: true },
          { publisher_did: 'did:plc:disabled-catalog', enabled: false },
          { publisher_did: '', enabled: true },
          { publisher_did: null, enabled: true },
        ]),
      )
      assert.equal(catalog.source, 'feed_catalog')
      assert.deepEqual(catalog.dids, ['did:plc:nl-catalog', 'did:plc:be-catalog'])

      invalidatePublisherDidCache()
      const emptyCatalog = await resolvePublisherDidInfo(makeFakeDb([]))
      assert.equal(emptyCatalog.source, 'env_fallback')
      assert.deepEqual(emptyCatalog.dids.sort(), ['did:plc:be-env', 'did:plc:nl-env'])

      invalidatePublisherDidCache()
      const failedCatalog = await resolvePublisherDidInfo(makeFakeDb([], true))
      assert.equal(failedCatalog.source, 'env_fallback')
      assert.equal(failedCatalog.error, 'simulated catalog failure')
      assert.deepEqual(failedCatalog.dids.sort(), ['did:plc:be-env', 'did:plc:nl-env'])
    },
  )

  console.log('publisher DID resolver tests passed')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
