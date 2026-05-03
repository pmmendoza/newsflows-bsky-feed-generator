/**
 * Layer A unit test for `buildAtUriDidMembershipFilter`
 * (src/methods/monitor.ts).
 *
 * No DB required. Compiles the helper's output through Kysely's
 * Postgres SQL compiler with a `DummyDriver`, then asserts the
 * emitted SQL string + parameter list against expectations for
 * each (dids, include) combination.
 *
 * Why this test exists: a prior implementation built an exclusive
 * upper bound by appending '0' to the DID. Under `en_US.utf8`
 * collation '/' is NOT lexicographically less than '0', so the
 * range filter was empty and `/api/compliance/engagement` silently
 * returned 0 events. This test guards against re-introducing any
 * comparison-style filter that depends on collation.
 *
 * Run: `npx ts-node scripts/test_publisher_did_filter.ts`
 * Exits non-zero on any assertion failure.
 *
 * Cross-references:
 *   - Fix commit + JSDoc: `src/methods/monitor.ts`
 *     (`buildAtUriDidMembershipFilter`).
 *   - Incident note:
 *     `BSKY/dev_feeds/blueskyranker_v2/dev/storage/incident_publisher_did_filter_collation_2026-05-03.md`.
 */

import {
  Kysely,
  DummyDriver,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
  sql,
} from 'kysely'
import { buildAtUriDidMembershipFilter } from '../src/methods/monitor'

// --- Test harness ----------------------------------------------------------

const db = new Kysely<any>({
  dialect: {
    createAdapter: () => new PostgresAdapter(),
    createDriver: () => new DummyDriver(),
    createIntrospector: (d) => new PostgresIntrospector(d),
    createQueryCompiler: () => new PostgresQueryCompiler(),
  },
})

let failed = 0
let passed = 0

function assert(cond: boolean, label: string, detail?: string) {
  if (cond) {
    passed++
    console.log(`  ✓ ${label}`)
  } else {
    failed++
    console.log(`  ✗ ${label}${detail ? ` — ${detail}` : ''}`)
  }
}

function compile(expr: ReturnType<typeof sql>) {
  return expr.compile(db)
}

// --- Cases -----------------------------------------------------------------

console.log('Case 1: empty dids + include=true → "false"')
{
  const c = compile(buildAtUriDidMembershipFilter(sql`x`, [], true))
  assert(c.sql.trim() === 'false', 'sql equals "false"', `actual: ${JSON.stringify(c.sql)}`)
  assert(c.parameters.length === 0, 'no parameters bound')
}

console.log('Case 2: empty dids + include=false → "true"')
{
  const c = compile(buildAtUriDidMembershipFilter(sql`x`, [], false))
  assert(c.sql.trim() === 'true', 'sql equals "true"', `actual: ${JSON.stringify(c.sql)}`)
  assert(c.parameters.length === 0, 'no parameters bound')
}

console.log('Case 3: single did + include=true')
{
  const c = compile(
    buildAtUriDidMembershipFilter(
      sql`e."subjectUri"`,
      ['did:plc:abc'],
      true,
    ),
  )
  assert(/split_part\(/i.test(c.sql), 'uses split_part(...)')
  assert(c.sql.includes(`'/'`) || c.sql.includes(`'/'`), `delimiter '/' present`)
  // The third positional argument is the integer 3 (component index).
  assert(/,\s*3\s*\)/.test(c.sql), 'extracts component 3')
  assert(/IN\s*\(/i.test(c.sql), 'uses IN (...)')
  assert(/\bNOT\b/.test(c.sql) === false, 'no NOT for include=true')
  assert(c.parameters.length === 1, '1 bound parameter')
  assert(c.parameters[0] === 'did:plc:abc', 'parameter is the DID')
  assert(c.sql.includes('e."subjectUri"::text') || c.sql.includes('e."subjectUri"'), 'column embedded')
}

console.log('Case 4: multi did + include=true')
{
  const dids = ['did:plc:a', 'did:plc:b', 'did:plc:c']
  const c = compile(
    buildAtUriDidMembershipFilter(sql`p."rootUri"`, dids, true),
  )
  assert(c.parameters.length === 3, '3 bound parameters')
  assert(
    c.parameters[0] === 'did:plc:a' &&
      c.parameters[1] === 'did:plc:b' &&
      c.parameters[2] === 'did:plc:c',
    'parameters in order',
  )
  assert(/\bNOT\b/.test(c.sql) === false, 'no NOT for include=true')
  // Three placeholders inside the IN clause: $1, $2, $3.
  assert(/IN\s*\(\s*\$1\s*,\s*\$2\s*,\s*\$3\s*\)/i.test(c.sql), 'IN ($1, $2, $3)')
}

console.log('Case 5: single did + include=false')
{
  const c = compile(
    buildAtUriDidMembershipFilter(
      sql`e."subjectUri"`,
      ['did:plc:abc'],
      false,
    ),
  )
  assert(/^\s*NOT\s*\(/i.test(c.sql), 'wrapped in NOT (...)')
  assert(/split_part\(/i.test(c.sql), 'inner uses split_part')
  assert(c.parameters.length === 1, '1 bound parameter')
  assert(c.parameters[0] === 'did:plc:abc', 'parameter is the DID')
}

console.log('Case 6: regression guard — emitted SQL must NOT contain old broken bound')
{
  const c = compile(
    buildAtUriDidMembershipFilter(
      sql`e."subjectUri"`,
      ['did:plc:abc'],
      true,
    ),
  )
  // The previous implementation produced "at://did:plc:abc/" and "at://did:plc:abc0".
  // If those substrings reappear, the broken pattern has been re-introduced.
  assert(c.sql.includes('at://') === false, 'no AT URI prefix literal in SQL')
  assert(/at:\/\/did:plc:abc0/.test(c.sql) === false, 'no upper-bound "did0" suffix')
  assert(/at:\/\/did:plc:abc\//.test(c.sql) === false, 'no lower-bound "did/" suffix')
  // Parameters carry only DIDs, no synthesized URI bounds.
  for (const p of c.parameters) {
    assert(typeof p === 'string' && !String(p).startsWith('at://'), `parameter "${p}" is a bare DID, not an AT-URI bound`)
  }
}

// --- Result ---------------------------------------------------------------

console.log('')
console.log(`passed=${passed} failed=${failed}`)
process.exit(failed === 0 ? 0 : 1)
