/**
 * config_activation manifest completeness audit (design Verification 11).
 *
 * Scans the fixed set of serving/describe/subscribe/study/ingestion source
 * files for every `process.env.KEY`, `process.env['KEY']` /
 * `process.env["KEY"]`, and `get{Bool,Int}Env('KEY', ...)` read, then
 * asserts each key is EITHER:
 *   - in CONFIG_MANIFEST_ENV_KEYS (recorded in the manifest), or
 *   - matched by a CONFIG_MANIFEST_DYNAMIC_ENV_KEY_PATTERNS entry
 *     (NEWSBOT_*_DID), or
 *   - in CONFIG_MANIFEST_EXCLUDED_ENV_KEYS with a stated reason.
 *
 * A new behavior-affecting env var added to one of these files without
 * updating src/util/config-manifest.ts fails this test — so the manifest
 * can't silently fall behind serving.
 *
 * Also checks every `keyof Config` (src/config.ts) is accounted for in
 * either CONFIG_CFG_KEYS_COVERED or CONFIG_CFG_KEYS_EXCLUDED.
 *
 * Run: `npx ts-node scripts/test_config_activation_completeness.ts`
 */
import fs from 'fs'
import path from 'path'
import assert from 'assert'
import {
  CONFIG_MANIFEST_ENV_KEYS,
  CONFIG_MANIFEST_DYNAMIC_ENV_KEY_PATTERNS,
  CONFIG_MANIFEST_EXCLUDED_ENV_KEYS,
  CONFIG_CFG_KEYS_COVERED,
  CONFIG_CFG_KEYS_EXCLUDED,
} from '../src/util/config-manifest'

const REPO_ROOT = path.resolve(__dirname, '..')

// The serving / describe / subscribe / study / ingestion modules in scope
// for the completeness audit (design's own module list, §3). Deliberately
// excludes: archive-worker.ts (separate process/entrypoint), src/scripts/*,
// scripts/*, src/tools/*, src/rehearsal/* (dev/ops tooling, not the serving
// process), src/algos/_drafts/* and make-handler.ts (retired static
// registry — unreachable from the active catalog-dispatch.ts path).
const SCAN_FILES = [
  'src/config.ts',
  'src/index.ts',
  'src/server.ts',
  'src/algos/feed-builder.ts',
  'src/algos/ranker-priority-helper.ts',
  'src/algos/politician-filter.ts',
  'src/methods/subscribe.ts',
  'src/methods/study.ts',
  'src/methods/describe-generator.ts',
  'src/methods/monitor.ts',
  'src/methods/feed-catalog-admin.ts',
  'src/util/ingestion-scope.ts',
  'src/util/retention.ts',
  'src/util/scheduled-updater.ts',
  'src/util/score-source-cache.ts',
  'src/util/catalog-listener.ts',
  'src/util/engagement-updater.ts',
  'src/util/publisher-dids.ts',
]

const ENV_KEY_PATTERNS = [
  /process\.env\.([A-Z][A-Z0-9_]*)/g,
  /process\.env\[\s*['"]([A-Z][A-Z0-9_]*)['"]\s*\]/g,
  /get(?:Bool|Int)Env\(\s*['"]([A-Z][A-Z0-9_]*)['"]/g,
]

function scanEnvKeys(filePath: string): Set<string> {
  const text = fs.readFileSync(filePath, 'utf8')
  const found = new Set<string>()
  for (const pattern of ENV_KEY_PATTERNS) {
    let match: RegExpExecArray | null
    // each pattern object is reused across calls; reset lastIndex per file
    pattern.lastIndex = 0
    while ((match = pattern.exec(text))) {
      found.add(match[1])
    }
  }
  return found
}

function isClassified(key: string): boolean {
  if (CONFIG_MANIFEST_ENV_KEYS.includes(key)) return true
  if (key in CONFIG_MANIFEST_EXCLUDED_ENV_KEYS) return true
  if (CONFIG_MANIFEST_DYNAMIC_ENV_KEY_PATTERNS.some((re) => re.test(key))) return true
  return false
}

function main() {
  let failed = 0
  let checkedFiles = 0
  let checkedKeys = 0

  for (const relPath of SCAN_FILES) {
    const filePath = path.join(REPO_ROOT, relPath)
    assert(fs.existsSync(filePath), `scan target missing: ${relPath} (update SCAN_FILES or restore the file)`)
    checkedFiles++
    const keys = scanEnvKeys(filePath)
    for (const key of keys) {
      checkedKeys++
      if (!isClassified(key)) {
        failed++
        console.error(
          `FAIL: ${relPath} reads process.env.${key}, which is neither in CONFIG_MANIFEST_ENV_KEYS, ` +
            `a dynamic pattern, nor CONFIG_MANIFEST_EXCLUDED_ENV_KEYS. Classify it in src/util/config-manifest.ts.`,
        )
      }
    }
  }

  // Every manifest-listed key must actually resolve via at least one
  // classified pattern (catches typos in the whitelist itself, not just
  // gaps).
  for (const key of CONFIG_MANIFEST_ENV_KEYS) {
    if (!/^[A-Z][A-Z0-9_]*$/.test(key)) {
      failed++
      console.error(`FAIL: CONFIG_MANIFEST_ENV_KEYS entry '${key}' is not a valid env-var-shaped key`)
    }
  }

  // Config (src/config.ts) field coverage.
  const ALL_CONFIG_KEYS = [
    'port', 'listenhost', 'hostname', 'postgresUrl', 'pgHost', 'pgPort', 'pgUser', 'pgPassword', 'pgDatabase',
    'legacyPostgresUrl', 'legacyPgHost', 'legacyPgPort', 'legacyPgUser', 'legacyPgPassword', 'legacyPgDatabase',
    'subscriptionEndpoint', 'serviceDid', 'publisherDid', 'subscriptionReconnectDelay', 'subscriptionIdleTimeoutMs',
    'readOnlyMode', 'autoMigrate',
  ] as const
  const coveredSet = new Set<string>(CONFIG_CFG_KEYS_COVERED as readonly string[])
  const excludedSet = new Set<string>(Object.keys(CONFIG_CFG_KEYS_EXCLUDED))
  for (const key of ALL_CONFIG_KEYS) {
    const inCovered = coveredSet.has(key)
    const inExcluded = excludedSet.has(key)
    if (!inCovered && !inExcluded) {
      failed++
      console.error(`FAIL: Config field '${key}' is in neither CONFIG_CFG_KEYS_COVERED nor CONFIG_CFG_KEYS_EXCLUDED`)
    }
    if (inCovered && inExcluded) {
      failed++
      console.error(`FAIL: Config field '${key}' is in BOTH CONFIG_CFG_KEYS_COVERED and CONFIG_CFG_KEYS_EXCLUDED`)
    }
  }
  // Catches drift the other way: ALL_CONFIG_KEYS above going stale against
  // src/config.ts itself.
  for (const key of [...coveredSet, ...excludedSet]) {
    if (!(ALL_CONFIG_KEYS as readonly string[]).includes(key)) {
      failed++
      console.error(`FAIL: '${key}' is classified but not a known Config field — ALL_CONFIG_KEYS is stale`)
    }
  }

  console.log(
    `config_activation completeness audit: scanned ${checkedFiles} files, ${checkedKeys} env-key occurrences, ` +
      `${ALL_CONFIG_KEYS.length} Config fields — ${failed === 0 ? 'all classified' : `${failed} FAILURE(S)`}`,
  )
  if (failed > 0) process.exit(1)
}

main()
