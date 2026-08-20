#!/usr/bin/env node
'use strict'

const fs = require('fs')

function check(text, feedIds, clock, contract) {
  const blocks = text.split(/^--- run /m).slice(1)
  if (!blocks.length) throw new Error('no completed run blocks')

  return blocks.map((block) => {
    const run = block.split('\n', 1)[0]
    const lines = block.match(/\[ENGAGEMENT_EXPORT_CONTRACT_OK\][^\n]*/g) || []
    for (const feedId of feedIds) {
      const matches = lines.filter((line) => line.includes(`feed_id=${feedId} `))
      if (matches.length !== 1) {
        throw new Error(`run ${run}: ${feedId} positive contract lines=${matches.length}, expected 1`)
      }
      const line = matches[0]
      if (!line.includes('science_eligible=True') ||
          !line.includes(`time_clock='${clock}'`) ||
          !line.includes(`contract_version='${contract}'`)) {
        throw new Error(`run ${run}: ${feedId} positive contract values differ`)
      }
    }
    return `run ${run}: positive_contracts=${feedIds.length}/${feedIds.length} ok`
  })
}

if (process.argv[2] === '--self-test') {
  const line = "[ENGAGEMENT_EXPORT_CONTRACT_OK] feed_id=newsflow-nl-2 science_eligible=True time_clock='content_time_v1' contract_version='newsflows-content-time/v2'"
  const fixture = `--- run 2026-08-21T00:00:00Z\n${line}\n`
  if (check(fixture, ['newsflow-nl-2'], 'content_time_v1', 'newsflows-content-time/v2').length !== 1) process.exit(2)
  for (const bad of [fixture + line + '\n', fixture.replace('science_eligible=True', 'science_eligible=False')]) {
    try { check(bad, ['newsflow-nl-2'], 'content_time_v1', 'newsflows-content-time/v2'); process.exit(2) } catch (_) {}
  }
  console.log('contract gate self-test ok')
  process.exit(0)
}

const [, , path, feeds, clock, contract] = process.argv
try {
  console.log(check(fs.readFileSync(path, 'utf8'), feeds.split(','), clock, contract).join('\n'))
} catch (error) {
  console.error(error.message)
  process.exit(2)
}
