import assert from 'assert'
import fs from 'fs'
import path from 'path'

const source = fs.readFileSync(
  path.resolve(__dirname, '../src/util/catalog-listener.ts'),
  'utf8',
)

assert.match(
  source,
  /catch \(err\) \{\s+await client\.end\(\)\.catch/,
  'failed LISTEN startup must close its connected client before retrying',
)

console.log('catalog listener failed-start cleanup: PASS')
