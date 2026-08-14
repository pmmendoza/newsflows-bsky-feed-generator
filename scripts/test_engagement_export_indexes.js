const fs = require('fs')
const path = require('path')

const source = fs.readFileSync(
  path.join(__dirname, 'create_engagement_export_indexes.sh'),
  'utf8',
)
function assertIndex(name, expected) {
  const match = source.match(new RegExp(
    `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${name}[\\s\\S]*?;`,
    'g',
  ))
  if (!match || match.length !== 1) {
    throw new Error(`expected exactly one concurrent ${name} index`)
  }
  const sql = match[0].replace(/\s+/g, ' ')
  for (const fragment of expected) {
    if (!sql.includes(fragment)) {
      throw new Error(`${name} missing: ${fragment}`)
    }
  }
}

assertIndex('post_comment_rootdid_createdat_idx', [
  'ON post (split_part("rootUri"::text, \'/\', 3), "createdAt")',
  'WHERE "rootUri" <> \'\'',
])
assertIndex('post_comment_rootdid_content_time_idx', [
  'ON post (split_part("rootUri"::text, \'/\', 3), content_time_utc)',
  'WHERE "rootUri" <> \'\'',
  "content_time_status = 'source_valid'",
])

console.log('engagement export index SQL shape passed')
