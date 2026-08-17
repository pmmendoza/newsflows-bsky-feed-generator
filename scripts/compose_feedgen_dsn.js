#!/usr/bin/env node
// Prints a PostgreSQL connection URL for the feedgen database, composed from the
// FEEDGEN_DB_* variables of the process environment (URL-encoding user and
// password), or FEEDGEN_POSTGRES_URL verbatim when it is already set. Used by
// scripts/content_time_revalidate_packet.sh inside the feedgen container so the
// DSN never appears on a command line or in evidence. Prints nothing else.
'use strict'
const env = process.env
if (env.FEEDGEN_POSTGRES_URL) {
  process.stdout.write(env.FEEDGEN_POSTGRES_URL)
  process.exit(0)
}
const missing = ['FEEDGEN_DB_USER', 'FEEDGEN_DB_PASSWORD', 'FEEDGEN_DB_HOST', 'FEEDGEN_DB_PORT', 'FEEDGEN_DB_DATABASE'].filter((k) => !env[k])
if (missing.length) {
  process.stderr.write(`compose_feedgen_dsn: missing ${missing.join(', ')}\n`)
  process.exit(2)
}
const u = encodeURIComponent
process.stdout.write(`postgresql://${u(env.FEEDGEN_DB_USER)}:${u(env.FEEDGEN_DB_PASSWORD)}@${env.FEEDGEN_DB_HOST}:${env.FEEDGEN_DB_PORT}/${env.FEEDGEN_DB_DATABASE}`)
