#!/usr/bin/env bash
set -euo pipefail

# Creates indexes needed by /api/compliance/engagement on large production DBs.
# Uses CREATE INDEX CONCURRENTLY to avoid blocking writes.

DB_CONTAINER="${DB_CONTAINER:-feedgen-db}"
DB_USER="${DB_USER:-feedgen}"
DB_NAME="${DB_NAME:-feedgen-db}"
LOCK_TIMEOUT="${LOCK_TIMEOUT:-0}"

echo "Creating engagement export indexes on container=${DB_CONTAINER} db=${DB_NAME} user=${DB_USER}"

docker exec -i "${DB_CONTAINER}" psql \
  -v ON_ERROR_STOP=1 \
  -v lock_timeout="${LOCK_TIMEOUT}" \
  -U "${DB_USER}" \
  -d "${DB_NAME}" <<'SQL'
SET statement_timeout = 0;
SELECT set_config('lock_timeout', :'lock_timeout', false);

CREATE INDEX CONCURRENTLY IF NOT EXISTS engagement_author_createdat_idx
  ON engagement (author, "createdAt");

CREATE INDEX CONCURRENTLY IF NOT EXISTS post_comment_author_createdat_idx
  ON post (author, "createdAt")
  WHERE "rootUri" <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS post_comment_rooturi_createdat_idx
  ON post ("rootUri", "createdAt")
  WHERE "rootUri" <> '';
SQL

echo "Index creation commands submitted successfully."
