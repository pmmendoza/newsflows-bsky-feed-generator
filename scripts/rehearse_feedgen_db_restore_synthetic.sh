#!/usr/bin/env bash
set -euo pipefail

if [[ "${FEEDGEN_DB_RESTORE_REHEARSAL:-}" != "1" ]]; then
  echo "SKIP: set FEEDGEN_DB_RESTORE_REHEARSAL=1, FEEDGEN_SOURCE_DSN, and FEEDGEN_TARGET_DSN"
  exit 0
fi

if [[ "${FEEDGEN_RESTORE_SYNTHETIC_ONLY:-}" != "1" ]]; then
  echo "ERROR: set FEEDGEN_RESTORE_SYNTHETIC_ONLY=1; this script is only for disposable synthetic rehearsal DBs" >&2
  exit 2
fi

if [[ -z "${FEEDGEN_SOURCE_DSN:-}" || -z "${FEEDGEN_TARGET_DSN:-}" ]]; then
  echo "ERROR: FEEDGEN_SOURCE_DSN and FEEDGEN_TARGET_DSN are required" >&2
  exit 2
fi

for tool in pg_dump pg_restore psql; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: required tool not found: $tool" >&2
    exit 2
  fi
done

dump_path="${FEEDGEN_RESTORE_DUMP_PATH:-}"
cleanup_dump=0
if [[ -z "$dump_path" ]]; then
  dump_path="$(mktemp "${TMPDIR:-/tmp}/feedgen-db-restore-XXXXXX.dump")"
  cleanup_dump=1
fi

cleanup() {
  if [[ "$cleanup_dump" == "1" ]]; then
    rm -f "$dump_path"
  fi
}
trap cleanup EXIT

readonly source_dsn="$FEEDGEN_SOURCE_DSN"
readonly target_dsn="$FEEDGEN_TARGET_DSN"

pg_dump \
  --format=custom \
  --no-owner \
  --no-acl \
  --schema=feedgen_ops \
  --schema=public \
  --exclude-table=feedgen_ops.archive_outbox \
  --file="$dump_path" \
  "$source_dsn"

psql "$target_dsn" -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
DROP SCHEMA IF EXISTS feedgen_ops CASCADE;
DROP SCHEMA IF EXISTS public CASCADE;
SQL

pg_restore \
  --no-owner \
  --no-acl \
  --dbname="$target_dsn" \
  "$dump_path"

summary="$(
  psql "$target_dsn" -v ON_ERROR_STOP=1 -At <<'SQL'
SELECT 'feed_catalog=' || COUNT(*) FROM feedgen_ops.feed_catalog;
SELECT 'subscriber=' || COUNT(*) FROM public.subscriber;
SELECT 'follows=' || COUNT(*) FROM public.follows;
SELECT 'post=' || COUNT(*) FROM public.post;
SELECT 'request_log=' || COUNT(*) FROM public.request_log;
SELECT 'request_posts=' || COUNT(*) FROM public.request_posts;
SELECT 'archive_outbox_regclass=' || (to_regclass('feedgen_ops.archive_outbox') IS NOT NULL);
SELECT 'ranker_schema=' || EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'ranker_prod');
SELECT 'research_archive_schema=' || EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'research_archive');
SQL
)"

required_patterns=(
  '^feed_catalog=[1-9][0-9]*$'
  '^subscriber=[1-9][0-9]*$'
  '^follows=[1-9][0-9]*$'
  '^post=[1-9][0-9]*$'
  '^archive_outbox_regclass=false$'
  '^ranker_schema=false$'
  '^research_archive_schema=false$'
)

for pattern in "${required_patterns[@]}"; do
  if ! grep -Eq "$pattern" <<<"$summary"; then
    echo "ERROR: restore verification missing pattern $pattern" >&2
    echo "$summary" >&2
    exit 1
  fi
done

dump_entries="$(
  pg_restore --list "$dump_path" \
    | awk '
      /TABLE DATA/ || /TABLE / || /SCHEMA / || /SEQUENCE / || /INDEX / {
        print
      }
    ' \
    | sed -E 's/^[[:space:]]*[0-9]+;[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]+//'
)"

echo "status=ok"
echo "mode=synthetic_no_archive_dump_restore"
echo "dump_format=custom"
echo "raw_values_in_output=false"
echo "$summary"
echo "dump_entry_count=$(wc -l <<<"$dump_entries" | tr -d ' ')"
