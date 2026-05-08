#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "error=run_with_sudo"
  echo "hint=sudo bash scripts/smoke_compliance_activity.sh"
  exit 2
fi

BASE="${BASE:-http://127.0.0.1:3020}"
DAYS="${DAYS:-2}"
LIMIT="${LIMIT:-5}"
ENV_FILE="${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}"
DB_CONTAINER="${DB_CONTAINER:-feedgen-db}"
APP_CONTAINER="${APP_CONTAINER:-feedgen}"

if [ ! -r "$ENV_FILE" ]; then
  echo "error=env_file_not_readable"
  echo "env_file=$ENV_FILE"
  exit 2
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

READ_KEY="${FEEDGEN_READ_API_KEY:-${FEEDGEN_MONITOR_API_KEY:-${FEEDGEN_RANKER_API_KEY:-${FEEDGEN_ADMIN_API_KEY:-}}}}"

require_var() {
  local name="$1"
  local value="$2"
  if [ -z "$value" ]; then
    echo "error=missing_$name"
    exit 2
  fi
}

require_var STUDY_TOKEN_API_KEY "${STUDY_TOKEN_API_KEY:-}"
require_var STUDY_JWT_SECRET "${STUDY_JWT_SECRET:-}"
require_var READ_KEY "$READ_KEY"

psql_at() {
  docker exec "$DB_CONTAINER" sh -lc "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Atc \"$1\""
}

DID="${DID:-$(psql_at "select requester_did from request_log where timestamp >= now() - interval '$DAYS days' group by requester_did order by max(timestamp) desc limit 1")}"
if [ -z "$DID" ]; then
  DID="$(psql_at "select did from subscriber limit 1")"
fi
require_var DID "$DID"

tmp_dir="$(mktemp -d /tmp/feedgen-compliance-activity-smoke.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

json_get_key() {
  python3 -c 'import json,sys; print(json.load(sys.stdin).get(sys.argv[1], ""))' "$1"
}

json_shape() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(sys.argv[1] + "=" + ",".join(sorted(data.keys())))' "$1"
}

expect_status() {
  local label="$1"
  local actual="$2"
  local expected="$3"
  echo "$label=$actual"
  if [ "$actual" != "$expected" ]; then
    echo "error=unexpected_status label=$label expected=$expected actual=$actual"
    exit 1
  fi
}

bad_token_key_status="$(
  curl -sS -o "$tmp_dir/bad_token_key.json" -w '%{http_code}' \
    -X POST "$BASE/api/study/token" \
    -H 'content-type: application/json' \
    -H 'api-key: wrong' \
    --data "{\"did\":\"$DID\"}"
)"
expect_status bad_token_key_status "$bad_token_key_status" 401

token_status="$(
  curl -sS -o "$tmp_dir/token.json" -w '%{http_code}' \
    -X POST "$BASE/api/study/token" \
    -H 'content-type: application/json' \
    -H "api-key: $STUDY_TOKEN_API_KEY" \
    --data "{\"did\":\"$DID\"}"
)"
expect_status token_status "$token_status" 200
TOKEN="$(json_get_key token < "$tmp_dir/token.json")"
require_var TOKEN "$TOKEN"

summary_status="$(
  curl -sS -o "$tmp_dir/summary.json" -w '%{http_code}' \
    "$BASE/api/study/compliance-summary" \
    -H "authorization: Bearer $TOKEN"
)"
expect_status summary_status "$summary_status" 200

activity_status="$(
  curl -sS -o "$tmp_dir/activity.json" -w '%{http_code}' \
    "$BASE/api/study/compliance-activity-summary" \
    -H "authorization: Bearer $TOKEN"
)"
expect_status activity_status "$activity_status" 200
json_shape activity_keys < "$tmp_dir/activity.json"

bad_jwt_status="$(
  curl -sS -o "$tmp_dir/bad_jwt.json" -w '%{http_code}' \
    "$BASE/api/study/compliance-activity-summary" \
    -H 'authorization: Bearer not-a-jwt'
)"
expect_status bad_jwt_status "$bad_jwt_status" 401

wrong_scope_token="$(
  docker exec "$APP_CONTAINER" node -e "const jwt=require('jsonwebtoken'); console.log(jwt.sign({sub:process.argv[1], scope:'wrong'}, process.env.STUDY_JWT_SECRET, {algorithm:'HS256', issuer:'newsflows-bsky-feed-generator', audience:'newsflows-study', expiresIn:60}))" "$DID"
)"
wrong_scope_status="$(
  curl -sS -o "$tmp_dir/wrong_scope.json" -w '%{http_code}' \
    "$BASE/api/study/compliance-activity-summary" \
    -H "authorization: Bearer $wrong_scope_token"
)"
expect_status wrong_scope_status "$wrong_scope_status" 403

expired_token="$(
  docker exec "$APP_CONTAINER" node -e "const jwt=require('jsonwebtoken'); console.log(jwt.sign({sub:process.argv[1], scope:'compliance:read'}, process.env.STUDY_JWT_SECRET, {algorithm:'HS256', issuer:'newsflows-bsky-feed-generator', audience:'newsflows-study', expiresIn:-1}))" "$DID"
)"
expired_status="$(
  curl -sS -o "$tmp_dir/expired.json" -w '%{http_code}' \
    "$BASE/api/study/compliance-activity-summary" \
    -H "authorization: Bearer $expired_token"
)"
expect_status expired_status "$expired_status" 401

admin_bad_status="$(
  curl -sS -o "$tmp_dir/admin_bad.json" -w '%{http_code}' \
    "$BASE/api/compliance/activity?scope=feed_posts&subscriber_did=$DID&limit=$LIMIT&days=$DAYS" \
    -H 'api-key: wrong'
)"
expect_status admin_bad_status "$admin_bad_status" 401

admin_missing_subscriber_status="$(
  curl -sS -o "$tmp_dir/admin_missing_subscriber.json" -w '%{http_code}' \
    "$BASE/api/compliance/activity?scope=feed_posts&limit=$LIMIT&days=$DAYS" \
    -H "api-key: $READ_KEY"
)"
expect_status admin_missing_subscriber_status "$admin_missing_subscriber_status" 400

admin_status="$(
  curl -sS -o "$tmp_dir/admin.json" -w '%{http_code}' \
    "$BASE/api/compliance/activity?scope=feed_posts&subscriber_did=$DID&limit=$LIMIT&days=$DAYS" \
    -H "api-key: $READ_KEY"
)"
expect_status admin_status "$admin_status" 200
json_shape admin_keys < "$tmp_dir/admin.json"

engagement_status="$(
  curl -sS -o "$tmp_dir/engagement.json" -w '%{http_code}' \
    "$BASE/api/compliance/engagement?subscriber_did=$DID&limit=1" \
    -H "api-key: $READ_KEY"
)"
expect_status engagement_status "$engagement_status" 200

compliance_status="$(
  curl -sS -o "$tmp_dir/compliance.json" -w '%{http_code}' \
    "$BASE/api/compliance?user_did=$DID&min_date=$(date -u -d "$DAYS days ago" +%Y-%m-%dT%H:%M:%SZ)" \
    -H "api-key: $READ_KEY"
)"
expect_status compliance_status "$compliance_status" 200

cursor_status="$(
  curl -sS -o "$tmp_dir/cursor_page1.json" -w '%{http_code}' \
    "$BASE/api/compliance/activity?scope=feed_posts&subscriber_did=$DID&limit=1&days=$DAYS" \
    -H "api-key: $READ_KEY"
)"
expect_status cursor_page1_status "$cursor_status" 200
next_cursor="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); print(data.get("next_cursor") or "")' "$tmp_dir/cursor_page1.json")"
if [ -n "$next_cursor" ]; then
  cursor_mismatch_status="$(
    curl -sS -o "$tmp_dir/cursor_mismatch.json" -w '%{http_code}' \
      "$BASE/api/compliance/activity?scope=feed_posts&subscriber_did=$DID&limit=2&days=$DAYS&cursor=$next_cursor" \
      -H "api-key: $READ_KEY"
  )"
  expect_status cursor_mismatch_status "$cursor_mismatch_status" 400
else
  echo "cursor_mismatch_status=skipped_no_next_cursor"
fi

empty_did="$(psql_at "select s.did from subscriber s where not exists (select 1 from request_log rl where rl.requester_did = s.did and rl.timestamp >= now() - interval '$DAYS days') limit 1")"
if [ -n "$empty_did" ]; then
  empty_token_status="$(
    curl -sS -o "$tmp_dir/empty_token.json" -w '%{http_code}' \
      -X POST "$BASE/api/study/token" \
      -H 'content-type: application/json' \
      -H "api-key: $STUDY_TOKEN_API_KEY" \
      --data "{\"did\":\"$empty_did\"}"
  )"
  expect_status empty_token_status "$empty_token_status" 200
  empty_token="$(json_get_key token < "$tmp_dir/empty_token.json")"
  empty_activity_status="$(
    curl -sS -o "$tmp_dir/empty_activity.json" -w '%{http_code}' \
      "$BASE/api/study/compliance-activity-summary" \
      -H "authorization: Bearer $empty_token"
  )"
  expect_status empty_activity_status "$empty_activity_status" 200
  python3 - "$tmp_dir/empty_activity.json" <<'PY'
import json
import sys
with open(sys.argv[1]) as f:
    data = json.load(f)
counts = (
    data["retrievals"]["count"],
    data["engagements"]["feed_post_count"],
    data["engagements"]["publisher_post_count"],
    sum(data["engagements"]["by_type"].values()),
)
print("empty_counts=%s/%s/%s/%s" % counts)
if counts != (0, 0, 0, 0):
    raise SystemExit("empty activity counts were not all zero")
PY
else
  echo "empty_activity_status=skipped_no_candidate"
fi

retrieval_compare="$(psql_at "with recent as (select id, result_count from request_log where timestamp >= now() - interval '$DAYS days'), cmp as (select count(*) filter (where coalesce(result_count,0)>0) result_nonempty, count(*) filter (where exists (select 1 from request_posts rp where rp.request_id = recent.id)) posts_nonempty, count(*) filter (where (coalesce(result_count,0)>0) <> exists (select 1 from request_posts rp where rp.request_id = recent.id)) mismatches from recent) select result_nonempty || '/' || posts_nonempty || '/' || mismatches from cmp")"
echo "retrieval_compare_result_posts_mismatch=$retrieval_compare"
case "$retrieval_compare" in
  */*/0) ;;
  *)
    echo "error=retrieval_definition_mismatch"
    exit 1
    ;;
esac

docker logs --since=5m "$APP_CONTAINER" 2>&1 \
  | grep -E 'Study compliance activity summary|Compliance activity endpoint' \
  | tail -5 \
  | sed 's/requester=[^ ]*/requester=<redacted>/; s/subscriber=[^ ]*/subscriber=<redacted>/' || true

echo "smoke_result=pass"
