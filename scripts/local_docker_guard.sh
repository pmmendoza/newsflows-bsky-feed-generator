#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/local_docker_guard.sh [options]

Starts the local Docker stack and stops it if the Postgres data dir exceeds a size limit.

Options:
  --compose-file <path>   Compose file to use (default: docker-compose.local.yml)
  --db-container <name>   Postgres container name (default: feedgen-db)
  --max-gb <int>          Stop if DB dir is >= this many GiB (default: 2)
  --poll-seconds <int>    Poll interval in seconds (default: 30)
  --duration-seconds <int> Stop after this many seconds (default: 0 = run until limit/CTRL-C)
  --no-build              Skip image rebuild (uses existing images)
EOF
}

COMPOSE_FILE="docker-compose.local.yml"
DB_CONTAINER="feedgen-db"
MAX_GB=2
POLL_SECONDS=30
DURATION_SECONDS=0
NO_BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compose-file)
      COMPOSE_FILE="${2:?missing value for --compose-file}"
      shift 2
      ;;
    --db-container)
      DB_CONTAINER="${2:?missing value for --db-container}"
      shift 2
      ;;
    --max-gb)
      MAX_GB="${2:?missing value for --max-gb}"
      shift 2
      ;;
    --poll-seconds)
      POLL_SECONDS="${2:?missing value for --poll-seconds}"
      shift 2
      ;;
    --duration-seconds)
      DURATION_SECONDS="${2:?missing value for --duration-seconds}"
      shift 2
      ;;
    --no-build)
      NO_BUILD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Compose file not found: $COMPOSE_FILE" >&2
  echo "Tip: run from the repo root (newsflows-bsky-feed-generator/)." >&2
  exit 1
fi

max_bytes=$((MAX_GB * 1024 * 1024 * 1024))
start_ts="$(date +%s)"

compose_up_args=(-f "$COMPOSE_FILE" up -d)
if [[ "$NO_BUILD" -eq 0 ]]; then
  compose_up_args+=(--build)
fi

cleanup() {
  docker compose -f "$COMPOSE_FILE" down >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker compose "${compose_up_args[@]}" >/dev/null

echo "Running stack (compose=$COMPOSE_FILE). Will stop if DB >= ${MAX_GB}GiB."

while true; do
  sleep "$POLL_SECONDS"

  # du -sk is widely supported; output is "<kilobytes> <path>"
  size_kb="$(docker exec "$DB_CONTAINER" sh -lc "du -sk /var/lib/postgresql/data 2>/dev/null | awk '{print \\$1}'" || true)"
  if [[ -z "${size_kb}" ]]; then
    echo "WARN: couldn't read DB size yet (container not ready?). Retrying..." >&2
    continue
  fi

  size_bytes=$((size_kb * 1024))
  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"
  printf "[%5ss] DB size: %.2f MiB\n" "$elapsed" "$(awk "BEGIN{print $size_bytes/1024/1024}")"

  if [[ "$size_bytes" -ge "$max_bytes" ]]; then
    echo "DB size exceeded limit (${MAX_GB}GiB). Stopping..."
    exit 0
  fi

  if [[ "$DURATION_SECONDS" -gt 0 && "$elapsed" -ge "$DURATION_SECONDS" ]]; then
    echo "Duration reached (${DURATION_SECONDS}s). Stopping..."
    exit 0
  fi
done

