# Feed Catalog Admin API Deploy/Rollback Runbook

Status: deployment planning note; no deploy performed
Date: 2026-05-10

This runbook covers deploying the feed catalog admin API hardening that
adds apply-grade update evidence and `if_current` stale-state protection.

## Current Deploy Surfaces

- Git remote for the active fork: `origin = https://github.com/pmmendoza/newsflows-bsky-feed-generator.git`
- Upstream remote: `upstream = https://github.com/JBGruber/newsflows-bsky-feed-generator.git`
- Current live deployment image family: `pmmendoza/bsky-feedgen`.
- Live server check on 2026-05-10:
  `/home/philipp/newsflows-bsky-feed-generator-v2/docker-compose.yml`
  uses `pmmendoza/bsky-feedgen:sprint16-r1-score-only-2026-05-06`, and
  the running `feedgen` container uses that same image.
- Warning: the tracked compose files in this checkout still contain stale
  `image: jbgruber/bsky-feedgen` defaults. Do not deploy those image lines
  unchanged. The server/deploy records since early May use
  `pmmendoza/bsky-feedgen:*`.
- Dockerfile: multi-stage Node 18 build in `Dockerfile`
- Compose files:
  - `docker-compose.yml`: canonical service name/container `feedgen`, host port `3020`
  - `docker-compose-pr2-v2.yml`: alternate `feedgen-v2`, host port `127.0.0.1:3021`
- No GitHub Actions workflow was present in this checkout, so Docker Hub
  publishing appears to be manual unless another repo-side workflow exists
  outside this working tree.

## Pre-Deploy Checks

```sh
cd /path/to/newsflows-bsky-feed-generator
git status --short
yarn test:feed-catalog-admin
yarn build
yarn test:execute
docker build -t pmmendoza/bsky-feedgen:feed-catalog-admin-$(git rev-parse --short HEAD) .
```

Optional local compose smoke:

```sh
docker compose -f docker-compose.local.yml up -d --build feedgen
curl -fsS -H "api-key: $FEEDGEN_ADMIN_API_KEY" \
  http://127.0.0.1:3020/api/admin/feed_catalog >/tmp/feed_catalog.json
```

Do not paste raw API keys into shell history or docs.

## GitHub Release Path

Recommended:

```sh
git switch -c feed-catalog-admin-safe-apply
git add src/methods/feed-catalog-admin.ts scripts/test_feed_catalog_admin.ts appendices/
git commit -m "Harden feed catalog admin update response"
git push origin feed-catalog-admin-safe-apply
```

Open a PR against the active deployment branch. Include:

- test output,
- build output,
- Docker image tag if built,
- rollback section from this runbook,
- note that there is no schema migration.

## Docker Hub Path

If Docker Hub credentials and image ownership are available:

```sh
SHA="$(git rev-parse --short HEAD)"
docker build \
  -t pmmendoza/bsky-feedgen:${SHA} \
  -t pmmendoza/bsky-feedgen:feed-catalog-admin-safe-apply-${SHA} \
  .
docker push pmmendoza/bsky-feedgen:${SHA}
docker push pmmendoza/bsky-feedgen:feed-catalog-admin-safe-apply-${SHA}
```

Avoid moving `latest` until the server has passed smoke checks, or use a
server-local compose build instead of Docker Hub if that is the current
operator practice.

Headless Docker login:

```sh
printf '%s\n' "$DOCKERHUB_PAT" | docker login \
  --username pmmendoza \
  --password-stdin
```

Use a Docker Hub personal access token, not the account password. The
server does not need a browser for token-based login. If the Docker CLI
offers browser/device-code login, avoid it for this deployment and use
`--username ... --password-stdin` so the method is deterministic over SSH.

## Server Deploy

Before changing the container, record the current state:

```sh
cd /home/philipp/newsflows-bsky-feed-generator-v2
git rev-parse HEAD
sudo docker-compose --env-file /etc/newsflows/secrets/feedgen.env ps
docker image inspect pmmendoza/bsky-feedgen --format '{{.Id}} {{json .RepoTags}}'
sudo docker logs --tail=80 feedgen >/tmp/feedgen-predeploy.log
```

Backup the database before deploying even though this API hardening has no
schema migration:

```sh
mkdir -p /var/lib/newsflows/backups/feedgen
docker exec feedgen-db pg_dump \
  -U "$FEEDGEN_DB_USER" \
  -d "$FEEDGEN_DB_BASE" \
  -Fc \
  -f /tmp/feedgen-pre-feed-catalog-admin.dump
docker cp feedgen-db:/tmp/feedgen-pre-feed-catalog-admin.dump \
  /var/lib/newsflows/backups/feedgen/
```

Deploy from Git/build:

```sh
git fetch origin
git checkout <approved-commit>
docker compose build feedgen
FEEDGEN_BUILD_SHA="$(git rev-parse HEAD)" docker compose up -d feedgen
```

Or deploy from a pushed Docker Hub tag:

```sh
docker pull pmmendoza/bsky-feedgen:<approved-tag>
# Edit/override compose image tag to the approved tag.
sudo docker-compose --env-file /etc/newsflows/secrets/feedgen.env \
  up -d --no-deps --no-build feedgen
```

On `newsflowsserver1`, use `docker-compose` with a dash plus
`--env-file /etc/newsflows/secrets/feedgen.env`. The `docker compose`
plugin is not installed there, and running compose without the env file
falls back to blank/default DB credentials.

## Post-Deploy Smoke

```sh
sudo docker-compose --env-file /etc/newsflows/secrets/feedgen.env ps feedgen
sudo docker logs --tail=120 feedgen
curl -fsS -H "api-key: $FEEDGEN_ADMIN_API_KEY" \
  http://127.0.0.1:3020/api/admin/feed_catalog >/tmp/feed_catalog_after.json
curl -fsS -H "api-key: $FEEDGEN_ADMIN_API_KEY" \
  http://127.0.0.1:3020/api/admin/feed_catalog/newsflow-nl-1 \
  >/tmp/feed_catalog_show_after.json
```

For the apply primitive, test only against an explicitly non-production
feed or a local test target until operator approval exists. Production
mutation should go through `bskyops`, not a manual curl.

## Container Rollback

If deploy smoke fails:

```sh
docker compose logs --tail=200 feedgen >/tmp/feedgen-failed-deploy.log
git checkout <previous-known-good-commit>
docker compose build feedgen
FEEDGEN_BUILD_SHA="<previous-known-good-commit>" docker compose up -d feedgen
```

If using Docker Hub tags:

```sh
docker pull pmmendoza/bsky-feedgen:<previous-known-good-tag>
# Restore compose image tag to previous-known-good-tag.
sudo docker-compose --env-file /etc/newsflows/secrets/feedgen.env \
  up -d --no-deps --no-build feedgen
```

Then re-run post-deploy smoke.

## Feed Catalog Mutation Rollback

The API deploy itself has no schema migration and should not require DB
rollback. If a later operator feed update is applied incorrectly, use the
`rollback.fields` returned by the dry-run/apply evidence:

```json
{
  "op": "update",
  "rkey": "newsflow-nl-1",
  "enabled": true,
  "if_current": {
    "enabled": false
  }
}
```

For broader corruption, restore from the pre-deploy database dump only
after explicit operator approval because that can discard legitimate live
activity.

## Stop Conditions

- Do not deploy from a dirty worktree.
- Do not move Docker Hub `latest` without smoke evidence.
- Do not run production catalog mutation by hand for this rollout.
- Do not write raw API keys to commands, docs, logs, run artifacts, or PR
  text.
- Stop and rollback if feedgen fails to start, admin reads fail, feed
  skeleton requests fail, or logs show repeated feed catalog errors.
