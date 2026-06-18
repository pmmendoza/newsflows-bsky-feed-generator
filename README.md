# NEWSFLOWS Feed Generator

Feedgen owns feed serving, scoped firehose ingestion, subscription/follow
storage, request logging, and the runtime `feedgen_ops.feed_catalog`
materialization. It does not own ecosystem planning truth.

Before changing publisher, feed, bot, ranker, health, secret-ref, Docker, or
runtime-root expectations, start from the BSKY root SSOTs:

- `config/newsflows/catalogs/publishers.yml` for publisher/feed/bot/ranker/
  health/secret-ref desired state.
- `config/newsflows/catalogs/host_topology.yml` for install/runtime/deploy
  topology.
- `docs/ecosystem_principles.md` for requirement classes, owner gates,
  rebuildability, and raw-free evidence rules.

Treat `feedgen_ops.feed_catalog` as feedgen-owned serving/readback state. Use
`bskyops` desired-state parity plus feedgen admin dry-run/apply for feed
changes; do not hand-edit Postgres rows as planning truth.

Retired priority endpoints: `/api/prioritize` and `/api/priorities` return
`410 retired_endpoint`. Ranker-priority feeds read
`ranker_prod.feed_current_priority.score`.

## Local Development Notes

To import subscribers, create csv file subscribers.csv in the project directory. Should have columns handle and did, supply either, the other will be looked up.

Start Feed generator (Terminal 1):

```bash
yarn start
```

Browser <http://localhost:3020/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:toz4no26o2x4vsbum7cp4bxp/app.bsky.feed.generator/newsflow-nl-1>

Make https address (Terminal 2):

```bash
ngrok http --url=vast-frank-mink.ngrok-free.app http://localhost:3020
```

Check `Forwarding` address and change to e.g.:

<https://vast-frank-mink.ngrok-free.app/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:toz4no26o2x4vsbum7cp4bxp/app.bsky.feed.generator/newsflow-nl-1>

Publish to Bluesky (Terminal 2):

```bash
yarn publishFeed
```

Subscription endpoint: <http://localhost:3000/api/subscribe?handle=news-flows-nl.bsky.social>

## Central Feed Catalog Ops

The non-secret desired state for publisher accounts, active and
expected-disabled feed rows, bot runtime rows, BSR expectations, health
expectations, and secret refs lives in the BSKY root catalog:

```text
config/newsflows/catalogs/publishers.yml
```

Feedgen owns the runtime materialization in `feedgen_ops.feed_catalog`. Treat
that table as serving/readback state, not a separate planning SSOT. Operator
changes should flow through `bskyops` parity and feedgen admin dry-run/apply:

```bash
bskyops ecosystem desired-state feedgen-parity --active-only --json
bskyops ecosystem desired-state feedgen-sync-packet --active-only --json
bskyops feed set <rkey> --dry-run --json
bskyops feed apply --dry-run-json <packet.json> --environment <target> --confirm-target <target>
```

Do not hand-edit feed rows in Postgres. Existing-row changes must use
`if_current` stale-state protection and readback; new active feed rows remain
feedgen-owner insert tasks with publication and health assumption checks.


## Build Image

1. Clone the repository and navigate to folder:

``` bash
git clone https://github.com/JBGruber/newsflows-bsky-feed-generator.git
cd newsflows-bsky-feed-generator
```

2. Build without caching to pull the newest version of all packages from GitHub:

``` bash
docker-compose down && \
  docker-compose build --no-cache && \
  docker-compose up -d
```

### Production note (docker-compose-deploy.yml)

On `newsflowsserver1`, we use a separate `docker-compose-deploy.yml` (gitignored) that **explicitly lists** environment variables instead of inheriting the full `.env`.

When adding new env vars (e.g. `STUDY_*` for the study endpoints), update **both**:
- `docker-compose.yml` (tracked, for dev/portable deploys)
- `docker-compose-deploy.yml` (server-only)

## Historical DockerHub Upload Note

``` bash
docker image push jbgruber/bsky-feedgen:latest
```

Do not use `latest` or maintainer-personal tags for NEWSFLOWS production
deploys. Production image refs, compose roots, and rebuild recipes are declared
in the BSKY root `host_topology.yml`; feedgen production deploys follow
`docs/runbooks/feedgen_production_deploy_runbook.md` from the BSKY root repo.

## Fork changelog (pmmendoza)

- **2026-06-08** — Document central desired-state feed catalog operations.
  Feedgen `feedgen_ops.feed_catalog` is the feedgen-owned runtime
  materialization/readback surface for `config/newsflows/catalogs/publishers.yml`;
  operator changes go through `bskyops` parity plus feedgen admin dry-run/apply
  evidence.
- **2026-05-02** — Repair old compliance engagement export performance for recent server-operations windows. `GET /api/compliance/engagement?scope=publisher&include_other_subscriber_activity=true` now pushes publisher/non-publisher target filtering into the base SQL, `scripts/create_engagement_export_indexes.sh` includes `post_comment_rooturi_createdat_idx` for comment-root lookups, and `/api/compliance/activity?scope=publisher_posts` uses the same target pushdown. Server smoke on `feedgen-v2`: old endpoint returned `200` for 1-day/7-day/30-day windows in sub-second time. Retention was not changed.
- **2026-05-02** — Add participant-safe compliance activity summary and bounded admin compliance activity endpoint. Canonical endpoint reference: `/Users/pm/Work/VUPD/projects/BSKY/docs/api_endpoints.md`. Feedgen-local pointer: [`appendices/compliance_activity_endpoints.md`](appendices/compliance_activity_endpoints.md). Server smoke helper: [`scripts/smoke_compliance_activity.sh`](scripts/smoke_compliance_activity.sh).
- **2026-04-30** — Add scoped API-key support for feedgen endpoints. Endpoint families now use `FEEDGEN_PRIORITY_API_KEY`, `FEEDGEN_RANKER_API_KEY`, `FEEDGEN_MONITOR_API_KEY`/`FEEDGEN_READ_API_KEY`, `FEEDGEN_ADMIN_API_KEY`, and `STUDY_TOKEN_API_KEY`; the legacy `PRIORITIZE_API_KEY` fallback was removed from active feedgen auth.
- **2026-02-06** — Improve `GET /api/compliance/engagement` performance for subscriber-oriented scopes. Changes: push engagement `type` + subscriber filtering into the base CTE path in `src/methods/monitor.ts`, skip comment-branch work when `types` excludes comments, and add structured per-request `duration_ms` logging. Add/standardize index helper script `scripts/create_engagement_export_indexes.sh` for `engagement(author, "createdAt")` and `post(author, "createdAt") WHERE "rootUri" <> ''` (with configurable lock timeout). Verification: `scope=subscriber` and `scope=subscriber_on_publisher` dropped from timeout-risk (~60s) to millisecond-level responses after index validity; `scope=publisher` remains a separate slow path.
- **2026-02-05** — Add additive engagement metadata for reply+quote collisions (`comment_root_uri`, `quote_subject_uri`, `publisher_target_any` + debug flags) and new `GET /api/compliance/engagement_legacy` endpoint (api-key protected). Optional legacy DB env vars added to `.env.example`.
- **2026-02-02** — Fix `updateEngagement()` Postgres bind-parameter overflow by chunking large URI lists in engagement/comment rollups. This prevents `08P01` crashes and allows `post.likes_count/repost_count/comments_count` to refresh correctly (branch `fix/update-engagement-parameter-limit`, commit `11ad8aa6b63ebae4f62d439b3c5f22304f87fe0f`).
- **Validation** — Deployed and verified on `newsflowsserver1` (188.34.141.44): `POST /api/update-engagement` completed (76,054 posts processed), `08P01` errors stopped, and "smoking gun" test URIs updated `likes_count 0 → 1` (see internal deployment audit: `../health-checker/dev/audit/feedgen_pr1_verification_20260202_151937Z/DEPLOYMENT_AUDIT_REPORT.md`).
- **2026-02-02** — Fix firehose cursor persistence by changing `sub_state` cursor writes from `UPDATE` to an UPSERT (`INSERT … ON CONFLICT DO UPDATE`), plus add minimal logs indicating whether we start from a stored cursor and when cursors are persisted (branch `hotfix/cursor-upsert`).
- **Validation** — Deployed and verified on `newsflowsserver1` (188.34.141.44): `sub_state` is populated and the cursor advances continuously; after a container restart, feedgen resumes from the stored cursor instead of replaying from scratch (see internal deployment audit: `../health-checker/dev/audit/feedgen_cursor_hotfix_verification_20260203_162152Z/DEPLOYMENT_AUDIT_REPORT.md`).
- **2026-02-03** — Add quote-post engagement capture (`engagement.type=3`) and `post.quote_count`, and include quotes in Feed 3 ranking (likes/reposts/comments/quotes weighted equally).
- **Scope note** — Quote counts are captured for publisher/newsbot posts and for subscriber-authored quotes; quote counts for follow-union posts may be incomplete by design (storage trade-off).
- **Verification note** — Quote delete semantics are not yet verified: deleting a quote post may or may not remove the derived `engagement.type=3` row yet (acceptable for now; document once confirmed).
