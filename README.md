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

Prioritize endpoint: <http://localhost:3000/api/prioritize/>


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

# Upload to dockerhub (for Contributors)

``` bash
docker image push jbgruber/bsky-feedgen:latest
```

## Fork changelog (pmmendoza)

- **2026-02-05** — Add additive engagement metadata for reply+quote collisions (`comment_root_uri`, `quote_subject_uri`, `publisher_target_any` + debug flags) and new `GET /api/compliance/engagement_legacy` endpoint (api-key protected). Optional legacy DB env vars added to `.env.example`.
- **2026-02-02** — Fix `updateEngagement()` Postgres bind-parameter overflow by chunking large URI lists in engagement/comment rollups. This prevents `08P01` crashes and allows `post.likes_count/repost_count/comments_count` to refresh correctly (branch `fix/update-engagement-parameter-limit`, commit `11ad8aa6b63ebae4f62d439b3c5f22304f87fe0f`).
- **Validation** — Deployed and verified on `newsflowsserver1` (188.34.141.44): `POST /api/update-engagement` completed (76,054 posts processed), `08P01` errors stopped, and “smoking gun” test URIs updated `likes_count 0 → 1` (see internal deployment audit: `NEWSFLOWS/health-checks/dev/audit/feedgen_pr1_verification_20260202_151937Z/DEPLOYMENT_AUDIT_REPORT.md`).
- **2026-02-02** — Fix firehose cursor persistence by changing `sub_state` cursor writes from `UPDATE` to an UPSERT (`INSERT … ON CONFLICT DO UPDATE`), plus add minimal logs indicating whether we start from a stored cursor and when cursors are persisted (branch `hotfix/cursor-upsert`).
- **Validation** — Deployed and verified on `newsflowsserver1` (188.34.141.44): `sub_state` is populated and the cursor advances continuously; after a container restart, feedgen resumes from the stored cursor instead of replaying from scratch (see internal deployment audit: `NEWSFLOWS/health-checks/dev/audit/feedgen_cursor_hotfix_verification_20260203_162152Z/DEPLOYMENT_AUDIT_REPORT.md`).
- **2026-02-03** — Add quote-post engagement capture (`engagement.type=3`) and `post.quote_count`, and include quotes in Feed 3 ranking (likes/reposts/comments/quotes weighted equally).
- **Scope note** — Quote counts are captured for publisher/newsbot posts and for subscriber-authored quotes; quote counts for follow-union posts may be incomplete by design (storage trade-off).
- **Verification note** — Quote delete semantics are not yet verified: deleting a quote post may or may not remove the derived `engagement.type=3` row yet (acceptable for now; document once confirmed).
