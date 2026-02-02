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

# Upload to dockerhub (for Contributors)

``` bash
docker image push jbgruber/bsky-feedgen:latest
```

## Fork changelog (pmmendoza)

- **2026-02-02** — Fix `updateEngagement()` Postgres bind-parameter overflow by chunking large URI lists in engagement/comment rollups. This prevents `08P01` crashes and allows `post.likes_count/repost_count/comments_count` to refresh correctly (branch `fix/update-engagement-parameter-limit`, commit `11ad8aa6b63ebae4f62d439b3c5f22304f87fe0f`).
- **Validation** — Deployed and verified on `newsflowsserver1` (188.34.141.44): `POST /api/update-engagement` completed (76,054 posts processed), `08P01` errors stopped, and “smoking gun” test URIs updated `likes_count 0 → 1` (see internal deployment audit: `NEWSFLOWS/health-checks/dev/audit/feedgen_pr1_verification_20260202_151937Z/DEPLOYMENT_AUDIT_REPORT.md`).
- **2026-02-02** — Fix firehose cursor persistence by changing `sub_state` cursor writes from `UPDATE` to an UPSERT (`INSERT … ON CONFLICT DO UPDATE`), plus add minimal logs indicating whether we start from a stored cursor and when cursors are persisted (branch `hotfix/cursor-upsert`).
