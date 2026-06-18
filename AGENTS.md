# Feedgen agent contract

This repository owns the NEWSFLOWS feed service implementation: scoped
firehose ingestion, subscription/follow storage, request logging, feed serving,
feedgen admin endpoints, and the runtime `feedgen_ops.feed_catalog`
materialization.

It does not own ecosystem planning truth. For publisher, feed, bot, ranker,
worker, health, secret-ref, Docker, path, or rebuild expectations, start from
the BSKY root catalogs:

- `config/newsflows/catalogs/publishers.yml`
- `config/newsflows/catalogs/host_topology.yml`
- `docs/ecosystem_principles.md`
- `dev/TARGET_STATE.md`

Treat `feedgen_ops.feed_catalog` as feedgen-owned serving/readback state. Use
`bskyops` desired-state parity plus feedgen admin dry-run/apply for feed
changes; do not hand-edit Postgres rows as planning truth.

Retired priority endpoints: `/api/prioritize` and `/api/priorities` return
`410 retired_endpoint`. Active ranker-priority feeds read
`ranker_prod.feed_current_priority.score`.

Production deploys must follow the BSKY root
`docs/runbooks/feedgen_production_deploy_runbook.md`. Do not use repo-local
`.env`, `docker-compose down`, volume deletion, or direct server edits for
normal app-only deploys.

Preserve user changes, check nested repo status before editing, and verify
with the smallest command bundle that proves the change.
