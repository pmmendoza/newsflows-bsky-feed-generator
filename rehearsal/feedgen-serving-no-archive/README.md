# Feedgen Serving No-Archive Rehearsal Profile

This profile is the first minimal runtime rehearsal target for feedgen. It is
only for disposable loopback roots and disposable Postgres databases. It must
not be applied to production or to a restored production database.

## Purpose

- Prove that feedgen can start from a canonical, feedgen-owned minimal serving
  schema without firehose ingestion, archive worker, ranker, bots, public Caddy,
  production data, or raw secrets.
- Exercise catalog-driven describe and chronological feed serving from
  `feedgen_ops.feed_catalog`.
- Keep `ranker_prod.*`, `feedgen_ops.archive_outbox`, and `research_archive.*`
  absent for this first profile.

## Required Environment

Use a disposable database and loopback listener.

```sh
export FEEDGEN_READ_ONLY_MODE=true
export FEEDGEN_ARCHIVE_OUTBOX_ENABLED=false
export FEEDGEN_DESCRIBE_FROM_CATALOG=true
export ENGAGEMENT_TIME_HOURS=72
export FEEDGEN_POSTGRES_URL=postgres://...
export FEEDGEN_HOSTNAME=localhost
export FEEDGEN_LISTENHOST=127.0.0.1
export FEEDGEN_PORT=3021
export FEEDGEN_SERVICE_DID=did:web:localhost
export FEEDGEN_PUBLISHER_DID=did:plc:loopback-publisher
```

`FEEDGEN_READ_ONLY_MODE=true` skips Kysely migrations, firehose, subscriber
CSV import, and schedulers. It does not make serving read-only: successful feed
requests still write `request_log` and `request_posts`. This profile therefore
creates those tables, but it does not create or write archive-outbox tables.

## Bootstrap Inputs

Run these files only against a disposable database. Both files require a
session-level guard so accidental production execution fails:

```sh
export PGOPTIONS="-c newsflows.allow_rehearsal_schema_bootstrap=true -c newsflows.allow_rehearsal_seed=true"
psql "$FEEDGEN_POSTGRES_URL" -v ON_ERROR_STOP=1 \
  -f rehearsal/feedgen-serving-no-archive/schema_bootstrap.sql \
  -f rehearsal/feedgen-serving-no-archive/seed_loopback.sql
```

The seed creates:

- one enabled chronological feed row for `newsflow-nl-1`;
- one subscriber requester DID;
- two followed DIDs;
- one publisher post and two followed-account posts.

## Expected Proof

Use a Bearer JWT whose decoded payload has:

```json
{"iss":"did:plc:loopback-requester"}
```

Then request:

```text
at://did:web:localhost/app.bsky.feed.generator/newsflow-nl-1
```

Expected result for `limit=3`:

- one publisher post first;
- two followed-account posts next;
- one `request_log` row;
- three `request_posts` rows;
- zero dependency on `ranker_prod.*`, `feedgen_ops.archive_outbox`, or
  `research_archive.*`.

## Exclusion Gates

This profile is invalid if any of the following are required for the proof:

- enabled `ranker-priority` feed rows;
- archive-worker process;
- `FEEDGEN_ARCHIVE_OUTBOX_ENABLED=true`;
- public Caddy/TLS;
- bot posting or FreshRSS;
- production secret material;
- production database or volume data.
