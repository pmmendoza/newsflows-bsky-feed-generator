# Feedgen Serving No-Archive Rehearsal Profile

This profile is the first minimal runtime rehearsal target for feedgen. It is
only for disposable loopback roots and disposable Postgres databases. It must
not be applied to production or to a restored production database.
Do not use the tracked compose files as-is for this proof; they are development
or deploy shapes and can inherit live endpoints or local subscriber files.

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

## Startup / Migration, Source-Build, And Synthetic Ingest Variants

The first proof can run with `FEEDGEN_READ_ONLY_MODE=true` when the disposable DB
is already bootstrapped. A second incremental proof may start feedgen with
`FEEDGEN_READ_ONLY_MODE=false` against an empty disposable DB to prove Kysely
migrations and normal app startup before applying this profile's feed catalog
schema and seed data.

A stronger rebuild proof should build a temporary feedgen image from a fresh
clone of the feedgen repo, run that image against disposable Postgres, and then
delete the image. Do not rely on the already-running production image when the
claim is source rebuildability.

For that variant:

- point `FEEDGEN_SUBSCRIPTION_ENDPOINT` at a non-live loopback endpoint such as
  `ws://127.0.0.1:9`;
- use a long `FEEDGEN_SUBSCRIPTION_RECONNECT_DELAY`;
- keep the DB subscriber-free until after startup, so the immediate follows
  scheduler cannot call the public follows API;
- apply `schema_bootstrap.sql` and `seed_loopback.sql` only after the app is
  reachable;
- verify `kysely_migration` rows, synthetic feed serving, and absence of
  archive/ranker/research schemas.

This variant proves startup and migration boundaries. The source-build variant
also proves that the committed feedgen source can produce a runnable image for
this minimal profile.

A further synthetic-ingest proof can run from the same fresh source-built image
without connecting to the live firehose:

```sh
FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
FEEDGEN_SYNTHETIC_FIREHOSE_REHEARSAL=1 \
  npx ts-node scripts/test_firehose_ingest_synthetic.ts
```

Add scoped-ingestion fixtures with:

```sh
FEEDGEN_TEST_DSN='postgresql://feedgen:feedgen@localhost:5436/feedgen-db-staging' \
FEEDGEN_SYNTHETIC_FIREHOSE_REHEARSAL=1 \
FEEDGEN_SYNTHETIC_FIREHOSE_SCOPED=1 \
  npx ts-node scripts/test_firehose_ingest_synthetic.ts
```

In server Docker rehearsal, call
`dist/rehearsal/synthetic-firehose-ingest.js` through Node inside the temporary
image and pass a disposable Postgres DSN. The proof builds a lexicon-valid
CAR-backed `com.atproto.sync.subscribeRepos#commit`, invokes
`FirehoseSubscription.handleEvent()`, and expects one `post` row plus one
type-2 `engagement` row.

This proves the CAR decode and database write path for synthetic post/like
events. With `FEEDGEN_SYNTHETIC_FIREHOSE_SCOPED=1`, it also enables scoped
ingestion, creates minimal disposable `feedgen_ops.feed_catalog`,
`subscriber`, and `follows` fixtures, stores the allowlisted synthetic repo, and
asserts that a non-allowlisted synthetic repo is filtered out.

These variants still do not prove live WebSocket firehose transport, reconnect
behavior, cursor persistence, signed commit/MST validity, archive worker
behavior, bots/FreshRSS supply, ranker integration, public edge/TLS, protected
credentials, or production data restore.

## Synthetic DB Dump/Restore Rehearsal

Use `scripts/rehearse_feedgen_db_restore_synthetic.sh` to prove the bounded
logical backup/restore mechanism for this disposable no-archive profile. The
script is guarded and skips unless explicitly enabled:

```sh
FEEDGEN_DB_RESTORE_REHEARSAL=1 \
FEEDGEN_RESTORE_SYNTHETIC_ONLY=1 \
FEEDGEN_SOURCE_DSN='postgresql://feedgen:feedgen@source:5432/feedgen-db-staging' \
FEEDGEN_TARGET_DSN='postgresql://feedgen:feedgen@target:5432/feedgen-db-staging' \
  bash scripts/rehearse_feedgen_db_restore_synthetic.sh
```

The source and target DSNs must point at disposable synthetic Postgres
instances. The script creates a custom-format `pg_dump` from the source DB,
restores it into the target DB, and verifies raw-free counts for
`feedgen_ops.feed_catalog`, `subscriber`, `follows`, `post`, `request_log`, and
`request_posts`. It explicitly excludes `feedgen_ops.archive_outbox` and checks
that `ranker_prod` and `research_archive` are absent. This proves only the
logical dump/restore mechanism and the minimal warm-start shape for this
profile; it does not authorize or validate a production dump.

## Exclusion Gates

This profile is invalid if any of the following are required for the proof:

- enabled `ranker-priority` feed rows;
- archive-worker process;
- `FEEDGEN_ARCHIVE_OUTBOX_ENABLED=true`;
- public Caddy/TLS;
- bot posting or FreshRSS;
- production secret material;
- production database or volume data.
