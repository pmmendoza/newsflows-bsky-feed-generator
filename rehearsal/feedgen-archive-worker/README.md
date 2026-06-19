# Feedgen Archive-Worker Rehearsal Profile

This profile materializes the minimal archive/research database surfaces needed
to prove `node dist/archive-worker.js` against disposable Postgres.

It is a rebuild rehearsal artifact, not a production migration. Production
schema changes still go through the feedgen owner deploy process.

## Files

- `schema_bootstrap.sql` creates the worker-required `feedgen_ops` outbox/DLQ
  tables and the `research_archive` request/post/capture/served surfaces.
- `seed_outbox.sql` inserts two synthetic outbox rows: one served-post row and
  one empty-result request row.

Both files are guarded and refuse to run unless the expected `PGOPTIONS` flags
are present:

```bash
export PGOPTIONS="-c newsflows.allow_archive_rehearsal_schema_bootstrap=true -c newsflows.allow_archive_rehearsal_seed=true"
```

## Claim Boundary

This profile proves that a rebuilt archive-worker can drain synthetic
`feedgen_ops.archive_outbox` rows into `research_archive` on disposable
Postgres.

It does not prove:

- production archive-worker deployment;
- production outbox drain;
- `FEEDGEN_RESEARCH_DB_URL` dual-write;
- participant or production data restore;
- Caddy, bots, ranker, or public feed behavior.

## Expected Readback

After running the worker against the seeded disposable database, the expected
aggregate state is:

```text
archive_outbox=0
dlq=0
request_event=2
post_snapshot=1
capture_source=1
served_post_event=1
```
