# Feed Catalog Admin Endpoints

Status: feedgen-owned runtime materialization contract for central catalog
operations through `bskyops`.
Date: 2026-06-08

These endpoints expose feedgen-owned read, dry-run validation, and
apply-grade update evidence for operator feed-catalog changes. Feedgen
owns validation, stale-state protection, atomic catalog mutation, and
before/after readback. `bskyops` still owns operator run folders, approval
flags, health gates, smoke checks, audit JSONL, and rollback orchestration.

The upstream desired state for study feed identity and policy is the BSKY root
catalog `config/newsflows/catalogs/publishers.yml`. The feedgen table
`feedgen_ops.feed_catalog` is the feedgen-owned runtime materialization and
readback surface for that desired state. Use `bskyops ecosystem desired-state
feedgen-parity --active-only --json` and `feedgen-sync-packet` before any
approved mutation.

## Auth

- Header: `api-key: $FEEDGEN_ADMIN_API_KEY`
- Raw secrets are never returned.

## Read Current Catalog

`GET /api/admin/feed_catalog`

Returns all catalog rows, sorted by `rkey`, with operator-oriented
status fields:

- `operator_status`: `active`, `paused`, `disabled`, or `retired`
- `published.status`: currently `unknown`
- `health.status`: currently `unknown`
- `raw_values_in_output:false`

`GET /api/admin/feed_catalog/:rkey`

Returns one catalog row or `404` with `{"error":"rkey=<rkey> not found"}`.

## Dry-Run Existing Feed Update

`POST /api/admin/feed_catalog/dry-run`

Supported request fields:

```json
{
  "rkey": "newsflow-nl-1",
  "enabled": false,
  "access_policy_id": "disabled",
  "study_id": "newsflows-main",
  "retired_at": null
}
```

Supported update fields are `enabled`, `access_policy_id`, `study_id`,
and `retired_at`. `access_policy_id` must be one of
`subscriber-default`, `study-only`, or `disabled`.

Dry-run response fields include:

- `mode:"dry-run"`, `dry_run:true`, and `would_write:false`
- `current` and `proposed` values for the supported update fields
- `changes`, `change_count`, `blockers`, and `warnings`
- `rollback.strategy:"restore-current-values"` and rollback fields
- `raw_values_in_output:false`

Dry-run blocks `study-only` access without a `study_id`, and blocks a
nonexistent `study_id` when feedgen can verify `study_catalog`.

## Existing Feed Update Apply Primitive

`POST /api/admin/feed_catalog` still supports insert and update. For
`op:"update"`, it now returns apply-grade evidence rather than only
`{ok:true}`.

Recommended `bskyops` update request:

```json
{
  "op": "update",
  "rkey": "newsflow-nl-1",
  "enabled": false,
  "if_current": {
    "enabled": true,
    "access_policy_id": "subscriber-default",
    "study_id": "newsflows-main",
    "retired_at": null
  }
}
```

`if_current` is optional for backwards compatibility, but `bskyops` live
apply should always send it. Feedgen rejects the write with HTTP `409`
when any provided current value differs from the row at apply time.

Successful update response:

```json
{
  "schema_version": 1,
  "mode": "apply",
  "operation": "feed.update",
  "target": "feed:newsflow-nl-1",
  "source": "feedgen",
  "status": "applied",
  "dry_run": false,
  "would_write": false,
  "applied": true,
  "wrote": true,
  "before": {
    "enabled": true,
    "access_policy_id": "subscriber-default",
    "study_id": "newsflows-main",
    "retired_at": null
  },
  "after": {
    "enabled": false,
    "access_policy_id": "subscriber-default",
    "study_id": "newsflows-main",
    "retired_at": null
  },
  "changes": [
    {
      "field": "enabled",
      "current": true,
      "proposed": false
    }
  ],
  "change_count": 1,
  "blockers": [],
  "warnings": [],
  "rollback": {
    "strategy": "restore-current-values",
    "fields": {
      "enabled": true
    }
  },
  "readback": {
    "schema_version": 1,
    "rkey": "newsflow-nl-1",
    "enabled": false,
    "raw_values_in_output": false
  },
  "raw_values_in_output": false
}
```

Conflict response:

```json
{
  "schema_version": 1,
  "mode": "apply",
  "operation": "feed.update",
  "target": "feed:newsflow-nl-1",
  "status": "conflict",
  "applied": false,
  "blockers": [
    {
      "code": "stale-current-values",
      "message": "feed_catalog row changed since dry-run/current-state capture",
      "mismatches": [
        {
          "field": "enabled",
          "expected": true,
          "actual": false
        }
      ]
    }
  ],
  "raw_values_in_output": false
}
```

The update path runs inside a database transaction. It reuses the same
validation and diff builder as dry-run, refuses dry-run blockers, performs
the update, then reads the row back before returning.

This endpoint is an apply primitive, not the complete operator workflow.
`bskyops` should call feedgen dry-run first, record run evidence and audit
JSONL, collect health context, then call this apply primitive only after
explicit operator approval. After apply, the workflow must read back the row
and run config-derived feed assumption checks before reporting the change
green.

## Local Verification

```sh
yarn test:feed-catalog-admin
yarn build
```
