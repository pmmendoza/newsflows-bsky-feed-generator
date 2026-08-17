#!/usr/bin/env bash
# Committed, hashed, rehearsable runner for the content-time v1->v2
# production revalidation packet. Implements the packet's operator
# procedure as code so nothing is hand-composed on the console: every
# query, every container invocation, every evidence file, and every gate
# is defined here, not in prose.
#
# Subcommands: preflight | preview <group> | apply <group> <label>
#              [--max-batches N] | readback | restore <group> |
#              secret-scan | finalize
#
# Groups: "main" (MAIN_DIDS, horizon = GREATEST(catalog
# publisher_post_max_age_days, HORIZON_MAIN_DAYS)) and "be" (BE_DID,
# horizon = GREATEST(catalog publisher_post_max_age_days,
# HORIZON_BE_DAYS)).
#
# All parameters are env vars with production defaults, each also settable
# via a --flag so the exact same script drives both the production run and
# the disposable-Postgres rehearsal (see
# scripts/rehearse_content_time_revalidate_packet.sh).
#
#   E                    evidence root (required, no default)
#   TREE                 built feedgen source tree (required, no default)
#   EXPECTED_SHA         git HEAD sha the tree must match (required for preflight)
#   EXPECTED_DIST_SHA256 sha256 of dist/tools/backfill-publisher-posts.js (required for preflight)
#   EXPECTED_CT_SHA256   sha256 of dist/util/content-time.js (required for preflight)
#   IMG                  feedgen image digest ref (required for preview/apply)
#   NETWORK              docker network            (default: newsflows-bsky-feed-generator-v2_default)
#   ENV_FILE             feedgen secrets env file   (default: /etc/newsflows/secrets/feedgen.env)
#   DB_CONTAINER         Postgres container name    (default: feedgen-db)
#   PSQL_DB              database name              (default: feedgen-db)
#   PSQL_USER            psql role                  (default: feedgen)
#   PACKET_SHA           64-hex packet id, required for apply/restore
#   MAIN_DIDS            comma-separated publisher DIDs for the "main" group
#   BE_DID               publisher DID for the "be" group
#   HORIZON_MAIN_DAYS    default: 3
#   HORIZON_BE_DAYS      default: 10
#   DOCKER               docker invocation prefix  (default: sudo -n docker)
#   RUNNER               "container" (default, production) or "host" (rehearsal-only,
#                        runs `node $TREE/dist/tools/...` directly -- see rehearsal script)
#
# Every evidence file this script writes is installed root:newsflows 0640
# and appended to $E/SHA256SUMS. Every subcommand refuses to overwrite an
# existing named output file (SHA256SUMS itself is the one running ledger
# that is intentionally regenerated on every write). Raw-free throughout:
# publisher URIs/DIDs are fine (the tool's own contract already allows
# them as cursors); no post text, no participant data, no DB password ever
# appears in an evidence file (enforced by the `secret-scan` subcommand).
set -euo pipefail

# --------------------------------------------------------------------------
# Parameters: env-var defaults, overridable by --flag.
# --------------------------------------------------------------------------

E="${E:-}"
TREE="${TREE:-}"
EXPECTED_SHA="${EXPECTED_SHA:-}"
EXPECTED_DIST_SHA256="${EXPECTED_DIST_SHA256:-}"
EXPECTED_CT_SHA256="${EXPECTED_CT_SHA256:-}"
IMG="${IMG:-}"
NETWORK="${NETWORK:-newsflows-bsky-feed-generator-v2_default}"
ENV_FILE="${ENV_FILE:-/etc/newsflows/secrets/feedgen.env}"
DB_CONTAINER="${DB_CONTAINER:-feedgen-db}"
PSQL_DB="${PSQL_DB:-feedgen-db}"
PSQL_USER="${PSQL_USER:-feedgen}"
PACKET_SHA="${PACKET_SHA:-}"
MAIN_DIDS="${MAIN_DIDS:-}"
BE_DID="${BE_DID:-}"
HORIZON_MAIN_DAYS="${HORIZON_MAIN_DAYS:-3}"
HORIZON_BE_DAYS="${HORIZON_BE_DAYS:-10}"
DOCKER="${DOCKER:-sudo -n docker}"
RUNNER="${RUNNER:-container}"
HOST_NODE="${HOST_NODE:-node}"

