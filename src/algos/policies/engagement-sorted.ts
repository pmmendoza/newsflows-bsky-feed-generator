/**
 * Sprint 11 / Task 5 — engagement-sorted ordering policy.
 *
 * Extracted from the 5 variant-3 handlers. Time-decayed engagement
 * score: (likes + reposts + comments + quotes) × (1 - (age/window)^2).
 *
 * Identical SQL was previously copy-pasted in all 5 country handlers;
 * this module is the single source.
 */
import { Kysely, sql } from 'kysely'
import { DatabaseSchema } from '../../db/schema'
import { applyPoliticianFilterIfEnabled } from '../politician-filter'

function engagementOrderExpr(timeLimit: string) {
  return sql`
    -- Base engagement score (likes + reposts + comments + quotes)
    COALESCE(
      (COALESCE(likes_count, 0) +
       COALESCE(repost_count, 0) +
       COALESCE(comments_count, 0) +
       COALESCE(quote_count, 0)),
      0
    )
    *
    -- Time decay factor (newer posts get higher multiplier)
    (1 - POWER(
      -- Age since timeLimit / Total time window
      (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM "indexedAt"::timestamp)) /
      (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM ${timeLimit}::timestamp)),
      2
    ))
  `
}

export function publisherQueryEngagement(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  _requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  publisherDid: string,
  shortname = '',
) {
  const base = db
    .selectFrom('post')
    // selectAll('post') not selectAll(): the eligibility LEFT JOIN adds a
    // `pe.uri` column; a bare `*` projects both and node-postgres keeps the
    // LAST, so pe.uri (NULL on fail-open rows) would clobber post.uri.
    .selectAll('post')
    .where('author', '=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
  return applyPoliticianFilterIfEnabled(base, shortname)
    .orderBy(engagementOrderExpr(timeLimit), 'desc')
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(cursorOffset)
    .limit(limit)
}

export function followsQueryEngagement(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  publisherDid: string,
  shortname = '',
) {
  const base = db
    .selectFrom('post')
    .selectAll('post') // see publisher leg: avoid pe.uri clobbering post.uri
    .where('author', '!=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .where((eb) => eb('author', 'in', requesterFollows))
  return applyPoliticianFilterIfEnabled(base, shortname)
    .orderBy(engagementOrderExpr(timeLimit), 'desc')
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(cursorOffset)
    .limit(limit)
}
