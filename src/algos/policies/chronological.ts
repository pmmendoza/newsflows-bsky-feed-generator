/**
 * Sprint 11 / Task 5 — chronological ordering policy.
 *
 * Extracted from the 5 variant-1 handlers (`feed-{nl,fr,cz,ir,uk}-1.ts`).
 * Behaviour: order by indexedAt DESC, cid DESC. No priority, no
 * engagement weighting. Per-feed handlers now delegate here.
 */
import { Kysely } from 'kysely'
import { DatabaseSchema } from '../../db/schema'
import { applyPoliticianFilterIfEnabled } from '../politician-filter'

export function publisherQueryChronological(
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
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(cursorOffset)
    .limit(limit)
}

export function followsQueryChronological(
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
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(cursorOffset)
    .limit(limit)
}
