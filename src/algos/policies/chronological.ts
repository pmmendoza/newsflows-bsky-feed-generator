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
    .selectAll()
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
    .selectAll()
    .where('author', '!=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .where((eb) => eb('author', 'in', requesterFollows))
  return applyPoliticianFilterIfEnabled(base, shortname)
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(cursorOffset)
    .limit(limit)
}
