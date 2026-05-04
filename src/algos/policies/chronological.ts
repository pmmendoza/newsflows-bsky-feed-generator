/**
 * Sprint 11 / Task 5 — chronological ordering policy.
 *
 * Extracted from the 5 variant-1 handlers (`feed-{nl,fr,cz,ir,uk}-1.ts`).
 * Behaviour: order by indexedAt DESC, cid DESC. No priority, no
 * engagement weighting. Per-feed handlers now delegate here.
 */
import { Kysely } from 'kysely'
import { DatabaseSchema } from '../../db/schema'

export function publisherQueryChronological(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  _requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  publisherDid: string,
) {
  return db
    .selectFrom('post')
    .selectAll()
    .where('author', '=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
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
) {
  return db
    .selectFrom('post')
    .selectAll()
    .where('author', '!=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .where((eb) => eb('author', 'in', requesterFollows))
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .offset(cursorOffset)
    .limit(limit)
}
