/**
 * Sprint 11 / Task 5 — ranker-priority ordering policy.
 *
 * Extracted from the 5 variant-2 handlers and the IR-4 actor-diversity
 * handler. Wraps `applyPriorityOrderForFeed`, which handles the
 * Sprint 5 score / priority precedence + per-feed canary env flag.
 */
import { Kysely } from 'kysely'
import { DatabaseSchema } from '../../db/schema'
import { applyPriorityOrderForFeed } from '../ranker-priority-helper'

export function publisherQueryRankerPriority(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  _requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  publisherDid: string,
  shortname: string,
) {
  const base = db
    .selectFrom('post')
    .selectAll('post')
    .where('author', '=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
  return applyPriorityOrderForFeed(base, shortname).offset(cursorOffset).limit(limit)
}

export function followsQueryRankerPriority(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  publisherDid: string,
  shortname: string,
) {
  const base = db
    .selectFrom('post')
    .selectAll('post')
    .where('author', '!=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .where((eb) => eb('author', 'in', requesterFollows))
  return applyPriorityOrderForFeed(base, shortname).offset(cursorOffset).limit(limit)
}
