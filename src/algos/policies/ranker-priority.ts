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
import { applyPoliticianFilterIfEnabled } from '../politician-filter'
import { PublisherTimeClock } from '../../db/schema'
import { applyPublisherTimeFilter } from '../publisher-time'

export function publisherQueryRankerPriority(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  _requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  publisherDid: string,
  shortname: string,
  feedId = shortname,
  publisherTimeClock: PublisherTimeClock = 'receipt_time',
  referenceTimeIso = new Date().toISOString(),
  rankerScoreMaxAgeHours?: number,
  contentTimeContractVersion: string | null = null,
) {
  const base = db
    .selectFrom('post')
    .selectAll('post')
    .where('author', '=', publisherDid)
  return applyPriorityOrderForFeed(
    applyPoliticianFilterIfEnabled(
      applyPublisherTimeFilter(base, publisherTimeClock, timeLimit, contentTimeContractVersion),
      shortname,
    ),
    feedId,
    publisherTimeClock,
    referenceTimeIso,
    rankerScoreMaxAgeHours,
  ).offset(cursorOffset).limit(limit)
}

export function followsQueryRankerPriority(
  db: Kysely<DatabaseSchema>,
  timeLimit: string,
  requesterFollows: string[],
  cursorOffset: number,
  limit: number,
  publisherDid: string,
  shortname: string,
  feedId = shortname,
  referenceTimeIso = new Date().toISOString(),
  rankerScoreMaxAgeHours?: number,
) {
  const base = db
    .selectFrom('post')
    .selectAll('post')
    .where('author', '!=', publisherDid)
    .where('post.indexedAt', '>=', timeLimit)
    .where((eb) => eb('author', 'in', requesterFollows))
  return applyPriorityOrderForFeed(
    applyPoliticianFilterIfEnabled(base, shortname),
    feedId,
    'receipt_time',
    referenceTimeIso,
    rankerScoreMaxAgeHours,
  ).offset(cursorOffset).limit(limit)
}
