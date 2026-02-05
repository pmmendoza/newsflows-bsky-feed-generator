import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import {
  didFromAtUri,
  getIngestionScope,
  scopedIngestionEnabled,
  trackSubscriberActivityEnabled,
  restrictPublisherEngagementToSubscribersEnabled,
} from './util/ingestion-scope'

// for saving embedded preview cards
function isExternalEmbed(embed: any): embed is { external: { uri: string, title: string, description: string } } {
  return embed && embed.external && typeof embed.external.uri === 'string';
}

function isPostUri(uri: string): boolean {
  return /^at:\/\/[^/]+\/app\.bsky\.feed\.post\//.test(uri)
}

function getQuotedRecord(embed: any): { uri: string; cid: string } | null {
  if (!embed) return null

  const direct = embed.record
  if (direct && typeof direct.uri === 'string') {
    return {
      uri: direct.uri,
      cid: typeof direct.cid === 'string' ? direct.cid : '',
    }
  }

  const withMedia = embed.record?.record
  if (withMedia && typeof withMedia.uri === 'string') {
    return {
      uri: withMedia.uri,
      cid: typeof withMedia.cid === 'string' ? withMedia.cid : '',
    }
  }

  return null
}

// Helper function to sanitize strings for PostgreSQL
function sanitizeForPostgres(text: string | null | undefined): string {
  if (text === null || text === undefined) return '';
  // Remove null bytes which cause PostgreSQL errors
  return text.replace(/\0/g, '');
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)

    const scopedIngestion = scopedIngestionEnabled()
    const trackSubscriberActivity = scopedIngestion && trackSubscriberActivityEnabled()
    const restrictPublisherEngagementToSubscribers =
      scopedIngestion && restrictPublisherEngagementToSubscribersEnabled()

    const scope =
      scopedIngestion || trackSubscriberActivity || restrictPublisherEngagementToSubscribers
        ? await getIngestionScope(this.db)
        : null

    const shouldStorePost = (create: { author: string; record: any }): boolean => {
      if (!scopedIngestion || !scope) return true

      if (scope.allowlistedAuthorDids.has(create.author)) return true

      const reply = create.record?.reply
      const rootDid = didFromAtUri(reply?.root?.uri)
      const parentDid = didFromAtUri(reply?.parent?.uri)

      if (rootDid && scope.allowlistedAuthorDids.has(rootDid)) return true
      if (parentDid && scope.allowlistedAuthorDids.has(parentDid)) return true

      if (trackSubscriberActivity && scope.subscriberDids.has(create.author)) return true

      return false
    }

    const shouldStoreEngagement = (
      create: { author: string; record: any },
    ): boolean => {
      const subjectUri = create.record?.subject?.uri
      return shouldStoreEngagementFor(create.author, subjectUri)
    }

    const shouldStoreEngagementFor = (
      author: string,
      subjectUri: string | null | undefined,
    ): boolean => {
      if (!subjectUri || !isPostUri(subjectUri)) return false
      if (!scopedIngestion || !scope) return true

      const subjectDid = didFromAtUri(subjectUri)
      if (subjectDid && scope.allowlistedAuthorDids.has(subjectDid)) {
        if (
          restrictPublisherEngagementToSubscribers &&
          scope.publisherDids.has(subjectDid) &&
          scope.subscriberDids.size > 0
        ) {
          return scope.subscriberDids.has(author)
        }
        return true
      }

      if (trackSubscriberActivity && scope.subscriberDids.has(author)) return true

      return false
    }

    const shouldStoreQuoteEngagement = (
      author: string,
      subjectUri: string | null | undefined,
    ): boolean => {
      if (!subjectUri || !isPostUri(subjectUri)) return false
      if (!scopedIngestion || !scope) return true

      if (trackSubscriberActivity && scope.subscriberDids.has(author)) return true

      const subjectDid = didFromAtUri(subjectUri)
      if (subjectDid && scope.publisherDids.has(subjectDid)) return true

      return false
    }

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    // for (const post of ops.posts.creates) {
    //   console.log(post.record.text)
    // }


    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter(shouldStorePost)
      .map((create) => {
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
          createdAt: create.record.createdAt,
          author: create.author,
          text: sanitizeForPostgres(create.record.text),
          rootUri: create.record.reply?.root?.uri || "",
          rootCid: create.record.reply?.root?.cid || "",
          // extract preview card info if present
          linkUrl: create.record.embed && isExternalEmbed(create.record.embed) ? create.record.embed.external.uri : "",
          linkTitle: sanitizeForPostgres(
            create.record.embed && isExternalEmbed(create.record.embed) ? create.record.embed.external.title : ""
          ),
          linkDescription: sanitizeForPostgres(
            create.record.embed && isExternalEmbed(create.record.embed) ? create.record.embed.external.description : ""
          ),
        }
      })

      
    // likes + reposts + quotes = engagement
    const engagementsToDelete = ops.reposts.deletes.map((del) => del.uri).concat(
      ops.likes.deletes.map((del) => del.uri)
    ).concat(
      ops.posts.deletes.map((del) => del.uri)
    )
    const engagementsToCreate = ops.reposts.creates
      .filter(shouldStoreEngagement)
      .map((create) => {
        return {
          uri: create.uri,
          cid: create.cid,
          subjectUri: create.record.subject.uri,
          subjectCid: create.record.subject.cid,
          type: 1,
          indexedAt: new Date().toISOString(),
          createdAt: create.record.createdAt,
          author: create.author,
        }
      }).concat(
        ops.likes.creates
          .filter(shouldStoreEngagement)
          .map((create) => {
            return {
              uri: create.uri,
              cid: create.cid,
              subjectUri: create.record.subject.uri,
              subjectCid: create.record.subject.cid,
              type: 2,
              indexedAt: new Date().toISOString(),
              createdAt: create.record.createdAt,
              author: create.author,
            }
          })
      ).concat(
        ops.posts.creates
          .flatMap((create) => {
            const quoted = getQuotedRecord(create.record?.embed)
            if (!quoted) return []
            if (!shouldStoreQuoteEngagement(create.author, quoted.uri)) return []
            return [{
              uri: create.uri,
              cid: create.cid,
              subjectUri: quoted.uri,
              subjectCid: quoted.cid,
              type: 3,
              indexedAt: new Date().toISOString(),
              createdAt: create.record.createdAt,
              author: create.author,
            }]
          })
      )

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }

    if (engagementsToDelete.length > 0) {
      await this.db
        .deleteFrom('engagement')
        .where('uri', 'in', engagementsToDelete)
        .execute()
    }
    if (engagementsToCreate.length > 0) {
      await this.db
        .insertInto('engagement')
        .values(engagementsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
