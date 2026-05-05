import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'

// Active feeds. Variant-4 (hybrid) and the UK family are preserved
// in _drafts/ — they are not registered for serving but kept for
// reference and future re-activation. See _drafts/README.md.
import * as feedNL1 from './feed-nl-1'
import * as feedNL2 from './feed-nl-2'
import * as feedNL3 from './feed-nl-3'

import * as feedIR1 from './feed-ir-1'
import * as feedIR2 from './feed-ir-2'
import * as feedIR3 from './feed-ir-3'
import * as feedIR4 from './feed-ir-4'
import * as feedIR5 from './feed-ir-5'

import * as feedFR1 from './feed-fr-1'
import * as feedFR2 from './feed-fr-2'
import * as feedFR3 from './feed-fr-3'

import * as feedCZ1 from './feed-cz-1'
import * as feedCZ2 from './feed-cz-2'
import * as feedCZ3 from './feed-cz-3'

type AlgoHandler = (ctx: AppContext, params: QueryParams, requesterDid: string) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {
    // NL
    [feedNL1.shortname]: feedNL1.handler,
    [feedNL2.shortname]: feedNL2.handler,
    [feedNL3.shortname]: feedNL3.handler,

    // IR (IR-4 = actor-diversity proof-of-concept; retired after one
    // successful Bluesky-app validation)
    [feedIR1.shortname]: feedIR1.handler,
    [feedIR2.shortname]: feedIR2.handler,
    [feedIR3.shortname]: feedIR3.handler,
    [feedIR4.shortname]: feedIR4.handler,
    [feedIR5.shortname]: feedIR5.handler,

    // FR
    [feedFR1.shortname]: feedFR1.handler,
    [feedFR2.shortname]: feedFR2.handler,
    [feedFR3.shortname]: feedFR3.handler,

    // CZ
    [feedCZ1.shortname]: feedCZ1.handler,
    [feedCZ2.shortname]: feedCZ2.handler,
    [feedCZ3.shortname]: feedCZ3.handler,
}

export default algos
