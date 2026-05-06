// Sprint 15 / T2 Phase 3 — static algo registry retired.
//
// Every active feed is now dispatched dynamically from
// `feedgen_ops.feed_catalog` via `src/algos/catalog-dispatch.ts`.
// The per-feed shim files (`feed-{cc}-{n}.ts`) have been moved to
// `src/algos/_drafts/` for reference and one-sprint rollback safety.
//
// This file is preserved as a stable import boundary in case any
// future tooling expects `import algos from '../algos'`. It
// exports an empty map; the dual-path fallback in
// `src/methods/feed-generation.ts` falls through to the dynamic
// resolver for every rkey.
//
// To restore a single shim during a Phase 3 rollback:
//   1. `git mv src/algos/_drafts/feed-{cc}-{n}.ts src/algos/`
//   2. Re-add `import * as feed{CC}{N} from './feed-{cc}-{n}'`
//      and `[feed{CC}{N}.shortname]: feed{CC}{N}.handler` here.
//   3. `tsc --noEmit` clean → image rebuild via
//      `dev/deploy/feedgen_rebuild_deploy.sh --apply`.

import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'

type AlgoHandler = (ctx: AppContext, params: QueryParams, requesterDid: string) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {}

export default algos
