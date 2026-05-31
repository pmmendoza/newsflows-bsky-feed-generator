import express from 'express'
import { AppContext } from '../config'
import { Server } from '../lexicon'

export default function registerPrioritizeEndpoint(server: Server, _ctx: AppContext) {
  const retired = (_req: express.Request, res: express.Response) => {
    return res.status(410).json({
      error: 'retired_endpoint',
      message:
        '/api/prioritize has been retired. Ranker output is written through ranker_prod score tables.',
      canonical_output: 'ranker_prod.feed_current_priority.score',
    })
  }

  server.xrpc.router.get('/api/prioritize', retired)
  server.xrpc.router.post('/api/prioritize', retired)
}
