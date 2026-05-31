import assert from 'assert'
import express from 'express'
import http from 'http'
import registerMonitorEndpoints from '../src/methods/monitor'
import registerPrioritizeEndpoint from '../src/methods/prioritize-posts'

type JsonResponse = {
  status: number
  body: any
}

async function listen(app: express.Application): Promise<http.Server> {
  const server = app.listen(0, '127.0.0.1')
  await new Promise<void>((resolve) => server.once('listening', resolve))
  return server
}

async function requestJson(
  server: http.Server,
  path: string,
  headers: Record<string, string> = {},
): Promise<JsonResponse> {
  const address = server.address()
  assert(address && typeof address === 'object', 'server must listen on a port')

  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: '127.0.0.1',
        port: address.port,
        path,
        method: 'GET',
        headers,
      },
      (res) => {
        let data = ''
        res.setEncoding('utf8')
        res.on('data', (chunk) => {
          data += chunk
        })
        res.on('end', () => {
          resolve({
            status: res.statusCode ?? 0,
            body: data ? JSON.parse(data) : null,
          })
        })
      },
    )
    req.on('error', reject)
    req.end()
  })
}

async function main() {
  const app = express()
  app.use(express.json())
  const fakeServer = { xrpc: { router: app } } as any
  const fakeCtx = {} as any

  process.env.FEEDGEN_READ_API_KEY = 'retired-priority-read-key'

  registerPrioritizeEndpoint(fakeServer, fakeCtx)
  registerMonitorEndpoints(fakeServer, fakeCtx)

  const server = await listen(app)
  try {
    const prioritize = await requestJson(server, '/api/prioritize?test=true')
    assert.equal(prioritize.status, 410)
    assert.equal(prioritize.body.error, 'retired_endpoint')
    assert.equal(
      prioritize.body.canonical_output,
      'ranker_prod.feed_current_priority.score',
    )

    const priorities = await requestJson(server, '/api/priorities', {
      'api-key': 'retired-priority-read-key',
    })
    assert.equal(priorities.status, 410)
    assert.equal(priorities.body.error, 'retired_endpoint')
    assert.equal(
      priorities.body.canonical_output,
      'ranker_prod.feed_current_priority.score',
    )
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((err) => (err ? reject(err) : resolve())),
    )
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
