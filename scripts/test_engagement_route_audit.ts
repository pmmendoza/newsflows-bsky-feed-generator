import assert from 'assert'
import express from 'express'
import http from 'http'

const bufferModule = require('buffer')
bufferModule.SlowBuffer ??= bufferModule.Buffer

type JsonResponse = { status: number; body: any }

async function requestJson(
  server: http.Server,
  path: string,
  apiKey?: string,
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
        headers: apiKey ? { 'api-key': apiKey } : {},
      },
      (res) => {
        let data = ''
        res.setEncoding('utf8')
        res.on('data', (chunk) => (data += chunk))
        res.on('end', () => resolve({
          status: res.statusCode ?? 0,
          body: data ? JSON.parse(data) : null,
        }))
      },
    )
    req.on('error', reject)
    req.end()
  })
}

async function main() {
  const registerMonitorEndpoints = (await import('../src/methods/monitor')).default
  const keyCases = [
    ['FEEDGEN_MONITOR_API_KEY', 'monitor'],
    ['FEEDGEN_READ_API_KEY', 'read'],
    ['FEEDGEN_RANKER_API_KEY', 'ranker'],
    ['FEEDGEN_ADMIN_API_KEY', 'admin'],
  ] as const
  const secretPrefix = 'RAW-SECRET-SENTINEL'
  const requester = 'did:plc:RAW-PARTICIPANT-SENTINEL'
  const publisher = 'did:plc:RAW-PUBLISHER-SENTINEL'

  for (const [envName] of keyCases) {
    process.env[envName] = `${secretPrefix}-${envName}`
  }

  const app = express()
  registerMonitorEndpoints({ xrpc: { router: app } } as any, {} as any)
  const server = app.listen(0, '127.0.0.1')
  await new Promise<void>((resolve) => server.once('listening', resolve))

  const captured: string[] = []
  const originalLog = console.log
  console.log = (...args: unknown[]) => captured.push(args.map(String).join(' '))

  try {
    const path = `/api/engagement?requester_did=${encodeURIComponent(requester)}&publisher_did=${encodeURIComponent(publisher)}`
    for (const [envName] of keyCases) {
      const response = await requestJson(server, path, process.env[envName])
      assert.equal(response.status, 400)
    }

    const unauthorized = await requestJson(server, path)
    assert.equal(unauthorized.status, 401)
  } finally {
    console.log = originalLog
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    )
    for (const [envName] of keyCases) delete process.env[envName]
  }

  const auditLines = captured.filter((line) => line.includes('feedgen_engagement_route_hit'))
  assert.equal(auditLines.length, 5, 'one raw-free audit record per route hit')

  const records = auditLines.map((line) => JSON.parse(line))
  assert.deepEqual(
    records.map((record) => [record.key_class, record.status]),
    [...keyCases.map(([, keyClass]) => [keyClass, 400]), ['unauthorized', 401]],
  )
  for (const record of records) {
    assert.equal(record.event, 'feedgen_engagement_route_hit')
    assert.equal(record.route, '/api/engagement')
    assert.deepEqual(
      Object.keys(record).sort(),
      ['event', 'key_class', 'route', 'status', 'timestamp'],
    )
  }

  const output = captured.join('\n')
  assert(!output.includes(secretPrefix), 'audit output must not contain API-key material')
  assert(!output.includes(requester), 'audit output must not contain requester DID')
  assert(!output.includes(publisher), 'audit output must not contain publisher DID')

  console.log('engagement route audit logging tests passed')
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
