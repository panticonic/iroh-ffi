import { test } from 'node:test'
import assert from 'node:assert/strict'
import pkg from '../index.js'
const { Endpoint, RelayMode } = pkg
const ALPN = Array.from(Buffer.from('iroh-ffi/stream-cancellation'))
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
async function bounded(promise) {
  let timer
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('native stream operation remained blocked')), 1500) })]) }
  finally { clearTimeout(timer) }
}
async function connected(run) {
  const endpoints = [], connections = []
  async function bind() {
    const b = Endpoint.builder(); b.applyMinimal(); b.alpns([ALPN]); b.relayMode(RelayMode.disabled())
    const endpoint = await b.bind(); endpoints.push(endpoint); return endpoint
  }
  try {
    const server = await bind(), client = await bind()
    const accepted = server.acceptNext().then(i => i.accept()).then(c => c.connect())
    const local = await client.connect(server.addr(), ALPN); connections.push(local)
    const remote = await accepted; connections.push(remote)
    await run(local, remote)
  } finally {
    for (const connection of connections) connection.close(0n, [])
    await Promise.all(endpoints.map(endpoint => endpoint.close()))
  }
}
async function pair(local, remote) {
  const accepting = remote.acceptBi(), outgoing = await local.openBi()
  await outgoing.send.writeAll([1])
  const incoming = await accepting; await incoming.recv.readExact(1)
  return [outgoing, incoming]
}
async function proveConnectionAlive(local, remote) {
  const [outgoing, incoming] = await pair(local, remote)
  await incoming.send.writeAll([7]); await incoming.send.finish()
  assert.deepEqual(await bounded(outgoing.recv.readExact(1)), [7])
  await outgoing.send.finish()
}
for (const method of ['read', 'readExact', 'readToEnd', 'receivedReset']) {
  test(`stop interrupts pending ${method} and preserves other streams`, () => connected(async (local, remote) => {
    const [outgoing, incoming] = await pair(local, remote)
    let settled = false
    const pending = outgoing.recv[method](1).then(value => ({ value }), error => ({ error })).finally(() => { settled = true })
    await delay(30); assert.equal(settled, false, 'read must be pending before cancellation')
    await bounded(outgoing.recv.stop(33n))
    const outcome = await bounded(pending)
    if (method === 'receivedReset') assert.equal(outcome.value, null)
    else assert.ok(outcome.error, 'interrupted read must reject')
    assert.equal(await bounded(incoming.send.stopped()), 33)
    await assert.rejects(outgoing.recv.readExact(1), /stopped/)
    await proveConnectionAlive(local, remote)
  }))
}
for (const observe of [false, true]) test(`reset interrupts flow-controlled writeAll (stopped observer: ${observe})`, () => connected(async (local, remote) => {
  const [outgoing, incoming] = await pair(local, remote)
  // The peer never consumes the payload, so the default stream receive window
  // fills. A stopped observer must not itself retain the send mutex.
  const observer = observe ? outgoing.send.stopped().then(value => ({ value }), error => ({ error })) : Promise.resolve()
  await delay(20)
  let settled = false
  const pending = outgoing.send.writeAll(new Array(8 * 1024 * 1024).fill(1)).then(value => ({ value }), error => ({ error })).finally(() => { settled = true })
  await bounded(incoming.recv.readExact(1)) // proves write entered the native data path
  await delay(100); assert.equal(settled, false, 'write must be flow-controlled before reset')
  const queuedWrite = outgoing.send.write([2]).then(value => ({ value }), error => ({ error }))
  await bounded(outgoing.send.reset(44n))
  assert.ok((await bounded(pending)).error, 'interrupted write must reject')
  assert.ok((await bounded(queuedWrite)).error, 'queued write must reject')
  assert.equal(await bounded(incoming.recv.receivedReset()), 44)
  await bounded(observer)
  await proveConnectionAlive(local, remote)
}))

test('stop cancels queued reads and invalid control codes do not cancel data', () => connected(async (local, remote) => {
  const [outgoing, incoming] = await pair(local, remote)
  const first = outgoing.recv.readExact(1)
  await assert.rejects(outgoing.recv.stop(1n << 63n))
  await assert.rejects(incoming.send.reset(1n << 63n))
  await incoming.send.writeAll([8])
  assert.deepEqual(await bounded(first), [8])
  const reads = [outgoing.recv.readExact(1), outgoing.recv.readExact(1)].map(p => p.then(value => ({ value }), error => ({ error })))
  await delay(30)
  await bounded(outgoing.recv.stop(55n))
  for (const read of reads) assert.ok((await bounded(read)).error)
  assert.equal(await bounded(incoming.send.stopped()), 55)
  await proveConnectionAlive(local, remote)
}))
