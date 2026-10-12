// M1.2 — the bridge's own contract: private socket, protocol version, and idempotency that survives
// a LOST RESPONSE (the operation is recorded before the caller sees success).
import assert from "node:assert/strict"
import test from "node:test"
import { chmodSync, existsSync, mkdtempSync, statSync, writeFileSync } from "node:fs"
import { connect } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { acquireWriter } from "../lib/service.ts"
import { BRIDGE_PROTOCOL, startBridge } from "../lib/bridge.ts"

const env = () => ({ PRIME_WORKFLOW_STATE: mkdtempSync(join(tmpdir(), "wf-bridge-")) })

function client(socket_path) {
  const socket = connect(socket_path)
  let buffer = ""
  const waiters = []
  socket.setEncoding("utf8")
  socket.on("data", (chunk) => {
    buffer += chunk
    let index = buffer.indexOf("\n")
    while (index >= 0) {
      const line = buffer.slice(0, index)
      buffer = buffer.slice(index + 1)
      const resolve = waiters.shift()
      if (resolve) resolve(JSON.parse(line))
      index = buffer.indexOf("\n")
    }
  })
  return {
    send: (request) =>
      new Promise((resolve) => {
        waiters.push(resolve)
        socket.write(`${JSON.stringify(request)}\n`)
      }),
    close: () => socket.destroy(),
  }
}

async function withBridge(fn) {
  const e = env()
  const writer = acquireWriter(e, { session_id: "s-test" })
  const bridge = await startBridge(e, writer)
  const c = client(bridge.socket_path)
  try {
    return await fn({ e, writer, bridge, c })
  } finally {
    c.close()
    await bridge.close()
    writer.release()
  }
}

test("the socket is private and the protocol version is enforced", async () => {
  await withBridge(async ({ bridge, c }) => {
    const mode = statSync(bridge.socket_path).mode & 0o777
    assert.equal(mode, 0o600, "socket must be 0600")
    const pong = await c.send({ v: BRIDGE_PROTOCOL, operation: "ping" })
    assert.equal(pong.ok, true)
    assert.equal(pong.result.epoch, bridge.epoch)
    const wrong = await c.send({ v: "other/9", operation: "ping" })
    assert.equal(wrong.ok, false)
    assert.match(wrong.error.message, /unsupported protocol/)
  })
})

test("a mutating request needs an operation_id; unknown operations are refused", async () => {
  await withBridge(async ({ c }) => {
    const noId = await c.send({ v: BRIDGE_PROTOCOL, operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] } })
    assert.equal(noId.ok, false)
    assert.match(noId.error.message, /needs an operation_id/)
    const unknown = await c.send({ v: BRIDGE_PROTOCOL, operation: "graph.execute" })
    assert.equal(unknown.ok, false)
    assert.match(unknown.error.message, /unknown operation/)
  })
})

test("idempotency: a repeated operation_id returns the recorded result without applying twice", async () => {
  await withBridge(async ({ c, bridge }) => {
    const create = {
      v: BRIDGE_PROTOCOL,
      operation_id: "op-create-1",
      operation: "run.create",
      driver_epoch: bridge.epoch,
      payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] },
    }
    const first = await c.send(create)
    assert.equal(first.ok, true)
    assert.equal(first.result.state_version, 1)
    // The same request again: re-execution would throw "run already exists"; the recorded reply wins.
    const again = await c.send(create)
    assert.equal(again.ok, true)
    assert.equal(again.result.state_version, 1)
    // A different payload under the same id is refused.
    const different = await c.send({ ...create, payload: { run_id: "r2", definition_digest: "d", steps: [{ id: "explore" }] } })
    assert.equal(different.ok, false)
    assert.match(different.error.message, /used with a different payload/)
    // Only r1 exists.
    const missing = await c.send({ v: BRIDGE_PROTOCOL, operation: "run.read", payload: { run_id: "r2" } })
    assert.equal(missing.ok, false)
  })
})

test("stale expected_run_version and stale driver_epoch are gates, not hints", async () => {
  await withBridge(async ({ c, bridge }) => {
    await c.send({ v: BRIDGE_PROTOCOL, operation_id: "c1", operation: "run.create", driver_epoch: bridge.epoch, payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] } })
    const staleVersion = await c.send({
      v: BRIDGE_PROTOCOL,
      operation_id: "c2",
      operation: "attempt.claim",
      driver_epoch: bridge.epoch,
      expected_run_version: 99,
      payload: { run_id: "r1", step_id: "explore", child_name: "r1--explore--1" },
    })
    assert.equal(staleVersion.ok, false)
    assert.match(staleVersion.error.message, /stale expected_run_version 99; the run is at 1/)
    const staleEpoch = await c.send({
      v: BRIDGE_PROTOCOL,
      operation_id: "c3",
      operation: "attempt.claim",
      driver_epoch: "stale-epoch",
      expected_run_version: 1,
      payload: { run_id: "r1", step_id: "explore", child_name: "r1--explore--1" },
    })
    assert.equal(staleEpoch.ok, false)
    assert.match(staleEpoch.error.message, /stale driver_epoch/)
    assert.equal(staleEpoch.error.code, 'STALE_DRIVER_EPOCH', 'only an explicit pre-admission refusal is retryable')
    assert.equal(staleEpoch.operation_id, 'c3', 'the refusal must be bound to its request')
    // The current epoch and version both pass.
    const claimed = await c.send({
      v: BRIDGE_PROTOCOL,
      operation_id: "c4",
      operation: "attempt.claim",
      driver_epoch: bridge.epoch,
      expected_run_version: 1,
      payload: { run_id: "r1", step_id: "explore", child_name: "r1--explore--1" },
    })
    assert.equal(claimed.ok, true)
    assert.equal(claimed.result.attempt_id.length > 0, true)
  })
})

test("result.accept over the socket: the schema is enforced and the digest comes back", async () => {
  await withBridge(async ({ e, bridge, c }) => {
    const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
    await c.send({ ...base, operation_id: "c1", operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] } })
    const claimed = await c.send({ ...base, operation_id: "c2", operation: "attempt.claim", expected_run_version: 1, payload: { run_id: "r1", step_id: "explore", child_name: "n" } })
    await c.send({ ...base, operation_id: "c3", operation: "attempt.admit", expected_run_version: 2, payload: { run_id: "r1", attempt_id: claimed.result.attempt_id, child_id: "sub-1" } })
    const file = join(e.PRIME_WORKFLOW_STATE, "out.json")
    writeFileSync(file, JSON.stringify({ files: ["a"] }))
    const schema = { required: ["files"], properties: { files: "array" }, additionalProperties: false }
    const accepted = await c.send({ ...base, operation_id: "c4", operation: "result.accept", expected_run_version: 3, payload: { run_id: "r1", attempt_id: claimed.result.attempt_id, source_path: file, schema } })
    assert.equal(accepted.ok, true)
    assert.match(accepted.result.digest, /^[0-9a-f]{64}$/)
    writeFileSync(file, JSON.stringify({ files: ["a"], extra: true }))
    const refused = await c.send({ ...base, operation_id: "c5", operation: "result.accept", expected_run_version: 4, payload: { run_id: "r1", attempt_id: claimed.result.attempt_id, source_path: file, schema } })
    assert.equal(refused.ok, false)
    assert.match(refused.error.message, /already accepted/)
  })
})

test("a socket file left by a dead host is replaced instead of blocking listen", async () => {
  const e = env()
  const writer = acquireWriter(e, {})
  const socket_path = join(e.PRIME_WORKFLOW_STATE, "workflow-state", "bridge.sock")
  writeFileSync(socket_path, "") // a stale file, as a killed host leaves it
  const bridge = await startBridge(e, writer, { socket_path })
  const c = client(bridge.socket_path)
  try {
    const pong = await c.send({ v: BRIDGE_PROTOCOL, operation: "ping" })
    assert.equal(pong.ok, true)
  } finally {
    c.close()
    await bridge.close()
    writer.release()
  }
})

test("M2: a persistence failure is an error reply, never a success", async () => {
  await withBridge(async ({ e, bridge, c }) => {
    const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
    await c.send({ ...base, operation_id: "f0", operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] } })
    // Appending needs the journal file itself writable; removing write on the file (not the dir)
    // is the honest persistence failure: dir write permission does not govern O_APPEND.
    const journalFile = join(e.PRIME_WORKFLOW_STATE, "workflow-state", "journal.jsonl")
    chmodSync(journalFile, 0o400)
    try {
      const claim = await c.send({ ...base, operation_id: "f1", operation: "attempt.claim", expected_run_version: 1, payload: { run_id: "r1", step_id: "explore", child_name: "n" } })
      assert.equal(claim.ok, false)
      assert.match(claim.error.message, /EACCES|permission denied/i)
    } finally {
      chmodSync(journalFile, 0o600)
    }
    const retry = await c.send({ ...base, operation_id: "f2", operation: "attempt.claim", expected_run_version: 1, payload: { run_id: "r1", step_id: "explore", child_name: "n" } })
    assert.equal(retry.ok, true, "after the write becomes possible again, the retry is a fresh operation")
  })
})

test("closing the bridge removes the socket", async () => {
  const e = env()
  const writer = acquireWriter(e, {})
  const bridge = await startBridge(e, writer)
  const path = bridge.socket_path
  assert.equal(existsSync(path), true)
  await bridge.close()
  assert.equal(existsSync(path), false)
  writer.release()
})
