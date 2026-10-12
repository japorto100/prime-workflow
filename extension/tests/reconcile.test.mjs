// M1.7 — reconciliation is re-labelling, never re-spawning: the ledger proves admission and delete,
// the reported kernel registry proves liveness, and absence of evidence becomes `unresolved`.
import assert from "node:assert/strict"
import test from "node:test"
import { mkdirSync, mkdtempSync, utimesSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { BRIDGE_PROTOCOL, handleRequest } from "../lib/bridge.ts"
import { decideReconciliation, parseLedger, readLedgerDir } from "../lib/reconcile.ts"
import { acquireWriter, admitStep, claimStep, createRun, readRun } from "../lib/service.ts"

const attempt = (over = {}) => ({
  attempt_id: "a1",
  step_id: "explore",
  child_name: "r1--explore--1",
  child_id: "sub-1",
  status: "admitted",
  lease_expires_at: null,
  driver_session_id: null,
  role: null,
  role_digest: null,
  result_ref: null,
  result_digest: null,
  result_origin: null,
  events: 1,
  ...over,
})

test("parseLedger: spawn/delete are read, unknown versions and bad lines are counted not guessed", () => {
  const text = [
    '{"v":1,"op":"meta","at":"t","sessionsDir":"/x/sessions"}',
    '{"v":1,"op":"spawn","at":"t","childId":"sub-1","name":"n1","depth":1}',
    '{"v":1,"op":"delete","at":"t","childId":"sub-1","reason":"user"}',
    '{"v":2,"op":"spawn","at":"t","childId":"sub-2","name":"n2"}',
    "{not json}",
    '{"v":1,"op":"rename","at":"t","childId":"sub-1"}',
  ].join("\n")
  const read = parseLedger(text)
  assert.deepEqual(read.spawns.map((s) => [s.name, s.child_id, s.depth]), [["n1", "sub-1", 1]])
  assert.deepEqual(read.deletes.map((d) => [d.child_id, d.reason]), [["sub-1", "user"]])
  assert.equal(read.unknown_lines, 3)
})

test("decision table: observed wins nothing, delete orphans, absence blocks, unproven admission blocks", () => {
  const spawns = [{ child_id: "sub-1", name: "r1--explore--1", depth: 1, at: "t" }]
  assert.deepEqual(decideReconciliation({ attempts: [attempt()], spawns, deletes: [], observed: [{ name: "r1--explore--1", child_id: "sub-1" }] }), [])
  const absent = decideReconciliation({ attempts: [attempt()], spawns, deletes: [], observed: [] })
  assert.equal(absent.length, 1)
  assert.equal(absent[0].action, "unresolved")
  assert.match(absent[0].reason, /not in the kernel registry/)
  const deleted = decideReconciliation({ attempts: [attempt()], spawns, deletes: [{ child_id: "sub-1", at: "t", reason: "user" }], observed: [] })
  assert.equal(deleted[0].action, "orphaned")
  const unproven = decideReconciliation({ attempts: [attempt({ status: "intended", child_id: null })], spawns: [], deletes: [], observed: [] })
  assert.equal(unproven[0].action, "unresolved")
  assert.match(unproven[0].reason, /admission is unproven/)
  const already = decideReconciliation({ attempts: [attempt({ status: "unresolved" })], spawns: [], deletes: [], observed: [] })
  assert.deepEqual(already, [], "an already-blocked attempt is not re-labelled every round")
})

test("reconcile.run through the bridge: ledger + reported registry, and nothing is spawned", () => {
  const state = mkdtempSync(join(tmpdir(), "wf-rec-state-"))
  const agent = mkdtempSync(join(tmpdir(), "wf-rec-agent-"))
  const env = { PRIME_WORKFLOW_STATE: state, PRIME_AGENT_CODING_AGENT_DIR: agent }
  const writer = acquireWriter(env, { session_id: "s" })
  createRun(env, writer, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
  const { attempt_id } = claimStep(env, writer, { run_id: "r1", step_id: "explore", child_name: "r1--explore--1" })
  admitStep(env, writer, { run_id: "r1", attempt_id, child_id: "sub-1" })

  const ledgerDir = join(agent, "rlm-ledger")
  mkdirSync(ledgerDir, { recursive: true })
  const ledgerFile = join(ledgerDir, "hash.jsonl")
  const spawnLine = JSON.stringify({ v: 1, op: "spawn", at: "t", childId: "sub-1", name: "r1--explore--1", depth: 1 })
  writeFileSync(ledgerFile, `${JSON.stringify({ v: 1, op: "meta", at: "t", sessionsDir: join(agent, "sessions") })}\n${spawnLine}\n`)

  const request = (operation_id, observed) => ({
    v: BRIDGE_PROTOCOL,
    operation_id,
    operation: "reconcile.run",
    driver_epoch: writer.epoch,
    expected_run_version: readRun(env, "r1").state_version,
    payload: { run_id: "r1", observed },
  })

  const absent = handleRequest(env, writer, request("rc1", []))
  assert.equal(absent.ok, true)
  assert.equal(absent.result.ledger_found, true)
  assert.equal(absent.result.actions[0].action, "unresolved")
  assert.equal(readRun(env, "r1").attempts[attempt_id].status, "unresolved")

  // Now the kernel reports the child: nothing changes, and in particular nothing is restarted.
  const live = handleRequest(env, writer, request("rc2", [{ name: "r1--explore--1", child_id: "sub-1", status: "running" }]))
  assert.equal(live.ok, true)
  assert.deepEqual(live.result.actions, [])

  // The ledger records a delete: the only terminal signal it has.
  writeFileSync(ledgerFile, `${JSON.stringify({ v: 1, op: "meta", at: "t", sessionsDir: join(agent, "sessions") })}\n${spawnLine}\n${JSON.stringify({ v: 1, op: "delete", at: "t2", childId: "sub-1", reason: "user" })}\n`)
  const orphan = handleRequest(env, writer, { ...request("rc3", []), expected_run_version: readRun(env, "r1").state_version })
  assert.equal(orphan.ok, true)
  assert.equal(orphan.result.actions[0].action, "orphaned")
  assert.equal(readRun(env, "r1").attempts[attempt_id].status, "orphaned")
  writer.release()
})


test("ledger selection: exact sessionsDir, newest mtime, and a spawn named meta cannot hide the meta line", () => {
  const dir = mkdtempSync(join(tmpdir(), "wf-ledger-"))
  const sessions = join(dir, "sessions")
  const child = join(sessions, "child-1")
  const spawn = (name, id) => `{"v":1,"op":"spawn","at":"t","childId":"${id}","name":"${name}","depth":1}`
  const meta = (where) => `{"v":1,"op":"meta","at":"t","sessionsDir":${JSON.stringify(where)}}`

  // Substring trap: `sessions/child-1` contains `sessions`; the exact root ledger is older.
  const older = join(dir, "aaaa.jsonl")
  writeFileSync(older, `${meta(sessions)}\n${spawn("wanted", "sub-wanted")}\n`)
  const substring = join(dir, "zzzz.jsonl")
  writeFileSync(substring, `${meta(child)}\n${spawn("wrong", "sub-wrong")}\n`)
  utimesSync(older, new Date(1000), new Date(1000))
  utimesSync(substring, new Date(2000), new Date(2000))
  const exact = readLedgerDir(dir, sessions)
  assert.deepEqual(exact.spawns.map((s) => s.child_id), ["sub-wanted"], "the exact sessionsDir wins over the substring and the newer mtime")

  // Among exact matches the newest mtime wins.
  const newer = join(dir, "bbbb.jsonl")
  writeFileSync(newer, `${meta(sessions)}\n${spawn("newest", "sub-newest")}\n`)
  utimesSync(newer, new Date(3000), new Date(3000))
  assert.deepEqual(readLedgerDir(dir, sessions).spawns.map((s) => s.child_id), ["sub-newest"])

  // A spawn line with name "meta" before the real meta line must not make the file foreign.
  const tricky = join(dir, "cccc.jsonl")
  writeFileSync(tricky, `${spawn("meta", "sub-tricky")}\n${meta(sessions)}\n`)
  utimesSync(tricky, new Date(4000), new Date(4000))
  assert.deepEqual(readLedgerDir(dir, sessions).spawns.map((s) => s.child_id), ["sub-tricky"])

  // No meta line at all: not this sessions dir.
  const noMeta = join(dir, "dddd.jsonl")
  writeFileSync(noMeta, `${spawn("x", "sub-x")}\n`)
  utimesSync(noMeta, new Date(5000), new Date(5000))
  assert.deepEqual(readLedgerDir(dir, sessions).spawns.map((s) => s.child_id), ["sub-tricky"], "a file without meta is skipped, not guessed")
  assert.equal(readLedgerDir(dir, join(dir, "elsewhere")), null)
})


test("kernel status (measured 24.09.2026): a completed child stays listed but is not alive", () => {
  const spawns = [{ child_id: "sub-1", name: "r1--explore--1", depth: 1, at: "t" }]
  const alive = decideReconciliation({ attempts: [attempt()], spawns, deletes: [], observed: [{ name: "r1--explore--1", child_id: "sub-1", status: "running" }] })
  assert.deepEqual(alive, [], "running is alive")
  const ended = decideReconciliation({ attempts: [attempt()], spawns, deletes: [], observed: [{ name: "r1--explore--1", child_id: "sub-1", status: "completed" }] })
  assert.equal(ended.length, 1)
  assert.equal(ended[0].action, "unresolved")
  assert.match(ended[0].reason, /ended in the kernel registry \(status completed\)/)
  const unknown = decideReconciliation({ attempts: [attempt()], spawns, deletes: [], observed: [{ name: "r1--explore--1", child_id: "sub-1", status: "in-between" }] })
  assert.deepEqual(unknown, [], "an unknown status stays conservative: alive")
  const intended = decideReconciliation({ attempts: [attempt({ status: "intended", child_id: null })], spawns, deletes: [], observed: [{ name: "r1--explore--1", child_id: null, status: "completed" }] })
  assert.equal(intended[0].action, "unresolved")
})
