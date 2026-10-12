// M1.1 — the host service's own failure cases, offline: one writer, epoch gate, torn-tail recovery,
// the claim invariant, and a fold that refuses to guess on unknown events.
import assert from "node:assert/strict"
import test from "node:test"
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import {
  acceptResult,
  acquireWriter,
  admitStep,
  appendEvent,
  claimStep,
  createRun,
  expandSteps,
  finishRun,
  foldRuns,
  JOURNAL_SCHEMA,
  markUnresolved,
  readEvents,
  readRun,
  readySteps,
  recoverTornTail,
  resolveRoots,
  settleStep,
  verifyAcceptedResult,
  WorkflowStateError,
} from "../lib/service.ts"

const env = () => ({ PRIME_WORKFLOW_STATE: mkdtempSync(join(tmpdir(), "wf-svc-")) })

/** Declare the run's program and freeze a valid result before a `succeeded` settle (new contract). */
const accept = (e, w, attempt_id, step = "explore", run_id = "r1") => {
	const run = readRun(e, run_id);
	if (run && run.attempts && run.attempts[attempt_id] && run.attempts[attempt_id].status === "intended") {
		admitStep(e, w, { run_id, attempt_id, child_id: `sub-${String(attempt_id).slice(0, 8)}` });
	}
	const file = join(e.PRIME_WORKFLOW_STATE, `accepted-${attempt_id}.json`);
	writeFileSync(file, JSON.stringify({ phase: step, summary: "s", findings: [], coverage: [] }));
	return acceptResult(e, w, { run_id, attempt_id, source_path: file });
};
const stateOf = (e) => join(e.PRIME_WORKFLOW_STATE, "workflow-state")
const journalOf = (e) => join(stateOf(e), "journal.jsonl")

test("resolveRoots: the override wins; outside a repository without one it refuses", () => {
  const e = env()
  const roots = resolveRoots(e)
  assert.equal(roots.definitions, join(e.PRIME_WORKFLOW_STATE, "workflows"))
  assert.equal(roots.state, join(e.PRIME_WORKFLOW_STATE, "workflow-state"))
  assert.throws(() => resolveRoots({}, tmpdir()), /no git repository/)
})

test("one writer: the second claim is refused; after release the next one may write", () => {
  const e = env()
  const a = acquireWriter(e, { session_id: "s-a" })
  assert.throws(() => acquireWriter(e, { session_id: "s-b" }), /another writer holds/)
  a.release()
  const b = acquireWriter(e, { session_id: "s-b" })
  b.release()
})

test("a DEAD holder is taken over at once; a live holder is refused even with an expired lease", () => {
  const e = env()
  const path = join(stateOf(e), "writer.lock")
  mkdirSync(stateOf(e), { recursive: true })
  const fresh = new Date().toISOString()
  const old = new Date(Date.now() - 10 * 60_000).toISOString()
  // Fresh lease, dead pid: a crashed host must not block the next session for two minutes.
  writeFileSync(path, JSON.stringify({ epoch: "old-1", at: fresh, pid: 999999, session_id: "dead" }))
  const taken = acquireWriter(e, { session_id: "next" })
  assert.notEqual(taken.epoch, "old-1", "the new writer gets a fresh epoch")
  assert.equal(JSON.parse(readFileSync(path, "utf8")).took_over_from, "old-1")
  taken.release()
  // Old lease, live pid: the hung-writer case the lease is for.
  writeFileSync(path, JSON.stringify({ epoch: "old-2", at: old, pid: process.pid, session_id: "live" }))
  assert.throws(() => acquireWriter(e, {}), /holder is still alive/)
  // No pid at all: only an expired lease allows takeover.
  writeFileSync(path, JSON.stringify({ epoch: "old-3", at: fresh, session_id: "unknown" }))
  assert.throws(() => acquireWriter(e, {}), /another writer holds/)
  writeFileSync(path, JSON.stringify({ epoch: "old-3", at: old, session_id: "unknown" }))
  acquireWriter(e, {}).release()
})

test("epoch gate: a writer that lost its claim cannot confirm anything", () => {
  const e = env()
  const old = acquireWriter(e, { session_id: "s-old" })
  createRun(e, old, { run_id: "r1", definition_digest: "d" })
  old.release()
  const fresh = acquireWriter(e, { session_id: "s-new" })
  assert.throws(() => appendEvent(e, old, { event: "run_created", run_id: "r2", definition_digest: "d" }), /stale writer epoch/)
  fresh.release()
})

test("run and attempt lifecycle folds with a state_version per event", () => {
  const e = env()
  const w = acquireWriter(e, { session_id: "s" })
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }], limits: { max_parallel: 2 } })
  assert.equal(readRun(e, "r1").state_version, 1)
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "r1--explore--1" })
  let run = readRun(e, "r1")
  assert.equal(run.state_version, 2)
  assert.equal(run.attempts[attempt_id].status, "intended")
  assert.deepEqual(run.steps.explore, [attempt_id])
  admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-abc" })
  run = readRun(e, "r1")
  assert.equal(run.attempts[attempt_id].status, "admitted")
  assert.equal(run.attempts[attempt_id].child_id, "sub-abc")
  accept(e, w, attempt_id)
  settleStep(e, w, { run_id: "r1", attempt_id, status: "succeeded" })
  assert.equal(readRun(e, "r1").attempts[attempt_id].status, "succeeded")
  w.release()
})

test("claim invariant: no second active attempt for the same step; a retry needs a terminal predecessor", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "verify" }] })
  const first = claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "r1--verify--1" })
  assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "r1--verify--2" }), /already has an active attempt/)
  settleStep(e, w, { run_id: "r1", attempt_id: first.attempt_id, status: "failed" })
  const second = claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "r1--verify--2" })
  assert.notEqual(second.attempt_id, first.attempt_id)
  w.release()
})

test("unresolved blocks the retry: a lease loss is not a licence for a second child", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "verify" }] })
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "r1--verify--1" })
  markUnresolved(e, w, { run_id: "r1", attempt_id, reason: "lease expired; liveness unknown" })
  assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "r1--verify--2" }), /already has an active attempt .*\(unresolved\)/)
  w.release()
})

test("torn tail: recovery secures the fragment, then the next append reconstructs the whole state", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "r1--explore--1" })
  const complete = readEvents(e)
  assert.equal(complete.torn, false)

  // A crash mid-line: the fragment is what a killed process leaves behind.
  appendFileSync(journalOf(e), '{"schema":"prime-workflow/journal/1","event":"attempt_adm')
  assert.equal(readEvents(e).torn, true)
  assert.throws(() => appendEvent(e, w, { event: "attempt_admitted", run_id: "r1", attempt_id, child_id: "sub-x" }), /torn tail/)

  const recovered = recoverTornTail(e, w)
  assert.equal(recovered.recovered, true)
  assert.equal(existsSync(recovered.secured), true)
  assert.match(readFileSync(recovered.secured, "utf8"), /"attempt_adm$/)
  assert.equal(readEvents(e).torn, false)

  admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-x" })
  const run = readRun(e, "r1")
  assert.equal(run.attempts[attempt_id].status, "admitted")
  assert.equal(run.attempts[attempt_id].child_id, "sub-x")
  assert.equal(run.state_version, 3, "created + intended + admitted")
  w.release()
})

test("an attempt booked with a role carries the role and its digest (M1.4)", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "verify" }] })
  const sha = "a".repeat(64)
  const binding = { role: "sota-verify", manifest_sha256: "b".repeat(64), role_sha256: sha, contract_sha256: "c".repeat(64) }
  assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "n", role: "sota-verify", role_digest: sha }), /full pinned source binding/)
  assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "n", role: "sota-verify", role_digest: sha, role_binding: { ...binding, manifest_sha256: "wrong" } }), /invalid hashes/)
  assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "n", role_digest: sha, role_binding: binding }), /disagrees/)
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "n", role: "sota-verify", role_digest: sha, role_binding: binding })
  const a = readRun(e, "r1").attempts[attempt_id]
  assert.equal(a.role, "sota-verify")
  assert.equal(a.role_digest, sha)
  assert.deepEqual(a.role_binding, binding)
  w.release()
})

test("result acceptance: schema-checked, frozen, and the digest names exactly those bytes (M1.5)", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" })
  admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-1" })
  const child = join(e.PRIME_WORKFLOW_STATE, "child.json")
  writeFileSync(child, JSON.stringify({ files: ["a"] }))
  const schema = { required: ["files"], properties: { files: "array" }, additionalProperties: false }
  const accepted = acceptResult(e, w, { run_id: "r1", attempt_id, source_path: child, schema })
  assert.match(accepted.digest, /^[0-9a-f]{64}$/)
  const attempt = readRun(e, "r1").attempts[attempt_id]
  assert.equal(attempt.result_ref, accepted.result_ref)
  assert.equal(attempt.result_digest, accepted.digest)
  assert.equal(attempt.result_origin, "child_file")
  assert.equal(verifyAcceptedResult(e, { result_ref: accepted.result_ref, digest: accepted.digest }), true)
  // Overwriting the child path does not change what was accepted: the frozen copy is the evidence.
  writeFileSync(child, JSON.stringify({ files: ["OTHER"] }))
  assert.equal(verifyAcceptedResult(e, { result_ref: accepted.result_ref, digest: accepted.digest }), true)
  w.release()
})

test("result rejection: unknown fields, a non-object, and a missing file are refused with nothing frozen", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" })
  admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-1" })
  const child = join(e.PRIME_WORKFLOW_STATE, "child.json")
  const schema = { required: ["files"], properties: { files: "array" }, additionalProperties: false }
  writeFileSync(child, JSON.stringify({ files: ["a"], extra: 1 }))
  assert.throws(() => acceptResult(e, w, { run_id: "r1", attempt_id, source_path: child, schema }), /unknown field "extra"/)
  writeFileSync(child, JSON.stringify(["not", "an", "object"]))
  assert.throws(() => acceptResult(e, w, { run_id: "r1", attempt_id, source_path: child, schema }), /must be a JSON object/)
  assert.throws(() => acceptResult(e, w, { run_id: "r1", attempt_id, source_path: join(e.PRIME_WORKFLOW_STATE, "nope.json") }), /does not exist/)
  assert.equal(readRun(e, "r1").attempts[attempt_id].result_ref, null, "nothing was frozen")
  w.release()
})

test("M3: ready-set is computed from persisted specs — deps, activity and stale inputs", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, {
    run_id: "r1",
    definition_digest: "d",
    steps: [
      { id: "explore" },
      { id: "verify", depends_on: ["explore"] },
      { id: "report", inputs: [{ ref: "spec.json", digest: "1" }] },
    ],
  })
  const run = () => readRun(e, "r1")
  assert.deepEqual(readySteps(run()).ready, ["explore", "report"])
  assert.equal(readySteps(run()).blocked.find((b) => b.step_id === "verify").reason, "dependencies not settled: explore")

  const first = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n1" })
  assert.equal(readySteps(run()).blocked.find((b) => b.step_id === "explore").reason, "an attempt is active")
  accept(e, w, first.attempt_id);
  settleStep(e, w, { run_id: "r1", attempt_id: first.attempt_id, status: "succeeded" })
  assert.deepEqual(readySteps(run()).ready, ["report", "verify"])

  // M2 case 9: a changed input digest blocks the dependent step as stale (the caller reports digests).
  const stale = readySteps(run(), { "spec.json": "2" })
  assert.equal(stale.blocked.find((b) => b.step_id === "report").reason, "input changed: spec.json")
  assert.equal(stale.ready.includes("report"), false)
  assert.deepEqual(readySteps(run(), { "spec.json": "1" }).ready, ["report", "verify"])

  // An expansion is an explicit event; the second one for the same id is refused.
  expandSteps(e, w, { run_id: "r1", cause: "explore found module X", steps: [{ id: "deep-dive", depends_on: ["verify"] }] })
  assert.ok(run().step_specs["deep-dive"])
  assert.throws(() => expandSteps(e, w, { run_id: "r1", cause: "again", steps: [{ id: "deep-dive" }] }), /already exists/)
  assert.equal(readySteps(run()).blocked.find((b) => b.step_id === "deep-dive").reason, "dependencies not settled: verify")
  w.release()
})

test("M3: the ready report is derived only from persisted events (no program replay)", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }, { id: "b", depends_on: ["a"] }] })
  const first = readySteps(readRun(e, "r1"))
  const second = readySteps(readRun(e, "r1"))
  assert.deepEqual(first, second, "same events, same report — deterministic")
  // Nothing about the caller's filesystem enters the decision; only the reported digests can.
  assert.deepEqual(first.ready, ["a"])
  w.release()
})

test("run.finish refuses while a step is open and closes a fully succeeded run", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }, { id: "b", depends_on: ["a"] }] })
  const a = claimStep(e, w, { run_id: "r1", step_id: "a", child_name: "n1" })
  accept(e, w, a.attempt_id);
  settleStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, status: "succeeded" })
  assert.throws(() => finishRun(e, w, { run_id: "r1" }), /not succeeded: b/)
  const b = claimStep(e, w, { run_id: "r1", step_id: "b", child_name: "n2" })
  accept(e, w, b.attempt_id);
  settleStep(e, w, { run_id: "r1", attempt_id: b.attempt_id, status: "succeeded" })
  const done = finishRun(e, w, { run_id: "r1" })
  assert.equal(done.status, "done")
  assert.throws(() => finishRun(e, w, { run_id: "r1" }), /already done/)
  w.release()
})

test("a run without declared steps accepts no claims and cannot be called done", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d" })
  assert.throws(() => finishRun(e, w, { run_id: "r1" }), /no steps and no attempts/)
  assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" }), /not declared/)
  assert.equal(readRun(e, "r1").status, "running")
  w.release()
})

test("M2: a terminal attempt cannot be transitioned again, an unknown one is refused", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }, { id: "verify" }] })
  const first = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n1" })
  accept(e, w, first.attempt_id);
  settleStep(e, w, { run_id: "r1", attempt_id: first.attempt_id, status: "succeeded" })
  assert.throws(() => settleStep(e, w, { run_id: "r1", attempt_id: first.attempt_id, status: "failed" }), /already succeeded/)
  assert.throws(() => settleStep(e, w, { run_id: "r1", attempt_id: "ghost", status: "failed" }), /no attempt ghost/)
  const second = claimStep(e, w, { run_id: "r1", step_id: "verify", child_name: "n2" })
  markUnresolved(e, w, { run_id: "r1", attempt_id: second.attempt_id, reason: "unknown" })
  assert.throws(() => markUnresolved(e, w, { run_id: "r1", attempt_id: second.attempt_id, reason: "again" }), /already unresolved/)
  w.release()
})

test("an admission is booked once; re-admitting an open attempt is refused", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" })
  admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-1" })
  assert.throws(() => admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-1" }), /is admitted, not intended/)
  markUnresolved(e, w, { run_id: "r1", attempt_id, reason: "late artifact expected" })
  assert.throws(() => admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-1" }), /is unresolved, not intended/)
  w.release()
})

test("a late artifact is accepted for an unresolved attempt (no manual re-admit)", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" })
  admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-1" })
  markUnresolved(e, w, { run_id: "r1", attempt_id, reason: "no file within the wait" })
  const file = join(e.PRIME_WORKFLOW_STATE, "late.json")
  writeFileSync(file, JSON.stringify({ files: ["late"] }))
  const accepted = acceptResult(e, w, { run_id: "r1", attempt_id, source_path: file })
  assert.match(accepted.digest, /^[0-9a-f]{64}$/)
  settleStep(e, w, { run_id: "r1", attempt_id, status: "succeeded" })
  assert.equal(readRun(e, "r1").attempts[attempt_id].status, "succeeded")
  w.release()
})

test("M2: a result file that is not JSON is refused and nothing is frozen", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
  const { attempt_id } = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" })
  admitStep(e, w, { run_id: "r1", attempt_id, child_id: "sub-1" })
  const file = join(e.PRIME_WORKFLOW_STATE, "broken.json")
  writeFileSync(file, "{not json")
  assert.throws(() => acceptResult(e, w, { run_id: "r1", attempt_id, source_path: file }), /not readable JSON/)
  assert.equal(readRun(e, "r1").attempts[attempt_id].result_ref, null)
  w.release()
})

test("an unknown journal event is refused, not skipped", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d" })
  appendFileSync(journalOf(e), `${JSON.stringify({ schema: JOURNAL_SCHEMA, event: "mystery" })}\n`)
  assert.throws(() => foldRuns(readEvents(e).events), /unknown journal event/)
  w.release()
})

test("a journal line with the wrong schema is refused", () => {
  const e = env()
  const w = acquireWriter(e, {})
  appendFileSync(journalOf(e), `${JSON.stringify({ schema: "other/1", event: "run_created", run_id: "r" })}\n`)
  assert.throws(() => foldRuns(readEvents(e).events), /expected prime-workflow\/journal\/1/)
  w.release()
})

test("a second run_created for the same id is refused; a foreign attempt is refused", () => {
  const e = env()
  const w = acquireWriter(e, {})
  createRun(e, w, { run_id: "r1", definition_digest: "d" })
  assert.throws(() => createRun(e, w, { run_id: "r1", definition_digest: "d" }), /already exists/)
  appendEvent(e, w, { event: "attempt_admitted", run_id: "r1", attempt_id: "nope", child_id: "x" })
  assert.throws(() => foldRuns(readEvents(e).events), /before it was intended/)
  w.release()
})

test("writing without any writer is refused (there is no anonymous writer)", () => {
  const e = env()
  const fake = { epoch: 1, session_id: null, rlm_depth: null, state_root: stateOf(e), release: () => {} }
  assert.throws(() => appendEvent(e, fake, { event: "run_created", run_id: "r", definition_digest: "d" }), /writer lock is gone|stale writer epoch/)
})

test("WorkflowStateError is a distinct error class (callers can tell state refusals apart)", () => {
  const e = env()
  const w = acquireWriter(e, {})
  assert.throws(() => acquireWriter(e, {}), WorkflowStateError)
  w.release()
})
