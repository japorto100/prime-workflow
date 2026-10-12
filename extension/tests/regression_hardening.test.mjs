// M0.12 — the review's defect reproductions, turned around: each of these asserts the CORRECTED
// behaviour. The old probes (Workflow-review-2026-09-24/review-probes.mjs) asserted the defect and
// are kept as pretest evidence; this file is what a regression must keep true.
//
// W01 parallel_safe has no silent effect · W02 structured fields survive · W03 record validates ·
// W04 identity is not a timestamp · W05 read validation · W06 revisions are kept · W07 no tiers ·
// W08 cycle names members only · W09 the instructions teach the real API · W10 no listener reliance ·
// W11 records carry identity. Plus M0.15 (gate rename + deprecation) and M0.17 (migration).
import assert from "node:assert/strict"
import test from "node:test"
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import primeLabWorkflow, {
  definitionDigest,
  readRunRecords,
  listWorkflows,
  loadRevision,
  loadWorkflow,
  migrateLegacyDefinitions,
  planFromTasks,
  recordRun,
  saveWorkflow,
} from "../index.ts"
import { openStore } from "../lib/storage.ts"

const env = () => ({ PRIME_AGENT_CODING_AGENT_DIR: mkdtempSync(join(tmpdir(), "wf-reg-")), PRIME_WORKFLOW_STATE: mkdtempSync(join(tmpdir(), "wf-reg-state-")) })
const host = () => { const t = []; return { registerTool: (x) => t.push(x), tools: t } }
const on = (e) => { const pi = host(); primeLabWorkflow(pi, { ...e, PRIME_WORKFLOW: "on" }); return pi }

const block = (key, { deps = "NONE", check = `pytest tests/test_${key}.py`, model } = {}) =>
  [
    `TASK_KEY=${key}`,
    `TASK_TITLE=Do ${key}`,
    `TASK_OBJECTIVE=Make ${key} true.`,
    `TASK_DEPS=${deps}`,
    `TASK_OWNS_PATHS=src/${key}.py`,
    `TASK_ACCEPTANCE_CHECK=${check}`,
    `TASK_PARALLEL_SAFE=${deps === "NONE" ? "true" : "false"}`,
    ...(model ? [`TASK_MODEL=${model}`] : []),
  ].join("\n")
const plan = (...blocks) => ["PROJECT_DONE=false", "REASON=r", "", blocks.join("\n\n")].join("\n")

test("W09: the generated instructions teach rlm.spawn/rlm.collect — not the old rlm('sub-task')", async () => {
  const e = env()
  const pi = on(e)
  const save = pi.tools.find((t) => t.name === "workflow_save")
  const read = pi.tools.find((t) => t.name === "workflow_plan")
  await save.execute("c", { name: "w", steps: [{ id: "a", prompt: "do a" }] })
  const out = await read.execute("c", { name: "w" })
  const text = out.content[0].text
  assert.match(text, /rlm\.spawn\(/)
  assert.match(text, /rlm\.collect\(/)
  assert.match(text, /answer_preview/)
  assert.doesNotMatch(text, /await rlm\('sub-task'\)/)
  assert.doesNotMatch(text, /list_subagents/)
})

test("W01/W02: levels are dependency levels; per-step parallel_safe and structured fields survive", async () => {
  const e = env()
  const pi = on(e)
  const tool = pi.tools.find((t) => t.name === "workflow_plan_from_tasks")
  const out = await tool.execute("c", { text: plan(block("a"), block("b", { deps: "a", model: "cheap" })), name: "w" })
  const text = out.content[0].text
  assert.doesNotMatch(text, /may run together/)
  assert.match(text, /dependency level, not a start signal/)
  assert.match(text, /\*\*a\*\* \(parallel_safe=true\)/)
  assert.match(text, /\*\*b\*\* \(parallel_safe=false\)/)
  const back = loadWorkflow(e, "w")
  assert.deepEqual(back.steps[0].owns_paths, ["src/a.py"])
  assert.equal(back.steps[0].parallel_safe, true)
  assert.deepEqual(back.steps[0].outcome_contract, { kind: "command", command: "pytest tests/test_a.py" })
  assert.equal(back.steps[1].model, undefined, "a tier is not stored as a model")
})

test("W03: workflow_record refuses an unknown step and an outcome outside the fixed set", async () => {
  const e = env()
  const pi = on(e)
  const save = pi.tools.find((t) => t.name === "workflow_save")
  const record = pi.tools.find((t) => t.name === "workflow_record")
  await save.execute("c", { name: "w", steps: [{ id: "a", prompt: "do a" }] })
  await assert.rejects(() => record.execute("c", { name: "w", step: "ghost", outcome: "passed" }), /no step "ghost" in workflow w/)
  await assert.rejects(() => record.execute("c", { name: "w", step: "a", outcome: "green" }), /outcome "green" is not one of/)
  await record.execute("c", { name: "w", step: "a", outcome: "passed" })
  assert.equal(readRunRecords(e).length, 1)
})

test("W04: two records in the same millisecond are two records — identity is not the clock", () => {
  const e = env()
  const RealDate = globalThis.Date
  globalThis.Date = class extends RealDate {
    constructor(...args) { super(...(args.length ? args : ["2026-09-24T08:00:00.123Z"])) }
    static now() { return 1790236800123 }
  }
  try {
    recordRun(e, "w", { step: "a", outcome: "passed" })
    recordRun(e, "w", { step: "b", outcome: "failed" })
  } finally {
    globalThis.Date = RealDate
  }
  const rows = readRunRecords(e)
  assert.equal(rows.length, 2)
  assert.deepEqual(rows.map((r) => r.step).sort(), ["a", "b"])
})

test("W11: records carry the identity fields, explicitly null when there is no runner", () => {
  const e = env()
  recordRun(e, "w", { step: "a", outcome: "passed", definition_digest: "d" })
  const row = readRunRecords(e)[0]
  for (const key of ["run_id", "attempt_id", "definition_digest", "session_id", "evidence_ref", "driver_session_id", "lease_expires_at"]) {
    assert.ok(key in row, `${key} missing`)
  }
  assert.equal(row.run_id, null)
  assert.equal(row.driver_session_id, null)
  assert.equal(row.definition_digest, "d")
})

test("W05: a stored blob with the right shape but invalid content is refused on read", () => {
  const e = env()
  const bad = join(e.PRIME_WORKFLOW_STATE, "workflows", "bad")
  mkdirSync(bad, { recursive: true })
  writeFileSync(join(bad, "rev-0001.json"), JSON.stringify({ name: "bad", steps: "not-an-array" }))
  assert.throws(() => loadWorkflow(e, "bad"), /is not a valid definition/)
})

test("W05: a step that depends on itself is refused before it is stored", () => {
  const e = env()
  const pi = on(e)
  const save = pi.tools.find((t) => t.name === "workflow_save")
  return save.execute("c", { name: "w", steps: [{ id: "a", prompt: "do a", depends_on: ["a"] }] }).then(
    () => assert.fail("a self-dependency must not be stored"),
    (error) => assert.match(String(error), /cycle among steps: a/),
  )
})

test("W06: the same name creates a new revision; the old revision stays readable", () => {
  const e = env()
  const first = saveWorkflow(e, "w", [{ id: "a", prompt: "first" }])
  const second = saveWorkflow(e, "w", [{ id: "b", prompt: "second" }])
  assert.equal(first.revision, 1)
  assert.equal(second.revision, 2)
  assert.equal(first.workflow_id, second.workflow_id, "identity is stable across revisions")
  assert.equal(loadRevision(e, first.workflow_id, 1).steps[0].id, "a")
  assert.equal(loadRevision(e, first.workflow_id, 2).steps[0].id, "b")
  assert.equal(loadWorkflow(e, "w").steps[0].id, "b")
  assert.deepEqual(listWorkflows(e), ["w"])
})

test("the digest does not depend on key order in the step object", () => {
  const a = definitionDigest([{ id: "a", prompt: "p", owns_paths: ["x"], parallel_safe: true }])
  const b = definitionDigest([{ parallel_safe: true, owns_paths: ["x"], prompt: "p", id: "a" }])
  assert.equal(a, b)
  assert.notEqual(a, definitionDigest([{ id: "a", prompt: "p", owns_paths: ["y"], parallel_safe: true }]))
})

test("M0.15: PRIME_WORKFLOW opens the gate; the legacy name still works and says so", async () => {
  const e = env()
  const fresh = host()
  primeLabWorkflow(fresh, e)
  assert.equal(fresh.tools.length, 0, "no gate, no tools")

  const receipts = mkdtempSync(join(tmpdir(), "wf-rec-"))
  const legacy = host()
  primeLabWorkflow(legacy, { ...e, PRIME_LAB_WORKFLOW: "on", PRIME_WORKFLOW_RECEIPTS: receipts })
  assert.equal(legacy.tools.length, 5)
  const lines = readFileSync(join(receipts, "workflow.receipts.jsonl"), "utf8").trim().split("\n").map((l) => JSON.parse(l))
  assert.ok(lines.some((l) => l.event === "workflow_gate_deprecated" && l.use === "PRIME_WORKFLOW"))
})

test("M0.17: a legacy plan with the marker is migrated; the old key stays untouched", () => {
  const e = env()
  const legacy = {
    name: "legacy",
    steps: [{ id: "a", prompt: "Do a. Acceptance check (must fail when the requirement is violated): pytest tests/a.py" }],
    saved_at: "2026-01-01T00:00:00.000Z",
    session_id: null,
  }
  openStore(e, "workflow").put("legacy", legacy)
  assert.throws(() => loadWorkflow(e, "legacy"), /predates schema/)
  const report = migrateLegacyDefinitions(e)
  assert.equal(report.migrated.length, 1)
  assert.equal(report.refused.length, 0)
  const back = loadWorkflow(e, "legacy")
  assert.equal(back.revision, 1)
  assert.deepEqual(back.steps[0].outcome_contract, { kind: "command", command: "pytest tests/a.py" })
  assert.deepEqual(openStore(e, "workflow").get("legacy"), legacy, "the legacy entry is not overwritten")
})

test("M0.17: a legacy plan without the marker is refused, not given a substitute check", () => {
  const e = env()
  openStore(e, "workflow").put("nomarker", { name: "nomarker", steps: [{ id: "a", prompt: "do a" }], saved_at: "x" })
  const report = migrateLegacyDefinitions(e)
  assert.equal(report.migrated.length, 0)
  assert.equal(report.refused.length, 1)
  assert.match(report.refused[0].reason, /no 'Acceptance check/)
})

test("M0.17: the tool reports counts (arrays would be dropped by the render client)", async () => {
  const e = env()
  openStore(e, "workflow").put("nomarker", { name: "nomarker", steps: [{ id: "a", prompt: "do a" }] })
  const pi = on(e)
  const tool = pi.tools.find((t) => t.name === "workflow_migrate_definitions")
  const out = await tool.execute("c", {})
  assert.equal(out.details.migrated, 0)
  assert.equal(out.details.refused, 1)
})

test("a plan with an explicit model keeps it; a tier is reported (W07 regression)", () => {
  const out = planFromTasks(plan(block("a", { model: "gpt-5.4-mini" }), block("b", { model: "cheap" })))
  assert.equal(out.steps[0].model, "gpt-5.4-mini")
  assert.equal(out.steps[1].model, undefined)
  assert.deepEqual(out.rejected.map((r) => r.reason), ["model tier 'cheap' is not a model; the child inherits the session model"])
})

// No filesystem write is asserted here on purpose: the point is that a second Store instance of the
// same namespace sees the write through the disk, not through an instance-local listener (W10).
test("W10: cross-instance visibility comes from the disk, never from onChange", () => {
  const e = env()
  const a = openStore(e, "workflow")
  const b = openStore(e, "workflow")
  let fired = 0
  a.onChange(() => fired++)
  b.put("x", { ok: true })
  assert.equal(a.get("x").ok, true, "the disk is the source")
  assert.equal(fired, 0, "the instance-local listener is not a cross-instance signal")
})
