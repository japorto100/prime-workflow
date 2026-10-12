// The capability owns the program, not the run. So the tests are about the two things ad-hoc
// orchestration actually gets wrong — a dependency on a step that is not there, and a cycle — and
// about refusing to store a workflow that cannot be ordered.
import assert from "node:assert/strict"
import test from "node:test"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import primeLabWorkflow, { WorkflowError, listWorkflows, loadWorkflow, order, saveWorkflow } from "../index.ts"

const env = () => ({ PRIME_AGENT_CODING_AGENT_DIR: mkdtempSync(join(tmpdir(), "wf-")), PRIME_WORKFLOW_STATE: mkdtempSync(join(tmpdir(), "wf-state-")) })
const envDir = env
const host = () => { const t = []; return { registerTool: (x) => t.push(x), tools: t } }
const step = (id, deps) => ({ id, prompt: `do ${id}`, ...(deps ? { depends_on: deps } : {}) })

test("independent steps are one wave; a chain is one wave each", () => {
  assert.deepEqual(order([step("a"), step("b")]), [["a", "b"]])
  assert.deepEqual(order([step("a"), step("b", ["a"]), step("c", ["b"])]), [["a"], ["b"], ["c"]])
})

test("a diamond collapses to three dependency levels, with the middle one level", () => {
  const waves = order([step("a"), step("b", ["a"]), step("c", ["a"]), step("d", ["b", "c"])])
  assert.deepEqual(waves, [["a"], ["b", "c"], ["d"]])
})

test("a dependency on a step that does not exist names both", () => {
  assert.throws(() => order([step("a", ["ghost"])]), /a depends on ghost, which is not a step/)
})

test("a cycle is reported with the steps in it, not as 'cyclic'", () => {
  // Kahn's residue also holds nodes merely blocked behind the cycle; reachability names the members.
  assert.throws(() => order([step("a", ["c"]), step("b", ["a"]), step("c", ["b"])]), /cycle among steps: a, b, c/)
})

test("a node blocked behind a cycle is not named as a cycle member (W08)", () => {
  // c depends on a (in the a↔b cycle) but is not itself in one: the message must name a and b only.
  assert.throws(() => order([step("a", ["b"]), step("b", ["a"]), step("c", ["a"])]), /cycle among steps: a, b$/)
})

test("a duplicate step id is refused before anything else is checked", () => {
  assert.throws(() => order([step("a"), step("a")]), /duplicate step id: a/)
})

test("a workflow that cannot be ordered is never stored", () => {
  // A saved workflow that cannot be ordered is a trap for whoever reads it back, and the reader is
  // a model in a later session.
  const e = env()
  assert.throws(() => saveWorkflow(e, "bad", [step("a", ["b"]), step("b", ["a"])]), WorkflowError)
  assert.deepEqual(listWorkflows(e), [], "nothing was written")
})

test("a saved workflow round-trips", () => {
  const e = env()
  saveWorkflow(e, "build", [step("compile"), step("test", ["compile"])])
  const back = loadWorkflow(e, "build")
  assert.deepEqual(back.steps.map((s) => s.id), ["compile", "test"])
  assert.ok(back.saved_at)
  assert.deepEqual(listWorkflows(e), ["build"])
})

test("a saved workflow records WHOSE it is — aus dem ctx DIESES Tool-Aufrufs, nicht aus einem gemerkten", async () => {
  // `saved_at` alone cannot answer "did THIS session plan?": sessions share the agent directory
  // and therefore the store, so the plan gate needs the id (measured 2026-09-06,
  // docs/verify/p4-p5-20260906/plan_gate.test.mjs, CROSS-SESSION).
  //
  // B-15 (09.09.2026): die Session wurde bis dahin bei `agent_start` gemerkt. Der Kommentar im Code
  // begründete das mit „the only place the session id is offered to an extension" — falsch: Primes
  // eigener Typ übergibt jedem Tool den Kontext als fünftes Argument von `execute`
  // (`dist/core/extensions/types.d.ts:362`). Und der gemerkte Wert war schädlich: ein Inline-RLM-Kind
  // teilt sich mit dem Elternteil eine Modulinstanz, sein `agent_start` kippte die Variable, und der
  // Elternteil speicherte seinen Plan danach unter der Session des Kindes.
  const e = env()
  assert.equal(saveWorkflow(e, "from-a-script", [step("a")]).session_id, null, "no caller, no owner")
  assert.equal(loadWorkflow(e, "from-a-script").session_id, null, "and it survives the round trip as null")
  assert.equal(saveWorkflow(e, "from-a-session", [step("a")], { session_id: "s-9" }).session_id, "s-9")
  assert.equal(loadWorkflow(e, "from-a-session").session_id, "s-9")

  const handlers = []
  const t = { registerTool: (x) => (t.tools ??= []).push(x), on: (name, h) => handlers.push([name, h]) }
  primeLabWorkflow(t, { ...e, PRIME_WORKFLOW: "on" })
  const save = t.tools.find((x) => x.name === "workflow_save")
  const ctxOf = (id, rlmDepth = 0) => ({ sessionManager: { getSessionId: () => id, getHeader: () => ({ rlmDepth }) } })

  await save.execute("call-0", { name: "no-context", steps: [step("a")] })
  assert.equal(loadWorkflow(e, "no-context").session_id, null, "ohne ctx gibt es keine Session zu beanspruchen")

  await save.execute("call-1", { name: "from-the-parent", steps: [step("a")] }, undefined, undefined, ctxOf("s-parent"))
  assert.equal(loadWorkflow(e, "from-the-parent").session_id, "s-parent")

  // Das Kind startet — vor B-15 übernahm die Closure hier seine Session für alles Folgende.
  for (const [name, h] of handlers) if (name === "agent_start") await h({}, ctxOf("s-child", 1))
  await save.execute("call-2", { name: "from-the-child", steps: [step("a")] }, undefined, undefined, ctxOf("s-child", 1))
  assert.equal(loadWorkflow(e, "from-the-child").session_id, "s-child")

  await save.execute("call-3", { name: "parent-again", steps: [step("a")] }, undefined, undefined, ctxOf("s-parent"))
  assert.equal(loadWorkflow(e, "parent-again").session_id, "s-parent", "der Plan des Elternteils gehört dem Elternteil")
})

test("names and step ids are checked against a shape", () => {
  const e = env()
  for (const bad of ["../escape", "a/b", "", "x".repeat(80)]) {
    assert.throws(() => saveWorkflow(e, bad, [step("a")]), WorkflowError)
  }
  assert.throws(() => saveWorkflow(e, "ok", [{ id: "../x", prompt: "p" }]), WorkflowError)
  assert.throws(() => saveWorkflow(e, "ok", [{ id: "a", prompt: "  " }]), /no prompt/)
  assert.throws(() => saveWorkflow(e, "ok", []), /at least one step/)
})

test("the bridge starts on a depth-0 session_start and not for children (host wiring)", async () => {
  const { existsSync } = await import("node:fs")
  const e = { ...env(), PRIME_WORKFLOW: "on" }
  const hooks = new Map()
  const pi = { registerTool: () => {}, on: (name, fn) => hooks.set(name, fn) }
  primeLabWorkflow(pi, e)
  const start = hooks.get("session_start")
  assert.equal(typeof start, "function", "the host wiring registers a session_start hook")
  const ctxOf = (depth) => ({ sessionManager: { getSessionId: () => "s", getHeader: () => ({ rlmDepth: depth }) } })
  await start({}, {})
  assert.equal(existsSync(join(e.PRIME_WORKFLOW_STATE, "workflow-state", "bridge.sock")), false, "an unknown depth must not start the bridge")
  await start({}, ctxOf(1))
  assert.equal(existsSync(join(e.PRIME_WORKFLOW_STATE, "workflow-state", "bridge.sock")), false, "a child must not start the bridge")
  await start({}, ctxOf(0))
  assert.equal(existsSync(join(e.PRIME_WORKFLOW_STATE, "workflow-state", "bridge.sock")), true, "the top-level session serves the socket")
  // A second activation in the same repo cannot take the writer: it skips with a receipt, not a throw.
  const second = { registerTool: () => {}, on: () => {} }
  assert.doesNotThrow(() => primeLabWorkflow(second, e))
})

test("the capability is OFF unless switched on", () => {
  const off = host(); primeLabWorkflow(off, env())
  assert.equal(off.tools.length, 0)
  const on = host(); primeLabWorkflow(on, { ...env(), PRIME_WORKFLOW: "on" })
  assert.deepEqual(on.tools.map((t) => t.name), ["workflow_save", "workflow_plan", "workflow_plan_from_tasks", "workflow_record", "workflow_migrate_definitions"])
})

test("the plan says what it does not do", async () => {
  // The boundary is the point: Prime's ExtensionAPI cannot execute Python or spawn an RLM child,
  // so a plan that read like an execution would be promising something it cannot deliver.
  const e = { ...env(), PRIME_WORKFLOW: "on" }
  const pi = host(); primeLabWorkflow(pi, e)
  const [save, plan] = pi.tools
  await save.execute("c", { name: "w", steps: [step("a"), step("b", ["a"])] }, undefined, undefined, {})
  const out = await plan.execute("c", { name: "w" }, undefined, undefined, {})
  assert.match(out.content[0].text, /does not execute anything/)
  assert.equal(out.details.waves, 2)
})

test("recording against a workflow that does not exist is refused", async () => {
  const e = { ...env(), PRIME_WORKFLOW: "on" }
  const pi = host(); primeLabWorkflow(pi, e)
  const record = pi.tools.find((t) => t.name === "workflow_record")
  await assert.rejects(
    () => record.execute("c", { name: "ghost", step: "a", outcome: "ok" }, undefined, undefined, {}),
    /no workflow ghost/,
  )
})

test("workflow_plan with no name lists what is saved", async () => {
  const e = { ...env(), PRIME_WORKFLOW: "on" }
  const pi = host(); primeLabWorkflow(pi, e)
  const [save, plan] = pi.tools
  await save.execute("c", { name: "one", steps: [step("a")] }, undefined, undefined, {})
  const out = await plan.execute("c", {}, undefined, undefined, {})
  assert.deepEqual(out.details.workflows, ["one"])
})

// NACHGETRAGEN 11.09.2026. Der Kommentar in `saveWorkflow` sagt seit dem 06.09. „Both doors end
// here, so the check is made here" — die Tuer schloss aber nur bei der EINEN Schreibweise, die
// `stepPrompt()` erzeugt. Handgeschriebene Prompts sind kein Randfall: `workflow_save` nimmt sie
// entgegen, und am selben Tag hat ein Agent genau so einen gespeichert. Gemessen: vier von fuenf
// Markerformen kamen mit `|| true` durch.
test("eine Abnahmezeile ohne die Marker-Form sperrt genauso", () => {
  const env = envDir()
  const kannNicht = "pytest -q tests/t.py || true"
  const formen = [
    ["exakt", `Tu es.\nAcceptance check (must fail when the requirement is violated): ${kannNicht}`],
    ["ohne Klammer", `Tu es.\nAcceptance check: ${kannNicht}`],
    ["Acceptance:", `Tu es.\nAcceptance: ${kannNicht}`],
    ["Check:", `Tu es.\nCheck: ${kannNicht}`],
    ["Abnahme:", `Tu es.\nAbnahme: ${kannNicht}`],
  ]
  for (const [name, prompt] of formen) {
    assert.throws(
      () => saveWorkflow(env, `w-${name.replace(/\W+/g, "-").toLowerCase()}`, [{ id: "a", prompt }]),
      /cannot fail/,
      `${name}: eine Abnahme, die nicht scheitern kann, darf durch keine Tuer`,
    )
  }
  assert.deepEqual(listWorkflows(env), [], "und nichts davon landet im Store")
})

// Die Gegenprobe, ohne die der Fall oben nur zeigt, dass etwas verweigert wird. Am Material
// kalibriert: vier Prosazeilen, die zufaellig mit einem der Schluesselwoerter beginnen.
test("Prosa mit demselben Schluesselwort wird nicht verweigert", () => {
  const env = envDir()
  const prosa = [
    "Check: make sure the tests pass before you finish.",
    "Acceptance: the reviewer signs off.",
    "Denk dran: Check the README first.",
    "Abnahme: zwei Augen, ein Commit.",
  ]
  prosa.forEach((prompt, i) => {
    saveWorkflow(env, `p-${i}`, [{ id: "a", prompt }])
  })
  assert.equal(listWorkflows(env).length, prosa.length, "keine dieser Zeilen ist eine Sperre")
})
