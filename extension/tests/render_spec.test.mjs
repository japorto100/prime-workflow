// P16f: the collapsed tool block is DATA, not a render function. A function cannot cross the
// daemon→client RPC boundary, so without `renderSpec` a connection-backed TUI prints a bare tool
// name and the whole result text — for `workflow_plan` the entire ordered plan.
//
// Every `fields[].path` must resolve against a REAL result: an invented key renders nothing while
// looking, in the source, exactly like a description of the call. Both branches of `workflow_plan`
// and both branches of `workflow_plan_from_tasks` are exercised, because one spec serves each pair
// and the keys the other branch does not produce must drop out rather than render "undefined".
// And the definitions must still load with the dist patches reverted, because `renderSpec` is not
// a field of Prime's `ToolDefinition` (dist/core/extensions/types.d.ts:334-367).
import assert from "node:assert/strict"
import test from "node:test"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import primeLabWorkflow from "../index.ts"

// The client patch's resolver, behaviour for behaviour
// (host-contract/dist-patch/prime-dist-patch.mjs:74-81).
const pick = (src, path) => String(path).split(".").reduce((o, k) => (o == null ? undefined : o[k]), src)
const renders = (v) => v !== undefined && v !== null && typeof v !== "object"

const env = () => ({ PRIME_WORKFLOW: "on", PRIME_AGENT_CODING_AGENT_DIR: mkdtempSync(join(tmpdir(), "wf-rs-")), PRIME_WORKFLOW_STATE: mkdtempSync(join(tmpdir(), "wf-rs-state-")) })
const host = () => { const tools = [], hooks = new Map(); return { registerTool: (x) => tools.push(x), on: (n, f) => hooks.set(n, f), tools, hooks } }

// Prime's own `ToolDefinition` field list. An unpatched host keeps these and nothing else.
const UPSTREAM_FIELDS = ["name", "label", "description", "promptSnippet", "promptGuidelines", "parameters",
  "renderShell", "replayBuiltInToolName", "prepareArguments", "executionMode", "execute", "renderCall", "renderResult"]
const unpatchedHost = () => {
  const tools = [], hooks = new Map()
  return {
    registerTool: (def) => {
      const kept = {}
      for (const key of UPSTREAM_FIELDS) if (key in def) kept[key] = def[key]
      tools.push(kept)
    },
    on: (n, f) => hooks.set(n, f),
    tools, hooks,
  }
}

function assertSpecShape(tool) {
  const spec = tool.renderSpec
  assert.ok(spec, `${tool.name} carries no renderSpec`)
  // The daemon patch moves it with `structuredClone` (prime-dist-patch.mjs:49). A function or any
  // other uncloneable value here would throw where the tool is registered, not where it renders.
  assert.doesNotThrow(() => structuredClone(spec), `${tool.name}: renderSpec must survive structuredClone`)
  assert.equal(typeof spec.badge, "string")
  assert.ok(spec.badge.length > 0, `${tool.name}: badge must name what the call reaches for`)
  assert.equal(typeof spec.collapsed, "string")
  const words = spec.collapsed.trim().split(/\s+/)
  assert.ok(words.length >= 2 && words.length <= 3, `${tool.name}: collapsed is "${spec.collapsed}", wanted 2-3 words`)
  assert.ok(Array.isArray(spec.fields))
  const keys = new Set()
  for (const f of spec.fields) {
    assert.equal(typeof f.key, "string"); assert.ok(f.key.length > 0)
    assert.ok(f.from === "args" || f.from === "details", `${tool.name}.${f.key}: from must be args|details`)
    assert.equal(typeof f.path, "string"); assert.ok(f.path.length > 0)
    assert.equal(keys.has(f.key), false, `${tool.name}: duplicate field key ${f.key}`)
    keys.add(f.key)
  }
}

function assertPathsRender(tool, details, args, optional = []) {
  for (const f of tool.renderSpec.fields) {
    const value = pick(f.from === "details" ? details : args, f.path)
    // The client drops null exactly as it drops undefined, so a branch that legitimately has
    // neither value is skipped on both.
    if ((value === undefined || value === null) && optional.includes(f.key)) continue
    assert.ok(renders(value), `${tool.name}.${f.key}: ${f.path} resolved to ${JSON.stringify(value)}, which the client drops`)
  }
}

// The shape the planner SKILL.md asks for, the same one plan_from_tasks.test.mjs builds.
const block = (key, deps = "NONE") => [
  `TASK_KEY=${key}`,
  `TASK_TITLE=Do ${key}`,
  `TASK_OBJECTIVE=Make ${key} true. It is needed.`,
  `TASK_DEPS=${deps}`,
  `TASK_OWNS_PATHS=src/${key}.py, tests/test_${key}.py`,
  `TASK_ACCEPTANCE_CHECK=pytest tests/test_${key}.py`,
  `TASK_PARALLEL_SAFE=${deps === "NONE" ? "true" : "false"}`,
].join("\n")
const plan = (...blocks) => ["PROJECT_DONE=false", "REASON=Two things are missing.", "", blocks.join("\n\n")].join("\n")

test("all five workflow tools carry a renderSpec of the honoured shape", () => {
  const pi = host()
  primeLabWorkflow(pi, env())
  assert.deepEqual(pi.tools.map((t) => t.name), ["workflow_save", "workflow_plan", "workflow_plan_from_tasks", "workflow_record", "workflow_migrate_definitions"])
  for (const tool of pi.tools) assertSpecShape(tool)
})

test("every field path resolves against a real result of its own tool", async () => {
  const pi = host()
  primeLabWorkflow(pi, env())
  const byName = Object.fromEntries(pi.tools.map((t) => [t.name, t]))

  const saveArgs = { name: "build", steps: [{ id: "a", prompt: "do a" }, { id: "b", prompt: "do b", depends_on: ["a"] }] }
  const saved = await byName.workflow_save.execute("c1", saveArgs)
  assertPathsRender(byName.workflow_save, saved.details, saveArgs)
  assert.equal(
    [byName.workflow_save.renderSpec.collapsed, ...byName.workflow_save.renderSpec.fields.map((f) => `${f.key}=${pick(saved.details, f.path)}`)].join(" "),
    "save workflow name=build steps=2 waves=2",
  )

  // The named branch: every field renders.
  const planArgs = { name: "build" }
  const planned = await byName.workflow_plan.execute("c2", planArgs)
  assertPathsRender(byName.workflow_plan, planned.details, planArgs)

  // The listing branch: `workflows` is an array the client drops, and `name`/`steps`/`waves` are
  // absent — so the line degrades to the label rather than printing "undefined".
  const listed = await byName.workflow_plan.execute("c3", {})
  for (const f of byName.workflow_plan.renderSpec.fields) {
    assert.equal(pick(listed.details, f.path), undefined, `${f.key} must be absent on the listing branch`)
  }
  assert.equal(renders(listed.details.workflows), false, "an array would be dropped, which is why it is not a field")

  const fromTasksArgs = { text: plan(block("a"), block("b", "a")), name: "planned" }
  const fromTasks = await byName.workflow_plan_from_tasks.execute("c4", fromTasksArgs)
  assertPathsRender(byName.workflow_plan_from_tasks, fromTasks.details, fromTasksArgs)
  assert.equal(fromTasks.details.project_done, false)

  // PROJECT_DONE=true stores nothing; `project_done` is the field that says so without expanding.
  const done = await byName.workflow_plan_from_tasks.execute("c5", { text: "PROJECT_DONE=true\nREASON=Nothing left.", name: "unused" })
  assertPathsRender(byName.workflow_plan_from_tasks, done.details, {}, ["name", "steps", "waves"])
  assert.equal(done.details.project_done, true)
  assert.equal(done.details.saved, false)

  // `workflow_record` answers `{ ok }` alone — its line is read from the arguments, and that is
  // the only place the step and its outcome exist.
  const recordArgs = { name: "build", step: "a", outcome: "passed" }
  const recorded = await byName.workflow_record.execute("c6", recordArgs)
  assertPathsRender(byName.workflow_record, recorded.details, recordArgs)
  assert.equal(
    [byName.workflow_record.renderSpec.collapsed, ...byName.workflow_record.renderSpec.fields.map((f) => `${f.key}=${pick(recordArgs, f.path)}`)].join(" "),
    "record outcome name=build step=a outcome=passed",
  )

  // `workflow_migrate_definitions` reports counts (arrays would be dropped by the client).
  const migrated = await byName.workflow_migrate_definitions.execute("c7", {})
  assertPathsRender(byName.workflow_migrate_definitions, migrated.details, {})
  assert.equal(migrated.details.migrated, 0, "a fresh store has nothing to migrate")
  assert.equal(migrated.details.refused, 0)
})

test("with the dist patches reverted the extension still loads and the tools still work", async () => {
  const pi = unpatchedHost()
  assert.doesNotThrow(() => primeLabWorkflow(pi, env()))
  assert.equal(pi.tools.length, 5)
  for (const tool of pi.tools) assert.equal("renderSpec" in tool, false, `${tool.name}: an unpatched host never sees the key`)

  const saved = await pi.tools[0].execute("c1", { name: "build", steps: [{ id: "a", prompt: "do a" }] })
  assert.equal(saved.details.steps, 1, "the upstream fallback path is untouched")
})
