// The planner side of P4: TASK_* blocks become Steps, waves come from TASK_DEPS, and every
// acceptance check that cannot fail is refused by a thrown error that names the key and the line —
// with nothing written to the store. Each banned form is one test, so a form that stops being
// caught turns exactly one line red.
import assert from "node:assert/strict"
import test from "node:test"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import primeLabWorkflow, { WorkflowError, bannedCheck, listWorkflows, loadWorkflow, planFromTasks, saveWorkflow } from "../index.ts"

const env = () => ({ PRIME_AGENT_CODING_AGENT_DIR: mkdtempSync(join(tmpdir(), "wf-tasks-")), PRIME_WORKFLOW_STATE: mkdtempSync(join(tmpdir(), "wf-tasks-state-")) })
const host = () => { const t = []; return { registerTool: (x) => t.push(x), tools: t } }

/** A block in the shape the SKILL.md asks for; `check` and `extra` vary per test. */
const block = (key, { deps = "NONE", check = `pytest tests/test_${key}.py`, model, title, objective } = {}) =>
  [
    `TASK_KEY=${key}`,
    `TASK_TITLE=${title ?? `Do ${key}`}`,
    `TASK_OBJECTIVE=${objective ?? `Make ${key} true. It is needed.`}`,
    `TASK_DEPS=${deps}`,
    `TASK_OWNS_PATHS=src/${key}.py, tests/test_${key}.py`,
    `TASK_ACCEPTANCE_CHECK=${check}`,
    `TASK_PARALLEL_SAFE=${deps === "NONE" ? "true" : "false"}`,
    ...(model ? [`TASK_MODEL=${model}`] : []),
  ].join("\n")

// Line numbers, for the error-message assertions below: 1 PROJECT_DONE, 2 REASON, 3 blank, block a
// at 4–10 (TASK_KEY 4, TITLE 5, OBJECTIVE 6, DEPS 7, OWNS 8, CHECK 9, PARALLEL 10, MODEL 11 if
// present), a blank, block b at 12–18 (TASK_KEY 12 … CHECK 17).
const plan = (...blocks) => ["PROJECT_DONE=false", "REASON=Two things are missing.", "", blocks.join("\n\n")].join("\n")

test("a valid DAG becomes steps with waves from TASK_DEPS", () => {
  const out = planFromTasks(plan(block("a"), block("b", { deps: "a" }), block("c", { deps: "a" }), block("d", { deps: "b, c" })))
  assert.equal(out.project_done, false)
  assert.equal(out.reason, "Two things are missing.")
  assert.deepEqual(out.steps.map((s) => s.id), ["a", "b", "c", "d"])
  assert.deepEqual(out.steps[3].depends_on, ["b", "c"])
  assert.equal(out.steps[0].depends_on, undefined, "NONE means no depends_on field")
  assert.deepEqual(out.waves, [["a"], ["b", "c"], ["d"]])
  assert.deepEqual(out.rejected, [])
  assert.match(out.steps[1].prompt, /^Do b: Make b true\. It is needed\. Owns: src\/b\.py, tests\/test_b\.py\. Acceptance check \(must fail when the requirement is violated\): pytest tests\/test_b\.py$/)
  assert.deepEqual(out.tasks[0].owns_paths, ["src/a.py", "tests/test_a.py"])
  assert.equal(out.tasks[0].parallel_safe, true)
  assert.equal(out.tasks[1].parallel_safe, false)
})

test("every step carries the structured fields the executor needs, not only prose", () => {
  // W02: parallel_safe/owns_paths/acceptance_check used to survive only as prose inside the prompt.
  const out = planFromTasks(plan(block("a"), block("b", { deps: "a" })))
  for (const step of out.steps) {
    assert.equal(step.outcome_contract.kind, "command")
    assert.equal(typeof step.outcome_contract.command, "string")
    assert.deepEqual(step.owns_paths, [`src/${step.id}.py`, `tests/test_${step.id}.py`])
    assert.equal(typeof step.parallel_safe, "boolean")
    assert.equal(step.purpose, `Do ${step.id}`)
  }
  assert.equal(out.steps[0].parallel_safe, true)
  assert.equal(out.steps[1].parallel_safe, false)
  assert.deepEqual(out.steps[0].outcome_contract, { kind: "command", command: "pytest tests/test_a.py" })
})

test("TASK_MODEL: the Argus tier words select nothing and are reported; an explicit id is kept", () => {
  // W07: "cheap" used to survive as a literal model name and could only fail at spawn time.
  const out = planFromTasks(plan(block("a", { model: "normal" }), block("b", { model: "cheap" }), block("c", { model: "gpt-5.4-mini" }), block("d")))
  assert.equal(out.steps[0].model, undefined, "normal is inherit")
  assert.equal(out.steps[1].model, undefined, "a tier is not a model; the child inherits")
  assert.equal(out.steps[2].model, "gpt-5.4-mini")
  assert.equal(out.steps[3].model, undefined, "no TASK_MODEL means inherit")
  assert.deepEqual(out.rejected.map((r) => [r.line, r.reason]), [
    [11, "model tier 'normal' is not a model; the child inherits the session model"],
    [20, "model tier 'cheap' is not a model; the child inherits the session model"],
  ])
})

test("TASK_MODEL: there is no tier table anymore — a passed mapping is ignored", () => {
  const out = planFromTasks(plan(block("a", { model: "CHEAP" }), block("b"), block("c", { model: "claude-sonnet-4-5" })))
  assert.equal(out.steps[0].model, undefined)
  assert.equal(out.steps[1].model, undefined)
  assert.equal(out.steps[2].model, "claude-sonnet-4-5")
  assert.equal(out.rejected.length, 1)
  assert.match(out.rejected[0].reason, /tier 'cheap' is not a model/)
})

test("TASK_MODEL that is neither a tier nor a model id is refused with key and line", () => {
  assert.throws(() => planFromTasks(plan(block("a", { model: "the fast one" }))), /TASK_MODEL at line 11 \(block a\) is not cheap, normal or a model id/)
})

// --- the ban list, one form per test -------------------------------------------------------

const refused = (check, pattern) => {
  const text = plan(block("a"), block("b", { deps: "a", check }))
  assert.throws(() => planFromTasks(text), WorkflowError)
  assert.throws(() => planFromTasks(text), /TASK_ACCEPTANCE_CHECK at line 17 \(block b\) cannot fail: /)
  assert.throws(() => planFromTasks(text), pattern)
}

test("banned: '|| true' in any position", () => {
  refused("pytest tests/ || true", /'\|\| true'/)
  refused("pytest tests/ ||true", /'\|\| true'/)
  refused("make check || :", /'\|\| true'/)
  assert.equal(bannedCheck("pytest -k 'a || b'"), null, "a shell || that is not followed by true is not the ban")
})

test("banned: 'or True'", () => {
  refused("python -c 'import x; assert x.ok() or True'", /'or True'/)
})

test("banned: 'exit 0' as the whole check, or forced at its end", () => {
  refused("exit 0", /'exit 0' as the whole check/)
  refused("pytest tests/; exit 0", /'exit 0' as the whole check/)
  refused("pytest tests/ || exit 0", /cannot fail/)
  assert.equal(bannedCheck("test $(./run.sh; echo $?) -eq 0"), null)
})

test("banned: the no-ops ':' and 'true'", () => {
  refused(":", /no-op command/)
  refused("true", /no-op command/)
  refused("/bin/true", /no-op command/)
})

test("banned: an empty check", () => {
  refused("", /empty check/)
  refused("   ", /empty check/)
})

test("banned: unconditional success in prose", () => {
  refused("always passes", /unconditional success/)
  refused("N/A", /unconditional success/)
  refused("none", /unconditional success/)
  refused("the check always succeeds after the edit", /unconditional success/)
  refused("echo done", /unconditional success/)
  assert.equal(bannedCheck("echo 'x' | grep -q x"), null, "echo feeding a pipeline is a real check")
})

test("banned: an 'unchanged file' claim with no command that reads the file", () => {
  refused("README.md is unchanged", /'unchanged file' claim without a command/)
  refused("no changes to the public API", /'unchanged file' claim without a command/)
  refused("config.toml remains identical", /'unchanged file' claim without a command/)
  // The same claim WITH a command is exactly what the contract asks for.
  assert.equal(bannedCheck("git diff --exit-code -- README.md  # README.md unchanged"), null)
  assert.equal(bannedCheck("sha256sum -c pins.sha256  # originals unchanged"), null)
  assert.equal(bannedCheck("cmp before.json after.json"), null)
})

// --- the 23 candidates of the adversarial probe -----------------------------------------------
//
// `docs/verify/p4-p5-20260906/ban_bypass.test.mjs` built a world where the requirement
// ("README.md keeps its original bytes") is ALREADY VIOLATED, ran each candidate in bash there,
// and called a candidate a bypass when the parser accepted it AND it still exited 0. Measured
// 2026-09-06 against the end-anchored regex list: 14 of 23 were bypasses.
//
// The table below is that probe's candidate list with the verdict this parser now returns, one
// row per candidate so a form that stops being caught names itself. The probe stays the oracle —
// it runs bash; this table only pins the verdicts so a regression here is a diff, not a rerun.
//
// ONE candidate is accepted, and that is not a leftover bypass: `verify_readme_unchanged.sh` is a
// script that does not exist, so it exits 127. The parser cannot know whether a script exists and
// must not guess. The eight other rows the probe counted as "not a bypass" are NOT legitimate
// checks — they are prose that bash runs as a command name (exit 127), or forms the old list
// already refused. Only one of the 23 is a check that can fail.
const PROBE_CANDIDATES = [
  ["|| true then more:            grep … || true && echo checked",   "grep -q ORIGINAL README.md || true && echo checked",    "'|| true'"],
  ["|| true with comment:         grep … || true # verified",        "grep -q ORIGINAL README.md || true # verified",         "'|| true'"],
  ["semicolon true:               grep … ; true",                    "grep -q ORIGINAL README.md; true",                     "the check's exit status is always 0"],
  ["semicolon colon:              grep … ; :",                       "grep -q ORIGINAL README.md; :",                        "the check's exit status is always 0"],
  ["|| echo:                      grep … || echo 'README changed'",  "grep -q ORIGINAL README.md || echo 'README changed'",   "the check's exit status is always 0"],
  ["|| printf:                    grep … || printf 'drift'",         "grep -q ORIGINAL README.md || printf 'drift\\n'",        "the check's exit status is always 0"],
  ["exit 0 not at end:            grep … ; exit 0 # done",           "grep -q ORIGINAL README.md; exit 0 # done",            "the check's exit status is always 0"],
  ["tautology test:               [ 1 = 1 ]",                        "[ 1 = 1 ]",                                            "the check's exit status is always 0"],
  ["true && echo:                 true && echo '… unchanged'",       "true && echo 'README.md unchanged'",                   "the check's exit status is always 0"],
  ["unchanged claim + ls:         prose: ls README.md",              "README.md remains unchanged: ls README.md",            "'unchanged file' claim without a command"],
  ["unchanged claim + cat:        prose: cat README.md",             "README.md is byte-identical: cat README.md",           "'unchanged file' claim without a command"],
  ["unchanged claim + test -f:    prose: test -f README.md",         "README.md not modified: test -f README.md",            "'unchanged file' claim without a command"],
  ["pipe swallows status:         cat … | grep -c … ; true",         "cat README.md | grep -c ORIGINAL > /dev/null; true",   "the check's exit status is always 0"],
  ["|| true fullwidth pipes:      grep … ｜｜ true",                  "grep -q ORIGINAL README.md ｜｜ true",                   "'|| true'"],
  ["nonexistent command:          verify_readme_unchanged.sh",       "verify_readme_unchanged.sh",                           null],
  ["unchanged + ls (runnable):    ls README.md # … unchanged",       "ls README.md  # README.md unchanged",                  "'unchanged' claim tested only for existence"],
  ["unchanged + test -f runnable: test -f … && echo unchanged",      "test -f README.md && echo unchanged",                  "'unchanged' claim tested only for existence"],
  ["unchanged + stat runnable:    stat … # byte-identical",          "stat README.md >/dev/null  # byte-identical",          "'unchanged' claim tested only for existence"],
  ["only prints (printf):         printf '… unchanged'",             "printf 'README.md unchanged\\n'",                       "the check's exit status is always 0"],
  ["only prints (echo + ;):       echo 'looks fine'; echo done",     "echo 'looks fine'; echo done",                         "the check's exit status is always 0"],
  ["|| true NBSP:                 grep … ||<U+00A0>true",           "grep -q ORIGINAL README.md ||\u00a0true",             "'|| true'"],
  ["|| true trailing spaces:      grep … || true␣␣␣",                "grep -q ORIGINAL README.md || true   ",                "'|| true'"],
  ["|| exit 0 mid:                grep … || exit 0 ; echo x",        "grep -q ORIGINAL README.md || exit 0 ; echo x",        "'|| true'"],
]

for (const [label, check, expected] of PROBE_CANDIDATES) {
  test(`probe candidate — ${label} → ${expected ?? "accepted (exits 127, it can fail)"}`, () => {
    assert.equal(bannedCheck(check), expected, JSON.stringify(check))
  })
}

test("the probe's arithmetic: 22 of its 23 candidates are refused, and the 23rd can fail", () => {
  const refused = PROBE_CANDIDATES.filter(([, check]) => bannedCheck(check) !== null)
  assert.equal(refused.length, 22, PROBE_CANDIDATES.filter(([, c]) => !bannedCheck(c)).map(([l]) => l).join("; "))
})

test("the ban belongs to the STORE: what planFromTasks refuses, workflow_save refuses too", async () => {
  // `workflow_save` took a step prompt verbatim and stored it, so the SAME acceptance check was a
  // refusal through one door and a stored plan through the other (meta-audit 2026-09-06). The
  // string below is built by `planFromTasks` itself, so there is no chance of testing a paraphrase.
  const e = env()
  const good = planFromTasks(plan(block("a", { check: "pytest tests/test_a.py" })))
  const prompt = good.steps[0].prompt
  assert.match(prompt, /Acceptance check \(must fail when the requirement is violated\): pytest tests\/test_a\.py$/)

  for (const check of ["pytest tests/ || true", "grep -q X README.md; true", "ls README.md  # README.md unchanged", "[ 1 = 1 ]"]) {
    const refusedBan = bannedCheck(check)
    assert.ok(refusedBan, `${check} must be banned at all`)
    assert.throws(() => planFromTasks(plan(block("a", { check }))), /cannot fail/, `planFromTasks accepted ${check}`)

    // The exact prompt the parser would have produced, handed straight to workflow_save.
    const smuggled = prompt.replace("pytest tests/test_a.py", check)
    const t = host()
    primeLabWorkflow(t, { ...e, PRIME_WORKFLOW: "on" })
    const save = t.tools.find((x) => x.name === "workflow_save")
    await assert.rejects(
      () => save.execute("id", { name: "smuggled", steps: [{ id: "one", prompt: smuggled }] }),
      new RegExp(`acceptance check that cannot fail`),
      `workflow_save stored ${check}`,
    )
    assert.deepEqual(listWorkflows(e), [], "and nothing reached the store")
  }
})

test("workflow_save leaves a prompt without an acceptance-check marker alone", () => {
  // The ban reads the marked check only. A free-text step prompt is not a check and must not be
  // guessed at — `saveWorkflow` is also how a plain multi-step program is stored.
  const e = env()
  const t = host()
  primeLabWorkflow(t, { ...e, PRIME_WORKFLOW: "on" })
  const saved = saveWorkflow(e, "plain", [{ id: "one", prompt: "true" }, { id: "two", prompt: "Read the store and report." }])
  assert.equal(saved.steps.length, 2)
  assert.deepEqual(listWorkflows(e), ["plain"])
})

test("the tokeniser does not refuse a check that can fail", () => {
  // Each of these has a shape the tokeniser has to reason about rather than pattern-match:
  // a `||` inside a quoted argument, a `;` inside a command substitution, an `echo` that only
  // feeds a pipeline, an `&& echo` whose left side can fail, and an identity claim next to a
  // command that really does read the bytes.
  for (const ok of [
    "pytest -k 'a || b'",
    "test $(./run.sh; echo $?) -eq 0",
    "echo 'x' | grep -q x",
    "sha256sum -c pins.sha256 && echo ok",
    "test -f out.json && jq -e '.ok' out.json",
    "make check && pytest tests/",
    "bash scripts/verify.sh 2>/dev/null",
    "diff <(sort a) <(sort b)",
    "git diff --exit-code -- README.md  # README.md unchanged",
    "[ -s out/report.json ]",
    "uv run --frozen python -m pytest -q tests/test_x.py",
  ]) assert.equal(bannedCheck(ok), null, ok)
})

test("a check that can fail is accepted in every shape the contract allows", () => {
  for (const ok of [
    "pytest tests/webapi/test_index_cache.py",
    "npm test -- stream.test.ts",
    "python scripts/benchmark_hotpath.py",
    "scope artifacts agree",
    "node --test tests/plan_from_tasks.test.mjs",
    "git diff --exit-code",
    "[ -s out/report.json ]",
  ]) assert.equal(bannedCheck(ok), null, ok)
})

// --- structure --------------------------------------------------------------------------------

test("a DEPS cycle is refused with the steps in it", () => {
  const text = plan(block("a", { deps: "c" }), block("b", { deps: "a" }), block("c", { deps: "b" }))
  assert.throws(() => planFromTasks(text), /cycle among steps: a, b, c/)
})

test("a dependency on a key that is not a task names both", () => {
  assert.throws(() => planFromTasks(plan(block("a", { deps: "ghost" }))), /a depends on ghost, which is not a step/)
})

test("a self-dependency is refused at its line", () => {
  assert.throws(() => planFromTasks(plan(block("a", { deps: "a" }))), /TASK_DEPS at line 7: a depends on itself/)
})

test("a duplicate TASK_KEY is refused with both lines", () => {
  assert.throws(() => planFromTasks(plan(block("a"), block("a"))), /TASK_KEY at line 12 repeats a \(first at line 4\)/)
})

test("a block missing a required key is refused by key and block", () => {
  const noCheck = ["PROJECT_DONE=false", "REASON=r", "", "TASK_KEY=a", "TASK_TITLE=t", "TASK_OBJECTIVE=o", "TASK_DEPS=NONE"].join("\n")
  assert.throws(() => planFromTasks(noCheck), /TASK_ACCEPTANCE_CHECK missing in block a \(TASK_KEY at line 4\)/)
  const noTitle = ["PROJECT_DONE=false", "REASON=r", "", "TASK_KEY=a", "TASK_OBJECTIVE=o", "TASK_ACCEPTANCE_CHECK=pytest"].join("\n")
  assert.throws(() => planFromTasks(noTitle), /TASK_TITLE missing in block a/)
})

test("a TASK_* key before any TASK_KEY is refused at its line", () => {
  assert.throws(() => planFromTasks("PROJECT_DONE=false\nREASON=r\nTASK_TITLE=t\n"), /TASK_TITLE at line 3 appeared before any TASK_KEY/)
})

test("a TASK_KEY that is not a storable step id is refused", () => {
  assert.throws(() => planFromTasks(plan(block("fix parser"))), /TASK_KEY at line 4 is not a step id: "fix parser"/)
  assert.throws(() => planFromTasks(plan(block("../x"))), /not a step id/)
})

test("the PROJECT_DONE header is required and must be true or false", () => {
  assert.throws(() => planFromTasks(block("a")), /PROJECT_DONE header missing/)
  assert.throws(() => planFromTasks("PROJECT_DONE=maybe\nREASON=r\n" + block("a")), /PROJECT_DONE is not true or false/)
  assert.throws(() => planFromTasks("PROJECT_DONE=false\nREASON=r\n"), /PROJECT_DONE=false with no TASK blocks/)
  assert.throws(() => planFromTasks("PROJECT_DONE=true\nREASON=r\n" + block("a")), /PROJECT_DONE=true with 1 TASK block/)
})

test("PROJECT_DONE=true is a plan with no steps and the reason", () => {
  const out = planFromTasks("PROJECT_DONE=true\nREASON=Already merged in 3f2a1.\n")
  assert.equal(out.project_done, true)
  assert.equal(out.reason, "Already merged in 3f2a1.")
  assert.deepEqual(out.steps, [])
  assert.deepEqual(out.waves, [])
})

test("lines the parser does not use are reported, not dropped: prose, unknown keys, a stray LEVEL", () => {
  const text = ["Here is my plan:", "PROJECT_DONE=false", "REASON=r", "", block("a"), "TASK_LEVEL=2", "TASK_HYPOTHESIS=h", "", "That is all."].join("\n")
  const out = planFromTasks(text)
  assert.deepEqual(out.steps.map((s) => s.id), ["a"])
  assert.deepEqual(out.rejected.map((r) => [r.line, r.reason]), [
    [1, "not a KEY=value line"],
    [12, "unknown key TASK_LEVEL"],
    [13, "unknown key TASK_HYPOTHESIS"],
    [15, "not a KEY=value line"],
  ])
})

test("the shapes a model actually produces are read: 'KEY: value', lowercase keys, backticks, a code fence", () => {
  const text = ["```", "project_done: false", "reason: r", "", "`TASK_KEY=a`", "task_title: T", "TASK_OBJECTIVE: O", "TASK_DEPS: none", "TASK_ACCEPTANCE_CHECK: pytest tests/", "```"].join("\n")
  const out = planFromTasks(text)
  assert.deepEqual(out.steps.map((s) => s.id), ["a"])
  assert.deepEqual(out.rejected, [])
})

// --- the tool ---------------------------------------------------------------------------------

const on = () => {
  const e = { ...env(), PRIME_WORKFLOW: "on" }
  const pi = host(); primeLabWorkflow(pi, e)
  return { e, tool: pi.tools.find((t) => t.name === "workflow_plan_from_tasks") }
}

test("the tool is registered and saves a valid plan through the same store as workflow_save", async () => {
  const { e, tool } = on()
  assert.ok(tool, "workflow_plan_from_tasks is registered")
  assert.deepEqual(tool.parameters.required, ["text", "name"])
  const out = await tool.execute("c", { text: plan(block("a"), block("b", { deps: "a", model: "cheap" })), name: "p4" }, undefined, undefined, {})
  assert.equal(out.details.saved, true)
  assert.equal(out.details.waves, 2)
  assert.match(out.content[0].text, /# p4 \(p4-[0-9a-f]{8} rev 1, [0-9a-f]{12}…\): 2 steps in 2 dependency levels/)
  assert.match(out.content[0].text, /\*\*b\*\* \(parallel_safe=false\)/)
  assert.match(out.content[0].text, /1 line\(s\) not used/)
  assert.deepEqual(listWorkflows(e), ["p4"])
  const back = loadWorkflow(e, "p4")
  assert.deepEqual(back.steps.map((s) => s.id), ["a", "b"])
  assert.equal(back.revision, 1)
  assert.equal(back.steps[1].model, undefined)
  assert.equal(back.steps[1].outcome_contract.kind, "command")
})

test("the tool throws on a banned check and writes nothing", async () => {
  // Prime never reads a returned isError (measured, host-contract/tests/tool_result_contract.test.mjs),
  // so a refusal has to be a throw — and the store has to be exactly as empty as before.
  const { e, tool } = on()
  await assert.rejects(
    () => tool.execute("c", { text: plan(block("a", { check: "make || true" })), name: "bad" }, undefined, undefined, {}),
    /workflow_plan_from_tasks: TASK_ACCEPTANCE_CHECK at line 9 \(block a\) cannot fail: '\|\| true'/,
  )
  assert.deepEqual(listWorkflows(e), [], "nothing was written")
})

test("the tool throws on a cycle and on a bad name, and writes nothing either way", async () => {
  const { e, tool } = on()
  await assert.rejects(
    () => tool.execute("c", { text: plan(block("a", { deps: "b" }), block("b", { deps: "a" })), name: "cyc" }, undefined, undefined, {}),
    /cycle among steps: a, b/,
  )
  await assert.rejects(
    () => tool.execute("c", { text: plan(block("a")), name: "../escape" }, undefined, undefined, {}),
    /not a workflow name/,
  )
  assert.deepEqual(listWorkflows(e), [])
})

test("the tool no longer maps tiers from the environment; it reports them", async () => {
  const e = { ...env(), PRIME_WORKFLOW: "on", PRIME_WORKFLOW_MODEL_CHEAP: "gpt-5.4-mini" }
  const pi = host(); primeLabWorkflow(pi, e)
  const tool = pi.tools.find((t) => t.name === "workflow_plan_from_tasks")
  const out = await tool.execute("c", { text: plan(block("a", { model: "cheap" }), block("b")), name: "tiers" }, undefined, undefined, {})
  const back = loadWorkflow(e, "tiers")
  assert.equal(back.steps[0].model, undefined)
  assert.equal(back.steps[1].model, undefined)
  assert.match(out.content[0].text, /tier 'cheap' is not a model/)
})

test("PROJECT_DONE=true through the tool saves nothing and says why", async () => {
  const { e, tool } = on()
  const out = await tool.execute("c", { text: "PROJECT_DONE=true\nREASON=done already", name: "nothing" }, undefined, undefined, {})
  assert.equal(out.details.saved, false)
  assert.match(out.content[0].text, /nothing saved\. REASON=done already/)
  assert.deepEqual(listWorkflows(e), [])
})

test("the tool reports the lines it did not use", async () => {
  const { tool } = on()
  const out = await tool.execute("c", { text: "preamble\n" + plan(block("a")) + "\nTASK_LEVEL=1", name: "r" }, undefined, undefined, {})
  assert.match(out.content[0].text, /2 line\(s\) not used:\n  line 1: not a KEY=value line: preamble\n  line \d+: unknown key TASK_LEVEL: TASK_LEVEL=1/)
  assert.equal(out.details.rejected.length, 2)
})
