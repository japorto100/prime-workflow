// Review regressions (implementation review 2026-09-24) — one pinned test per finding that was
// reproducible before the correction round: G02/G03/G04/G07/G08/G09/G10/G11/G12/G13/G17.
import assert from "node:assert/strict"
import test from "node:test"
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { spawn } from "node:child_process"
import { connect } from "node:net"
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
	markUnresolved,
	readEvents,
	readRun,
	recoverTornTail,
	settleStep,
	verifyAcceptedResult,
} from "../lib/service.ts"
import { BRIDGE_PROTOCOL, MAX_RESPONSE_BYTES, startBridge } from "../lib/bridge.ts"

const env = () => ({ PRIME_WORKFLOW_STATE: mkdtempSync(join(tmpdir(), "wf-review-")) })

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
	const writer = acquireWriter(e, { session_id: "s-review" })
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

const accepted = (e, w, attempt_id, step = "explore", run_id = "r1") => {
	const run = readRun(e, run_id)
	if (run && run.attempts && run.attempts[attempt_id] && run.attempts[attempt_id].status === "intended") {
		admitStep(e, w, { run_id, attempt_id, child_id: `sub-${String(attempt_id).slice(0, 8)}` })
	}
	const file = join(e.PRIME_WORKFLOW_STATE, `accepted-${attempt_id}.json`)
	writeFileSync(file, JSON.stringify({ phase: step, summary: "s", findings: [], coverage: [] }))
	return acceptResult(e, w, { run_id, attempt_id, source_path: file })
}

test("G02/G04/G17: a claim cannot exceed the declared program — unknown step, unsettled dep, limits", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, {
		run_id: "r1",
		definition_digest: "d",
		steps: [{ id: "a" }, { id: "b", depends_on: ["a"] }, { id: "c" }],
		limits: { max_parallel: 1 },
	})
	assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "ghost", child_name: "n" }), /not declared/)
	assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "b", child_name: "n" }), /dependency-blocked/)
	const a = claimStep(e, w, { run_id: "r1", step_id: "a", child_name: "n-a" })
	assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "c", child_name: "n-c" }), /max_parallel=1/)
	accepted(e, w, a.attempt_id, "a")
	settleStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, status: "succeeded" })
	assert.ok(claimStep(e, w, { run_id: "r1", step_id: "b", child_name: "n-b" }).attempt_id)
	w.release()
})

test("G07/G08: a refused transition writes nothing to the journal", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }] })
	const before = readEvents(e).events.length
	assert.throws(() => expandSteps(e, w, { run_id: "r1", cause: "x", steps: [{ id: "b", depends_on: ["ghost"] }] }), /not declared/)
	assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "ghost", child_name: "n" }), /not declared/)
	assert.equal(readEvents(e).events.length, before, "refusals must not poison the journal")
	assert.equal(readRun(e, "r1").state_version, 1)
	w.release()
})

test("G03/G05: succeeded needs an accepted result; a result is accepted once", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
	const a = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" })
	admitStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, child_id: "sub-1" })
	assert.throws(() => settleStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, status: "succeeded" }), /no accepted result/)
	accepted(e, w, a.attempt_id)
	assert.throws(() => accepted(e, w, a.attempt_id), /accepted once/)
	settleStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, status: "succeeded" })
	assert.equal(readRun(e, "r1").attempts[a.attempt_id].status, "succeeded")
	w.release()
})

test("G09: an op_intent without an op_result refuses the replay as interrupted", async () => {
	await withBridge(async ({ e, writer, bridge, c }) => {
		const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
		await c.send({ ...base, operation_id: "c0", operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] } })
		appendEvent(e, writer, { event: "op_intent", operation_id: "crash-1", operation: "attempt.claim", request_sha256: "x" })
		const replay = await c.send({ ...base, operation_id: "crash-1", operation: "attempt.claim", expected_run_version: 1, payload: { run_id: "r1", step_id: "explore", child_name: "n" } })
		assert.equal(replay.ok, false)
		assert.match(replay.error.message, /interrupted before its result was recorded/)
		assert.equal(readRun(e, "r1").state_version, 1, "an interrupted intent must not have applied")
	})
})

test("G10: releasing an old writer does not remove the successor's lock", () => {
	const e = env()
	const w1 = acquireWriter(e, { session_id: "old" })
	writeFileSync(join(e.PRIME_WORKFLOW_STATE, "workflow-state", "writer.lock"), JSON.stringify({ epoch: "dead", at: 0, pid: 999999, session_id: "dead" }))
	const w2 = acquireWriter(e, { session_id: "new" })
	w1.release()
	const lock = JSON.parse(readFileSync(join(e.PRIME_WORKFLOW_STATE, "workflow-state", "writer.lock"), "utf8"))
	assert.equal(lock.epoch, w2.epoch, "the successor still holds the lock")
	w2.release()
})

test("G11: two epochs in the same millisecond are different strings", () => {
	const e = env()
	const w1 = acquireWriter(e, {})
	const first = w1.epoch
	w1.release()
	const w2 = acquireWriter(e, {})
	assert.match(first, /^\d+-[0-9a-f]{8}$/)
	assert.notEqual(first, w2.epoch)
	w2.release()
})

test("G12/G13: expected_run_version is required and operation_id is a token", async () => {
	await withBridge(async ({ bridge, c }) => {
		const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
		await c.send({ ...base, operation_id: "c0", operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] } })
		const noVersion = await c.send({ ...base, operation_id: "c1", operation: "attempt.claim", payload: { run_id: "r1", step_id: "explore", child_name: "n" } })
		assert.equal(noVersion.ok, false)
		assert.match(noVersion.error.message, /needs an integer expected_run_version/)
		const badId = await c.send({ ...base, operation_id: "../escape", operation: "attempt.claim", expected_run_version: 1, payload: { run_id: "r1", step_id: "explore", child_name: "n" } })
		assert.equal(badId.ok, false)
		assert.match(badId.error.message, /unusable shape/)
	})
})

test("G01/P01: steps declared through the bridge land in the journal and gate claims", async () => {
	await withBridge(async ({ e, bridge, c }) => {
		const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
		const created = await c.send({ ...base, operation_id: "c0", operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }, { id: "verify", depends_on: ["explore"] }] } })
		assert.equal(created.ok, true)
		const refused = await c.send({ ...base, operation_id: "c1", operation: "attempt.claim", expected_run_version: 1, payload: { run_id: "r1", step_id: "verify", child_name: "n" } })
		assert.equal(refused.ok, false)
		assert.match(refused.error.message, /dependency-blocked/)
		const claimed = await c.send({ ...base, operation_id: "c2", operation: "attempt.claim", expected_run_version: 1, payload: { run_id: "r1", step_id: "explore", child_name: "n" } })
		assert.equal(claimed.ok, true)
		const journal = readEvents(e).events.map((event) => JSON.stringify(event)).join("\n")
		assert.match(journal, /"explore"/)
		assert.match(journal, /"verify"/)
	})
})


// ---------------------------------------------------------------- review round 2

test("R2: a traversing run_id is refused everywhere a path or key is built from it", async () => {
	const e = env()
	const w = acquireWriter(e, {})
	assert.throws(() => createRun(e, w, { run_id: "../escape", definition_digest: "d" }), /unusable shape/)
	assert.throws(() => createRun(e, w, { run_id: "ok", definition_digest: "d", steps: [{ id: "../x" }] }), /unusable shape/)
	createRun(e, w, { run_id: "ok", definition_digest: "d", steps: [{ id: "a" }] })
	assert.equal(verifyAcceptedResult(e, { result_ref: "../outside.json", digest: "x" }), false)
	assert.equal(verifyAcceptedResult(e, { result_ref: "runs/ok/results/a--01.json", digest: "" }), false)
	w.release()
	await withBridge(async ({ bridge, c }) => {
		const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
		await c.send({ ...base, operation_id: "r2c0", operation: "run.create", payload: { run_id: "ok", definition_digest: "d", steps: [{ id: "a" }] } })
		const reply = await c.send({ ...base, operation_id: "r2c1", operation: "result.accept", expected_run_version: 1, payload: { run_id: "../runs/ok", attempt_id: "nope", source_path: "/tmp/x" } })
		assert.equal(reply.ok, false)
		assert.match(reply.error.message, /unusable shape/)
	})
})

test("R2: a closed run accepts no further transitions", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }] })
	const a = claimStep(e, w, { run_id: "r1", step_id: "a", child_name: "n" })
	settleStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, status: "failed" })
	finishRun(e, w, { run_id: "r1", outcome: "failed" })
	const before = readEvents(e).events.length
	assert.throws(() => admitStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, child_id: "sub-x" }), /accepts no further admissions/)
	assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "a", child_name: "n2" }), /no new attempts/)
	assert.throws(() => settleStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, status: "abandoned" }), /accepts no further attempt transitions/)
	assert.throws(() => markUnresolved(e, w, { run_id: "r1", attempt_id: a.attempt_id, reason: "x" }), /accepts no further attempt transitions/)
	assert.equal(readEvents(e).events.length, before, "refusals after the close must not write")
	w.release()
})

test("R2: max_repairs is enforced, lease_ms must be a positive integer", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }], limits: { max_repairs: 1 } })
	const first = claimStep(e, w, { run_id: "r1", step_id: "a", child_name: "n1" })
	settleStep(e, w, { run_id: "r1", attempt_id: first.attempt_id, status: "failed" })
	const repair = claimStep(e, w, { run_id: "r1", step_id: "a", child_name: "n2" })
	settleStep(e, w, { run_id: "r1", attempt_id: repair.attempt_id, status: "failed" })
	assert.throws(() => claimStep(e, w, { run_id: "r1", step_id: "a", child_name: "n3" }), /reached max_repairs=1/)
	createRun(e, w, { run_id: "r2", definition_digest: "d", steps: [{ id: "a" }] })
	assert.throws(() => claimStep(e, w, { run_id: "r2", step_id: "a", child_name: "n", lease_ms: 0 }), /positive integer/)
	assert.throws(() => claimStep(e, w, { run_id: "r2", step_id: "a", child_name: "n", lease_ms: 1.5 }), /positive integer/)
	assert.ok(claimStep(e, w, { run_id: "r2", step_id: "a", child_name: "n", lease_ms: 1000 }).attempt_id)
	w.release()
})

test("R2: succeeded requires the frozen bytes to still match; a refused accept leaves no copy", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "explore" }] })
	const a = claimStep(e, w, { run_id: "r1", step_id: "explore", child_name: "n" })
	admitStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, child_id: "sub-1" })
	const frozen = accepted(e, w, a.attempt_id)
	rmSync(join(e.PRIME_WORKFLOW_STATE, "workflow-state", frozen.result_ref), { force: true })
	assert.throws(() => settleStep(e, w, { run_id: "r1", attempt_id: a.attempt_id, status: "succeeded" }), /no longer matches its digest/)

	createRun(e, w, { run_id: "r2", definition_digest: "d", steps: [{ id: "explore" }] })
	const b = claimStep(e, w, { run_id: "r2", step_id: "explore", child_name: "n2" })
	admitStep(e, w, { run_id: "r2", attempt_id: b.attempt_id, child_id: "sub-2" })
	const file = join(e.PRIME_WORKFLOW_STATE, "orphan-source.json")
	writeFileSync(file, JSON.stringify({ phase: "explore", summary: "s", findings: [], coverage: [] }))
	const journal = join(e.PRIME_WORKFLOW_STATE, "workflow-state", "journal.jsonl")
	chmodSync(journal, 0o400)
	try {
		assert.throws(() => acceptResult(e, w, { run_id: "r2", attempt_id: b.attempt_id, source_path: file }))
	} finally {
		chmodSync(journal, 0o640)
	}
	const orphan = join(e.PRIME_WORKFLOW_STATE, "workflow-state", "runs", "r2", "results", "explore--01.json")
	assert.equal(existsSync(orphan), false, "a result that was never booked must not leave a frozen copy")
	w.release()
})

test("R2: two torn fragments in the same millisecond are secured separately", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }] })
	const path = join(e.PRIME_WORKFLOW_STATE, "workflow-state", "journal.jsonl")
	const prefix = readFileSync(path)
	writeFileSync(path, Buffer.concat([prefix, Buffer.from([0xe2, 0x82])]))
	const first = recoverTornTail(e, w)
	writeFileSync(path, Buffer.concat([prefix, Buffer.from([0xf0, 0x9f])]))
	const second = recoverTornTail(e, w)
	assert.ok(first.secured && second.secured)
	assert.notEqual(first.secured, second.secured, "each secured fragment needs its own file")
	assert.notDeepEqual(readFileSync(first.secured), readFileSync(second.secured))
	w.release()
})

test("R2: simultaneous takeovers of a dead lock produce exactly one writer", async () => {
	const e = env()
	const state = join(e.PRIME_WORKFLOW_STATE, "workflow-state")
	mkdirSync(state, { recursive: true })
	const script = join(e.PRIME_WORKFLOW_STATE, "race-child.mjs")
	const service = new URL("../lib/service.ts", import.meta.url).pathname
	writeFileSync(
		script,
		[
			`import { acquireWriter } from ${JSON.stringify(service)};`,
			'import { writeFileSync } from "node:fs";',
			'const ready = process.env.WF_RACE_READY;',
			'const go = process.env.WF_RACE_GO;',
			`writeFileSync(ready, "ready");`,
			'while (true) { try { const { existsSync } = await import("node:fs"); if (existsSync(go)) break; } catch {} await new Promise((r) => setTimeout(r, 5)); }',
			'try {',
			'  const w = acquireWriter({ PRIME_WORKFLOW_STATE: process.env.PRIME_WORKFLOW_STATE }, { session_id: "child" });',
			'  console.log("acquired " + w.epoch);',
			'  await new Promise((r) => setTimeout(r, 1500));',
			'} catch (error) {',
			'  console.log("refused");',
			'}',
		].join("\n"),
	)
	for (let round = 0; round < 2; round += 1) {
		writeFileSync(join(state, "writer.lock"), JSON.stringify({ epoch: "dead-" + round, at: new Date(0).toISOString(), pid: 999999, session_id: "dead" }))
		const go = join(e.PRIME_WORKFLOW_STATE, `go-${round}`)
		const children = []
		for (let i = 0; i < 8; i += 1) {
			const ready = join(e.PRIME_WORKFLOW_STATE, `ready-${round}-${i}`)
			children.push(
				new Promise((done) => {
					const child = spawn(process.execPath, ["--experimental-strip-types", script], {
						env: { ...process.env, ...e, WF_RACE_READY: ready, WF_RACE_GO: go },
					})
					let out = ""
					child.stdout.on("data", (chunk) => {
						out += chunk
					})
					child.on("close", () => done(out))
				}),
			)
		}
		while (readdirSync(e.PRIME_WORKFLOW_STATE).filter((n) => n.startsWith(`ready-${round}-`)).length < 8) await new Promise((r) => setTimeout(r, 10))
		writeFileSync(go, "go")
		const outputs = await Promise.all(children)
		const acquired = outputs.filter((out) => out.includes("acquired"))
		assert.equal(acquired.length, 1, `round ${round}: ${outputs.join(" | ")}`)
		rmSync(join(state, "writer.lock"), { force: true })
	}
})

test("R2: a live bridge is not replaced; a dead socket file still is", async () => {
	const e = env()
	const w = acquireWriter(e, { session_id: "s" })
	const socket_path = join(e.PRIME_WORKFLOW_STATE, "workflow-state", "bridge.sock")
	const bridge = await startBridge(e, w, { socket_path })
	try {
		await assert.rejects(startBridge(e, w, { socket_path }), /live bridge already listens/)
	} finally {
		await bridge.close()
	}
	assert.equal(existsSync(socket_path), false)
	const again = await startBridge(e, w, { socket_path })
	await again.close()
	w.release()
	assert.equal(MAX_RESPONSE_BYTES, 4 * 1024 * 1024)
})


test("QA: max_children is gone; the bridge names it as unknown instead of double-counting", async () => {
	await withBridge(async ({ bridge, c }) => {
		const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
		const reply = await c.send({ ...base, operation_id: "mc1", operation: "run.create", payload: { run_id: "r-mc", definition_digest: "d", limits: { max_children: 2 } } })
		assert.equal(reply.ok, false)
		assert.match(reply.error.message, /limits\.max_children is not a known limit/)
		const accepted = await c.send({ ...base, operation_id: "mc2", operation: "run.create", payload: { run_id: "r-mc", definition_digest: "d", limits: { max_agents_total: 2, max_parallel: 1 } } })
		assert.equal(accepted.ok, true)
	})
})

test("QA: an interrupted run.create names the run that already exists", async () => {
	await withBridge(async ({ e, writer, bridge, c }) => {
		const base = { v: BRIDGE_PROTOCOL, driver_epoch: bridge.epoch }
		const created = await c.send({ ...base, operation_id: "q1", operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }] } })
		assert.equal(created.ok, true)
		appendEvent(e, writer, { event: "op_intent", operation_id: "q-crash", operation: "run.create", request_sha256: "x" })
		const replay = await c.send({ ...base, operation_id: "q-crash", operation: "run.create", payload: { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }] } })
		assert.equal(replay.ok, false)
		assert.match(replay.error.message, /interrupted/)
		assert.match(replay.error.message, /run r1 exists — run\.read it and continue it/)
	})
})

test("QA: the run-wide agent budget defaults to 50 and is recorded; explicit values win", () => {
	const e = env()
	const w = acquireWriter(e, {})
	createRun(e, w, { run_id: "r1", definition_digest: "d", steps: [{ id: "a" }] })
	assert.equal(readRun(e, "r1").limits.max_agents_total, 50, "no limits declared: the default budget is recorded")
	createRun(e, w, { run_id: "r2", definition_digest: "d", steps: [{ id: "a" }], limits: { max_agents_total: 2, max_parallel: 1 } })
	const r2 = readRun(e, "r2")
	assert.equal(r2.limits.max_agents_total, 2, "an explicit budget outweighs the default")
	assert.equal(r2.limits.max_parallel, 1)
	assert.throws(
		() => createRun(e, w, { run_id: "r3", definition_digest: "d", steps: [{ id: "a" }], limits: { max_agents_total: 0 } }),
		/positive integer/,
	)
	w.release()
})
