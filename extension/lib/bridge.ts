/**
 * The local bridge between Python (the executor) and the workflow host service (M1.2).
 *
 * **Why a socket at all.** A Python skill cannot call extension tools and the host-request map is
 * closed; a package-owned Unix socket is the supported way around that (measured for JEV). The
 * socket is private: a fresh `mkdtemp` (0700) holds it, the socket itself is 0600, and there is no
 * TCP surface.
 *
 * **Idempotency is the point, not a detail.** If the host commits an operation and the answer is
 * lost on the wire, the Python side will retry. Every mutating request therefore carries an
 * `operation_id`; the same id with the same payload returns the recorded result without applying
 * anything twice, the same id with a different payload is refused, and the result is persisted
 * BEFORE the bridge reports success.
 *
 * **Epoch and version are gates, not annotations.** `driver_epoch` must be the epoch of the writer
 * that currently holds the lock; `expected_run_version` must be the run's current `state_version`.
 * A driver that lost its claim, or a caller reasoning from stale state, is refused.
 *
 * Protocol `prime-workflow/bridge/1`: one JSON request per line, one JSON response per line.
 */

import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, rmSync, unlinkSync } from "node:fs";
import { connect, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { agentDir } from "./agent-dir.ts";
import { ACTIVE, decideReconciliation, type ObservedChild, readLedgerDir } from "./reconcile.ts";
import {
	acceptResult,
	admitStep,
	appendEvent,
	claimStep,
	createRun,
	type Env,
	expandSteps,
	finishRun,
	markOrphaned,
	markUnresolved,
	type ResultSchema,
	type RunLimits,
	readEvents,
	readRun,
	readySteps,
	recoverTornTail,
	type StepSpec,
	settleStep,
	usableId,
	WorkflowStateError,
	type Writer,
} from "./service.ts";

export const BRIDGE_PROTOCOL = "prime-workflow/bridge/1";
export const MAX_REQUEST_BYTES = 64 * 1024;
/** Aligned with the Python client's RESPONSE_LIMIT: a projection that the service can produce must
 *  also be deliverable, otherwise a big run is applied but can never be confirmed (review round 2). */
export const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

export type BridgeRequest = {
	v?: unknown;
	operation_id?: unknown;
	operation?: unknown;
	driver_epoch?: unknown;
	expected_run_version?: unknown;
	payload?: unknown;
};

export type BridgeReply =
	| { ok: true; v: string; operation_id: string; result: unknown }
	| { ok: false; v: string; error: { class: string; message: string; code?: string } };

function sha256(value: unknown): string {
	return createHash("sha256")
		.update(JSON.stringify(value ?? null))
		.digest("hex");
}

/** Operations the bridge accepts, and what each needs from the request. */
const MUTATING = new Set([
	"run.create",
	"attempt.claim",
	"attempt.admit",
	"attempt.settle",
	"attempt.unresolved",
	"result.accept",
	"reconcile.run",
	"steps.expand",
	"run.finish",
]);
/** Every mutating run-scoped operation must state the version it reasoned from (review G12). */
const VERSION_REQUIRED = new Set([...MUTATING].filter((op) => op !== "run.create"));
/** Operation ids become journal data; a shape check keeps path-like ids out (review G13). */
const OPERATION_ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

function fail(errorClass: string, message: string, code?: string, operation_id?: string): BridgeReply {
	return {
		ok: false,
		v: BRIDGE_PROTOCOL,
		...(operation_id ? { operation_id } : {}),
		error: { class: errorClass, message, ...(code ? { code } : {}) },
	};
}

function asString(value: unknown, what: string): string {
	if (typeof value !== "string" || !value) throw new WorkflowStateError(`${what} must be a non-empty string`);
	return value;
}

function asOptionalString(value: unknown, what: string): string | undefined {
	if (value === undefined || value === null) return undefined;
	return asString(value, what);
}

function asId(value: unknown, what: string): string {
	const text = asString(value, what);
	if (!OPERATION_ID_SHAPE.test(text))
		throw new WorkflowStateError(`${what} has an unusable shape: ${JSON.stringify(text)}`);
	return text;
}

function asSteps(value: unknown): StepSpec[] {
	if (value === undefined || value === null) return [];
	if (!Array.isArray(value)) throw new WorkflowStateError("steps must be an array");
	return value.map((raw, index) => {
		const spec = raw as Record<string, unknown>;
		if (!spec || typeof spec !== "object") throw new WorkflowStateError(`steps[${index}] must be an object`);
		const id = asString(spec.id, `steps[${index}].id`);
		const depends_on =
			spec.depends_on === undefined
				? undefined
				: Array.isArray(spec.depends_on)
					? spec.depends_on.map((d) => asString(d, `steps[${index}].depends_on[]`))
					: (() => {
							throw new WorkflowStateError(`steps[${index}].depends_on must be an array`);
						})();
		const inputs =
			spec.inputs === undefined
				? undefined
				: Array.isArray(spec.inputs)
					? spec.inputs.map((i, j) => {
							const input = i as Record<string, unknown>;
							return {
								ref: asString(input?.ref, `steps[${index}].inputs[${j}].ref`),
								digest: asString(input?.digest, `steps[${index}].inputs[${j}].digest`),
							};
						})
					: (() => {
							throw new WorkflowStateError(`steps[${index}].inputs must be an array`);
						})();
		return { id, ...(depends_on === undefined ? {} : { depends_on }), ...(inputs === undefined ? {} : { inputs }) };
	});
}

function asLimits(value: unknown): RunLimits | null {
	if (value === undefined || value === null) return null;
	if (typeof value !== "object" || Array.isArray(value)) throw new WorkflowStateError("limits must be an object");
	const allowed = new Set(["max_parallel", "max_attempts_per_step", "max_repairs", "max_agents_total"]);
	const out: RunLimits = {};
	for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
		if (!allowed.has(key)) throw new WorkflowStateError(`limits.${key} is not a known limit`);
		if (typeof raw !== "number" || !Number.isInteger(raw) || raw <= 0)
			throw new WorkflowStateError(`limits.${key} must be a positive integer`);
		(out as Record<string, number>)[key] = raw;
	}
	return out;
}

function asObserved(value: unknown): ObservedChild[] {
	if (value === undefined || value === null) return [];
	if (!Array.isArray(value)) throw new WorkflowStateError("observed must be an array");
	return value.map((raw, index) => {
		const child = raw as Record<string, unknown>;
		if (!child || typeof child !== "object") throw new WorkflowStateError(`observed[${index}] must be an object`);
		return {
			...(child.name === undefined || child.name === null
				? {}
				: { name: asString(child.name, `observed[${index}].name`) }),
			...(child.child_id === undefined || child.child_id === null
				? {}
				: { child_id: asString(child.child_id, `observed[${index}].child_id`) }),
			...(child.status === undefined || child.status === null ? {} : { status: String(child.status) }),
		};
	});
}

/**
 * Idempotency lives in the journal, not in a side file (review G09): the intent is appended BEFORE
 * the transition, the recorded reply after it. A replay finds the reply; an interrupted operation
 * (intent without reply) is refused instead of being applied twice. No path is built from an
 * operation id, so traversal has no surface (review G13).
 */
function findOpRecord(
	env: Env,
	operation_id: string,
): { state: "none" } | { state: "result"; reply: BridgeReply; request_sha256: string } | { state: "intent" } {
	for (const event of readEvents(env).events) {
		if (event.operation_id !== operation_id) continue;
		if (event.event === "op_result") {
			return { state: "result", reply: event.reply as BridgeReply, request_sha256: String(event.request_sha256) };
		}
	}
	for (const event of readEvents(env).events) {
		if (event.operation_id === operation_id && event.event === "op_intent") return { state: "intent" };
	}
	return { state: "none" };
}

/**
 * Execute one request against the service. Mutating operations are validated, gated and recorded;
 * reads are not recorded. The writer is the server's single writer — requests never carry their own
 * pen. Payloads are validated here at runtime; a TypeScript assertion is not a check.
 */
export function handleRequest(env: Env, writer: Writer, raw: BridgeRequest): BridgeReply {
	const operation = typeof raw?.operation === "string" ? raw.operation : "";
	const operation_id = typeof raw?.operation_id === "string" ? raw.operation_id : "";
	if (raw?.v !== BRIDGE_PROTOCOL) {
		return fail("WorkflowStateError", `unsupported protocol ${JSON.stringify(raw?.v)}; expected ${BRIDGE_PROTOCOL}`);
	}
	if (!operation) return fail("WorkflowStateError", "request has no operation");

	const mutating = MUTATING.has(operation);
	if (mutating) {
		if (!operation_id)
			return fail("WorkflowStateError", `operation ${operation} is mutating and needs an operation_id`);
		if (!OPERATION_ID_SHAPE.test(operation_id))
			return fail("WorkflowStateError", `operation_id ${JSON.stringify(operation_id)} has an unusable shape`);
		const request_sha256 = sha256({ operation, payload: raw.payload ?? null });
		const seen = findOpRecord(env, operation_id);
		if (seen.state === "result") {
			if (seen.request_sha256 !== request_sha256)
				return fail("WorkflowStateError", `operation_id ${operation_id} was used with a different payload`);
			return seen.reply;
		}
		if (seen.state === "intent") {
			// A crash between the transition and its recorded reply must never re-apply. For
			// `run.create` the transition may already have happened, so name the way out instead of
			// leaving the caller with a dead operation_id and a confusing "already exists" (QA round).
			let hint = "";
			if (operation === "run.create") {
				const run_id = String((raw.payload as Record<string, unknown> | undefined)?.run_id ?? "");
				if (run_id && usableId(run_id) && readRun(env, run_id)) {
					hint = `; run ${run_id} exists — run.read it and continue it`;
				}
			}
			return fail(
				"WorkflowStateError",
				`operation ${operation_id} was interrupted before its result was recorded${hint}; reconcile before retrying`,
			);
		}
		if (typeof raw.driver_epoch !== "string" || raw.driver_epoch !== writer.epoch) {
			return fail(
				"WorkflowStateError",
				`stale driver_epoch ${JSON.stringify(raw.driver_epoch)}; the current epoch is ${writer.epoch}`,
				"STALE_DRIVER_EPOCH",
				operation_id,
			);
		}
		if (VERSION_REQUIRED.has(operation)) {
			if (typeof raw.expected_run_version !== "number" || !Number.isInteger(raw.expected_run_version)) {
				return fail("WorkflowStateError", `operation ${operation} needs an integer expected_run_version`);
			}
		}
	}

	const payload = (raw.payload ?? {}) as Record<string, unknown>;
	const versionGate = (): string | null => {
		const run_id = String(payload.run_id ?? "");
		if (run_id && !usableId(run_id)) return `run_id ${JSON.stringify(run_id)} has an unusable shape`;
		const run = readRun(env, run_id);
		if (!run) return `no run ${run_id}`;
		if (Number(raw.expected_run_version) !== run.state_version) {
			return `stale expected_run_version ${String(raw.expected_run_version)}; the run is at ${run.state_version}`;
		}
		return null;
	};

	const execute = (): BridgeReply => {
		try {
			switch (operation) {
				case "ping":
					return {
						ok: true,
						v: BRIDGE_PROTOCOL,
						operation_id,
						result: { epoch: writer.epoch, state_root: writer.state_root },
					};
				case "run.read": {
					const run_id = asString(payload.run_id, "run_id");
					const run = readRun(env, run_id);
					if (!run) return fail("WorkflowStateError", `no run ${run_id}`);
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: run };
				}
				case "run.create": {
					if (raw.expected_run_version !== undefined)
						return fail("WorkflowStateError", "run.create takes no expected_run_version");
					const run = createRun(env, writer, {
						run_id: asString(payload.run_id, "run_id"),
						definition_digest: asString(payload.definition_digest, "definition_digest"),
						definition_object: asOptionalString(payload.definition_object, "definition_object") ?? null,
						...(payload.subject === undefined || payload.subject === null
							? {}
							: typeof payload.subject === "object" && !Array.isArray(payload.subject)
								? { subject: payload.subject as Record<string, unknown> }
								: (() => {
										throw new WorkflowStateError("subject must be an object");
									})()),
						...(asLimits(payload.limits) === null ? {} : { limits: asLimits(payload.limits) as RunLimits }),
						steps: asSteps(payload.steps),
					});
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: run };
				}
				case "attempt.claim": {
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					const lease = payload.lease_ms === undefined ? undefined : Number(payload.lease_ms);
					if (lease !== undefined && (!Number.isInteger(lease) || lease <= 0))
						return fail("WorkflowStateError", "lease_ms must be a positive integer");
					const claimed = claimStep(env, writer, {
						run_id: asString(payload.run_id, "run_id"),
						step_id: asId(payload.step_id, "step_id"),
						child_name: asString(payload.child_name, "child_name"),
						...(lease === undefined ? {} : { lease_ms: lease }),
						...(asOptionalString(payload.role, "role") === undefined ? {} : { role: asString(payload.role, "role") }),
						...(asOptionalString(payload.role_digest, "role_digest") === undefined
							? {}
							: { role_digest: asString(payload.role_digest, "role_digest") }),
						...(payload.role_binding === undefined ? {} : { role_binding: payload.role_binding }),
					});
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: claimed };
				}
				case "attempt.admit": {
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					admitStep(env, writer, {
						run_id: asString(payload.run_id, "run_id"),
						attempt_id: asString(payload.attempt_id, "attempt_id"),
						child_id: asString(payload.child_id, "child_id"),
					});
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: readRun(env, String(payload.run_id)) };
				}
				case "attempt.settle": {
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					settleStep(env, writer, {
						run_id: asString(payload.run_id, "run_id"),
						attempt_id: asString(payload.attempt_id, "attempt_id"),
						status: asString(payload.status, "status") as "succeeded" | "failed" | "abandoned",
						...(asOptionalString(payload.note, "note") === undefined ? {} : { note: String(payload.note) }),
					});
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: readRun(env, String(payload.run_id)) };
				}
				case "attempt.unresolved": {
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					markUnresolved(env, writer, {
						run_id: asString(payload.run_id, "run_id"),
						attempt_id: asString(payload.attempt_id, "attempt_id"),
						reason: asString(payload.reason, "reason"),
					});
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: readRun(env, String(payload.run_id)) };
				}
				case "run.ready": {
					// A computation, never a dispatch: the caller decides what to start.
					const run_id = asString(payload.run_id, "run_id");
					const run = readRun(env, run_id);
					if (!run) return fail("WorkflowStateError", `no run ${run_id}`);
					const rawDigests = payload.current_input_digests ?? {};
					if (typeof rawDigests !== "object" || Array.isArray(rawDigests))
						return fail("WorkflowStateError", "current_input_digests must be an object");
					const digests: Record<string, string> = {};
					for (const [key, value] of Object.entries(rawDigests as Record<string, unknown>))
						digests[key] = asString(value, `current_input_digests.${key}`);
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: readySteps(run, digests) };
				}
				case "steps.expand": {
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					const run = expandSteps(env, writer, {
						run_id: asString(payload.run_id, "run_id"),
						steps: asSteps(payload.steps),
						cause: asString(payload.cause, "cause"),
					});
					return {
						ok: true,
						v: BRIDGE_PROTOCOL,
						operation_id,
						result: { state_version: run.state_version, step_specs: run.step_specs },
					};
				}
				case "run.finish": {
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					const outcome = asOptionalString(payload.outcome, "outcome");
					if (outcome !== undefined && !["done", "failed", "abandoned"].includes(outcome))
						return fail("WorkflowStateError", `outcome ${JSON.stringify(outcome)} is not done|failed|abandoned`);
					const run = finishRun(env, writer, {
						run_id: String(payload.run_id),
						...(outcome === undefined ? {} : { outcome: outcome as "done" | "failed" | "abandoned" }),
						...(asOptionalString(payload.note, "note") === undefined ? {} : { note: String(payload.note) }),
					});
					return {
						ok: true,
						v: BRIDGE_PROTOCOL,
						operation_id,
						result: { status: run.status, state_version: run.state_version },
					};
				}
				case "reconcile.run": {
					const run_id = asString(payload.run_id, "run_id");
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					const run = readRun(env, run_id);
					if (!run) return fail("WorkflowStateError", `no run ${run_id}`);
					const root = agentDir(env);
					const ledger = readLedgerDir(join(root, "rlm-ledger"), join(root, "sessions"));
					const actions = decideReconciliation({
						attempts: Object.values(run.attempts).filter((a) => ACTIVE.has(a.status)),
						spawns: ledger?.spawns ?? [],
						deletes: ledger?.deletes ?? [],
						observed: asObserved(payload.observed),
					});
					for (const action of actions) {
						if (action.action === "unresolved")
							markUnresolved(env, writer, { run_id, attempt_id: action.attempt_id, reason: action.reason });
						else markOrphaned(env, writer, { run_id, attempt_id: action.attempt_id, reason: action.reason });
					}
					return {
						ok: true,
						v: BRIDGE_PROTOCOL,
						operation_id,
						result: {
							actions,
							ledger_found: ledger !== null,
							unknown_ledger_lines: ledger?.unknown_lines ?? 0,
							state_version: (readRun(env, run_id) as { state_version: number }).state_version,
						},
					};
				}
				case "result.accept": {
					const gate = versionGate();
					if (gate) return fail("WorkflowStateError", gate);
					const origin = asOptionalString(payload.origin, "origin");
					if (origin !== undefined && !["child_file", "caller_reported"].includes(origin))
						return fail("WorkflowStateError", `origin ${JSON.stringify(origin)} is not child_file|caller_reported`);
					const accepted = acceptResult(env, writer, {
						run_id: asString(payload.run_id, "run_id"),
						attempt_id: asString(payload.attempt_id, "attempt_id"),
						source_path: asString(payload.source_path, "source_path"),
						...(payload.schema === undefined || payload.schema === null
							? {}
							: typeof payload.schema === "object" && !Array.isArray(payload.schema)
								? { schema: payload.schema as ResultSchema }
								: (() => {
										throw new WorkflowStateError("schema must be an object");
									})()),
						...(origin === undefined ? {} : { origin: origin as "child_file" | "caller_reported" }),
					});
					return { ok: true, v: BRIDGE_PROTOCOL, operation_id, result: accepted };
				}
				default:
					return fail("WorkflowStateError", `unknown operation ${JSON.stringify(operation)}`);
			}
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			const cls = error instanceof WorkflowStateError ? "WorkflowStateError" : "Error";
			return fail(cls, message);
		}
	};

	if (!mutating) return execute();

	const request_sha256 = sha256({ operation, payload: raw.payload ?? null });
	try {
		// Intent BEFORE the transition: if we crash in between, the replay is refused, never re-applied.
		appendEvent(env, writer, { event: "op_intent", operation_id, operation, request_sha256 });
		const reply = execute();
		// The recorded reply is durable before the caller sees it; if THIS append fails the intent
		// stands alone, so a replay is refused instead of applying the transition twice.
		appendEvent(env, writer, { event: "op_result", operation_id, operation, request_sha256, reply });
		return reply;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		const cls = error instanceof WorkflowStateError ? "WorkflowStateError" : "Error";
		return fail(cls, `operation ${operation} could not be recorded: ${message}`);
	}
}

export type Bridge = { socket_path: string; epoch: string; close: () => Promise<void> };

/** True when a live server accepts connections at this path; a dead socket file refuses them. */
function listenerAlive(socket_path: string): Promise<boolean> {
	return new Promise<boolean>((resolve) => {
		const probe = connect(socket_path);
		const done = (alive: boolean): void => {
			probe.removeAllListeners();
			probe.destroy();
			resolve(alive);
		};
		probe.once("connect", () => done(true));
		probe.once("error", () => done(false));
	});
}

/**
 * Start the server on a fresh private socket. The writer is claimed once for the server's lifetime;
 * if another writer holds it, the bridge refuses to start rather than sharing the pen.
 */
export async function startBridge(env: Env, writer: Writer, options: { socket_path?: string } = {}): Promise<Bridge> {
	// A leftover torn tail from a killed process is recovered before serving, under the writer lock.
	recoverTornTail(env, writer);
	// The extension serves at a fixed, discoverable path (`<state_root>/bridge.sock`) so the Python
	// skill can find it; tests keep the private mkdtemp path.
	const own_dir = options.socket_path ? null : mkdtempSync(join(tmpdir(), "prime-workflow-bridge-"));
	const dir = own_dir ?? dirname(options.socket_path as string);
	mkdirSync(dir, { recursive: true });
	const socket_path = options.socket_path ?? join(dir, "workflow.sock");
	// A socket file left by a dead previous host would make listen() fail; we hold the writer, so
	// anything still LISTENING here is a live server, not a leftover — replacing it would silently
	// orphan that service and its clients (review round 2).
	if (existsSync(socket_path)) {
		if (await listenerAlive(socket_path)) {
			throw new WorkflowStateError(`a live bridge already listens at ${socket_path}; refusing to replace it`);
		}
		try {
			unlinkSync(socket_path);
		} catch {
			// best effort; listen() will report if it is still there
		}
	}
	const server: Server = createServer((socket: Socket) => {
		let buffer = "";
		// A peer that connects but never finishes a line must not hold the bridge forever.
		socket.setTimeout(10_000, () => socket.destroy());
		socket.setEncoding("utf8");
		socket.on("data", (chunk: string) => {
			buffer += chunk;
			if (Buffer.byteLength(buffer, "utf8") > MAX_REQUEST_BYTES) {
				socket.end(`${JSON.stringify(fail("WorkflowStateError", `request exceeds ${MAX_REQUEST_BYTES} bytes`))}\n`);
				return;
			}
			let index = buffer.indexOf("\n");
			while (index >= 0) {
				const line = buffer.slice(0, index);
				buffer = buffer.slice(index + 1);
				if (line.trim()) {
					let request: BridgeRequest;
					try {
						request = JSON.parse(line) as BridgeRequest;
					} catch (error) {
						socket.write(`${JSON.stringify(fail("WorkflowStateError", `request is not JSON: ${String(error)}`))}\n`);
						index = buffer.indexOf("\n");
						continue;
					}
					let reply: BridgeReply;
					try {
						reply = handleRequest(env, writer, request);
					} catch (error) {
						// A handler that throws must still answer: a silent socket is worse than an error.
						const message = error instanceof Error ? error.message : String(error);
						reply = fail("Error", `request failed before a reply could be formed: ${message}`);
					}
					const text = `${JSON.stringify(reply)}\n`;
					if (Buffer.byteLength(text, "utf8") > MAX_RESPONSE_BYTES) {
						socket.write(
							`${JSON.stringify(fail("WorkflowStateError", `response exceeds ${MAX_RESPONSE_BYTES} bytes`))}\n`,
						);
					} else {
						socket.write(text);
					}
				}
				index = buffer.indexOf("\n");
			}
		});
		socket.on("error", () => {
			// A client that goes away must not take the server with it.
		});
	});
	return new Promise<Bridge>((resolve, reject) => {
		server.once("error", (error) => {
			try {
				if (own_dir) rmSync(own_dir, { recursive: true, force: true });
				else unlinkSync(socket_path);
			} catch {
				// best effort
			}
			reject(error);
		});
		server.listen(socket_path, () => {
			try {
				chmodSync(socket_path, 0o600);
			} catch (error) {
				reject(error);
				return;
			}
			server.unref();
			resolve({
				socket_path,
				epoch: writer.epoch,
				close: () =>
					new Promise<void>((done) => {
						server.close(() => {
							try {
								if (own_dir) rmSync(own_dir, { recursive: true, force: true });
								else unlinkSync(socket_path);
							} catch {
								// best effort
							}
							done();
						});
					}),
			});
		});
	});
}
