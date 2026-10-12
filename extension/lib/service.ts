/**
 * The workflow host service — M1.1. The durable side of a run: journal, exclusive writer, folds.
 *
 * **What this is not.** It does not spawn children, does not run a graph, and does not decide what
 * happens next. Python/RLM owns orchestration (see plan-archive/WORKFLOW_PLAN.md §0.6); this module owns the
 * durable execution contract: who may write, what was admitted, what is still open, and what a
 * resume is allowed to believe.
 *
 * **Why a journal and not key-value files.** The store's temp+rename is atomic per file and offers
 * no multi-file transaction. A run changes several facts at once (attempt intended → admitted →
 * settled), and a crash between two writes must leave a state a reader can trust. One appended,
 * fsync'd line per event is the only atomic unit this filesystem gives us.
 *
 * **One writer per repo root.** A second writer is refused; a stale lock whose holder PID is DEAD
 * is taken over immediately through an atomic rename (exactly one of two simultaneous takeovers can
 * win), a live holder never — not even with an expired lease. Every line carries
 * `epoch`, `session_id`, `rlm_depth`; a state change is refused when the writer's epoch is no
 * longer the lock's epoch, so a writer that lost its claim cannot confirm anything afterwards.
 *
 * **Torn tail is recovered, not ignored.** Under the writer lock the fragment is secured next to
 * the journal and the file is cut back to its last complete line before anything is appended —
 * otherwise the fragment would become corrupt content in the middle of the journal.
 *
 * **RLM ledger (M1.0 probe).** `<agentDir>/rlm-ledger/<hash>.jsonl` is readable by the host and
 * proves admission/identity, never liveness. Nothing here reads it yet; M1.7 combines it with a
 * kernel observation reported through the bridge.
 */
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import {
	closeSync,
	existsSync,
	fsyncSync,
	mkdirSync,
	openSync,
	readFileSync,
	readSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync,
	writeSync,
} from "node:fs";
import { dirname, join, resolve, sep } from "node:path";

export const JOURNAL_SCHEMA = "prime-workflow/journal/1";

export class WorkflowStateError extends Error {}

/** Every identifier that ever becomes part of a path or a journal key must pass this shape. */
const ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/**
 * The run-wide agent budget that applies when a run declares none (owner decision 24.09.2026:
 * 50). It is RECORDED on `run_created`, never applied invisibly, and an explicit
 * `max_agents_total` overrides it.
 */
export const DEFAULT_MAX_AGENTS_TOTAL = 50;

function assertIdShape(what: string, value: string): void {
	if (!ID_SHAPE.test(value)) throw new WorkflowStateError(`${what} ${JSON.stringify(value)} has an unusable shape`);
}

/** The same shape, as a predicate — the bridge gates a run_id before it touches the journal. */
export function usableId(value: string): boolean {
	return ID_SHAPE.test(value);
}

/** Resolve `ref` below `root` or refuse: a traversing run_id/step_id must not write anywhere else. */
function containedPath(root: string, ref: string): string {
	const target = resolve(root, ref);
	if (!target.startsWith(resolve(root) + sep)) {
		throw new WorkflowStateError(`path ${JSON.stringify(ref)} escapes the state root`);
	}
	return target;
}

export type Env = Record<string, string | undefined>;

export type Roots = { definitions: string; state: string };

/** `PRIME_WORKFLOW_STATE` = the `.prime`-equivalent root (tests, forced layouts). Otherwise the
 *  git toplevel; outside a repository the service refuses instead of inventing a location. */
export function resolveRoots(env: Env, cwd: string = process.cwd()): Roots {
	const override = env.PRIME_WORKFLOW_STATE;
	if (override) return { definitions: join(override, "workflows"), state: join(override, "workflow-state") };
	let top = "";
	try {
		top = execFileSync("git", ["rev-parse", "--show-toplevel"], {
			cwd,
			encoding: "utf8",
			stdio: ["ignore", "pipe", "ignore"],
		}).trim();
	} catch {
		top = "";
	}
	if (!top) {
		throw new WorkflowStateError(
			`no git repository in ${cwd}; set PRIME_WORKFLOW_STATE to a directory to hold the workflow state`,
		);
	}
	return { definitions: join(top, ".prime", "workflows"), state: join(top, ".prime", "workflow-state") };
}

function journalPath(env: Env): string {
	return join(resolveRoots(env).state, "journal.jsonl");
}

function lockPath(env: Env): string {
	return join(resolveRoots(env).state, "writer.lock");
}

function fsyncDir(path: string): void {
	try {
		const fd = openSync(path, "r");
		try {
			fsyncSync(fd);
		} finally {
			closeSync(fd);
		}
	} catch {
		// A directory fsync is best-effort here; the file fsync is the load-bearing part.
	}
}

export type WriterIdentity = { session_id?: string | null; rlm_depth?: number | null };
/** Epoch is a STRING: `Date.now()` alone collided for two takeovers in the same millisecond
 *  (review G11), which let an old writer append after losing its claim. */
export type Writer = WriterIdentity & { epoch: string; state_root: string; release: () => void };

const LOCK_LEASE_MS = 120_000;

/** Is a pid still there? EPERM means it exists but belongs to someone else. */
function pidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return (error as NodeJS.ErrnoException).code === "EPERM";
	}
}

/**
 * Claim the single writer slot for this repo root.
 *
 * The lease is informational: an expired lock is still a lock this service refuses to take over
 * (Q1). Recovery of a stale writer is an explicit reconciliation step, not a side effect of asking
 * for the pen.
 */
/** Read the lock file, or null when it is absent/unreadable. */
function readLockFile(path: string): { epoch?: unknown; at?: unknown; pid?: unknown; session_id?: unknown } | null {
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return null;
	}
}

/** May this process take the lock over? A dead PID or a lockless expired lease, never a live holder. */
function takeoverDecision(
	held: { at?: unknown; pid?: unknown; epoch?: unknown },
	now: number,
): { tookOver: boolean; tookOverFrom: string | null } {
	const at = typeof held?.at === "string" ? Date.parse(held.at) : Number.NaN;
	const stale = !Number.isFinite(at) || now - at > LOCK_LEASE_MS;
	const pid = typeof held?.pid === "number" ? held.pid : null;
	// A DEAD holder can never write again, so that takeover is determinate and immediate (a
	// crashed host must not cost every later session two minutes). A LIVE holder is refused even
	// with an expired lease — that is the hung-writer case the lease exists for. No pid in the
	// file: only an expired lease allows takeover. The epoch bump makes the old writer's writes
	// refusable either way.
	return {
		tookOver: pid === null ? stale : !pidAlive(pid),
		tookOverFrom: typeof held?.epoch === "string" ? held.epoch : null,
	};
}

function heldMessage(path: string, held: { epoch?: unknown; pid?: unknown; session_id?: unknown } | null): string {
	const pid = typeof held?.pid === "number" ? held.pid : null;
	return `another writer holds ${path}${pid === null ? "" : " and its holder is still alive"}: epoch=${String(held?.epoch)} pid=${String(held?.pid)} session=${String(held?.session_id)}`;
}

/**
 * Take the takeover guard: a short-lived `wx` file that serializes remove-and-recreate across
 * processes. Without it, a process that read the DEAD lock could later rename away the FRESH lock
 * a winner had already created (review round 2). The guard is best-effort and bounded: a guard
 * left by a crashed process older than 10 s is removed, and after ~3 s of contention we give up
 * loudly rather than spin forever.
 */
function acquireTakeoverGuard(guard: string): boolean {
	const deadline = Date.now() + 3000;
	for (;;) {
		try {
			writeFileSync(guard, `${process.pid}\n`, { flag: "wx" });
			return true;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "EEXIST") return false;
			try {
				if (Date.now() - statSync(guard).mtimeMs > 10_000) {
					rmSync(guard, { force: true });
					continue;
				}
			} catch {
				// raced away — try again
			}
			if (Date.now() > deadline) return false;
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
		}
	}
}

export function acquireWriter(env: Env, who: WriterIdentity = {}): Writer {
	const { state } = resolveRoots(env);
	mkdirSync(state, { recursive: true });
	const path = join(state, "writer.lock");
	const guard = `${path}.guard`;
	// Bounded retry loop: takeovers are serialized under a guard, and a fresh create is an atomic
	// `wx` — exactly one process can end up holding the lock (review round 2).
	for (let attempt = 0; attempt < 5; attempt += 1) {
		const now = Date.now();
		let tookOverFrom: string | null = null;
		if (existsSync(path)) {
			const held = readLockFile(path);
			const decision = takeoverDecision(held ?? {}, now);
			if (!decision.tookOver) throw new WorkflowStateError(heldMessage(path, held));
			tookOverFrom = decision.tookOverFrom;
			if (!acquireTakeoverGuard(guard)) {
				throw new WorkflowStateError(
					`could not acquire the takeover guard at ${guard}; another process is mid-takeover`,
				);
			}
			try {
				// Re-read UNDER the guard: another process may have taken over between our read and
				// the guard. If it did, its lock is live and we must not touch it.
				if (existsSync(path)) {
					const current = readLockFile(path);
					const decided = takeoverDecision(current ?? {}, Date.now());
					if (!decided.tookOver) throw new WorkflowStateError(heldMessage(path, current));
					tookOverFrom = decided.tookOverFrom ?? tookOverFrom;
					rmSync(path, { force: true });
				}
				const epoch = `${now}-${randomUUID().slice(0, 8)}`;
				const payload = {
					epoch,
					at: new Date(now).toISOString(),
					pid: process.pid,
					session_id: who.session_id ?? null,
					rlm_depth: who.rlm_depth ?? null,
					...(tookOverFrom === null ? {} : { took_over_from: tookOverFrom }),
				};
				try {
					writeFileSync(path, `${JSON.stringify(payload)}\n`, { flag: "wx" });
				} catch (error) {
					// A guardless fresh creator can win between our re-read and our create; loop and
					// re-evaluate instead of leaking a raw EEXIST (review round 2).
					if ((error as NodeJS.ErrnoException).code === "EEXIST") continue;
					throw new WorkflowStateError(
						`writer lock ${path} could not be created: ${error instanceof Error ? error.message : String(error)}`,
					);
				}
				fsyncDir(state);
				return makeWriter(path, epoch, who, state);
			} finally {
				rmSync(guard, { force: true });
			}
		}
		const epoch = `${now}-${randomUUID().slice(0, 8)}`;
		const payload = {
			epoch,
			at: new Date(now).toISOString(),
			pid: process.pid,
			session_id: who.session_id ?? null,
			rlm_depth: who.rlm_depth ?? null,
		};
		try {
			writeFileSync(path, `${JSON.stringify(payload)}\n`, { flag: "wx" });
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "EEXIST") continue; // someone else won; re-evaluate
			throw new WorkflowStateError(
				`writer lock ${path} could not be created: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
		fsyncDir(state);
		return makeWriter(path, epoch, who, state);
	}
	throw new WorkflowStateError(`could not acquire the writer lock at ${path}: another process kept winning the race`);
}

function makeWriter(path: string, epoch: string, who: WriterIdentity, state: string): Writer {
	return {
		epoch,
		session_id: who.session_id ?? null,
		rlm_depth: who.rlm_depth ?? null,
		state_root: state,
		release: () => {
			// Only remove OUR lock: a late release of a superseded writer must not unlock the
			// successor (review G10).
			try {
				const held = JSON.parse(readFileSync(path, "utf8")) as { epoch?: unknown };
				if (held?.epoch !== epoch) return;
				rmSync(path);
			} catch {
				// already gone, or unreadable — nothing we own to remove
			}
		},
	};
}

/** The epoch gate: a write is only allowed while the lock still names this writer's epoch. */
function assertWriter(env: Env, writer: Writer): void {
	const path = lockPath(env);
	if (!existsSync(path))
		throw new WorkflowStateError("writer lock is gone; this writer may not confirm anything after losing its claim");
	let held: { epoch?: unknown } | null = null;
	try {
		held = JSON.parse(readFileSync(path, "utf8"));
	} catch {
		held = null;
	}
	if (held?.epoch !== writer.epoch) {
		throw new WorkflowStateError(
			`stale writer epoch ${writer.epoch}; the current epoch is ${String(held?.epoch)} — a driver that lost its claim cannot confirm transitions`,
		);
	}
}

/** The journal must end on a line boundary before anything is appended to it. */
function journalEndsCleanly(path: string): boolean {
	if (!existsSync(path)) return true;
	const size = statSync(path).size;
	if (size === 0) return true;
	const fd = openSync(path, "r");
	try {
		const last = Buffer.alloc(1);
		readSync(fd, last, 0, 1, size - 1);
		return last[0] === 0x0a;
	} finally {
		closeSync(fd);
	}
}

/** Append one event as one prepared line, with a real full-write loop and fsync. */
export function appendEvent(env: Env, writer: Writer, event: Record<string, unknown>): void {
	assertWriter(env, writer);
	const path = journalPath(env);
	mkdirSync(dirname(path), { recursive: true });
	if (!journalEndsCleanly(path)) {
		throw new WorkflowStateError(
			"the journal has a torn tail; run recoverTornTail before appending, or the fragment becomes damaged content in the middle",
		);
	}
	const line = JSON.stringify({
		schema: JOURNAL_SCHEMA,
		at: new Date().toISOString(),
		epoch: writer.epoch,
		session_id: writer.session_id ?? null,
		rlm_depth: writer.rlm_depth ?? null,
		...event,
	});
	const fd = openSync(path, "a");
	try {
		const buf = Buffer.from(`${line}\n`, "utf8");
		let written = 0;
		while (written < buf.length) {
			const n = writeSync(fd, buf, written, buf.length - written);
			if (n <= 0) throw new WorkflowStateError("short write to the journal");
			written += n;
		}
		fsyncSync(fd);
	} finally {
		closeSync(fd);
	}
}

/** Read the journal; `torn` reports an unterminated trailing line (recovery happens under the lock). */
export function readEvents(env: Env): { events: Record<string, unknown>[]; torn: boolean } {
	const path = journalPath(env);
	if (!existsSync(path)) return { events: [], torn: false };
	const text = readFileSync(path, "utf8");
	if (text.length === 0) return { events: [], torn: false };
	const parts = text.split("\n");
	const trailing = parts.pop() ?? "";
	const torn = trailing.length > 0;
	const events: Record<string, unknown>[] = [];
	parts.forEach((line, index) => {
		if (!line.trim()) return;
		try {
			events.push(JSON.parse(line) as Record<string, unknown>);
		} catch (error) {
			throw new WorkflowStateError(`journal line ${index + 1} is not readable JSON: ${String(error)}`);
		}
	});
	return { events, torn };
}

/**
 * Secure the torn fragment and cut the journal back to its last complete line. Only the writer may
 * do this, and it must happen BEFORE the next append — otherwise the fragment ends up in the middle
 * of the file and stops being a tail problem.
 */
export function recoverTornTail(env: Env, writer: Writer): { recovered: boolean; secured?: string } {
	assertWriter(env, writer);
	const path = journalPath(env);
	if (!existsSync(path)) return { recovered: false };
	const bytes = readFileSync(path);
	if (bytes.length === 0 || bytes[bytes.length - 1] === 0x0a) return { recovered: false };
	// Byte-wise, not string-wise: a fragment can end mid-UTF-8-sequence, and decoding/encoding it
	// would alter the very bytes the fragment is evidence of (review G16). 0x0a never appears inside
	// a multi-byte UTF-8 sequence, so the cut is exact.
	const cut = bytes.lastIndexOf(0x0a) + 1;
	const stamp = new Date().toISOString().replace(/[:.]/g, "-");
	// The stamp alone has millisecond resolution: two recoveries in the same tick would overwrite
	// the first fragment. The random suffix keeps each secured fragment its own file.
	const secured = `${path}.torn-${stamp}-${randomUUID().slice(0, 8)}`;
	writeFileSync(secured, bytes.subarray(cut), { flag: "wx" });
	writeFileSync(path, bytes.subarray(0, cut));
	fsyncDir(dirname(path));
	return { recovered: true, secured };
}

/** Terminal attempt states; `unresolved` is deliberately NOT terminal (it blocks a retry). */
export const ACTIVE_ATTEMPT_STATES: ReadonlySet<string> = new Set(["intended", "admitted", "running", "unresolved"]);

export type AttemptProjection = {
	attempt_id: string;
	step_id: string;
	child_name: string;
	child_id: string | null;
	status: string;
	lease_expires_at: string | null;
	driver_session_id: string | null;
	/** The registry role this attempt ran as, and the digests that make the spawn resolvable. */
	role: string | null;
	role_digest: string | null;
	role_binding: RoleSourceBinding | null;
	/** The accepted result: the frozen copy, its digest over exactly those bytes, and the origin. */
	result_ref: string | null;
	result_digest: string | null;
	result_origin: string | null;
	events: number;
};

/** Recorded source provenance. Only the RLM host can attest actual parent tools. */
export type RoleSourceBinding = { role: string; manifest_sha256: string; role_sha256: string; contract_sha256: string };

function checkedRoleBinding(raw: unknown, role?: string | null, digest?: string | null): RoleSourceBinding | null {
	if (raw === undefined || raw === null) {
		if (role || digest) throw new WorkflowStateError("a role claim requires the full pinned source binding");
		return null;
	}
	if (!raw || typeof raw !== "object" || Array.isArray(raw))
		throw new WorkflowStateError("role_binding must be an object");
	const b = raw as Record<string, unknown>;
	if (
		typeof role !== "string" ||
		!role ||
		typeof digest !== "string" ||
		Object.keys(b).sort().join(",") !== "contract_sha256,manifest_sha256,role,role_sha256" ||
		b.role !== role ||
		b.role_sha256 !== digest ||
		![b.manifest_sha256, b.role_sha256, b.contract_sha256].every(
			(v) => typeof v === "string" && /^[0-9a-f]{64}$/.test(v),
		)
	) {
		throw new WorkflowStateError("role_binding disagrees with role/digest or contains invalid hashes");
	}
	return b as RoleSourceBinding;
}

export type StepSpec = { id: string; depends_on?: string[]; inputs?: { ref: string; digest: string }[] };

export type RunLimits = {
	max_parallel?: number;
	max_attempts_per_step?: number;
	max_repairs?: number;
	max_agents_total?: number;
};

export type RunProjection = {
	run_id: string;
	definition_digest: string | null;
	/** Immutable definition captured on creation; absent in older events. */
	definition_object: string | null;
	driver_session_id: string | null;
	/** What the run is about (question/subject), so a resume can prove it is the same work. */
	subject: Record<string, unknown> | null;
	/** Admission limits, stored AND enforced at claim time (review G02). */
	limits: RunLimits | null;
	status: string;
	state_version: number;
	attempts: Record<string, AttemptProjection>;
	/** step id → attempt ids, in booking order. */
	steps: Record<string, string[]>;
	/** The persisted program: initial steps plus every expansion, with deps and inputs. */
	step_specs: Record<string, { depends_on: string[]; inputs: { ref: string; digest: string }[] }>;
};

/** Turn persisted step specs into the projection's map; a malformed spec is refused, not repaired. */
function foldStepSpecs(
	run_id: string,
	steps: unknown,
): Record<string, { depends_on: string[]; inputs: { ref: string; digest: string }[] }> {
	const out: Record<string, { depends_on: string[]; inputs: { ref: string; digest: string }[] }> = {};
	if (steps === undefined || steps === null) return out;
	if (!Array.isArray(steps)) throw new WorkflowStateError(`run ${run_id} carries a non-array step list`);
	for (const raw of steps) {
		const spec = raw as StepSpec;
		if (!spec || typeof spec.id !== "string" || !spec.id)
			throw new WorkflowStateError(`run ${run_id} carries a step without an id`);
		if (out[spec.id]) throw new WorkflowStateError(`run ${run_id} carries step ${spec.id} twice`);
		out[spec.id] = {
			depends_on: Array.isArray(spec.depends_on) ? spec.depends_on.map(String) : [],
			inputs: Array.isArray(spec.inputs)
				? spec.inputs.map((i) => ({ ref: String(i.ref), digest: String(i.digest) }))
				: [],
		};
	}
	return out;
}

/**
 * The declared program must be resolvable: every dependency names a declared step, and there is no
 * cycle. Validated BEFORE anything is appended (review G07/G17) — a program that only fails at fold
 * time would poison every later read of the same journal.
 */
function assertResolvable(run_id: string, specs: Record<string, { depends_on: string[] }>): void {
	for (const [id, spec] of Object.entries(specs)) {
		for (const dep of spec.depends_on) {
			if (!(dep in specs))
				throw new WorkflowStateError(`run ${run_id}: step ${id} depends on ${dep}, which is not declared`);
		}
	}
	const remaining = new Map(Object.entries(specs).map(([id, spec]) => [id, new Set(spec.depends_on)]));
	let changed = true;
	while (changed) {
		changed = false;
		for (const [id, deps] of [...remaining]) {
			if (deps.size !== 0) continue;
			remaining.delete(id);
			for (const other of remaining.values()) other.delete(id);
			changed = true;
		}
	}
	if (remaining.size)
		throw new WorkflowStateError(`run ${run_id} has a cycle among steps: ${[...remaining.keys()].sort().join(", ")}`);
}

/** A run projection folded from the journal. Unknown events are refused, not skipped. */
export function foldRuns(events: Record<string, unknown>[]): Map<string, RunProjection> {
	const runs = new Map<string, RunProjection>();
	const run = (id: unknown): RunProjection => {
		const r = runs.get(String(id));
		if (!r) throw new WorkflowStateError(`journal names run ${String(id)} before it was created`);
		return r;
	};
	const attempt = (r: RunProjection, id: unknown): AttemptProjection => {
		const a = r.attempts[String(id)];
		if (!a) throw new WorkflowStateError(`journal names attempt ${String(id)} before it was intended`);
		return a;
	};
	for (const event of events) {
		if (event.schema !== JOURNAL_SCHEMA) {
			throw new WorkflowStateError(
				`journal event has schema ${JSON.stringify(event.schema)}; expected ${JOURNAL_SCHEMA}`,
			);
		}
		const name = String(event.event ?? "");
		if (name === "run_created") {
			const run_id = String(event.run_id);
			if (runs.has(run_id)) throw new WorkflowStateError(`run ${run_id} was created twice`);
			runs.set(run_id, {
				run_id,
				definition_digest: (event.definition_digest as string | null) ?? null,
				definition_object: (event.definition_object as string | null) ?? null,
				driver_session_id: (event.driver_session_id as string | null) ?? null,
				subject: (event.subject as Record<string, unknown> | null) ?? null,
				limits: (event.limits as RunLimits | null) ?? null,
				status: "running",
				state_version: 1,
				attempts: {},
				steps: {},
				step_specs: foldStepSpecs(run_id, event.steps),
			});
			continue;
		}
		if (name === "steps_expanded") {
			const r = run(event.run_id);
			const added = foldStepSpecs(String(event.run_id), event.steps);
			for (const [id, spec] of Object.entries(added)) {
				if (r.step_specs[id]) throw new WorkflowStateError(`step ${id} was expanded twice in run ${r.run_id}`);
				r.step_specs[id] = spec;
			}
			r.state_version += 1;
			continue;
		}
		if (name === "attempt_intended") {
			const r = run(event.run_id);
			const attempt_id = String(event.attempt_id);
			r.attempts[attempt_id] = {
				attempt_id,
				step_id: String(event.step_id),
				child_name: String(event.child_name),
				child_id: null,
				status: "intended",
				lease_expires_at: (event.lease_expires_at as string | null) ?? null,
				driver_session_id: (event.driver_session_id as string | null) ?? null,
				role: (event.role as string | null) ?? null,
				role_digest: (event.role_digest as string | null) ?? null,
				role_binding: (event.role_binding as RoleSourceBinding | null) ?? null,
				result_ref: null,
				result_digest: null,
				result_origin: null,
				events: 1,
			};
			const list = r.steps[String(event.step_id)] ?? [];
			list.push(attempt_id);
			r.steps[String(event.step_id)] = list;
			r.state_version += 1;
			continue;
		}
		if (name === "attempt_admitted") {
			const r = run(event.run_id);
			const a = attempt(r, event.attempt_id);
			a.status = "admitted";
			a.child_id = (event.child_id as string | null) ?? null;
			a.events += 1;
			r.state_version += 1;
			continue;
		}
		if (name === "attempt_settled") {
			const r = run(event.run_id);
			const a = attempt(r, event.attempt_id);
			const status = String(event.status);
			if (!["succeeded", "failed", "abandoned"].includes(status)) {
				throw new WorkflowStateError(`attempt_settled carries status ${status}`);
			}
			a.status = status;
			a.events += 1;
			r.state_version += 1;
			continue;
		}
		if (name === "result_accepted") {
			const r = run(event.run_id);
			const a = attempt(r, event.attempt_id);
			a.result_ref = String(event.result_ref);
			a.result_digest = String(event.result_digest);
			a.result_origin = String(event.result_origin ?? "child_file");
			a.events += 1;
			r.state_version += 1;
			continue;
		}
		if (name === "op_intent" || name === "op_result") {
			// Bridge bookkeeping, not run state: known and deliberately ignored by the fold.
			continue;
		}
		if (name === "run_finished") {
			const r = run(event.run_id);
			const outcome = String(event.outcome);
			if (!["done", "failed", "abandoned"].includes(outcome)) {
				throw new WorkflowStateError(`run_finished carries outcome ${outcome}`);
			}
			r.status = outcome;
			r.state_version += 1;
			continue;
		}
		if (name === "attempt_unresolved" || name === "attempt_orphaned") {
			const r = run(event.run_id);
			const a = attempt(r, event.attempt_id);
			a.status = name === "attempt_unresolved" ? "unresolved" : "orphaned";
			a.events += 1;
			r.state_version += 1;
			continue;
		}
		throw new WorkflowStateError(`unknown journal event ${JSON.stringify(name)}; refusing to guess what the state is`);
	}
	return runs;
}

export function listRuns(env: Env): RunProjection[] {
	return [...foldRuns(readEvents(env).events).values()].sort((a, b) => (a.run_id < b.run_id ? -1 : 1));
}

export function readRun(env: Env, run_id: string): RunProjection | null {
	return foldRuns(readEvents(env).events).get(run_id) ?? null;
}

export function createRun(
	env: Env,
	writer: Writer,
	input: {
		run_id: string;
		definition_digest: string;
		definition_object?: string | null;
		subject?: Record<string, unknown>;
		limits?: Record<string, unknown>;
		steps?: StepSpec[];
	},
): RunProjection {
	if (!input.run_id) throw new WorkflowStateError("a run needs a run_id");
	assertIdShape("run_id", input.run_id);
	if (!input.definition_digest) throw new WorkflowStateError(`run ${input.run_id} has no definition_digest`);
	if (readRun(env, input.run_id)) throw new WorkflowStateError(`run ${input.run_id} already exists`);
	const declared = foldStepSpecs(input.run_id, input.steps ?? []);
	for (const id of Object.keys(declared)) assertIdShape("step_id", id);
	assertResolvable(input.run_id, declared);
	const declaredLimits = input.limits ?? {};
	const explicitTotal = declaredLimits.max_agents_total;
	if (explicitTotal !== undefined && (!Number.isInteger(explicitTotal) || (explicitTotal as number) <= 0)) {
		throw new WorkflowStateError(`max_agents_total ${JSON.stringify(explicitTotal)} must be a positive integer`);
	}
	const limits = {
		...declaredLimits,
		max_agents_total: (explicitTotal as number | undefined) ?? DEFAULT_MAX_AGENTS_TOTAL,
	};
	appendEvent(env, writer, {
		event: "run_created",
		run_id: input.run_id,
		definition_digest: input.definition_digest,
		definition_object: input.definition_object ?? null,
		subject: input.subject ?? null,
		limits,
		steps: input.steps ?? [],
		driver_session_id: writer.session_id ?? null,
	});
	return readRun(env, input.run_id) as RunProjection;
}

/** A later discovery expands the program; the expansion is an event, never an inference. */
export function expandSteps(
	env: Env,
	writer: Writer,
	input: { run_id: string; steps: StepSpec[]; cause: string },
): RunProjection {
	const run = readRun(env, input.run_id);
	if (!run) throw new WorkflowStateError(`no run ${input.run_id}`);
	if (run.status !== "running")
		throw new WorkflowStateError(`run ${input.run_id} is ${run.status}; no new work can be declared`);
	// Validated BEFORE the append: duplicates within the request, collisions with the program, and
	// resolvability of the merged program (review G07/G17).
	const added = foldStepSpecs(input.run_id, input.steps);
	if (!Object.keys(added).length)
		throw new WorkflowStateError(`run ${input.run_id}: an expansion needs at least one step`);
	for (const id of Object.keys(added)) {
		assertIdShape("step_id", id);
		if (run.step_specs[id]) throw new WorkflowStateError(`step ${id} already exists in run ${input.run_id}`);
	}
	assertResolvable(input.run_id, { ...run.step_specs, ...added });
	appendEvent(env, writer, { event: "steps_expanded", run_id: input.run_id, cause: input.cause, steps: input.steps });
	return readRun(env, input.run_id) as RunProjection;
}

/**
 * Close a run. `done` is only allowed when EVERY persisted step has a succeeded attempt — the
 * sessionless run on 24.09.2026 ended with four succeeded steps and a run stuck at `running`,
 * because nothing performed this transition.
 */
export function finishRun(
	env: Env,
	writer: Writer,
	input: { run_id: string; outcome?: "done" | "failed" | "abandoned"; note?: string },
): RunProjection {
	const run = readRun(env, input.run_id);
	if (!run) throw new WorkflowStateError(`no run ${input.run_id}`);
	if (run.status !== "running") throw new WorkflowStateError(`run ${input.run_id} is already ${run.status}`);
	const outcome = input.outcome ?? "done";
	if (outcome === "done") {
		const specIds = Object.keys(run.step_specs);
		if (specIds.length) {
			const open = specIds.filter((id) => stepStatus(run, id) !== "succeeded");
			if (open.length)
				throw new WorkflowStateError(`run ${input.run_id} has steps that are not succeeded: ${open.join(", ")}`);
		} else {
			// A run created before step specs were passed (measured 24.09.2026) can still be closed:
			// every step (by its latest attempt) must be succeeded and there must be at least one.
			// What cannot be checked is coverage; the decision is still "no open or failed step".
			const stepIds = Object.keys(run.steps);
			if (!stepIds.length)
				throw new WorkflowStateError(`run ${input.run_id} has no steps and no attempts; refusing to call it done`);
			const bad = stepIds.filter((id) => stepStatus(run, id) !== "succeeded");
			if (bad.length)
				throw new WorkflowStateError(
					`run ${input.run_id} has no step specs and steps that are not succeeded: ${bad.join(", ")}`,
				);
		}
	}
	appendEvent(env, writer, {
		event: "run_finished",
		run_id: input.run_id,
		outcome,
		...(input.note ? { note: input.note } : {}),
	});
	return readRun(env, input.run_id) as RunProjection;
}

export type StepStatus = "none" | "active" | "succeeded" | "failed";

/**
 * The step's status is the status of its LATEST attempt, not of "any attempt": a retry after a
 * failed/abandoned predecessor is exactly the resume case, and the older attempt must not keep the
 * step green (or red) once a newer one exists.
 */
export function stepStatus(run: RunProjection, step_id: string): StepStatus {
	const attempts = (run.steps[step_id] ?? []).map((id) => run.attempts[id]).filter(Boolean);
	if (!attempts.length) return "none";
	if (attempts.some((a) => ACTIVE_ATTEMPT_STATES.has(a.status))) return "active";
	return (attempts[attempts.length - 1] as AttemptProjection).status === "succeeded" ? "succeeded" : "failed";
}

export type ReadyReport = { ready: string[]; blocked: { step_id: string; reason: string }[] };

/**
 * M3 — which persisted steps may run now. This **computes**, it does not dispatch: the caller
 * decides what to spawn. A step whose recorded input digest no longer matches the current digest is
 * blocked as stale — that is how a source change invalidates dependent work (M2 case 9).
 */
export function readySteps(run: RunProjection, current_input_digests: Record<string, string> = {}): ReadyReport {
	const report: ReadyReport = { ready: [], blocked: [] };
	for (const [id, spec] of Object.entries(run.step_specs)) {
		const status = stepStatus(run, id);
		if (status === "succeeded") continue;
		if (status === "active") {
			report.blocked.push({ step_id: id, reason: "an attempt is active" });
			continue;
		}
		const unsettled = spec.depends_on.filter((dep) => stepStatus(run, dep) !== "succeeded");
		if (unsettled.length) {
			report.blocked.push({ step_id: id, reason: `dependencies not settled: ${unsettled.join(", ")}` });
			continue;
		}
		const stale = spec.inputs.filter(
			(i) => i.ref in current_input_digests && current_input_digests[i.ref] !== i.digest,
		);
		if (stale.length) {
			report.blocked.push({ step_id: id, reason: `input changed: ${stale.map((s) => s.ref).join(", ")}` });
			continue;
		}
		report.ready.push(id);
	}
	report.ready.sort();
	return report;
}

/**
 * One active attempt per (run, step); a retry is only legal after a terminal predecessor.
 *
 * Beyond that, the claim ENFORCES the recorded program (review G02/G04): the run must be running,
 * the step must be declared, every dependency must be settled, and the declared limits must hold.
 * `readySteps()` computes the same thing as advice; this is the door that actually closes.
 */
export function claimStep(
	env: Env,
	writer: Writer,
	input: {
		run_id: string;
		step_id: string;
		child_name: string;
		lease_ms?: number;
		role?: string | null;
		role_digest?: string | null;
		role_binding?: unknown;
	},
): { attempt_id: string } {
	if (!input.step_id) throw new WorkflowStateError("a claim needs a step_id");
	if (!input.child_name) throw new WorkflowStateError("a claim needs a child_name");
	const binding = checkedRoleBinding(input.role_binding, input.role, input.role_digest);
	if (input.lease_ms !== undefined && (!Number.isInteger(input.lease_ms) || input.lease_ms <= 0)) {
		throw new WorkflowStateError(`lease_ms ${JSON.stringify(input.lease_ms)} must be a positive integer`);
	}
	const run = readRun(env, input.run_id);
	if (!run) throw new WorkflowStateError(`no run ${input.run_id}`);
	if (run.status !== "running") throw new WorkflowStateError(`run ${input.run_id} is ${run.status}; no new attempts`);
	const spec = run.step_specs[input.step_id];
	if (!spec) {
		throw new WorkflowStateError(
			`step ${input.step_id} is not declared in run ${input.run_id}; a claim cannot invent work`,
		);
	}
	const unsettled = spec.depends_on.filter((dep) => stepStatus(run, dep) !== "succeeded");
	if (unsettled.length) {
		throw new WorkflowStateError(`step ${input.step_id} is dependency-blocked: not settled: ${unsettled.join(", ")}`);
	}
	for (const a of Object.values(run.attempts)) {
		if (a.step_id === input.step_id && ACTIVE_ATTEMPT_STATES.has(a.status)) {
			throw new WorkflowStateError(
				`step ${input.step_id} already has an active attempt ${a.attempt_id} (${a.status}); retry requires a terminal predecessor`,
			);
		}
	}
	const limits = run.limits ?? {};
	const all = Object.values(run.attempts);
	const ofStep = all.filter((a) => a.step_id === input.step_id);
	const active = all.filter((a) => ACTIVE_ATTEMPT_STATES.has(a.status));
	if (limits.max_agents_total !== undefined && all.length >= limits.max_agents_total) {
		throw new WorkflowStateError(`run ${input.run_id} reached max_agents_total=${limits.max_agents_total}`);
	}
	if (limits.max_attempts_per_step !== undefined && ofStep.length >= limits.max_attempts_per_step) {
		throw new WorkflowStateError(`step ${input.step_id} reached max_attempts_per_step=${limits.max_attempts_per_step}`);
	}
	// A repair is every attempt after the first: `max_repairs` is a declared budget and must not be
	// inert (review round 2).
	if (limits.max_repairs !== undefined && ofStep.length - 1 >= limits.max_repairs) {
		throw new WorkflowStateError(`step ${input.step_id} reached max_repairs=${limits.max_repairs}`);
	}
	if (limits.max_parallel !== undefined && active.length >= limits.max_parallel) {
		throw new WorkflowStateError(
			`run ${input.run_id} reached max_parallel=${limits.max_parallel} (${active.length} active)`,
		);
	}
	const attempt_id = randomUUID();
	appendEvent(env, writer, {
		event: "attempt_intended",
		run_id: input.run_id,
		step_id: input.step_id,
		attempt_id,
		child_name: input.child_name,
		lease_expires_at: new Date(Date.now() + (input.lease_ms ?? LOCK_LEASE_MS)).toISOString(),
		driver_session_id: writer.session_id ?? null,
		...(input.role ? { role: input.role } : {}),
		...(input.role_digest ? { role_digest: input.role_digest } : {}),
		...(binding ? { role_binding: binding } : {}),
	});
	return { attempt_id };
}

export function admitStep(
	env: Env,
	writer: Writer,
	input: { run_id: string; attempt_id: string; child_id: string },
): void {
	if (!input.child_id) throw new WorkflowStateError("an admission needs a child_id");
	const run = readRun(env, input.run_id);
	if (!run) throw new WorkflowStateError(`no run ${input.run_id}`);
	if (run.status !== "running")
		throw new WorkflowStateError(`run ${input.run_id} is ${run.status}; it accepts no further admissions`);
	const attempt = run.attempts[input.attempt_id];
	if (!attempt) throw new WorkflowStateError(`no attempt ${input.attempt_id} in run ${input.run_id}`);
	// Admission belongs to a claim that has not been admitted yet. Re-admitting an open attempt was
	// the manual step a real resume needed (measured 24.09.2026); it must be refused, not appended.
	if (attempt.status !== "intended") {
		throw new WorkflowStateError(
			`attempt ${input.attempt_id} is ${attempt.status}, not intended; an admission is booked once`,
		);
	}
	appendEvent(env, writer, {
		event: "attempt_admitted",
		run_id: input.run_id,
		attempt_id: input.attempt_id,
		child_id: input.child_id,
	});
}

/** A transition label is only legal while the attempt is still open; terminal is terminal. */
function openAttempt(env: Env, run_id: string, attempt_id: string): AttemptProjection {
	const run = readRun(env, run_id);
	if (!run) throw new WorkflowStateError(`no run ${run_id}`);
	// A closed run accepts no further transitions (review round 2: admit/accept/settle after
	// finishRun ended with run=failed and attempt=succeeded).
	if (run.status !== "running")
		throw new WorkflowStateError(`run ${run_id} is ${run.status}; it accepts no further attempt transitions`);
	const attempt = run.attempts[attempt_id];
	if (!attempt) throw new WorkflowStateError(`no attempt ${attempt_id} in run ${run_id}`);
	if (!ACTIVE_ATTEMPT_STATES.has(attempt.status)) {
		throw new WorkflowStateError(
			`attempt ${attempt_id} is already ${attempt.status}; a terminal attempt cannot be transitioned again`,
		);
	}
	return attempt;
}

export const SETTLE_STATUSES: ReadonlySet<string> = new Set(["succeeded", "failed", "abandoned"]);

export function settleStep(
	env: Env,
	writer: Writer,
	input: { run_id: string; attempt_id: string; status: "succeeded" | "failed" | "abandoned"; note?: string },
): void {
	// Runtime-validated BEFORE the append (review G08): a typed union is not a check when the value
	// arrives as JSON through the bridge.
	if (!SETTLE_STATUSES.has(String(input.status))) {
		throw new WorkflowStateError(
			`settle status ${JSON.stringify(input.status)} is not one of ${[...SETTLE_STATUSES].join(", ")}`,
		);
	}
	const attempt = openAttempt(env, input.run_id, input.attempt_id);
	if (input.status === "succeeded") {
		// `succeeded` means an admitted child produced an accepted result (review G03). Without both,
		// the claim of success has no referent.
		if (!attempt.child_id)
			throw new WorkflowStateError(`attempt ${input.attempt_id} has no admitted child; it cannot be succeeded`);
		if (!attempt.result_ref)
			throw new WorkflowStateError(`attempt ${input.attempt_id} has no accepted result; it cannot be succeeded`);
		// The frozen bytes must still exist and hash to the accepted digest: a deleted or altered
		// copy cannot back a success claim (review round 2).
		if (!verifyAcceptedResult(env, { result_ref: attempt.result_ref, digest: attempt.result_digest ?? "" })) {
			throw new WorkflowStateError(
				`attempt ${input.attempt_id}: accepted result ${attempt.result_ref} is missing or no longer matches its digest; it cannot be succeeded`,
			);
		}
	}
	appendEvent(env, writer, {
		event: "attempt_settled",
		run_id: input.run_id,
		attempt_id: input.attempt_id,
		status: input.status,
		...(input.note ? { note: input.note } : {}),
	});
}

export function markUnresolved(
	env: Env,
	writer: Writer,
	input: { run_id: string; attempt_id: string; reason: string },
): void {
	const attempt = openAttempt(env, input.run_id, input.attempt_id);
	if (attempt.status === "unresolved") {
		throw new WorkflowStateError(
			`attempt ${input.attempt_id} is already unresolved; new evidence (settle, delete) is needed, not the same label again`,
		);
	}
	appendEvent(env, writer, {
		event: "attempt_unresolved",
		run_id: input.run_id,
		attempt_id: input.attempt_id,
		reason: input.reason,
	});
}

export function markOrphaned(
	env: Env,
	writer: Writer,
	input: { run_id: string; attempt_id: string; reason: string },
): void {
	openAttempt(env, input.run_id, input.attempt_id);
	appendEvent(env, writer, {
		event: "attempt_orphaned",
		run_id: input.run_id,
		attempt_id: input.attempt_id,
		reason: input.reason,
	});
}

export type ResultSchema = {
	required?: string[];
	properties?: Record<string, "string" | "number" | "boolean" | "object" | "array">;
	additionalProperties?: boolean;
};

function typeOf(value: unknown): string {
	if (Array.isArray(value)) return "array";
	if (value === null) return "null";
	return typeof value;
}

/** A small, strict validator — required fields, declared types, and no unknown fields when asked. */
export function validateResult(value: unknown, schema?: ResultSchema): string | null {
	if (value === null || typeof value !== "object" || Array.isArray(value)) return "a result must be a JSON object";
	const record = value as Record<string, unknown>;
	if (!schema) return null;
	for (const key of schema.required ?? []) if (!(key in record)) return `missing required field ${JSON.stringify(key)}`;
	for (const [key, kind] of Object.entries(schema.properties ?? {})) {
		if (!(key in record)) continue;
		if (typeOf(record[key]) !== kind) return `field ${JSON.stringify(key)} is ${typeOf(record[key])}, expected ${kind}`;
	}
	if (schema.additionalProperties === false) {
		const allowed = new Set(Object.keys(schema.properties ?? {}));
		for (const key of Object.keys(record)) if (!allowed.has(key)) return `unknown field ${JSON.stringify(key)}`;
	}
	return null;
}

/**
 * Accept a child's result: validate, freeze the exact bytes into the run folder, and book the
 * digest. `answer_preview` is never a result, and a later overwrite of the child path cannot
 * change what was accepted — the frozen copy is what the digest names.
 */
export function acceptResult(
	env: Env,
	writer: Writer,
	input: {
		run_id: string;
		attempt_id: string;
		source_path: string;
		schema?: ResultSchema;
		origin?: "child_file" | "caller_reported";
	},
): { result_ref: string; digest: string; size_bytes: number } {
	assertIdShape("run_id", input.run_id);
	const run = readRun(env, input.run_id);
	if (!run) throw new WorkflowStateError(`no run ${input.run_id}`);
	if (run.status !== "running")
		throw new WorkflowStateError(`run ${input.run_id} is ${run.status}; it accepts no further results`);
	const attempt = run.attempts[input.attempt_id];
	if (!attempt) throw new WorkflowStateError(`no attempt ${input.attempt_id} in run ${input.run_id}`);
	// `unresolved` is allowed on purpose: a child whose artifact lands after the wait is the same
	// child, and refusing its file forced a manual re-admit in the real resume (24.09.2026).
	if (!["admitted", "running", "unresolved"].includes(attempt.status)) {
		throw new WorkflowStateError(
			`attempt ${input.attempt_id} is ${attempt.status}; only an open or unresolved attempt can accept a result`,
		);
	}
	// The frozen copy is immutable: a second acceptance could silently replace the evidence of an
	// already successful attempt (review G05).
	if (attempt.result_ref) {
		throw new WorkflowStateError(
			`attempt ${input.attempt_id} already accepted ${attempt.result_ref}; a result is accepted once`,
		);
	}
	if (!existsSync(input.source_path)) throw new WorkflowStateError(`result file ${input.source_path} does not exist`);
	const bytes = readFileSync(input.source_path);
	let parsed: unknown;
	try {
		parsed = JSON.parse(bytes.toString("utf8"));
	} catch (error) {
		throw new WorkflowStateError(`result file is not readable JSON: ${String(error)}`);
	}
	const invalid = validateResult(parsed, input.schema);
	if (invalid) throw new WorkflowStateError(`result rejected: ${invalid}`);
	const digest = createHash("sha256").update(bytes).digest("hex");
	assertIdShape("step_id", attempt.step_id);
	const index = (run.steps[attempt.step_id] ?? []).indexOf(attempt.attempt_id);
	const result_ref = join(
		"runs",
		input.run_id,
		"results",
		`${attempt.step_id}--${String(index + 1).padStart(2, "0")}.json`,
	);
	// Belt and braces after the shape gates: the resolved target must stay below the state root,
	// so no run_id/step_id can ever write outside it (review round 2, run_id traversal).
	const target = containedPath(writer.state_root, result_ref);
	mkdirSync(dirname(target), { recursive: true });
	const temp = `${target}.${process.pid}.tmp`;
	writeFileSync(temp, bytes);
	renameSync(temp, target);
	fsyncDir(dirname(target));
	try {
		appendEvent(env, writer, {
			event: "result_accepted",
			run_id: input.run_id,
			attempt_id: input.attempt_id,
			step_id: attempt.step_id,
			result_ref,
			result_digest: digest,
			size_bytes: bytes.length,
			result_origin: input.origin ?? "child_file",
		});
	} catch (error) {
		// A result that was never booked must not leave a frozen copy behind (review round 2):
		// the next successful accept uses a fresh attempt index anyway, so the orphan is pure noise.
		rmSync(target, { force: true });
		throw error;
	}
	return { result_ref, digest, size_bytes: bytes.length };
}

/** The frozen bytes still hash to the accepted digest — the check that makes "accepted" mean it. */
export function verifyAcceptedResult(env: Env, input: { result_ref: string; digest: string }): boolean {
	if (!input.digest) return false;
	const root = resolveRoots(env).state;
	let path: string;
	try {
		path = containedPath(root, input.result_ref);
	} catch {
		return false;
	}
	if (!existsSync(path)) return false;
	return createHash("sha256").update(readFileSync(path)).digest("hex") === input.digest;
}
