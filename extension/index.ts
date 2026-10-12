/**
 * `workflow_save` / `workflow_plan` / `workflow_record` — a named, validated orchestration program.
 *
 * Ported from DeepSeek-Harness's `workflow`, in the only shape this side of the boundary allows,
 * and the difference is stated rather than glossed.
 *
 * **What DSH does.** The model authors an orchestration script that distributes work across many
 * subagents, and the runtime executes it in an isolated worker thread — "containment, not a
 * security boundary", in its own words.
 *
 * **What Prime allows an extension to do.** Not that. `ExtensionAPI` is UI surfaces, hooks and
 * `registerTool` (`types.d.ts`); there is no way to execute Python, and no way to spawn an RLM
 * child. Prime's model can already orchestrate — `await rlm('sub-task')`, `rlm.list_subagents()`,
 * `agent_message.send(...)` — from its own REPL. So the executor is not the gap.
 *
 * **What is.** Prime has no place to put a *program*. Every orchestration is written fresh into a
 * REPL cell, unnamed, unvalidated, and gone when the session ends; a keybinding called
 * "Show the program that spawned subagents" exists, but no program API does. This capability owns
 * the durable half: a named workflow, checked for the two things ad-hoc orchestration gets wrong
 * — a dependency on a step that does not exist, and a cycle — and handed back in an order the
 * model can execute. The model still runs it, which is also where DSH's own boundary sits.
 *
 * It persists through `prime-lab-storage`, which is what that library was built for.
 *
 * **The planner side (P4).** `workflow_plan_from_tasks` / `planFromTasks()` turn an Argus-style
 * planner answer (TASK_* blocks) into Steps, one wave per dependency level, and refuse — before
 * anything is stored — every acceptance check that cannot fail. The contract text the planner is
 * held to is `PLANNER-CONTRACT-v2.md` next to this file.
 *
 * **Off unless configured.** `PRIME_WORKFLOW=on` (legacy alias `PRIME_LAB_WORKFLOW`, which writes a
 * deprecation receipt). `TASK_MODEL` is an explicit `provider/model` selector or absent; the Argus
 * tier words are reported and treated as inherit (W07).
 */

import { createHash, randomUUID } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { agentDir } from "./lib/agent-dir.ts";
import { type Bridge, startBridge } from "./lib/bridge.ts";
import type { Invariant } from "./lib/diagnostics.ts";
import { type ReceiptSession, sessionFields, sessionOf, UNKNOWN_SESSION } from "./lib/receipt-session.ts";
import { acquireWriter, resolveRoots } from "./lib/service.ts";

/**
 * What the step must produce, as a contract rather than a shell-only check.
 *
 * `command` is the implementation-planner path (`TASK_ACCEPTANCE_CHECK` → here) and is linted by
 * the existing ban list. `artifact_schema` and `findings` serve explore/verify/judge/research
 * steps, which have no shell command that can fail on "the analysis is complete". `run.done` means
 * "contract fulfilled", never "the project is error-free" — an audit that finds problems succeeded.
 */
export type OutcomeContract =
	| { kind: "command"; command: string }
	| { kind: "artifact_schema"; schema: Record<string, unknown> }
	| { kind: "findings"; coverage: string[] };

export type Step = {
	id: string;
	/** What the child is asked to do. */
	prompt: string;
	/** One-line statement of the step's purpose (the planner's TASK_TITLE). */
	purpose?: string;
	/** Step ids that must finish first. */
	depends_on?: string[];
	/** Optional explicit model selector (`provider/model`). Absent = the child inherits the session model. */
	model?: string;
	/** Optional role id from the roles registry. Absent = freestyle rlm.spawn. */
	role?: string;
	/** Paths this step may change (the write-set; the conflict source). */
	owns_paths?: string[];
	/** Concurrency permission — a wave groups dependency levels, not automatic parallelism. */
	parallel_safe?: boolean;
	/** What must be true when the step is done. */
	outcome_contract?: OutcomeContract;
	/** Inputs this step consumes, by reference and digest. */
	inputs?: { ref: string; digest: string }[];
	/** How often this step may be retried (new attempts). */
	retry_limit?: number;
};

export const DEFINITION_SCHEMA = "prime-lab-workflow/definition/2";

export type Workflow = {
	schema: typeof DEFINITION_SCHEMA;
	/** Stable identity, independent of the display name; a name change does not change it. */
	workflow_id: string;
	/** Display/lookup name — mutable, not identity. */
	name: string;
	/** Monotone revision for this workflow_id; every save creates the next one. */
	revision: number;
	/** sha256 over the canonical steps (and the outcome contract inside them). */
	definition_digest: string;
	steps: Step[];
	saved_at: string;
	/**
	 * The Prime session that saved it, when the caller could name one.
	 *
	 * `saved_at` alone cannot answer "did THIS session plan?" — two sessions share an agent
	 * directory and therefore share the store, so a plan saved by a neighbour a second ago is
	 * newer than this session's start and passes a timestamp test (measured 2026-09-06,
	 * `docs/verify/p4-p5-20260906/plan_gate.test.mjs`, CROSS-SESSION). `null` when the caller had
	 * no session — a script, a test, an offline run — and the gate treats that as "not mine".
	 */
	session_id?: string | null;
};

export type SaveOptions = { session_id?: string | null };

export class WorkflowError extends Error {}

const NAME_SHAPE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const WORKFLOW_ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/;

/**
 * E2: definitions live in the shareable root — `<defs>/<workflow_id>/rev-NNNN.json` (§11.1) — and
 * are plain files: no store library, no agent-dir namespace. The workflow_id (not the mutable name)
 * is the directory, because identity is stable and the display name is not.
 */
function definitionsDir(env: Record<string, string | undefined>): string {
	return resolveRoots(env).definitions;
}

function revisionFile(env: Record<string, string | undefined>, workflow_id: string, revision: number): string {
	return join(definitionsDir(env), workflow_id, `rev-${String(revision).padStart(4, "0")}.json`);
}

function writeDefinitionFile(env: Record<string, string | undefined>, workflow: Workflow): void {
	const path = revisionFile(env, workflow.workflow_id, workflow.revision);
	mkdirSync(dirname(path), { recursive: true });
	const temp = `${path}.${process.pid}.tmp`;
	writeFileSync(temp, `${JSON.stringify(workflow, null, 2)}\n`, "utf8");
	renameSync(temp, path);
}

function readDefinitionFile(
	env: Record<string, string | undefined>,
	workflow_id: string,
	revision: number,
): unknown | undefined {
	const path = revisionFile(env, workflow_id, revision);
	if (!existsSync(path)) return undefined;
	return JSON.parse(readFileSync(path, "utf8"));
}

/** Legacy entries still sit in the old agent-dir store; migration reads them, nothing else does. */
function legacyEntries(env: Record<string, string | undefined>): { key: string; value: unknown }[] {
	const dir = join(agentDir(env), "storage", "workflow");
	if (!existsSync(dir)) return [];
	return readdirSync(dir)
		.filter((name) => name.endsWith(".json"))
		.sort()
		.map((name) => {
			const key = name.slice(0, -".json".length);
			try {
				return { key, value: JSON.parse(readFileSync(join(dir, name), "utf8")) as unknown };
			} catch {
				return { key, value: undefined };
			}
		});
}

/**
 * Canonical JSON for hashing: keys sorted, `undefined` dropped, arrays in order.
 *
 * The digest must not depend on key order in a hand-written or model-written object, or the same
 * program would hash differently every save and revision equality would be meaningless.
 */
function canonical(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map((v) => canonical(v)).join(",")}]`;
	if (value && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>)
			.filter(([, v]) => v !== undefined)
			.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
		return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(",")}}`;
	}
	return JSON.stringify(value) ?? "null";
}

/** The immutable identity of a definition revision: sha256 over its canonical steps. */
export function definitionDigest(steps: Step[]): string {
	return createHash("sha256").update(canonical(steps)).digest("hex");
}

/** Every stored revision on disk; a corrupt file is surfaced, never skipped. */
function allDefinitions(env: Record<string, string | undefined>): { key: string; value: unknown }[] {
	const root = definitionsDir(env);
	if (!existsSync(root)) return [];
	const out: { key: string; value: unknown }[] = [];
	for (const workflow_id of readdirSync(root).sort()) {
		let files: string[] = [];
		try {
			files = readdirSync(join(root, workflow_id))
				.filter((name) => /^rev-\d{4}\.json$/.test(name))
				.sort();
		} catch {
			continue;
		}
		for (const file of files) {
			const key = `${workflow_id}/${file}`;
			try {
				out.push({ key, value: JSON.parse(readFileSync(join(root, workflow_id, file), "utf8")) as unknown });
			} catch {
				out.push({ key, value: undefined });
			}
		}
	}
	return out;
}

/**
 * What must stay true of this capability's own wiring — DSH's `runtime-diagnostics` shape, declared
 * by the owner and checked by the carrier while the composition is live.
 *
 * A workflow is only worth saving if it can be read back in a later session, and the reader is a
 * model that will be told "no workflow <name>" either way. The check reads each saved entry rather
 * than only listing the names, because listing cannot see a half-written file — and a half-written
 * file is what a crashed process leaves behind.
 *
 * It is not a second implementation of the capability: nothing here orders a graph or validates a
 * step. It asserts that what was stored is still readable, which is the wiring, not the work.
 * M0.6 sharpened it: a current entry must ALSO pass schema/shape/DAG validation, because a
 * readable-but-invalid definition was exactly the hole `loadWorkflow` used to have.
 */
export function invariants(env: Record<string, string | undefined>): Invariant[] {
	return [
		{
			owner: "workflow",
			name: "every saved definition validates on read (schema, steps, DAG)",
			check: () => {
				try {
					for (const entry of allDefinitions(env)) {
						if (isDefinition(entry.value)) validateDefinition(entry.value);
						else if (!isLegacyDefinition(entry.value)) return false;
					}
					return true;
				} catch {
					return false;
				}
			},
			remedy:
				"a saved definition is unreadable or fails schema/shape/DAG validation; loadWorkflow refuses it instead of handing it to the model",
		},
	];
}

/**
 * Topological order, or an explanation of why there is none.
 *
 * Two failures and they are the two an ad-hoc orchestration actually produces: a dependency naming
 * a step that was renamed or never written, and a cycle introduced while reordering. Kahn's
 * algorithm reports both by construction. What is left over when nothing more can be scheduled is
 * the cycle **plus the steps blocked behind it** — naming all of them as "the cycle" was wrong
 * (W08), so the members are found by reachability and only those are named.
 */
export function order(steps: Step[]): string[][] {
	const ids = new Set(steps.map((s) => s.id));
	if (ids.size !== steps.length) {
		const seen = new Set<string>();
		let duplicate: Step | undefined;
		for (const step of steps) {
			if (seen.has(step.id)) {
				duplicate = step;
				break;
			}
			seen.add(step.id);
		}
		throw new WorkflowError(`duplicate step id: ${duplicate?.id}`);
	}
	for (const step of steps) {
		for (const dependency of step.depends_on ?? []) {
			if (!ids.has(dependency)) {
				throw new WorkflowError(`step ${step.id} depends on ${dependency}, which is not a step`);
			}
		}
	}
	const remaining = new Map(steps.map((s) => [s.id, new Set(s.depends_on ?? [])]));
	const waves: string[][] = [];
	while (remaining.size) {
		const ready = [...remaining.entries()]
			.filter(([, deps]) => deps.size === 0)
			.map(([id]) => id)
			.sort();
		if (!ready.length) {
			// Load-bearing in a way a mutation probe cannot demonstrate politely: with this branch
			// removed the loop never terminates, because nothing is ever scheduled and nothing is
			// ever removed. Measured 2026-09-05 — the probe hung and had to be killed by pid. A
			// guard whose absence is an infinite loop deserves saying so next to it.
			//
			// W08: `remaining` also holds nodes merely blocked behind a cycle. A node is in the
			// cycle iff it can reach itself through remaining edges; only those are named.
			const inCycle = new Set<string>();
			for (const start of remaining.keys()) {
				const stack = [...(remaining.get(start) ?? [])];
				const seen = new Set<string>();
				while (stack.length) {
					const current = stack.pop() as string;
					if (current === start) {
						inCycle.add(start);
						break;
					}
					if (seen.has(current)) continue;
					seen.add(current);
					for (const dep of remaining.get(current) ?? []) stack.push(dep);
				}
			}
			throw new WorkflowError(`cycle among steps: ${[...inCycle].sort().join(", ")}`);
		}
		for (const id of ready) remaining.delete(id);
		for (const deps of remaining.values()) for (const id of ready) deps.delete(id);
		waves.push(ready);
	}
	return waves;
}

/**
 * The acceptance check carried inside a step prompt, or null.
 *
 * `stepPrompt()` writes `Acceptance check (must fail when the requirement is violated): <check>`
 * and the model that executes the step reads the check off exactly that. Anything after the colon
 * to the end of the prompt is the check, because a check may itself contain a colon.
 */
export function acceptanceCheckIn(prompt: string): string | null {
	const m = /Acceptance check\s*\([^)]*\)\s*:\s*/i.exec(prompt);
	return m ? prompt.slice(m.index + m[0].length) : null;
}

/**
 * Derselbe Gegenstand, absichtlich loser gelesen — und NUR fuer die Sperre, nie fuer die
 * Ausfuehrung.
 *
 * WARUM ZWEI EXTRAKTOREN. `acceptanceCheckIn` definiert, was das Modell liest: es muss exakt
 * die Form treffen, die `stepPrompt()` schreibt, sonst liest der Ausfuehrende etwas anderes als
 * das, was geprueft wurde. Die SPERRE hat aber eine andere Frage — „behauptet dieser Prompt
 * ueberhaupt eine Abnahme?" — und die darf nicht an einer Klammer haengen.
 *
 * GEMESSEN 11.09.2026, und es ist ein Defekt, kein Schoenheitsfehler: `Acceptance check: pytest
 * -q t.py || true` (ohne den Klammerzusatz) wurde **gespeichert**, waehrend derselbe String
 * durch `planFromTasks` verweigert wird. Ebenso `Acceptance:`, `Check:`, `Abnahme:`. Der
 * Kommentar unten in `saveWorkflow` sagt seit dem 06.09. „Both doors end here, so the check is
 * made here" — die Tuer schloss aber nur bei EINER Schreibweise. Der meta-audit-Fix von damals
 * hat den Fall geschlossen, den er sah, und die Klasse offengelassen; genau die Form, die dieses
 * Labor als [[feedback_fix_gilt_nur_fuer_einen_pfad]] fuehrt. Handgeschriebene Step-Prompts sind
 * kein Randfall: `workflow_save` nimmt sie entgegen, und der Agent hat am selben Tag genau so
 * einen gespeichert.
 *
 * AM MATERIAL KALIBRIERT, nicht geraten: die fuenf Markerformen oben werden getroffen, und vier
 * Prosazeilen (`Check: make sure the tests pass before you finish.`, `Acceptance: the reviewer
 * signs off.`, `Denk dran: Check the README first.`, `Abnahme: zwei Augen, ein Commit.`)
 * loesen nichts aus — nicht weil der Extraktor sie meidet, sondern weil `bannedCheck` auf Prosa
 * nichts findet. Der Preis ist benannt: ein Prompt, der ueber ein Verbot SPRICHT (`Check: wir
 * haben `|| true` verboten`), wird verweigert. Das ist laut und behebbar; das Gegenteil war
 * still.
 */
export function acceptanceClaimIn(prompt: string): string | null {
	const m = /(?:acceptance(?:\s+check)?|abnahme(?:pr(?:ü|ue)fung)?|check)\s*(?:\([^)]*\)\s*)?:\s*/i.exec(prompt);
	return m ? prompt.slice(m.index + m[0].length) : null;
}

/** Is this a value saved under the current definition schema? */
function isDefinition(value: unknown): value is Workflow {
	const v = value as Workflow | undefined;
	return (
		!!v &&
		typeof v === "object" &&
		v.schema === DEFINITION_SCHEMA &&
		typeof v.workflow_id === "string" &&
		typeof v.revision === "number" &&
		Array.isArray(v.steps)
	);
}

/** Legacy (pre-schema) entries: `{name, steps:[{id,prompt}], saved_at, session_id?}`. */
function isLegacyDefinition(
	value: unknown,
): value is { name: string; steps: unknown[]; saved_at?: string; session_id?: string | null } {
	const v = value as { name?: unknown; steps?: unknown; schema?: unknown } | undefined;
	return !!v && typeof v === "object" && v.schema === undefined && typeof v.name === "string" && Array.isArray(v.steps);
}

/**
 * Validate a definition on READ, not only on write (W05). A syntactically valid JSON with
 * `steps: "not-an-array"` used to pass the inventory check and reach the model as a plan.
 */
export function validateDefinition(def: Workflow): Workflow {
	if (def.schema !== DEFINITION_SCHEMA) {
		throw new WorkflowError(
			`definition ${def.name} has schema ${JSON.stringify(def.schema)}; expected ${DEFINITION_SCHEMA} (migrate with workflow_migrate_definitions or save a new revision)`,
		);
	}
	if (!Array.isArray(def.steps) || !def.steps.length) {
		throw new WorkflowError(`definition ${def.name} has no steps array`);
	}
	for (const step of def.steps) {
		if (!step || typeof step.id !== "string" || !NAME_SHAPE.test(step.id)) {
			throw new WorkflowError(
				`definition ${def.name} has an invalid step id: ${JSON.stringify((step as Step | undefined)?.id)}`,
			);
		}
		if (typeof step.prompt !== "string" || !step.prompt.trim()) {
			throw new WorkflowError(`definition ${def.name} step ${step.id} has no prompt`);
		}
	}
	order(def.steps);
	return def;
}

/** The newest revision saved under this name, or null. Legacy/corrupt entries are not a match. */
export function findByName(env: Record<string, string | undefined>, name: string): Workflow | null {
	const matches = allDefinitions(env)
		.map((entry) => entry.value)
		.filter((value): value is Workflow => isDefinition(value) && value.name === name);
	if (!matches.length) return null;
	return matches.sort((a, b) => a.revision - b.revision)[matches.length - 1] as Workflow;
}

/** One exact revision, by identity — the reader a run binds to. */
export function loadRevision(env: Record<string, string | undefined>, workflow_id: string, revision: number): Workflow {
	if (!WORKFLOW_ID_SHAPE.test(workflow_id)) throw new WorkflowError(`not a workflow id: ${workflow_id}`);
	const found = readDefinitionFile(env, workflow_id, revision) as Workflow | undefined;
	if (!found) throw new WorkflowError(`no revision ${revision} of ${workflow_id}`);
	return validateDefinition(found);
}

export function saveWorkflow(
	env: Record<string, string | undefined>,
	name: string,
	steps: Step[],
	options: SaveOptions = {},
): Workflow {
	if (!NAME_SHAPE.test(name)) throw new WorkflowError(`not a workflow name: ${name}`);
	if (!Array.isArray(steps) || !steps.length) throw new WorkflowError("a workflow needs at least one step");
	for (const step of steps) {
		if (!step || typeof step.id !== "string" || !NAME_SHAPE.test(step.id)) {
			throw new WorkflowError(`not a step id: ${String((step as Step | undefined)?.id)}`);
		}
		if (typeof step.prompt !== "string" || !step.prompt.trim()) {
			throw new WorkflowError(`step ${step.id} has no prompt`);
		}
		if (step.model !== undefined && (typeof step.model !== "string" || !MODEL_ID_SHAPE.test(step.model))) {
			throw new WorkflowError(
				`step ${step.id} has an invalid model (want provider/model): ${JSON.stringify(step.model)}`,
			);
		}
		// The ban belongs to the STORE, not to one parser. `planFromTasks` refused a check that
		// cannot fail and `workflow_save` did not, so the same string was one refusal and one
		// stored plan depending on which door it came through (meta-audit 2026-09-06). Both doors
		// end here, so the check is made here. The structured `outcome_contract.command` is the
		// primary door; a hand-written prompt without a contract is read with the two extractors
		// (the exact marker first — it is what the executor reads — then the loose claim line).
		if (step.outcome_contract?.kind === "command") {
			if (!step.outcome_contract.command.trim())
				throw new WorkflowError(`step ${step.id} has an empty outcome command`);
			const ban = bannedCheck(step.outcome_contract.command);
			if (ban) {
				throw new WorkflowError(
					`step ${step.id} has an outcome command that cannot fail: ${ban}: ${JSON.stringify(step.outcome_contract.command)}`,
				);
			}
		} else if (step.outcome_contract === undefined) {
			const exakt = acceptanceCheckIn(step.prompt);
			const check = exakt !== null ? exakt : acceptanceClaimIn(step.prompt);
			if (check !== null) {
				const ban = bannedCheck(check);
				if (ban) {
					const woher = exakt !== null ? "" : " (erkannt an einer freien Abnahmezeile, nicht an der Marker-Form)";
					throw new WorkflowError(
						`step ${step.id} has an acceptance check that cannot fail: ${ban}${woher}: ${JSON.stringify(check)}`,
					);
				}
			}
		}
	}
	// Validated BEFORE it is stored: a saved workflow that cannot be ordered is a trap for whoever
	// reads it back, and the person who reads it back is a model in a later session.
	order(steps);
	// W06: the same name never silently replaces an older definition. Identity is stable and the
	// revision is monotone; every revision stays readable under its own key.
	const previous = findByName(env, name);
	const workflow_id = previous?.workflow_id ?? `${name}-${randomUUID().slice(0, 8)}`;
	const revision = (previous?.revision ?? 0) + 1;
	const workflow: Workflow = {
		schema: DEFINITION_SCHEMA,
		workflow_id,
		name,
		revision,
		definition_digest: definitionDigest(steps),
		steps,
		saved_at: new Date().toISOString(),
		session_id: options.session_id ?? null,
	};
	writeDefinitionFile(env, workflow);
	return workflow;
}

export function loadWorkflow(env: Record<string, string | undefined>, name: string): Workflow {
	if (!NAME_SHAPE.test(name)) throw new WorkflowError(`not a workflow name: ${name}`);
	const found = findByName(env, name);
	if (found) return validateDefinition(found);
	// A legacy entry with this name is not silently ignored — it is a migration, said out loud.
	for (const entry of legacyEntries(env)) {
		if (isLegacyDefinition(entry.value) && entry.value.name === name) {
			throw new WorkflowError(
				`workflow ${name} predates schema ${DEFINITION_SCHEMA}; migrate it (workflow_migrate_definitions) or save a new revision`,
			);
		}
	}
	// A readable entry with this name that is neither a current nor a legacy definition used to be
	// handed back as-is (W05); it is refused, and the refusal says which key holds it.
	for (const entry of allDefinitions(env)) {
		const value = entry.value as { name?: unknown } | undefined;
		if (value && typeof value === "object" && value.name === name) {
			throw new WorkflowError(
				`workflow ${name} is stored at ${entry.key} but is not a valid definition; it is neither schema ${DEFINITION_SCHEMA} nor a legible legacy plan`,
			);
		}
	}
	throw new WorkflowError(`no workflow ${name}`);
}

export function listWorkflows(env: Record<string, string | undefined>): string[] {
	const names = new Set<string>();
	for (const entry of allDefinitions(env)) {
		if (isDefinition(entry.value)) names.add(entry.value.name);
		else if (isLegacyDefinition(entry.value)) names.add(entry.value.name);
	}
	// Legacy names are listed too, so an unmigrated program is visible instead of invisible.
	for (const entry of legacyEntries(env)) {
		if (isLegacyDefinition(entry.value)) names.add(entry.value.name);
	}
	return [...names].sort();
}

export type RunOutcome = "passed" | "failed" | "blocked" | "skipped";
export const RUN_OUTCOMES: ReadonlySet<string> = new Set(["passed", "failed", "blocked", "skipped"]);

export type RunEntry = {
	step: string;
	outcome: RunOutcome;
	note?: string;
	run_id?: string | null;
	attempt_id?: string | null;
	definition_digest?: string | null;
	session_id?: string | null;
	evidence_ref?: string | null;
};

export function readRunRecords(env: Record<string, string | undefined>): Record<string, unknown>[] {
	const path = join(resolveRoots(env).state, "records.jsonl");
	if (!existsSync(path)) return [];
	return readFileSync(path, "utf8")
		.split("\n")
		.filter((line) => line.trim())
		.map((line) => JSON.parse(line) as Record<string, unknown>);
}

export function recordRun(env: Record<string, string | undefined>, name: string, entry: RunEntry): void {
	if (!RUN_OUTCOMES.has(entry.outcome)) {
		throw new WorkflowError(`outcome ${JSON.stringify(entry.outcome)} is not one of ${[...RUN_OUTCOMES].join(", ")}`);
	}
	// E2: plain append-only notes under the project state root; W04 keeps unique identity, never a
	// timestamp — two records in the same millisecond (or a frozen clock) must be two records.
	const path = join(resolveRoots(env).state, "records.jsonl");
	mkdirSync(dirname(path), { recursive: true });
	appendFileSync(
		path,
		`${JSON.stringify({
			workflow: name,
			record_key: `${name}.${randomUUID()}`,
			at: new Date().toISOString(),
			run_id: entry.run_id ?? null,
			step: entry.step,
			attempt_id: entry.attempt_id ?? null,
			definition_digest: entry.definition_digest ?? null,
			session_id: entry.session_id ?? null,
			evidence_ref: entry.evidence_ref ?? null,
			driver_session_id: null,
			lease_expires_at: null,
			outcome: entry.outcome,
			...(entry.note ? { note: entry.note } : {}),
		})}\n`,
	);
}

export type MigrationReport = {
	migrated: { name: string; workflow_id: string; revision: number }[];
	refused: { key: string; reason: string }[];
};

/**
 * M0.17 — legacy entries are transferred or refused, never silently overwritten.
 *
 * A legacy step carries its acceptance check only in prose; if the exact marker is present the
 * check becomes a `kind=command` contract. A step with no marker is refused (the old entry stays
 * untouched) rather than given a formal substitute contract that checks nothing.
 */
export function migrateLegacyDefinitions(env: Record<string, string | undefined>): MigrationReport {
	const report: MigrationReport = { migrated: [], refused: [] };
	for (const entry of legacyEntries(env)) {
		try {
			if (isDefinition(entry.value)) continue;
			if (!isLegacyDefinition(entry.value)) {
				report.refused.push({ key: entry.key, reason: "neither a current definition nor a legacy one" });
				continue;
			}
			const steps: Step[] = entry.value.steps.map((raw) => {
				const r = raw as { id?: unknown; prompt?: unknown; depends_on?: unknown; model?: unknown };
				const prompt = String(r?.prompt ?? "");
				const check = acceptanceCheckIn(prompt);
				if (check === null) {
					throw new WorkflowError(
						`legacy step ${String(r?.id)} has no 'Acceptance check (…):' marker; re-save it with an outcome contract`,
					);
				}
				const step: Step = { id: String(r?.id ?? ""), prompt, outcome_contract: { kind: "command", command: check } };
				if (Array.isArray(r?.depends_on)) step.depends_on = r.depends_on.map(String);
				if (typeof r?.model === "string" && MODEL_ID_SHAPE.test(r.model)) step.model = r.model;
				return step;
			});
			const saved = saveWorkflow(env, entry.value.name, steps, { session_id: entry.value.session_id ?? null });
			report.migrated.push({ name: saved.name, workflow_id: saved.workflow_id, revision: saved.revision });
		} catch (error) {
			report.refused.push({ key: entry.key, reason: String((error as Error).message).split("\n")[0] });
		}
	}
	return report;
}

// ---------------------------------------------------------------------------------------------
// Argus-style TASK blocks → Steps. The planner side of P4.
//
// The Argus Planner (argus_skill/planner/planner.py, bounded_dag.py) answers with a key-value
// footer: a PROJECT_DONE/REASON header, then one TASK_* block per node. Argus's own parser is
// lenient in exactly the places this one is not: a block without a title or objective is skipped
// (`continue`), unknown keys are skipped, and the acceptance check is stored as an opaque string.
// A check that cannot fail is a step that cannot be wrong, and the model that executes the plan
// later reads "passed" off it. So this parser refuses those, by throwing — Prime never reads a
// returned `isError` (measured, see `fail()` below) — and it refuses before anything is stored.
//
// There is deliberately no level field. The child inherits the parent's allowlist, skills and
// prompt (`agent-session.js:7739`), and placement in a wave is derived from TASK_DEPS by `order()`.
// `TASK_MODEL` is an explicit `provider/model` selector; the legacy tier words are reported, not stored.
// ---------------------------------------------------------------------------------------------

export type Task = {
	key: string;
	title: string;
	objective: string;
	deps: string[];
	owns_paths: string[];
	acceptance_check: string;
	parallel_safe: boolean;
	/** `cheap` / `normal` (legacy tier words) or an explicit model id the planner wrote. */
	model: string;
	/** 1-based line of the block's TASK_KEY, for error messages that can be found in the text. */
	line: number;
	/** 1-based line of TASK_MODEL, when the block had one — the reject names the right line. */
	model_line?: number;
};

/** A line the parser did not use. Reported, never silently dropped. */
export type RejectedLine = { line: number; text: string; reason: string };

export type TaskPlan = {
	project_done: boolean;
	reason: string;
	tasks: Task[];
	steps: Step[];
	rejected: RejectedLine[];
	waves: string[][];
};

const TASK_KEYS = new Set([
	"TASK_KEY",
	"TASK_TITLE",
	"TASK_OBJECTIVE",
	"TASK_DEPS",
	"TASK_OWNS_PATHS",
	"TASK_ACCEPTANCE_CHECK",
	"TASK_PARALLEL_SAFE",
	"TASK_MODEL",
]);
const HEADER_KEYS = new Set(["PROJECT_DONE", "REASON"]);
// `KEY=value` or `KEY: value`, key case-insensitive, the whole line optionally wrapped in
// backticks — the shapes a model actually produces when asked for `KEY=value`.
const KV_LINE = /^([A-Za-z_]+)\s*[=:]\s*(.*)$/;
function unwrapBackticks(line: string): string {
	return line.length >= 2 && line.startsWith("`") && line.endsWith("`") ? line.slice(1, -1).trim() : line;
}
const MODEL_ID_SHAPE = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/;

// ---------------------------------------------------------------------------------------------
// The acceptance-check lint.
//
// **WHAT LEVEL THIS IS.** A SYNTACTIC LINT OVER THE TEXT OF THE CHECK. Nothing here runs a shell,
// and nothing here knows what the step's requirement is. It answers one narrow question — "does
// this command line always exit 0, whatever the world looks like?" — from the text alone. It
// therefore cannot catch a check that runs, can fail in principle, and simply does not test the
// requirement (`ls README.md` for "README.md is unchanged" is caught here only because the word
// "unchanged" is in the text next to a command that reads no bytes). The gate that would catch
// that class is EXECUTION: run the check inside a fixture where the requirement is violated and
// require a non-zero exit. That gate does not exist yet; `docs/verify/p4-p5-20260906/
// ban_bypass.test.mjs` is the probe that built the violated world by hand, and it is the closest
// this repository currently comes.
//
// Measured 2026-09-06: the previous end-anchored regex list passed 14 of that probe's 23
// candidates. The list stays — its messages name the form a planner actually wrote — and a
// tokeniser runs after it for everything an end-anchor cannot see (`|| true && echo`, `; true`,
// `|| echo`, a comment after the swallow, a fullwidth `｜｜`).
// ---------------------------------------------------------------------------------------------

/**
 * Unicode look-alikes fold to ASCII before anything is matched: NFKC maps FULLWIDTH VERTICAL LINE
 * to `|` and NO-BREAK SPACE to a space, which is what `grep … ｜｜ true` and `… || true`
 * were. Only the lint sees the normalised text; the check is stored exactly as the planner wrote
 * it, because a stored check is evidence and evidence is not tidied.
 */
function normaliseCheck(check: string): string {
	return check.normalize("NFKC");
}

/** Quote/escape state machine shared by every scanner below. Returns true while `i` is quoted. */
function scanQuoted(text: string): boolean[] {
	const quoted: boolean[] = new Array(text.length).fill(false);
	let single = false;
	let double = false;
	for (let i = 0; i < text.length; i++) {
		const c = text[i];
		if (!single && c === "\\") {
			quoted[i] = true;
			if (i + 1 < text.length) quoted[++i] = true;
			continue;
		}
		if (c === "'" && !double) {
			quoted[i] = true;
			single = !single;
			continue;
		}
		if (c === '"' && !single) {
			quoted[i] = true;
			double = !double;
			continue;
		}
		quoted[i] = single || double;
	}
	return quoted;
}

/**
 * Split off an unquoted `#` comment. A comment is stripped BEFORE the tokeniser looks at the code,
 * which is what `grep … || true # verified` needed; the full text (comment included) is what the
 * prose bans keep reading, because `git diff --exit-code # README.md unchanged` says what it
 * checks in the comment.
 */
export function stripComment(check: string): { code: string; comment: string } {
	const quoted = scanQuoted(check);
	for (let i = 0; i < check.length; i++) {
		if (check[i] !== "#" || quoted[i]) continue;
		if (i > 0 && !/\s/.test(check[i - 1])) continue; // `foo#bar` is one word, not a comment
		return { code: check.slice(0, i), comment: check.slice(i + 1) };
	}
	return { code: check, comment: "" };
}

/**
 * Split on the given operators at nesting depth 0, outside quotes. Longest operator wins, so `&&`
 * is never read as two `&`. `$( … )`, `( … )`, `{ … }` and backticks are opaque: an operator
 * inside them belongs to the sub-shell, which is why `test $(./run.sh; echo $?) -eq 0` stays one
 * command.
 */
function splitTop(code: string, operators: string[]): { parts: string[]; ops: string[] } {
	const ordered = [...operators].sort((a, b) => b.length - a.length);
	const quoted = scanQuoted(code);
	const parts: string[] = [];
	const ops: string[] = [];
	let depth = 0;
	let backtick = false;
	let start = 0;
	for (let i = 0; i < code.length; i++) {
		if (quoted[i]) continue;
		const c = code[i];
		if (c === "`") {
			backtick = !backtick;
			continue;
		}
		if (backtick) continue;
		if (c === "(" || c === "{") depth++;
		else if (c === ")" || c === "}") depth = Math.max(0, depth - 1);
		if (depth > 0) continue;
		const op = ordered.find((o) => code.startsWith(o, i));
		if (!op) continue;
		parts.push(code.slice(start, i));
		ops.push(op);
		i += op.length - 1;
		start = i + 1;
	}
	parts.push(code.slice(start));
	return { parts, ops };
}

/** Words of one simple command, quotes removed, redirections dropped. */
function words(command: string): string[] {
	const quoted = scanQuoted(command);
	const raw: string[] = [];
	let current = "";
	let started = false;
	for (let i = 0; i < command.length; i++) {
		const c = command[i];
		if (!quoted[i] && /\s/.test(c)) {
			if (started) raw.push(current);
			current = "";
			started = false;
			continue;
		}
		started = true;
		if (quoted[i] && (c === "'" || c === '"' || c === "\\")) continue;
		current += c;
	}
	if (started) raw.push(current);
	const out: string[] = [];
	for (let i = 0; i < raw.length; i++) {
		const w = raw[i];
		if (/^\d*(>>|>|<<|<|&>|>&)/.test(w)) {
			if (/^\d*(>>|>|<<|<|&>|>&)$/.test(w)) i++; // the target is the next word
			continue;
		}
		if (/^[A-Za-z_][A-Za-z0-9_]*=/.test(w) && !out.length) continue; // leading VAR=value
		out.push(w);
	}
	return out;
}

const NOOP_HEADS = new Set([":", "true", "/bin/true", "/usr/bin/true"]);
const PRINT_HEADS = new Set(["echo", "/bin/echo", "printf", "/usr/bin/printf"]);
const TEST_HEADS = new Set(["test", "[", "[["]);

/**
 * `[ 1 = 1 ]` and its family. Only literal operands count: anything carrying `$` or a backtick is
 * a value this parser cannot know, and calling it a tautology would refuse a real check.
 */
function tautologyTest(args: string[]): boolean {
	const a = args.filter((w) => w !== "]" && w !== "]]");
	if (!a.length || a.some((w) => w.includes("$") || w.includes("`"))) return false;
	if (a.length === 3 && ["=", "==", "-eq"].includes(a[1]) && a[0] === a[2]) return true;
	if (a.length === 2 && a[0] === "-n" && a[1] !== "") return true;
	if (a.length === 2 && a[0] === "-z" && a[1] === "") return true;
	if (a.length === 1 && a[0] !== "" && !a[0].startsWith("-")) return true; // `[ literal ]`
	return false;
}

/** Head word of a simple command, or "" when there is none. */
function head(command: string): string {
	return words(command)[0] ?? "";
}

/** A command whose exit status is 0 no matter what. */
function alwaysZeroSimple(command: string): boolean {
	const trimmed = command.trim();
	if (!trimmed) return false;
	if (/^\(.*\)$/s.test(trimmed) || /^\{.*\}$/s.test(trimmed)) {
		return statusAlwaysZero(trimmed.slice(1, -1).replace(/;\s*$/, ""));
	}
	const w = words(trimmed);
	if (!w.length) return false;
	const [h, ...args] = w;
	if (NOOP_HEADS.has(h)) return true;
	if (PRINT_HEADS.has(h)) return true;
	if ((h === "exit" || h === "return") && args[0] === "0") return true;
	if (TEST_HEADS.has(h)) return tautologyTest(args);
	return false;
}

/** A pipeline takes the status of its LAST command (no `pipefail` in a one-line check). */
function alwaysZeroPipeline(pipeline: string): boolean {
	const { parts } = splitTop(pipeline, ["|"]);
	const last = [...parts].reverse().find((p) => p.trim());
	return last === undefined ? false : alwaysZeroSimple(last);
}

/** `A && B` is always-0 only if both are; `A || B` is always-0 if either is. Folded left. */
function alwaysZeroAndOr(statement: string): boolean {
	const { parts, ops } = splitTop(statement, ["&&", "||"]);
	let acc = alwaysZeroPipeline(parts[0]);
	for (let i = 1; i < parts.length; i++) {
		const right = alwaysZeroPipeline(parts[i]);
		acc = ops[i - 1] === "&&" ? acc && right : acc || right;
	}
	return acc;
}

/** The status of a `;`-list is the status of its LAST statement. */
function statusAlwaysZero(code: string): boolean {
	const { parts } = splitTop(code, [";", "\n"]);
	const last = [...parts].reverse().find((p) => p.trim());
	return last === undefined ? false : alwaysZeroAndOr(last);
}

/** `… || true`, `… || :`, `… || exit 0` in ANY position — the swallow, wherever it sits. */
function orNoopAnywhere(code: string): boolean {
	for (const statement of splitTop(code, [";", "\n"]).parts) {
		const { parts, ops } = splitTop(statement, ["&&", "||"]);
		for (let i = 1; i < parts.length; i++) {
			if (ops[i - 1] !== "||") continue;
			const w = words(parts[i].trim());
			if (!w.length) continue;
			if (NOOP_HEADS.has(w[0])) return true;
			if ((w[0] === "exit" || w[0] === "return") && w[1] === "0") return true;
		}
	}
	return false;
}

/** Every simple command head in the check, for the "what does this actually read?" question. */
function commandHeads(code: string): string[] {
	const heads: string[] = [];
	for (const statement of splitTop(code, [";", "\n"]).parts) {
		for (const branch of splitTop(statement, ["&&", "||"]).parts) {
			for (const piped of splitTop(branch, ["|"]).parts) {
				const h = head(piped.trim());
				if (h) heads.push(h);
			}
		}
	}
	return heads;
}

const IDENTITY_CLAIM =
	/\b(unchanged|untouched|not (be )?(modified|changed|touched)|no changes?|remains? (the same|identical|unchanged)|stays? (the same|identical|unchanged)|byte[- ]identical)\b/i;

/**
 * Commands that answer "is it there?" and never "is it the same?". A check that claims byte
 * identity and runs only these is the shape `ls README.md  # README.md unchanged` — it survives
 * every regex, it runs, and it exits 0 on a file that was rewritten a second ago.
 */
const EXISTENCE_ONLY = new Set([
	"ls",
	"stat",
	"test",
	"[",
	"[[",
	"find",
	"file",
	"readlink",
	"realpath",
	"dirname",
	"basename",
	...NOOP_HEADS,
	...PRINT_HEADS,
]);

/**
 * The ban list. Every entry is a check that cannot fail, and each is matched the way it is
 * written in a real planner answer, not a paraphrase of it. Ordered so the message names the form.
 *
 *   || true / ||true / || :          — a shell that swallows its own failure
 *   or True                          — the Python spelling of the same thing
 *   exit 0 / return 0                — as the whole check, or forced at its end (`; exit 0`, `|| exit 0`)
 *   : / true / /bin/true / true;     — the no-op commands
 *   empty                            — nothing to run, nothing to observe
 *   unconditional success            — prose that asserts the result instead of testing it
 *   exit status always 0             — the tokeniser: `; true`, `|| echo`, `[ 1 = 1 ]`, `… && echo`
 *   "unchanged file" without command — a claim about bytes with nothing that reads the bytes
 *   "unchanged" tested for existence — a claim about bytes checked with `ls` / `test -f` / `stat`
 */
type Ban = { name: string; hit: (check: string) => boolean };

const COMMAND_TOKEN =
	/(^|[;&|(]\s*)(git|sha256sum|sha1sum|sha512sum|md5sum|b2sum|cmp|diff|difft|test|\[\[?|stat|python3?|uv|node|npm|pnpm|bun|cargo|make|pytest|rg|grep|find|fd|ls|cat|wc|head|tail|jq|yq|curl|xh|bash|sh|\.\/[^\s]+)\b/;

const BANNED: Ban[] = [
	// Nothing to run: empty, whitespace, or a line that is only a comment.
	{ name: "empty check", hit: (c) => stripComment(c).code.trim() === "" },
	{ name: "'|| true'", hit: (c) => /\|\|\s*(true|:)\s*(;|$)/i.test(c) },
	{ name: "'or True'", hit: (c) => /\bor\s+True\b/.test(c) },
	{
		name: "'exit 0' as the whole check",
		hit: (c) => /^\s*(exit|return)\s+0\s*;?\s*$/i.test(c) || /(;|&&|\|\|)\s*(exit|return)\s+0\s*;?\s*$/i.test(c),
	},
	{
		name: "no-op command (':' or 'true')",
		hit: (c) => /^\s*(:|true|\/bin\/true|\/usr\/bin\/true)\s*;?\s*$/i.test(c),
	},
	{
		name: "unconditional success",
		hit: (c) =>
			/^\s*(n\/?a|none|no check( needed| required)?|not applicable|ok|done|passes|success|succeeds|always (passes|succeeds|true)|trivial(ly)?( true| passes)?)\s*\.?\s*$/i.test(
				c,
			) ||
			/\balways (passes|succeeds)\b/i.test(c) ||
			/^\s*echo\b[^|;&]*$/i.test(c),
	},
	// The tokeniser. Everything above is a regex over the whole line and therefore blind to
	// position; these two read the command structure.
	{ name: "'|| true'", hit: (c) => orNoopAnywhere(stripComment(c).code) },
	{ name: "the check's exit status is always 0", hit: (c) => statusAlwaysZero(stripComment(c).code) },
	{
		name: "'unchanged file' claim without a command",
		hit: (c) => IDENTITY_CLAIM.test(c) && !COMMAND_TOKEN.test(c),
	},
	{
		name: "'unchanged' claim tested only for existence",
		hit: (c) => {
			if (!IDENTITY_CLAIM.test(c)) return false;
			const heads = commandHeads(stripComment(c).code);
			return heads.length > 0 && heads.every((h) => EXISTENCE_ONLY.has(h));
		},
	},
];

/**
 * The ban that fires, or null when the check may fail. Exported so a test can probe each form.
 *
 * The text is NFKC-normalised first, so a fullwidth `｜｜` and a non-breaking space before `true`
 * are the same swallow as the ASCII one. This is a lint over text — see the level note above the
 * ban list: it does not execute the check, and a check that runs but tests the wrong thing is
 * outside what it can see.
 */
export function bannedCheck(check: string): string | null {
	const normalised = normaliseCheck(check);
	for (const ban of BANNED) if (ban.hit(normalised)) return ban.name;
	return null;
}

function keyValueBool(raw: string): boolean {
	return ["true", "1", "yes", "y"].includes(raw.trim().toLowerCase());
}

function splitList(raw: string): string[] {
	if (raw.trim().toLowerCase() === "none") return [];
	return raw
		.split(/[,|]/)
		.map((s) => s.trim())
		.filter(Boolean);
}

/**
 * `TASK_MODEL` is an explicit `provider/model` selector; the tier words are the Argus inheritance.
 *
 * They select nothing (W07: "cheap" was passed through as a model name and could only fail at spawn
 * time), so they are reported as unused lines and treated as "inherit". A value that is neither a
 * tier nor a model shape is refused.
 */
function stepModel(model: string): { model?: string; tier: string | null } {
	const trimmed = model.trim();
	const lowered = trimmed.toLowerCase();
	if (lowered === "cheap" || lowered === "normal") return { tier: lowered };
	if (!MODEL_ID_SHAPE.test(trimmed)) {
		throw new WorkflowError(`TASK_MODEL is not a model id: ${JSON.stringify(model)}`);
	}
	return { tier: null, model: trimmed };
}

function stepPrompt(task: Task): string {
	const parts = [`${task.title}: ${task.objective}`];
	if (task.owns_paths.length) parts.push(`Owns: ${task.owns_paths.join(", ")}.`);
	parts.push(`Acceptance check (must fail when the requirement is violated): ${task.acceptance_check}`);
	return parts.join(" ");
}

/**
 * Parse a planner answer into Steps, waves and the lines it did not use.
 *
 * Throws `WorkflowError` naming the key and the 1-based line for: a TASK_* key before any
 * TASK_KEY, a duplicate or unstorable TASK_KEY, a block missing TASK_TITLE / TASK_OBJECTIVE /
 * TASK_ACCEPTANCE_CHECK, a banned acceptance check, an unusable TASK_MODEL, a missing
 * PROJECT_DONE header, and — via `order()` — a dependency on a key that is not a task, or a cycle.
 * Nothing is stored here; `workflow_plan_from_tasks` saves only what came back.
 */
export function planFromTasks(text: string): TaskPlan {
	if (typeof text !== "string") throw new WorkflowError("planFromTasks: text must be a string");
	const rejected: RejectedLine[] = [];
	const header: Record<string, string> = {};
	const tasks: Task[] = [];
	const seenKeys = new Map<string, number>();
	let current: Task | null = null;
	let currentSeen = new Set<string>();

	const finish = () => {
		if (!current) return;
		for (const required of ["TASK_TITLE", "TASK_OBJECTIVE", "TASK_ACCEPTANCE_CHECK"]) {
			if (!currentSeen.has(required)) {
				throw new WorkflowError(`${required} missing in block ${current.key} (TASK_KEY at line ${current.line})`);
			}
		}
		tasks.push(current);
		current = null;
		currentSeen = new Set();
	};

	const lines = text.split(/\r?\n/);
	for (let index = 0; index < lines.length; index++) {
		const lineNo = index + 1;
		const raw = lines[index];
		const trimmed = raw.trim();
		if (!trimmed || /^```/.test(trimmed)) continue;
		const m = KV_LINE.exec(unwrapBackticks(trimmed));
		const key = m ? m[1].toUpperCase() : "";
		if (!m || (!TASK_KEYS.has(key) && !HEADER_KEYS.has(key))) {
			rejected.push({ line: lineNo, text: trimmed, reason: m ? `unknown key ${key}` : "not a KEY=value line" });
			continue;
		}
		const value = m[2].trim();
		if (HEADER_KEYS.has(key)) {
			if (current) rejected.push({ line: lineNo, text: trimmed, reason: `${key} after the first TASK_KEY` });
			else header[key] = value;
			continue;
		}
		if (key === "TASK_KEY") {
			finish();
			if (!NAME_SHAPE.test(value))
				throw new WorkflowError(`TASK_KEY at line ${lineNo} is not a step id: ${JSON.stringify(value)}`);
			const first = seenKeys.get(value);
			if (first !== undefined)
				throw new WorkflowError(`TASK_KEY at line ${lineNo} repeats ${value} (first at line ${first})`);
			seenKeys.set(value, lineNo);
			current = {
				key: value,
				title: "",
				objective: "",
				deps: [],
				owns_paths: [],
				acceptance_check: "",
				parallel_safe: false,
				model: "",
				line: lineNo,
			};
			currentSeen = new Set(["TASK_KEY"]);
			continue;
		}
		if (!current) throw new WorkflowError(`${key} at line ${lineNo} appeared before any TASK_KEY`);
		if (currentSeen.has(key)) throw new WorkflowError(`${key} at line ${lineNo} repeats within block ${current.key}`);
		currentSeen.add(key);
		switch (key) {
			case "TASK_TITLE":
				if (!value) throw new WorkflowError(`TASK_TITLE at line ${lineNo} is empty (block ${current.key})`);
				current.title = value;
				break;
			case "TASK_OBJECTIVE":
				if (!value) throw new WorkflowError(`TASK_OBJECTIVE at line ${lineNo} is empty (block ${current.key})`);
				current.objective = value;
				break;
			case "TASK_DEPS":
				current.deps = splitList(value);
				if (current.deps.includes(current.key))
					throw new WorkflowError(`TASK_DEPS at line ${lineNo}: ${current.key} depends on itself`);
				break;
			case "TASK_OWNS_PATHS":
				current.owns_paths = splitList(value);
				break;
			case "TASK_ACCEPTANCE_CHECK": {
				const ban = bannedCheck(value);
				if (ban) {
					throw new WorkflowError(
						`TASK_ACCEPTANCE_CHECK at line ${lineNo} (block ${current.key}) cannot fail: ${ban}: ${JSON.stringify(value)}`,
					);
				}
				current.acceptance_check = value;
				break;
			}
			case "TASK_PARALLEL_SAFE":
				current.parallel_safe = keyValueBool(value);
				break;
			case "TASK_MODEL": {
				const lowered = value.toLowerCase();
				current.model_line = lineNo;
				if (lowered === "cheap" || lowered === "normal") current.model = lowered;
				else if (MODEL_ID_SHAPE.test(value)) current.model = value;
				else
					throw new WorkflowError(
						`TASK_MODEL at line ${lineNo} (block ${current.key}) is not cheap, normal or a model id: ${JSON.stringify(value)}`,
					);
				break;
			}
		}
	}
	finish();

	if (!("PROJECT_DONE" in header))
		throw new WorkflowError("PROJECT_DONE header missing: the planner did not finish its answer");
	const done = header.PROJECT_DONE.trim().toLowerCase();
	if (done !== "true" && done !== "false")
		throw new WorkflowError(`PROJECT_DONE is not true or false: ${JSON.stringify(header.PROJECT_DONE)}`);
	const project_done = done === "true";
	const reason = header.REASON ?? "";
	if (project_done && tasks.length)
		throw new WorkflowError(`PROJECT_DONE=true with ${tasks.length} TASK block(s): a finished project has no tasks`);
	if (!project_done && !tasks.length) throw new WorkflowError("PROJECT_DONE=false with no TASK blocks");

	const steps: Step[] = tasks.map((task) => {
		const resolved: { model?: string; tier: string | null } = task.model ? stepModel(task.model) : { tier: null };
		if (resolved.tier) {
			rejected.push({
				line: task.model_line ?? task.line,
				text: `TASK_MODEL=${task.model}`,
				reason: `model tier '${resolved.tier}' is not a model; the child inherits the session model`,
			});
		}
		return {
			id: task.key,
			purpose: task.title,
			prompt: stepPrompt(task),
			...(task.deps.length ? { depends_on: task.deps } : {}),
			...(task.owns_paths.length ? { owns_paths: task.owns_paths } : {}),
			parallel_safe: task.parallel_safe,
			outcome_contract: { kind: "command", command: task.acceptance_check } as OutcomeContract,
			...(resolved.model ? { model: resolved.model } : {}),
		};
	});
	// `order()` names a dependency on a key that is not a task, and names the cycle.
	const waves = steps.length ? order(steps) : [];
	return { project_done, reason, tasks, steps, rejected, waves };
}

function planText(workflow: Workflow, waves: string[][]): string {
	const byId = new Map(workflow.steps.map((s) => [s.id, s]));
	const lines = [
		`# ${workflow.name} (${workflow.workflow_id} rev ${workflow.revision}, ${workflow.definition_digest.slice(0, 12)}…): ${workflow.steps.length} steps in ${waves.length} dependency levels`,
		"",
	];
	waves.forEach((wave, index) => {
		// W01: a level is a dependency statement, not a permission to run together. Only steps with
		// `parallel_safe=true` carry that permission, and it is rendered per step.
		lines.push(`## level ${index + 1} — dependency level, not a start signal`);
		for (const id of wave) {
			const step = byId.get(id) as Step;
			const facts = [
				`parallel_safe=${step.parallel_safe === true}`,
				...(step.model ? [`model=${step.model}`] : []),
				...(step.role ? [`role=${step.role}`] : []),
			];
			lines.push(`- **${id}** (${facts.join(" ")}): ${step.prompt}`);
		}
		lines.push("");
	});
	lines.push(
		"Execution: spawn one child per step whose dependencies are settled, then settle them together:",
		'`h = await rlm.spawn(<step prompt>, name="<run>--<step>--<n>")`, then',
		"`snapshots = await rlm.collect([h1, h2], timeout_ms=...)`.",
		"`collect` returns snapshots, not results: a step's real result is the file it wrote (or an",
		"`agent_message` reply) — `answer_preview` is a preview, and a timeout is a snapshot, not a",
		"failure. Only steps marked `parallel_safe=true` may start together; a step may start as soon",
		"as its own `depends_on` are settled.",
		"This capability does not execute anything — it owns the program, not the run.",
	);
	return lines.join("\n");
}

export default function primeLabWorkflow(pi: any, env: Record<string, string | undefined> = process.env): void {
	// M0.15: `PRIME_WORKFLOW` is the product name; the old Lab name still opens the gate for one
	// cycle and says so in a receipt instead of failing silently.
	const gate = (env.PRIME_WORKFLOW ?? "").toLowerCase();
	const legacyGate = gate !== "on" && (env.PRIME_LAB_WORKFLOW ?? "").toLowerCase() === "on";
	if (gate !== "on" && !legacyGate) return;
	if (typeof pi?.registerTool !== "function") return;

	// A tool says it failed by throwing. Measured 2026-09-05 in
	// `host-contract/tests/tool_result_contract.test.mjs`: `AgentToolResult` has three fields and
	// `isError` is not one of them; `executePreparedToolCall` writes `isError: false` for every
	// promise that resolves. The earlier `fail()` returned an unread key, so a refused workflow
	// reached the model as a successful call whose text said otherwise. The prefix survives because
	// the loop builds the model's result from the thrown message.
	const fail = (message: string): never => {
		throw new WorkflowError(message);
	};

	// Whose plan this is. Ohne die Session-ID kann das Plan-Gate nur Zeitstempel vergleichen, und
	// eine Nachbarsitzung, die sich dieses Agent-Verzeichnis teilt, zählt still als „wir haben
	// einen Plan".
	//
	// **Sie wird beim Speichern aus dem `ctx` des Tool-Aufrufs gelesen, nicht bei `agent_start`
	// gemerkt (B-15, 09.09.2026).** Der frühere Kommentar behauptete, `agent_start` sei „the only
	// place the session id is offered to an extension" — das ist falsch: Primes eigener Typ gibt
	// jedem Tool den Kontext als fünftes Argument
	// (`dist/core/extensions/types.d.ts:362`, `execute(toolCallId, params, signal, onUpdate, ctx)`),
	// und `prime-lab-jobs` liest ihn dort seit jeher. Die gemerkte Variante war zudem falsch: ein
	// Inline-RLM-Kind teilt sich mit dem Elternteil eine Modulinstanz (Belege in
	// `host-contract/receipt-session.ts`), sein `agent_start` kippte die Closure, und danach
	// speicherte der ELTERNTEIL seinen Plan unter der Session des Kindes — womit sein eigenes
	// Plan-Gate ihn nie wiederfand und jede weitere Änderung als ungeplant meldete.
	const sessionIdOf = (ctx: unknown): string | null => sessionOf(ctx).id;

	/**
	 * Die Quittung. Sie fehlte bis zum 11.09.2026 vollstaendig.
	 *
	 * Gemessen an der Sitzung vom 09.-11.09.: zwoelf echte Aufrufe dieser vier Werkzeuge, sieben
	 * gespeicherte Programme, drei Laufeintraege — und null Zeilen in der Quittungsschicht. Die
	 * Artefakte unter `storage/workflow/` belegen, DASS gespeichert wurde, aber sie tragen keine
	 * Session, kein `rlm_depth` und keinen abgelehnten Aufruf; `workflow_plan` und ein verweigertes
	 * `workflow_save` hinterlassen ueberhaupt nichts. Die Sonde
	 * `host-contract/tests/receipt_session_all.test.mjs`, die fuer jede Faehigkeit prueft, ob ihre
	 * Quittung eine Session traegt, kannte dieses Paket deshalb gar nicht.
	 *
	 * `who` wird durchgereicht und nie gemerkt (B-15): ein Inline-RLM-Kind teilt sich mit dem
	 * Elternteil eine Modulinstanz und schriebe sonst unter der falschen Session.
	 */
	const receipt = (row: Record<string, unknown>, who: ReceiptSession = UNKNOWN_SESSION): void => {
		try {
			// WP2 (25.09.): Quittungen ziehen nach <agentDir>/receipts/ (write-new); die Altdatei
			// `harness/workflow.receipts.jsonl` bleibt als Historie liegen.
			const dir = env.PRIME_WORKFLOW_RECEIPTS ?? env.PRIME_LAB_WORKFLOW_RECEIPTS ?? join(agentDir(env), "receipts");
			mkdirSync(dir, { recursive: true });
			const line = { at: new Date().toISOString(), ...sessionFields(who), ...row };
			appendFileSync(join(dir, "workflow.receipts.jsonl"), `${JSON.stringify(line)}\n`);
		} catch {
			// Eine Quittung darf das Werkzeugergebnis nicht mitreissen.
		}
	};

	if (legacyGate) {
		receipt({ event: "workflow_gate_deprecated", variable: "PRIME_LAB_WORKFLOW", use: "PRIME_WORKFLOW" });
	}

	// The host side of the bridge: one writer per repo root serves `<state_root>/bridge.sock`, so
	// the Python skill can reach the service. Started on the top-level session's `session_start`
	// only — children fire that hook too (B-15), and a unit test that never fires hooks must not
	// touch a repo. Failure is a receipt, never an activation error.
	let bridge: Bridge | null = null;
	const ensureBridge = async (ctx: unknown): Promise<void> => {
		if (bridge) return;
		try {
			const roots = resolveRoots(env);
			// The driver session travels on the writer, so `run_created.driver_session_id` names the
			// session that actually coordinates (review: all runs had `null`).
			const writer = acquireWriter(env, { session_id: sessionIdOf(ctx), rlm_depth: 0 });
			bridge = await startBridge(env, writer, { socket_path: join(roots.state, "bridge.sock") });
			receipt({ event: "workflow_bridge_started", socket: bridge.socket_path, epoch: bridge.epoch }, sessionOf(ctx));
		} catch (error) {
			receipt(
				{ event: "workflow_bridge_skipped", error: String((error as Error).message).split("\n")[0] },
				sessionOf(ctx),
			);
		}
	};
	if (typeof pi?.on === "function") {
		pi.on("session_start", async (_event: unknown, ctx: unknown) => {
			// Explicitly depth 0 or nothing: an unknown depth is not the top-level session
			// (receipt-session doctrine — null is "unknown", never 0). Review round 2.
			if (sessionOf(ctx).depth !== 0) return;
			await ensureBridge(ctx);
		});
	}

	// `renderSpec` — the collapsed tool block, as data rather than as functions. Render functions
	// cannot cross the daemon→client RPC boundary, so a connection-backed TUI otherwise prints the
	// bare tool name and the whole result text — for `workflow_plan` that is the entire plan. This
	// plain object crosses, and the dist patches `render-spec-daemon` / `render-spec-client`
	// (instrumentation/control/host-contract/dist-patch/prime-dist-patch.mjs) render one line from
	// it while the block is COLLAPSED. Honoured fields, and nothing else: `badge` (dim, after the
	// tool name in the call line — static, so it names what the call REACHES FOR), `collapsed`
	// (first token of the result line), and `fields:[{key, from:"args"|"details", path}]` rendered
	// as `key=value`, with dot-paths resolved and undefined/null/object values dropped.
	//
	// The field is not part of Prime's `ToolDefinition` (dist/core/extensions/types.d.ts:334-367);
	// `pi` is untyped here, and with the patches reverted the key is simply never read — the tools
	// register and the upstream fallback prints exactly what it printed before.
	//
	// Every path is a key the matching `execute` returns. `workflow_plan` has two branches — the
	// listing answers `{ ok, workflows }`, an array that would be dropped, and the named branch
	// answers `{ ok, name, waves, steps }`; the undefined keys of the other branch drop out on
	// their own, which is why one spec serves both. `workflow_plan_from_tasks` carries
	// `project_done`, because PROJECT_DONE=true stores NOTHING and that has to be visible without
	// expanding. `workflow_record` answers `{ ok }` alone, so its line is read from the arguments
	// — the only tool here where `from:"args"` says more than the result does.
	const RENDER_SPEC = {
		save: {
			badge: "store",
			collapsed: "save workflow",
			fields: [
				{ key: "name", from: "details", path: "name" },
				{ key: "steps", from: "details", path: "steps" },
				{ key: "waves", from: "details", path: "waves" },
			],
		},
		plan: {
			badge: "store",
			collapsed: "workflow plan",
			fields: [
				{ key: "name", from: "details", path: "name" },
				{ key: "steps", from: "details", path: "steps" },
				{ key: "waves", from: "details", path: "waves" },
			],
		},
		planFromTasks: {
			badge: "store",
			collapsed: "plan from tasks",
			fields: [
				{ key: "project_done", from: "details", path: "project_done" },
				{ key: "name", from: "details", path: "name" },
				{ key: "steps", from: "details", path: "steps" },
				{ key: "waves", from: "details", path: "waves" },
			],
		},
		record: {
			badge: "store",
			collapsed: "record outcome",
			fields: [
				{ key: "name", from: "args", path: "name" },
				{ key: "step", from: "args", path: "step" },
				{ key: "outcome", from: "args", path: "outcome" },
			],
		},
		migrate: {
			badge: "store",
			collapsed: "migrate definitions",
			fields: [
				{ key: "migrated", from: "details", path: "migrated" },
				{ key: "refused", from: "details", path: "refused" },
			],
		},
	};

	pi.registerTool({
		name: "workflow_save",
		label: "Save a named orchestration program",
		renderSpec: RENDER_SPEC.save,
		description:
			"Store a multi-step workflow under a name. Each step has an id, a prompt, and optional " +
			"depends_on ids. The dependency graph is validated before it is stored: a dependency on " +
			"a step that does not exist, or a cycle, is refused with the ids involved.",
		parameters: {
			type: "object",
			properties: {
				name: { type: "string" },
				steps: {
					type: "array",
					items: {
						type: "object",
						properties: {
							id: { type: "string" },
							prompt: { type: "string" },
							depends_on: { type: "array", items: { type: "string" } },
							model: { type: "string" },
						},
						required: ["id", "prompt"],
					},
				},
			},
			required: ["name", "steps"],
		},
		execute: async (_id: string, params: any, _s: unknown, _u: unknown, ctx: any) => {
			try {
				const saved = saveWorkflow(env, String(params?.name ?? ""), params?.steps ?? [], {
					session_id: sessionIdOf(ctx),
				});
				const waves = order(saved.steps);
				receipt(
					{ event: "workflow_saved", name: saved.name, steps: saved.steps.length, waves: waves.length },
					sessionOf(ctx),
				);
				return {
					content: [{ type: "text", text: `saved ${saved.name}: ${saved.steps.length} steps, ${waves.length} waves` }],
					details: { ok: true, name: saved.name, steps: saved.steps.length, waves: waves.length },
				};
			} catch (error) {
				receipt(
					{
						event: "workflow_refused",
						tool: "workflow_save",
						name: String(params?.name ?? ""),
						error: String((error as Error).message).split("\n")[0],
					},
					sessionOf(ctx),
				);
				fail(`workflow_save: ${String((error as Error).message)}`);
			}
		},
	});

	pi.registerTool({
		name: "workflow_plan",
		label: "Read a workflow back as an ordered plan",
		renderSpec: RENDER_SPEC.plan,
		description:
			"Return a saved workflow in dependency order, grouped into waves that may run together. " +
			"Omit the name to list what is saved.",
		parameters: { type: "object", properties: { name: { type: "string" } } },
		execute: async (_id: string, params: any, _s: unknown, _u: unknown, ctx: any) => {
			try {
				const name = params?.name === undefined ? "" : String(params.name);
				if (!name) {
					const names = listWorkflows(env);
					receipt({ event: "workflow_listed", workflows: names.length }, sessionOf(ctx));
					return {
						content: [{ type: "text", text: names.length ? names.join("\n") : "no workflows saved" }],
						details: { ok: true, workflows: names },
					};
				}
				const workflow = loadWorkflow(env, name);
				const waves = order(workflow.steps);
				receipt({ event: "workflow_read", name, steps: workflow.steps.length, waves: waves.length }, sessionOf(ctx));
				return {
					content: [{ type: "text", text: planText(workflow, waves) }],
					details: { ok: true, name, waves: waves.length, steps: workflow.steps.length },
				};
			} catch (error) {
				receipt(
					{
						event: "workflow_refused",
						tool: "workflow_plan",
						name: String(params?.name ?? ""),
						error: String((error as Error).message).split("\n")[0],
					},
					sessionOf(ctx),
				);
				fail(`workflow_plan: ${String((error as Error).message)}`);
			}
		},
	});

	pi.registerTool({
		name: "workflow_plan_from_tasks",
		label: "Save a planner's TASK blocks as a workflow",
		renderSpec: RENDER_SPEC.planFromTasks,
		description:
			"Parse an Argus-style planner answer (PROJECT_DONE/REASON header, then TASK_KEY, " +
			"TASK_TITLE, TASK_OBJECTIVE, TASK_DEPS, TASK_OWNS_PATHS, TASK_ACCEPTANCE_CHECK, " +
			"TASK_PARALLEL_SAFE, TASK_MODEL blocks) into a named workflow and store it. Waves come " +
			"from TASK_DEPS. Refused, and nothing stored, when an acceptance check cannot fail " +
			"('|| true', 'or True', 'exit 0', ':' / 'true', empty, unconditional success, or an " +
			"'unchanged file' claim with no command), when a dependency names no task, or on a cycle. " +
			"PROJECT_DONE=true stores nothing and returns the reason.",
		parameters: {
			type: "object",
			properties: {
				text: { type: "string", description: "The planner's answer, verbatim." },
				name: { type: "string", description: "Workflow name to store under." },
			},
			required: ["text", "name"],
		},
		execute: async (_id: string, params: any, _s: unknown, _u: unknown, ctx: any) => {
			try {
				// Parsed first, stored second: a thrown parse error leaves the store untouched.
				const plan = planFromTasks(String(params?.text ?? ""));
				const skipped = plan.rejected.length
					? `\n${plan.rejected.length} line(s) not used:\n${plan.rejected.map((r) => `  line ${r.line}: ${r.reason}: ${r.text}`).join("\n")}`
					: "";
				if (plan.project_done) {
					receipt(
						{ event: "workflow_project_done", name: String(params?.name ?? ""), rejected: plan.rejected.length },
						sessionOf(ctx),
					);
					return {
						content: [{ type: "text", text: `PROJECT_DONE=true, nothing saved. REASON=${plan.reason}${skipped}` }],
						details: { ok: true, project_done: true, saved: false, reason: plan.reason, rejected: plan.rejected },
					};
				}
				const saved = saveWorkflow(env, String(params?.name ?? ""), plan.steps, { session_id: sessionIdOf(ctx) });
				receipt(
					{
						event: "workflow_saved",
						from: "tasks",
						name: saved.name,
						steps: saved.steps.length,
						waves: plan.waves.length,
						rejected: plan.rejected.length,
					},
					sessionOf(ctx),
				);
				return {
					content: [{ type: "text", text: `${planText(saved, plan.waves)}${skipped}` }],
					details: {
						ok: true,
						project_done: false,
						saved: true,
						name: saved.name,
						steps: saved.steps.length,
						waves: plan.waves.length,
						reason: plan.reason,
						rejected: plan.rejected,
					},
				};
			} catch (error) {
				receipt(
					{
						event: "workflow_refused",
						tool: "workflow_plan_from_tasks",
						name: String(params?.name ?? ""),
						error: String((error as Error).message).split("\n")[0],
					},
					sessionOf(ctx),
				);
				fail(`workflow_plan_from_tasks: ${String((error as Error).message)}`);
			}
		},
	});

	pi.registerTool({
		name: "workflow_record",
		label: "Record what a workflow step did",
		renderSpec: RENDER_SPEC.record,
		description:
			"Append an outcome for one step of a workflow run, so a later session can see what " +
			"happened rather than infer it from the transcript.",
		parameters: {
			type: "object",
			properties: {
				name: { type: "string" },
				step: { type: "string" },
				outcome: { type: "string" },
				note: { type: "string" },
			},
			required: ["name", "step", "outcome"],
		},
		execute: async (_id: string, params: any, _s: unknown, _u: unknown, ctx: any) => {
			try {
				const name = String(params?.name ?? "");
				// W03: the workflow must exist AND the step must be one of its steps; an arbitrary
				// success text against an unknown step used to be accepted.
				const workflow = loadWorkflow(env, name);
				const step = String(params?.step ?? "");
				if (!workflow.steps.some((s) => s.id === step)) {
					throw new WorkflowError(
						`no step ${JSON.stringify(step)} in workflow ${name} (steps: ${workflow.steps.map((s) => s.id).join(", ")})`,
					);
				}
				recordRun(env, name, {
					step,
					outcome: String(params?.outcome ?? "") as RunOutcome,
					...(params?.note ? { note: String(params.note) } : {}),
					definition_digest: workflow.definition_digest,
					session_id: sessionIdOf(ctx),
				});
				receipt(
					{ event: "workflow_step_recorded", name, step, outcome: String(params?.outcome ?? "") },
					sessionOf(ctx),
				);
				return { content: [{ type: "text", text: "recorded" }], details: { ok: true } };
			} catch (error) {
				receipt(
					{
						event: "workflow_refused",
						tool: "workflow_record",
						name: String(params?.name ?? ""),
						error: String((error as Error).message).split("\n")[0],
					},
					sessionOf(ctx),
				);
				fail(`workflow_record: ${String((error as Error).message)}`);
			}
		},
	});

	pi.registerTool({
		name: "workflow_migrate_definitions",
		label: "Migrate pre-schema workflows to the current definition schema",
		renderSpec: RENDER_SPEC.migrate,
		description:
			"Transfer legacy saved workflows (no schema field) to the current definition schema, or " +
			"refuse them with the reason. Nothing is silently overwritten: every legacy entry keeps " +
			"its old key, and a migrated definition becomes a new revision. A legacy step whose " +
			"prompt has no 'Acceptance check (…):' marker is refused — it gets no substitute check.",
		parameters: { type: "object", properties: {} },
		execute: async (_id: string, _params: any, _s: unknown, _u: unknown, ctx: any) => {
			try {
				const report = migrateLegacyDefinitions(env);
				receipt(
					{ event: "workflow_migrated", migrated: report.migrated.length, refused: report.refused.length },
					sessionOf(ctx),
				);
				return {
					content: [{ type: "text", text: JSON.stringify(report, null, 2) }],
					details: { ok: true, migrated: report.migrated.length, refused: report.refused.length, report },
				};
			} catch (error) {
				receipt(
					{
						event: "workflow_refused",
						tool: "workflow_migrate_definitions",
						error: String((error as Error).message).split("\n")[0],
					},
					sessionOf(ctx),
				);
				fail(`workflow_migrate_definitions: ${String((error as Error).message)}`);
			}
		},
	});
}
