/** Explicit experimental template adapter. No catalogue/store, auto-executor,
 * CH kind or promotion: instantiate through the existing run journal only.
 */
import { createHash } from "node:crypto";
import { createRun, type StepSpec, WorkflowStateError } from "./service.ts";

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type Observation = {
	run_id: string;
	source_digest: string;
	partition: "training" | "evaluation";
	outcome: "positive" | "negative" | "unknown";
};
export type TemplateCandidate = {
	version: 1;
	id: string;
	revision: string;
	applicability: string;
	input_names: string[];
	steps: StepSpec[];
	roles: string[];
	result_forms: Record<string, Json>;
	limits: Record<string, Json>;
	method_refs: string[];
	effects: "read_only" | "mutating";
	stop_rule: string;
	retry_rule: string;
	evidence: Observation[];
};
const fail = (message: string): never => {
	throw new WorkflowStateError(`template candidate: ${message}`);
};
const idPattern = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const hashPattern = /^[a-f0-9]{64}$/;
const sha = (text: string) => createHash("sha256").update(text).digest("hex");

function canonical(value: unknown, depth = 0): string {
	if (depth > 24) return fail("JSON nesting limit");
	if (value === null || typeof value === "boolean") return JSON.stringify(value);
	if (typeof value === "string") {
		if (value.length > 32768) return fail("string limit");
		return JSON.stringify(value);
	}
	if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
	if (Array.isArray(value)) {
		if (value.length > 256) return fail("array limit");
		return `[${value.map((item) => canonical(item, depth + 1)).join(",")}]`;
	}
	if (value && typeof value === "object" && [Object.prototype, null].includes(Object.getPrototypeOf(value))) {
		const keys = Reflect.ownKeys(value);
		if (
			keys.length > 256 ||
			keys.some(
				(key) => typeof key !== "string" || !Object.hasOwn(Object.getOwnPropertyDescriptor(value, key)!, "value"),
			)
		)
			return fail("object fields");
		return `{${(keys as string[])
			.sort()
			.map((key) => `${JSON.stringify(key)}:${canonical((value as Record<string, unknown>)[key], depth + 1)}`)
			.join(",")}}`;
	}
	return fail("only finite JSON data is supported");
}

function bounded(value: unknown): string {
	const result = canonical(value);
	if (Buffer.byteLength(result) > 65536) return fail("definition exceeds 64 KiB");
	return result;
}

function text(value: unknown, name: string): void {
	if (typeof value !== "string" || !value.trim() || value.length > 4096) fail(`invalid ${name}`);
}

export function freezeTemplate(candidate: TemplateCandidate): {
	digest: string;
	definition: string;
	status: "candidate";
} {
	// Work on detached JSON so caller edits cannot change what was validated/pinned.
	const definition = bounded(candidate);
	const value = JSON.parse(definition) as TemplateCandidate;
	const keys = [
		"version",
		"id",
		"revision",
		"applicability",
		"input_names",
		"steps",
		"roles",
		"result_forms",
		"limits",
		"method_refs",
		"effects",
		"stop_rule",
		"retry_rule",
		"evidence",
	];
	if (Object.keys(value).some((key) => !keys.includes(key)) || keys.some((key) => !Object.hasOwn(value, key)))
		fail("candidate fields");
	if (value.version !== 1 || typeof value.id !== "string" || !idPattern.test(value.id)) fail("version or id");
	for (const key of ["revision", "applicability", "stop_rule", "retry_rule"] as const) text(value[key], key);
	if (!["read_only", "mutating"].includes(value.effects)) fail("effects");
	for (const key of ["input_names", "roles", "method_refs"] as const) {
		if (!Array.isArray(value[key])) fail(key);
		for (const entry of value[key]) text(entry, key);
		if (new Set(value[key]).size !== value[key].length) fail(`duplicate ${key}`);
	}
	if (!Array.isArray(value.steps) || !value.steps.length) fail("steps");
	for (const key of ["result_forms", "limits"] as const) {
		if (!value[key] || typeof value[key] !== "object" || Array.isArray(value[key])) fail(key);
	}
	if (!Array.isArray(value.evidence)) fail("evidence");
	const partitions = { training: new Set<string>(), evaluation: new Set<string>() };
	for (const observation of value.evidence) {
		if (
			!observation ||
			!["training", "evaluation"].includes(observation.partition) ||
			!["positive", "negative", "unknown"].includes(observation.outcome) ||
			!hashPattern.test(observation.source_digest)
		)
			fail("evidence binding");
		text(observation.run_id, "evidence run");
		partitions[observation.partition].add(`run:${observation.run_id}`);
		partitions[observation.partition].add(`source:${observation.source_digest}`);
	}
	if ([...partitions.training].some((key) => partitions.evaluation.has(key))) fail("training/evaluation overlap");
	return Object.freeze({ digest: sha(definition), definition, status: "candidate" });
}

/** Caller deliberately selects a specific candidate digest. This persists a
 * definition, never starts an agent, changes a Spec, or promotes a candidate.
 */
export function createRunFromTemplate(
	env: Parameters<typeof createRun>[0],
	writer: Parameters<typeof createRun>[1],
	candidate: TemplateCandidate,
	input: { run_id: string; selected_digest: string; inputs: Record<string, Json>; subject: Record<string, Json> },
) {
	const frozen = freezeTemplate(candidate);
	if (input.selected_digest !== frozen.digest) fail("selected version changed");
	const value = JSON.parse(frozen.definition) as TemplateCandidate;
	const supplied = Object.keys(input.inputs).sort();
	if (canonical(supplied) !== canonical([...value.input_names].sort())) fail("input names do not match");
	const definition = bounded({
		version: 1,
		template_digest: frozen.digest,
		template: value,
		inputs: input.inputs,
		subject: input.subject,
	});
	const instance = JSON.parse(definition);
	return createRun(env, writer, {
		run_id: input.run_id,
		definition_digest: sha(definition),
		definition_object: definition,
		subject: instance.subject,
		limits: value.limits,
		steps: value.steps,
	});
}
