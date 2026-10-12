/**
 * Runtime invariants, owned by the capability that declares them — ported from DeepSeek-Harness's
 * `runtime-diagnostics` (`packages/runtime-diagnostics`, read at `d347e703`).
 *
 * DSH's idea is small and unusual: a package declares what must remain true of *its own* wiring,
 * and the runtime checks it **while the composition runs** rather than in a test. A violation is
 * attributed to the owning package, not to whoever noticed. There is a global switch and a
 * per-package filter. Prime has no counterpart.
 *
 * **Why this is the DSH package worth porting first.** The failure it addresses is the one this
 * laboratory produces most: an indicator that still renders green after it stopped reaching its
 * subject. In one night, measured: a leak detector that matched only one of three routes and
 * reported a contaminated run CLEAN; a classifier branch reading a field that was in 0 of 1016
 * rows; twelve `.mjs` suites that ran in no pytest invocation at all; a test that stayed green
 * under mutation of the very guard it named. Every one of those is a thing an invariant could
 * have asserted about itself at runtime, and none of them was visible in a test suite that passed.
 *
 * **What an invariant is allowed to be.** It must be cheap, side-effect free, and about the
 * *wiring* rather than the work: "the receipt file I write is writable", "the store I read exists
 * and parses", "the hook I registered is still the only handler on that event". An invariant that
 * re-does the capability's job is a second implementation and will drift from the first.
 *
 * **Failure is reported, never thrown.** A diagnostic that can break a session is a liability
 * larger than the defect it looks for; a capability whose invariant throws is recorded as
 * `errored` and the rest keep running.
 */

export type Invariant = {
	/** The capability that owns this, and that a violation is attributed to. */
	owner: string;
	/** Short, stated as the thing that must be TRUE. */
	name: string;
	/** Cheap, side-effect free, synchronous. Return true when the invariant holds. */
	check: () => boolean;
	/** What a reader should do about it, in one sentence. */
	remedy?: string;
};

export type Violation = {
	owner: string;
	name: string;
	status: "violated" | "errored";
	remedy?: string;
	error?: string;
};

export type DiagnosticsReport = {
	schema: "prime-agent-lab/runtime-diagnostics/1";
	checked: number;
	held: number;
	violations: Violation[];
	skipped_owners: string[];
};

const registry: Invariant[] = [];

/** Declared by a capability at wiring time; the carrier runs them. */
export function declareInvariant(invariant: Invariant): void {
	registry.push(invariant);
}

/** Test seam. Production code never needs this; a test that shares global state does. */
export function clearInvariants(): void {
	registry.length = 0;
}

export function declaredInvariants(): readonly Invariant[] {
	return registry;
}

/**
 * Run every declared invariant whose owner is enabled.
 *
 * `enabled` is the DSH per-package filter: absent means all, otherwise only the named owners run.
 * Owners present in the registry but not enabled are reported under `skipped_owners`, so a filter
 * that silently disables everything is visible in the receipt rather than indistinguishable from
 * a clean run — which is the failure mode this module exists for, applied to itself.
 */
export function runDiagnostics(enabled?: readonly string[]): DiagnosticsReport {
	const allow = enabled === undefined ? null : new Set(enabled);
	const violations: Violation[] = [];
	const skipped = new Set<string>();
	let checked = 0;
	for (const invariant of registry) {
		if (allow && !allow.has(invariant.owner)) {
			skipped.add(invariant.owner);
			continue;
		}
		checked += 1;
		try {
			if (!invariant.check()) {
				violations.push({
					owner: invariant.owner,
					name: invariant.name,
					status: "violated",
					...(invariant.remedy ? { remedy: invariant.remedy } : {}),
				});
			}
		} catch (error) {
			violations.push({
				owner: invariant.owner,
				name: invariant.name,
				status: "errored",
				error: String((error as Error)?.message ?? error).slice(0, 200),
			});
		}
	}
	return {
		schema: "prime-agent-lab/runtime-diagnostics/1",
		checked,
		held: checked - violations.length,
		violations,
		skipped_owners: [...skipped].sort(),
	};
}
