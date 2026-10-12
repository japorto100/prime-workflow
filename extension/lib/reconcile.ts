/**
 * M1.7 — reconciliation: what may be believed about an open attempt, and what must not be.
 *
 * Two observations exist and they say different things:
 *
 * 1. **The RLM ledger** (`<agentDir>/rlm-ledger/<hash>.jsonl`) proves admission and identity —
 *    `spawn` maps name ↔ childId ↔ parent/child sessions — and `delete` is the one terminal signal.
 *    Measured in `knowledge/workflow-reconciliation-probe.md`: no "settled" and no "failed".
 * 2. **The kernel registry** (`rlm.list_subagents()`), reported by the Python side, is the only
 *    fresh liveness source. Its provenance stays visible: it is an observation, not host truth.
 *
 * The rules are therefore conservative: a lease loss or an absent registry entry is NOT proof that
 * the child ended, so the attempt becomes `unresolved` (retry stays blocked) unless the ledger shows
 * a delete. Nothing here spawns, restarts or retries — reconciliation only re-labels what is known.
 */
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import type { AttemptProjection } from "./service.ts";

export const ACTIVE = new Set(["intended", "admitted", "running", "unresolved"]);

export type LedgerSpawn = { child_id: string; name: string; depth: number; at: string };
export type LedgerDelete = { child_id: string; at: string; reason: string | null };
export type ObservedChild = { name?: string | null; child_id?: string | null; status?: string | null };

/**
 * Kernel statuses that mean "this child has ENDED". Measured 24.09.2026 in a sessionless run: a
 * child goes `running` → `completed` and **stays listed** in `list_subagents()`, so presence alone
 * is not liveness. Unknown values stay conservative (treated as alive): absence/unknown is never a
 * death certificate (doctrine: a missing entry produced `unresolved`, not `succeeded`).
 */
export const OBSERVED_TERMINAL_STATUSES: ReadonlySet<string> = new Set(["completed", "failed", "error"]);

function observedIsAlive(child: ObservedChild): boolean {
	const status = String(child.status ?? "")
		.trim()
		.toLowerCase();
	return !OBSERVED_TERMINAL_STATUSES.has(status);
}

export type LedgerRead = {
	spawns: LedgerSpawn[];
	deletes: LedgerDelete[];
	/** Lines this reader did not understand (unknown `v`/`op`, bad JSON) — reported, never guessed. */
	unknown_lines: number;
};

/** Tolerant by design: an unknown version/op is counted, not interpreted. */
export function parseLedger(text: string): LedgerRead {
	const read: LedgerRead = { spawns: [], deletes: [], unknown_lines: 0 };
	for (const line of text.split("\n")) {
		if (!line.trim()) continue;
		let event: Record<string, unknown>;
		try {
			event = JSON.parse(line) as Record<string, unknown>;
		} catch {
			read.unknown_lines += 1;
			continue;
		}
		const version = Number(event.v);
		if (version !== 1) {
			read.unknown_lines += 1;
			continue;
		}
		if (event.op === "spawn" && typeof event.childId === "string" && typeof event.name === "string") {
			read.spawns.push({
				child_id: event.childId,
				name: event.name,
				depth: Number(event.depth ?? 0),
				at: String(event.at ?? ""),
			});
		} else if (event.op === "delete" && typeof event.childId === "string") {
			read.deletes.push({
				child_id: event.childId,
				at: String(event.at ?? ""),
				reason: (event.reason as string | null) ?? null,
			});
		} else if (event.op === "meta") {
			// The meta line carries sessionsDir; the caller used it to pick the file.
		} else {
			read.unknown_lines += 1;
		}
	}
	return read;
}

/**
 * The newest ledger file that belongs to this sessions dir, or null.
 *
 * Identity, not substring: a file counts only when its `meta.sessionsDir` EXACTLY equals the
 * caller's sessions dir, and among several matches the newest by mtime wins. The previous
 * `line.includes()` filter matched a child sessions dir inside a parent path and could skip the
 * real ledger because a `spawn` line happened to contain the substring "meta" (review round 2).
 */
export function readLedgerDir(ledger_dir: string, sessions_dir: string): LedgerRead | null {
	if (!existsSync(ledger_dir)) return null;
	const candidates = readdirSync(ledger_dir)
		.filter((n) => n.endsWith(".jsonl"))
		.map((name) => {
			const path = join(ledger_dir, name);
			let mtime = 0;
			try {
				mtime = statSync(path).mtimeMs;
			} catch {
				mtime = 0;
			}
			return { path, mtime, name };
		})
		.sort((a, b) => b.mtime - a.mtime || (a.name < b.name ? -1 : 1));
	for (const candidate of candidates) {
		let text: string;
		try {
			text = readFileSync(candidate.path, "utf8");
		} catch {
			continue;
		}
		let sessionsDir: unknown = null;
		for (const line of text.split("\n")) {
			if (!line.trim()) continue;
			let event: Record<string, unknown>;
			try {
				event = JSON.parse(line) as Record<string, unknown>;
			} catch {
				continue;
			}
			if (event.op === "meta") {
				sessionsDir = event.sessionsDir ?? null;
				break;
			}
		}
		if (typeof sessionsDir !== "string" || sessionsDir !== sessions_dir) continue;
		return parseLedger(text);
	}
	return null;
}

export type ReconcileAction = { attempt_id: string; action: "unresolved" | "orphaned"; reason: string };

/**
 * The decision table. `observed` is the kernel registry as reported by Python; its absence is not
 * evidence of death, which is exactly why absence produces `unresolved` and not `succeeded`.
 */
export function decideReconciliation(input: {
	attempts: AttemptProjection[];
	spawns: LedgerSpawn[];
	deletes: LedgerDelete[];
	observed: ObservedChild[];
}): ReconcileAction[] {
	const deleted = new Set(input.deletes.map((d) => d.child_id));
	const spawnedByName = new Map(input.spawns.map((s) => [s.name, s]));
	const actions: ReconcileAction[] = [];
	for (const attempt of input.attempts) {
		// A later terminal signal may resolve an already-blocked attempt: a delete is new evidence.
		if (attempt.child_id && deleted.has(attempt.child_id)) {
			actions.push({
				attempt_id: attempt.attempt_id,
				action: "orphaned",
				reason: `ledger records delete for ${attempt.child_id}`,
			});
			continue;
		}
		// Observed liveness is evidence in its own right (review G14): an admitted child that the
		// kernel reports running is alive even when the ledger (or another ledger file) has no entry.
		const observed = attempt.child_id
			? input.observed.find((o) => o.child_id === attempt.child_id)
			: input.observed.find((o) => o.name === attempt.child_name);
		if (observed && observedIsAlive(observed)) continue;
		if (attempt.status === "unresolved") continue; // already blocked; nothing new to say
		const spawn = spawnedByName.get(attempt.child_name);
		if (!spawn) {
			actions.push({
				attempt_id: attempt.attempt_id,
				action: "unresolved",
				reason: `no ledger admission for ${attempt.child_name}; admission is unproven`,
			});
			continue;
		}
		// The ledger's name maps to a DIFFERENT child than the admitted one: the original child is
		// missing and must not be masked by the namesake (review G15).
		if (attempt.child_id && spawn.child_id !== attempt.child_id) {
			actions.push({
				attempt_id: attempt.attempt_id,
				action: "unresolved",
				reason: `ledger knows ${attempt.child_name} as ${spawn.child_id}, not as the admitted ${attempt.child_id}; the original child is unaccounted for`,
			});
			continue;
		}
		// A listed child with a TERMINAL status is not alive: the attempt needs an explicit decision
		// (a late artifact may still arrive, so `unresolved` blocks the retry instead of failing it).
		const observedStatus = observed ? String(observed.status ?? "").trim() : "";
		actions.push({
			attempt_id: attempt.attempt_id,
			action: "unresolved",
			reason:
				observedStatus && !observedIsAlive(observed as ObservedChild)
					? `${spawn.child_id} ended in the kernel registry (status ${observedStatus}) and the ledger has no terminal entry; the attempt needs an explicit decision`
					: `${spawn.child_id} is not in the kernel registry and the ledger has no terminal entry; liveness unknown`,
		});
	}
	return actions;
}
