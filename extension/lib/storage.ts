/**
 * A typed store for extensions, with change events — ported from DeepSeek-Harness's `storage`.
 *
 * DSH's package is domain storage for non-session data over swappable JSON/SQLite backends, and it
 * registers **no tools and no prompts**: it exists for other packages, not for the model. Prime has
 * only `settings-manager`, which persists settings and nothing else.
 *
 * **This was refused once, and the refusal is worth keeping visible.** On the lab side it would
 * have been an abstraction over two stores that wanted different things — an FTS5 index rebuilt
 * whole, and an append-only registry with fsync. The reversal condition written down then was "a
 * third store that *needs* change events", and building the Prime-side `session_query` and `jobs`
 * produced something better than a third store: two capabilities that each hand-rolled a place to
 * put files under `<agentDir>`, with their own directory conventions and their own answer to what
 * happens when the disk is full. That is the duplication the package removes.
 *
 * **What it is not.** Not a database and not a cache. It is a namespaced, typed, atomic
 * key-value store with a change listener, so a capability can persist a little state without
 * inventing a directory layout, and another capability can react to it without polling.
 *
 * **Atomic, because the alternative is a file that exists and is half-written.** Every put is a
 * temp file plus a rename, which is the one write that cannot be observed partially on a POSIX
 * filesystem. This repository has a live example of the alternative: `rebuild_index` in one of its
 * own case fixtures writes in place and leaves a zero-byte file when interrupted, which is the
 * provocation an entire measurement case is built around.
 */

import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export type ChangeKind = "put" | "delete";
export type ChangeListener = (event: { namespace: string; key: string; kind: ChangeKind }) => void;

const KEY_SHAPE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function resolveAgentDir(env: Record<string, string | undefined>): string {
	const raw = env.PRIME_AGENT_CODING_AGENT_DIR;
	if (!raw) return join(homedir(), ".prime/agent");
	return raw.startsWith("~/") ? join(homedir(), raw.slice(2)) : raw;
}

export class StorageError extends Error {}

/**
 * One namespace of one store. A namespace is a directory; a key is a file in it.
 *
 * Keys are checked against a shape rather than escaped, because escaping invites the question of
 * whether the escaping is complete and a shape does not. `..`, `/` and an empty key are all simply
 * not keys.
 */
export class Store {
	private readonly root: string;
	private readonly listeners = new Set<ChangeListener>();
	// Plain field, not a parameter property: Node's strip-only loader rejects those, and that is
	// how Prime loads every extension. This is the second capability in this tree to hit it, which
	// is why tests/test_capability_suites.py now checks for the syntax rather than waiting for the
	// next one.
	readonly namespace: string;

	constructor(env: Record<string, string | undefined>, namespace: string) {
		if (!KEY_SHAPE.test(namespace)) throw new StorageError(`not a namespace: ${namespace}`);
		this.namespace = namespace;
		this.root = join(resolveAgentDir(env), "storage", namespace);
	}

	private pathFor(key: string): string {
		if (!KEY_SHAPE.test(key)) throw new StorageError(`not a key: ${key}`);
		return join(this.root, `${key}.json`);
	}

	/** Fires listeners after the write lands, never before — an event for a write that failed is a lie. */
	private emit(key: string, kind: ChangeKind): void {
		for (const listener of this.listeners) {
			try {
				listener({ namespace: this.namespace, key, kind });
			} catch (error) {
				// A listener that throws must not undo a write that already happened.
				console.error(`[prime-lab-storage] listener failed on ${this.namespace}/${key}: ${String(error)}`);
			}
		}
	}

	onChange(listener: ChangeListener): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	has(key: string): boolean {
		return existsSync(this.pathFor(key));
	}

	get<T = unknown>(key: string): T | undefined {
		const path = this.pathFor(key);
		if (!existsSync(path)) return undefined;
		try {
			return JSON.parse(readFileSync(path, "utf8")) as T;
		} catch (error) {
			throw new StorageError(`${this.namespace}/${key} is not readable JSON: ${String(error)}`);
		}
	}

	/** Temp file plus rename: the one write a reader cannot observe half-finished. */
	put(key: string, value: unknown): void {
		const path = this.pathFor(key);
		mkdirSync(this.root, { recursive: true });
		const temp = `${path}.${process.pid}.tmp`;
		writeFileSync(temp, `${JSON.stringify(value, null, 2)}\n`, "utf8");
		renameSync(temp, path);
		this.emit(key, "put");
	}

	delete(key: string): boolean {
		const path = this.pathFor(key);
		if (!existsSync(path)) return false;
		rmSync(path);
		this.emit(key, "delete");
		return true;
	}

	keys(): string[] {
		if (!existsSync(this.root)) return [];
		return readdirSync(this.root)
			.filter((name) => name.endsWith(".json"))
			.map((name) => name.slice(0, -".json".length))
			.sort();
	}
}

/** The capability's only export to other capabilities: a store, by namespace. */
export function openStore(env: Record<string, string | undefined>, namespace: string): Store {
	return new Store(env, namespace);
}

/**
 * Registers nothing.
 *
 * DSH's `storage` declares that it registers no tools and no prompts, and that is the property
 * worth keeping: a capability that other capabilities build on must not also be competing for the
 * model's attention. The default export exists so the carrier can load it the same way it loads
 * everything else, and so that loading it is visible in the receipt.
 */
export default function primeLabStorage(_pi: unknown): void {
	// Intentionally empty. See above.
}
