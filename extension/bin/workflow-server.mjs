#!/usr/bin/env node
/**
 * Standalone runner for the Workflow Bridge.
 *
 * Use this to run prime-workflow as an out-of-process sidecar service,
 * e.g. for Prime Agent v0.10+ (which no longer hosts in-process TypeScript extensions)
 * or for headless CI / multi-agent environments.
 */

import { join } from "node:path";
import { startBridge } from "../lib/bridge.ts";
import { acquireWriter, resolveRoots } from "../lib/service.ts";

const cwd = process.argv[2] ? process.argv[2] : process.cwd();
console.log(`[prime-workflow] Resolving state roots for cwd: ${cwd}`);

let roots;
try {
	roots = resolveRoots(cwd);
} catch (err) {
	console.error(`[prime-workflow] Error resolving roots: ${err.message}`);
	process.exit(1);
}

console.log(`[prime-workflow] Definitions root: ${roots.definitions}`);
console.log(`[prime-workflow] State root:       ${roots.state}`);

const socketPath = process.env.PRIME_WORKFLOW_SOCKET || join(roots.state, "bridge.sock");
const env = {
	state_root: roots.state,
	definitions_root: roots.definitions,
	agent_dir: process.env.PRIME_AGENT_CODING_AGENT_DIR || join(process.env.HOME || "", ".prime/agent"),
};

const sessionId = `standalone-${process.pid}-${Date.now().toString(36)}`;
let writer;
try {
	writer = acquireWriter(roots.state, sessionId, 0);
} catch (err) {
	console.error(`[prime-workflow] Failed to acquire exclusive writer lock: ${err.message}`);
	process.exit(1);
}

console.log(`[prime-workflow] Acquired writer lock with epoch: ${writer.epoch}`);

let bridge;
try {
	bridge = await startBridge(env, writer, { socket_path: socketPath });
	console.log(`[prime-workflow] Bridge active at: ${bridge.socket_path}`);
	console.log(`[prime-workflow] Protocol: prime-workflow/bridge/1`);
	console.log(`[prime-workflow] Ready for Python skill connections.`);
} catch (err) {
	console.error(`[prime-workflow] Failed to start bridge: ${err.message}`);
	process.exit(1);
}

const shutdown = async () => {
	console.log("\n[prime-workflow] Shutting down bridge...");
	try {
		await bridge.close();
		console.log("[prime-workflow] Bridge closed cleanly.");
	} catch (err) {
		console.error(`[prime-workflow] Error during shutdown: ${err.message}`);
	}
	process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
