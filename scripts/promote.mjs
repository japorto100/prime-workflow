#!/usr/bin/env node
// Promote prime-workflow into ~/.prime/agent/
//
// Usage:
//   node scripts/promote.mjs            # Dry-run: hash packages, report drift, show actions
//   node scripts/promote.mjs --apply    # Perform backup, copy extension and skill, write receipts
//   node scripts/promote.mjs --check    # Check if installed copy matches the repository source
//   node scripts/promote.mjs --dest-agent-dir <path>  # Target custom agent directory

import { createHash } from "node:crypto";
import { copyFile, chmod, lstat, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "..");

const args = process.argv.slice(2);
let apply = false;
let check = false;
let destAgentDir = process.env.PRIME_AGENT_DIR || join(homedir(), ".prime", "agent");

for (let i = 0; i < args.length; i++) {
	if (args[i] === "--apply") apply = true;
	else if (args[i] === "--check") check = true;
	else if (args[i] === "--dest-agent-dir" && i + 1 < args.length) {
		destAgentDir = resolve(args[++i]);
	} else if (args[i] === "--help" || args[i] === "-h") {
		console.log("Usage: node scripts/promote.mjs [--apply | --check] [--dest-agent-dir <path>]");
		process.exit(0);
	} else {
		console.error(`Unknown argument: ${args[i]}`);
		process.exit(2);
	}
}

if (apply && check) {
	console.error("Cannot combine --apply and --check");
	process.exit(2);
}

const EXT_DIR = join(destAgentDir, "extensions");
const SKILL_DIR = join(destAgentDir, "skills");
const BACKUP_DIR = join(destAgentDir, "backups");
const RECEIPTS_DIR = join(destAgentDir, "receipts");

const PLAN = [
	{
		name: "workflow",
		type: "extension",
		src: join(ROOT, "extension"),
		dest: join(EXT_DIR, "workflow"),
	},
	{
		name: "workflow-skill",
		type: "skill",
		src: join(ROOT, "skill"),
		dest: join(SKILL_DIR, "workflow"),
	},
];

const SKIP = new Set([
	".git",
	"node_modules",
	".venv",
	"__pycache__",
	".pytest_cache",
	".DS_Store",
	"uv.lock",
]);

async function exists(path) {
	try {
		return await lstat(path);
	} catch (error) {
		if (error.code === "ENOENT") return null;
		throw error;
	}
}

async function sha256(file) {
	const buf = await readFile(file);
	return createHash("sha256").update(buf).digest("hex");
}

async function manifestOf(dir) {
	const sRoot = await exists(dir);
	if (!sRoot || !sRoot.isDirectory()) return null;

	const entries = [];
	const sources = new Map();

	async function visit(currentDir, prefix = "") {
		const items = await readdir(currentDir, { withFileTypes: true });
		items.sort((a, b) => a.name.localeCompare(b.name, "en"));
		for (const item of items) {
			if (SKIP.has(item.name)) continue;
			if (item.name.endsWith(".pyc") || item.name.endsWith(".sock")) continue;

			const rel = prefix ? `${prefix}/${item.name}` : item.name;
			const full = join(currentDir, item.name);
			const st = await lstat(full);

			if (st.isDirectory()) {
				entries.push({ path: rel, type: "dir", mode: st.mode & 0o777 });
				await visit(full, rel);
			} else if (st.isFile()) {
				const hash = await sha256(full);
				entries.push({ path: rel, type: "file", mode: st.mode & 0o777, size: st.size, sha256: hash });
				sources.set(rel, full);
			}
		}
	}

	await visit(dir);
	entries.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
	const aggregate = createHash("sha256").update(JSON.stringify(entries)).digest("hex");
	return { files: sources.size, aggregate, entries, sources };
}

async function copyTree(manifest, dest) {
	await mkdir(dest, { recursive: true, mode: 0o755 });
	for (const entry of manifest.entries) {
		const target = join(dest, entry.path);
		if (entry.type === "dir") {
			await mkdir(target, { recursive: true, mode: 0o755 });
		} else if (entry.type === "file") {
			const source = manifest.sources.get(entry.path);
			await mkdir(dirname(target), { recursive: true, mode: 0o755 });
			await copyFile(source, target);
			await chmod(target, entry.mode);
		}
	}
	// Restore directory modes in reverse order
	for (const entry of manifest.entries.filter((e) => e.type === "dir").reverse()) {
		try {
			await chmod(join(dest, entry.path), entry.mode);
		} catch {
			// ignore
		}
	}
}

function stamp() {
	return new Date().toISOString().replace(/[:.]/g, "-");
}

async function run() {
	const mode = apply ? "apply" : check ? "check" : "dry-run";
	console.log(`[promote] prime-workflow (mode: ${mode}, target: ${destAgentDir})\n`);

	let hasDrift = false;

	for (const item of PLAN) {
		const srcManifest = await manifestOf(item.src);
		if (!srcManifest) {
			console.error(`✖ source directory missing for ${item.name}: ${item.src}`);
			hasDrift = true;
			continue;
		}

		const destManifest = await manifestOf(item.dest);
		const isInstalled = destManifest !== null;
		const isIdentical = isInstalled && destManifest.aggregate === srcManifest.aggregate;

		if (check) {
			if (!isInstalled) {
				console.log(`✖ [NOT INSTALLED] ${item.name} -> ${item.dest}`);
				hasDrift = true;
			} else if (isIdentical) {
				console.log(`✔ [SYNC] ${item.name} is identical (${srcManifest.files} files, sha256: ${srcManifest.aggregate.slice(0, 12)})`);
			} else {
				console.log(`✖ [DRIFT] ${item.name} differs between repo and installed copy`);
				console.log(`    repo:      sha256:${srcManifest.aggregate.slice(0, 12)} (${srcManifest.files} files)`);
				console.log(`    installed: sha256:${destManifest.aggregate.slice(0, 12)} (${destManifest.files} files)`);
				hasDrift = true;
			}
			continue;
		}

		if (!apply) {
			// Dry-run
			console.log(`[DRY-RUN] ${item.name} (${item.type}):`);
			console.log(`  Source: ${item.src} (${srcManifest.files} files, aggregate: ${srcManifest.aggregate.slice(0, 12)})`);
			console.log(`  Target: ${item.dest}`);
			if (isIdentical) {
				console.log(`  Status: Already up to date (no changes required)`);
			} else if (isInstalled) {
				console.log(`  Status: Update required (backup will be created under ${BACKUP_DIR}/)`);
			} else {
				console.log(`  Status: Fresh install required`);
			}
			console.log();
			continue;
		}

		// Apply mode
		console.log(`[APPLY] Promoting ${item.name}...`);
		if (isIdentical) {
			console.log(`  ✔ Already up-to-date (${srcManifest.files} files, sha256: ${srcManifest.aggregate.slice(0, 12)})`);
			continue;
		}

		if (isInstalled) {
			const backupTarget = join(BACKUP_DIR, `${item.name}-${stamp()}`);
			await mkdir(dirname(backupTarget), { recursive: true });
			console.log(`  📦 Backing up existing installation to: ${backupTarget}`);
			await copyTree(destManifest, backupTarget);
			await rm(item.dest, { recursive: true, force: true });
		}

		console.log(`  🚀 Copying ${srcManifest.files} files to ${item.dest}...`);
		await copyTree(srcManifest, item.dest);

		// Verify promotion
		const verified = await manifestOf(item.dest);
		if (!verified || verified.aggregate !== srcManifest.aggregate) {
			throw new Error(`Promotion verification failed for ${item.name}: digest mismatch after copy!`);
		}

		// Write receipt
		await mkdir(RECEIPTS_DIR, { recursive: true });
		const receiptPath = join(RECEIPTS_DIR, `promote.${item.name}.json`);
		const receipt = {
			package: item.name,
			type: item.type,
			promoted_at: new Date().toISOString(),
			aggregate_sha256: srcManifest.aggregate,
			files_count: srcManifest.files,
			target: item.dest,
		};
		await writeFile(receiptPath, JSON.stringify(receipt, null, 2) + "\n", "utf8");
		console.log(`  ✔ Promoted and verified successfully (receipt: ${receiptPath})\n`);
	}

	if (check && hasDrift) {
		process.exit(1);
	}
}

run().catch((err) => {
	console.error(`\n✖ Promotion aborted: ${err.message}`);
	process.exit(1);
});
