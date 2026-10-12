// Load-smoke for lab packages: resolves Prime's host modules for jiti (aliases), then loads
// index.ts under jiti/Node (the real loading condition). Optional argv[2] = sub-entry.
//
// Host resolution is portable and fail-closed — no machine constant anywhere:
//   1. PRIME_AGENT_DIST  the dist root itself (documented; must hold index.js)
//   2. PRIME_AGENT_ROOT  a package root or a Node prefix; all documented layouts are probed:
//        <root>/package.json · <root>/node_modules/prime-agent/package.json ·
//        <root>/lib/node_modules/prime-agent/package.json
//   3. without an explicit root: `prime-agent` on PATH (a path under dist/ is cut at the dist
//      segment; a prefix bin uses lib/node_modules), the running Node's prefix, and this file's
//      own module resolution.
// An explicit root that resolves nowhere is a SKIP (exit 2) and NEVER a fallback to some other
// install: a smoke that silently tested a different host than the one named is worse than none.
import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join, sep } from "node:path";
import { existsSync } from "node:fs";

const here = dirname(fileURLToPath(import.meta.url));

/** package.json anchors named by the explicit environment, in their documented layouts. */
function explicitAnchors() {
	const anchors = [];
	const push = (p) => { if (p && !anchors.includes(p)) anchors.push(p); };
	if (process.env.PRIME_AGENT_DIST) push(join(process.env.PRIME_AGENT_DIST, "..", "package.json"));
	if (process.env.PRIME_AGENT_ROOT) {
		for (const rel of ["package.json", "node_modules/prime-agent/package.json", "lib/node_modules/prime-agent/package.json"]) {
			push(join(process.env.PRIME_AGENT_ROOT, rel));
		}
	}
	return anchors;
}

/** Portable fallbacks — used only when neither PRIME_AGENT_DIST nor PRIME_AGENT_ROOT is set. */
function ambientAnchors() {
	const anchors = [];
	const push = (p) => { if (p && !anchors.includes(p)) anchors.push(p); };
	try {
		const bin = execFileSync("bash", ["-lc", "command -v prime-agent"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
		const marker = `${sep}dist${sep}`;
		const at = bin.indexOf(marker);
		if (at > 0) push(join(bin.slice(0, at), "package.json"));
		else push(join(dirname(bin), "..", "lib", "node_modules", "prime-agent", "package.json"));
	} catch { /* no prime-agent on PATH */ }
	push(join(dirname(dirname(process.execPath)), "lib", "node_modules", "prime-agent", "package.json"));
	try { push(createRequire(import.meta.url).resolve("prime-agent/package.json")); } catch { /* not a dependency */ }
	return anchors;
}

// An explicit DIST that is not a dist is a refusal, not a reason to go looking elsewhere.
if (process.env.PRIME_AGENT_DIST && !existsSync(join(process.env.PRIME_AGENT_DIST, "index.js"))) {
	console.error(`SKIP: PRIME_AGENT_DIST=${process.env.PRIME_AGENT_DIST} holds no index.js; refusing to fall back to another install`);
	process.exit(2);
}
const explicit = explicitAnchors();
const anchors = explicit.length ? explicit : ambientAnchors();
let requireFromPrime = null;
let primePackage = null;
for (const anchor of anchors) {
	try {
		const req = createRequire(anchor);
		req.resolve("jiti");
		requireFromPrime = req;
		primePackage = dirname(anchor);
		break;
	} catch { /* try the next layout */ }
}
if (!requireFromPrime) {
	const hint = explicit.length ? `explicit ${explicit.join(", ")} did not resolve Prime` : "no Prime host on PATH/prefix";
	console.error(`SKIP: could not resolve Prime's jiti (${hint}; set PRIME_AGENT_DIST or PRIME_AGENT_ROOT)`);
	process.exit(2);
}

const dist = process.env.PRIME_AGENT_DIST ?? (primePackage && existsSync(join(primePackage, "dist", "index.js")) ? join(primePackage, "dist") : null);
const alias = {};
if (dist && existsSync(join(dist, "index.js"))) {
	alias["@earendil-works/pi-coding-agent"] = join(dist, "index.js");
	alias["@mariozechner/pi-coding-agent"] = join(dist, "index.js");
	try {
		const pj = createRequire(join(dist, "core", "extensions", "loader.js"));
		for (const n of ["@earendil-works/pi-agent-core", "@earendil-works/pi-ai", "@earendil-works/pi-tui"]) {
			try { alias[n] = pj.resolve(`${n}/dist/index.js`); } catch {}
		}
		for (const n of ["typebox", "typebox/compile", "typebox/value"]) {
			try { alias[n] = pj.resolve(n); } catch {}
		}
	} catch {}
}

const { createJiti } = await import(requireFromPrime.resolve("jiti"));
const jiti = createJiti(import.meta.url, { interopDefault: true, alias });
const entry = process.argv[2] ? join(here, process.argv[2]) : join(here, "index.ts");
const mod = await jiti.import(entry);
const fn = mod?.default ?? mod;
if (typeof fn !== "function") { console.error("FAIL: default export is not a function"); process.exit(1); }
console.log("OK: entry loads under jiti/Node; default export is a function");
