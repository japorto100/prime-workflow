# prime-workflow

> **Hardened Multi-Agent Orchestration, State-Machine Workflows, and Outcome Contracts for Prime Agent**

[![Tests](https://img.shields.io/badge/tests-186%20passing-brightgreen)](#testing)
[![Version](https://img.shields.io/badge/version-v0.1.0--beta.1-blue)](#status)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Compatible: Prime Agent v0.9.8 & v0.10+](https://img.shields.io/badge/Prime%20Agent-v0.9.8%20%7C%20v0.10%2B-orange)](#modes-of-operation)

> [!WARNING]
> **Status: Beta (v0.1.0-beta.1) — Architecture Blueprint & Community Staging Preview**
> This repository is a preview release providing both backwards-compatibility for Prime Agent v0.9.8 (via in-process TS extension) and an architectural reference/sidecar blueprint for v0.10+ (Rust transition, addressing discussions [#3376](https://github.com/PrimeIntellect-ai/prime-agent/discussions/3376) and [#3200](https://github.com/PrimeIntellect-ai/prime-agent/discussions/3200)).
> While core contracts have 152 TypeScript tests and 34 Python tests passing, broad multi-environment production soak testing is ongoing. Use with care in production setups.

---

## What is prime-workflow?

`prime-workflow` is a production-grade orchestration and state-machine layer for [Prime Agent](https://github.com/PrimeIntellect-ai/prime-agent).

Instead of ad-hoc, uncoordinated subagent spawning (`rlm.spawn`) that suffers from zombie processes, duplicate executions, and unverified outputs, `prime-workflow` provides:
* **Strict State Ownership:** An append-only journal (`journal.jsonl`) with atomic `fsync` and automatic **Torn-Tail Recovery**.
* **Outcome Contracts:** Verifiable acceptance checks (`command`, `artifact_schema`, `findings`) instead of fragile string parsing (`answer_preview`).
* **Concurrency & Claim Leases:** Epoch gates (`driver_epoch`) and atomic claim-leases prevent duplicate spawns, even during network stalls or reconnects.
* **Frozen Result Bytes:** Accepted artifacts are cryptographically bound (SHA-256) and copied to `runs/<run_id>/results/`, guaranteeing immutability.
* **Dialectical Recipes:** Built-in recipes like `audit-readonly` pitting `verify` against `contrarian` before a `judge` adjudicates.

---

## Comparison: prime-workflow vs. Prime Agent v0.10 Factory

With Prime Agent v0.10.0, upstream introduced the "Factory" ([PR #3199](https://github.com/PrimeIntellect-ai/prime-agent/pull/3199), [#3285](https://github.com/PrimeIntellect-ai/prime-agent/pull/3285)). While Factory introduces helpful DAG syntactic sugar and TUI visual graphs, `prime-workflow` provides the **industrial hardening** that production teams require:

| Feature | Upstream Factory (v0.10) | prime-workflow |
|---|---|---|
| **Workflow Definition** | `MACHINE.md` (declarative states) | Typisierte `Definition` (monotone Revisions & Digest) |
| **Output Verification** | Port types (`"type": "json"`) | **Strict `outcome_contract` + Schema validation** (`additionalProperties: false`) |
| **Duplicate Spawn Prevention** | In-kernel task queue | **Atomic Claim-Leases & `driver_epoch` gates** |
| **Crash & Interruption Safety** | Standard in-memory / basic ledger | **Append-only journal with fsync & Torn-Tail Recovery** |
| **Artifact Immutability** | Workspace file references | **Cryptographically frozen bytes (SHA-256 copy)** |
| **Review Strategy** | Linear `review-sweep` | **Dialectical `audit-readonly` (Explore → Verify ∥ Contrarian → Judge)** |
| **Runtime Independence** | Bound to v0.10 Rust daemon | **Runs natively on v0.9.8 or standalone on v0.10+** |

Detailed architectural analysis: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Modes of Operation

`prime-workflow` is designed as a dual-mode package:

### Mode 1: Native In-Process Extension (Prime Agent v0.9.8)
Place `extension/` into your Prime Agent extension directory (e.g. `~/.prime/agent/extensions/workflow` or project `.prime/agent/extensions/workflow`):
```bash
# Enable in settings.json or via environment:
export PRIME_WORKFLOW=on
```
The extension automatically spins up the bridge socket on session start (depth 0).

### Mode 2: Out-of-Process Sidecar Service (Prime Agent v0.10+ & Headless CI)
Since Prime Agent v0.10 removed in-process TypeScript extension loading ([PR #3189](https://github.com/PrimeIntellect-ai/prime-agent/pull/3189)), `prime-workflow` includes a standalone server runner:
```bash
# Start the bridge server in your project root:
node extension/bin/workflow-server.mjs
```
The server binds to `<repo>/.prime/workflow-state/bridge.sock`. The Python skill (`skill/`) inside Prime Agent v0.10's kernel connects directly to this socket!

---

## Quickstart (Python Skill)

Install or copy `skill/` into your agent skills path (`~/.prime/agent/skills/workflow`):

```python
import workflow
from prime_workflow import recipes

# Run the dialectical code audit recipe:
result = await recipes.run_audit_readonly(
    subject="crates/pa-core",
    timeout_s=300
)

print("Audit Status:", result["status"])
print("Judge Verdict:", result["judge_summary"])
```

### The `audit-readonly` Flow:
```
explore (read-only mapping)
   ├──► verify      (verifies claims against actual code)
   └──► contrarian  (attacks findings, hunts blind spots & missing tests)
          │
          ▼
        judge       (adjudicates final verdict bound to all 3 SHA-256 digests)
```

---

## Directory Structure

```
prime-workflow/
├── extension/             # Host service & Unix domain socket bridge (TypeScript)
│   ├── bin/               # Standalone workflow-server.mjs (for v0.10 sidecar mode)
│   ├── lib/               # service.ts (Journal/State), bridge.ts (Socket), reconcile.ts
│   ├── tests/             # 9 comprehensive regression test suites
│   ├── index.ts           # v0.9.8 extension entry point
│   └── package.json
├── skill/                 # Python client skill (runs inside IPython kernel)
│   ├── pyproject.toml
│   ├── SKILL.md
│   ├── src/prime_workflow/
│   │   ├── recipes.py     # audit-readonly and standard-parallel recipes
│   │   └── invocations.py
│   └── tests/             # Client pytest test suites
└── docs/                  # Architecture invariants & contract specs
    └── ARCHITECTURE.md
```

---

## Testing & Verification

The test suites enforce all run-contract invariants, torn-tail recovery, claim leases, and bridge idempotency:

```bash
# Run extension tests (Node 22/24):
npm test

# Run Python skill tests:
npm run test:python

# Check TypeScript types:
npm run typecheck
```

---

## License

[MIT](LICENSE) © 2026 lipfi
