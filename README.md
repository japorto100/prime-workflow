# prime-workflow

> **Hardened Multi-Agent Orchestration, State-Machine Workflows, and Outcome Contracts for Prime Agent**

[![Tests](https://img.shields.io/badge/tests-186%20passing-brightgreen)](#testing)
[![Version](https://img.shields.io/badge/version-v0.1.0--beta.1%20(preview)-blue)](#status)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20macOS%20(POSIX%20only)-success)](#system-requirements)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Target](https://img.shields.io/badge/Primary%20Target-Prime%20Agent%20v0.9.8-orange)](#modes-of-operation)

> [!WARNING]
> **Status: Beta (v0.1.0-beta.1) — Early Community Preview & Architecture Blueprint**
> * **Noch ungetestet im Produktiveinsatz:** Diese Version befindet sich im frühen Beta-Stadium. Zwar sind die Unit- und Integrationsverträge mit 152 TypeScript- und 34 Python-Tests abgedeckt, der **reale Praxiseinsatz in ausgedehnten Multi-Agenten-Sessions ist jedoch noch nicht durchgetestet** — die Erprobung und Weiterentwicklung laufen aktiv.
> * **Betriebssystem: Nur Linux & macOS (Kein Windows!):** Das System setzt zwingend POSIX-Dateiberechtigungen (`0o600`) und Unix Domain Sockets (`bridge.sock`) voraus. **Windows wird nicht unterstützt.**
> * **Primärer Fokus auf v0.9.8:** Dieses Repository richtet sich primär an Anwender von **Prime Agent v0.9.8**, die eine verlässliche In-Process Workflow- und State-Machine-Erweiterung suchen.
> * **Methoden-Blueprint für v0.10+ (Kein Node.js für Rust):** Wir empfehlen *keineswegs*, Node.js in das native Rust-Ökosystem von Prime v0.10 zurückzubringen. Stattdessen dienen die hier gezeigten **Methoden und Techniken** (Append-Only Journal mit Torn-Tail Recovery, atomare Claim-Leases, `driver_epoch`-Fencing und kryptografisch gefrorene SHA-256-Bytes) als architektonischer Blueprint für eine eventuelle spätere Reimplementierung in nativem Rust.
> * **Community-Sharing:** Wir teilen diese Basis offen mit der Community ([#3376](https://github.com/PrimeIntellect-ai/prime-agent/discussions/3376), [#3200](https://github.com/PrimeIntellect-ai/prime-agent/discussions/3200)). Feedback, Issue-Meldungen und PRs sind herzlich willkommen!

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

## Architectural Complement: prime-workflow & Prime Agent v0.10 Factory

With Prime Agent v0.10.0, upstream introduced the "Factory" ([PR #3199](https://github.com/PrimeIntellect-ai/prime-agent/pull/3199), [#3285](https://github.com/PrimeIntellect-ai/prime-agent/pull/3285)), offering DAG state machines and a TUI live page. `prime-workflow` focuses specifically on crash-resilient storage, concurrency fencing, and formal contract immutability. It is designed to serve both as a backwards-compatible extension for v0.9.8 and as a reference blueprint for how these resilience invariants can be natively integrated into Prime Agent's Rust core:

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

### Mode 2: Conceptual Reference Prototype (Prime Agent v0.10+ & Testing)
Since Prime Agent v0.10 removed in-process TypeScript extension loading ([PR #3189](https://github.com/PrimeIntellect-ai/prime-agent/pull/3189)), `prime-workflow` provides an out-of-process prototype runner:
```bash
# Start the prototype bridge server in your project root:
node extension/bin/workflow-server.mjs
```
*Note: This is provided as a developer reference prototype for experimenting with the state invariants on v0.10, not as a recommendation to permanently introduce Node runtimes into Prime's Rust core.*

---

## System Requirements & Limitations

* **Operating System:** **Linux or macOS (POSIX only)**.
  > [!CAUTION]
  > **Windows is NOT supported.** The communication layer relies on POSIX Unix Domain Sockets (`sockaddr_un`) and strict filesystem permissions (`0o600`).
* **Node.js:** Node.js `>= 22.6.0` (required for native ESM type-stripping).
* **Python:** Python `>= 3.12` (managed via `uv`).
* **Path Length:** Standard POSIX socket paths are limited to 104–108 characters. Ensure the project root path is not excessively nested.

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

## Installation & Deployment (Promote into Prime Agent)

The repository includes a safe, atomic promotion script that installs both the extension and skill into your local Prime Agent environment:

```bash
# 1. Dry-run: inspect what would be installed/updated without making changes
npm run promote:dry-run

# 2. Apply: backup existing copies to ~/.prime/agent/backups/ and install
npm run promote

# 3. Check: verify that your live installed copy is synchronized with the repository
npm run promote:check
```

The script automatically sets proper POSIX permissions, maintains SHA-256 integrity, and generates audit receipts under `~/.prime/agent/receipts/`.

---

## License

[MIT](LICENSE) © 2026 lipfi
