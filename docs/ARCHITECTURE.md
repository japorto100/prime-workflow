# prime-workflow Architecture & Run-Contract Invariants

## 1. Overview & Separation of Concerns

`prime-workflow` provides deterministic, repeatable, and crash-resilient multi-agent orchestration for Prime Agent.

It strictly separates the **State Owner** from the **Execution Dispatcher**:
* **Host Service (`extension/lib/service.ts`):** Owns the program, journal, run state, claim leases, admission bookkeeping, and outcome acceptance. It **never** starts agents autonomously.
* **Kernel Dispatcher (`skill/src/prime_workflow`):** Python recipes running inside Prime Agent's IPython kernel claim tasks, spawn child agents (`rlm.spawn`), collect results, and request state transitions over an idempotent local Unix domain socket bridge (`extension/lib/bridge.ts`).

---

## 2. The Four Persistent Objects

Every object is versioned via a `schema` string and validated **at read time**:

### 2.1 Definition (`prime-lab-workflow/definition/2`)
* Represents the immutable plan of work.
* Stored in project-shared directory `<repo>/.prime/workflows/<workflow_id>/rev-NNNN.json`.
* Key fields:
  * `workflow_id`: Stable identifier.
  * `revision`: Monotonically increasing revision counter.
  * `definition_digest`: SHA-256 over canonicalized steps.
  * `steps`: Array of `StepV2` definitions. Each step carries:
    * `outcome_contract`: `{ kind: "command" }` | `{ kind: "artifact_schema" }` | `{ kind: "findings" }`
    * `parallel_safe`: Explicit concurrency permit (not just dependency-derived).
    * `owns_paths`: Conflict-avoidance path set.
    * `model`: Optional explicit provider/model (or empty to inherit).

### 2.2 Run (`prime-lab-workflow/run/1`)
* Represents an execution instance of a specific definition.
* Key fields:
  * `run_id`: Unique run identifier.
  * `state_version`: Monotonic CAS base (`expected_run_version`).
  * `definition_digest`: Exact hash of the bound definition.
  * `driver_session_id`, `driver_epoch`: Exactly one active coordinator.
  * `limits`: Hard caps (`max_parallel`, `max_children`, `max_repairs`, `max_agents_total`).
  * `status`: Enumerated state (`created`, `running`, `reconciliation_required`, `done`, `failed`, `abandoned`).

### 2.3 Attempt (`prime-lab-workflow/attempt/1`)
* Represents an individual attempt of a single step.
* Deterministic child name: `<run>--<step>--<attempt>`.
* Key fields:
  * `status`: `intended` → `admitted` → `running` → `succeeded` / `failed` / `unresolved` / `orphaned`.
  * `lease_expires_at`: Claim lease duration. While active, **no other attempt may claim this step**.
  * `driver_epoch`: Checked against writer epoch; prevents zombie writes.

### 2.4 Result (`prime-lab-workflow/result/1`)
* Represents a verified outcome artifact.
* Written to an attempt-isolated path: `<artifacts>/<run_id>/<step>--<token>.json`.
* **Frozen Bytes:** The accepted bytes are copied into `runs/<run_id>/results/` and hashed (SHA-256). Any later mutation of workspace files does not invalidate the recorded proof.

---

## 3. The Core Invariants

1. **At most one active Attempt per (run, step):** Enforced via atomic claim-leases and epoch checks. Retries are only permitted after a predecessor reaches a terminal state.
2. **Exactly one Driver per Run:** Transitions are verified against `driver_epoch`. A reconnecting or stale driver cannot confirm old operations.
3. **No acceptance without Identity & Schema:** Results are never parsed from flippant text (`answer_preview`). A result requires a matching attempt token, schema conformance (`additionalProperties: false`), and valid SHA-256 bytes.
4. **Append-Only Journal with Torn-Tail Recovery:** All state transitions append to `<repo>/.prime/workflow-state/journal.jsonl` with `fsync`. If a crash interrupts a write, the recovery mechanism trims the uncommitted tail (`journal.jsonl.torn`) back to the last valid newline before resuming.
5. **Reconciliation distinguishes Liveness from History:** If an active child vanishes without a recorded terminal event, it is marked `unresolved`, and the run transitions to `reconciliation_required`. The engine **never blindly respawns**.
6. **Idempotent Bridge Communication:** All mutating RPC requests carry `operation_id`, `driver_epoch`, and `expected_run_version`. Lost network responses can be safely retried without duplicate actions.

---

## 4. The `audit-readonly` Recipe

A hardened dialectical audit workflow:
```
explore (read-only mapping)
   ├──► verify      (verifies claims against actual code)
   └──► contrarian  (attacks findings, hunts blind spots & missing tests)
          │
          ▼
        judge       (adjudicates final verdict bound to all 3 SHA-256 digests)
```
Unlike simplistic linear pipelines, this forces competing perspectives (soundness vs. adversarial probing) before committing conclusions.
