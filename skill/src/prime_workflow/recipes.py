"""Recipes over the workflow bridge: `audit-readonly` and `standard-parallel`.

A recipe is a *user* of the service, never a hidden engine: it claims and spawns, waits for the
child to settle, freezes the result, and lets the service enforce the run contract. The service
never starts anything; dispatch stays here.

Correction round 24.09.2026 (external review):
- every attempt gets its **own artifact path** (`<artifacts>/<run_id>/<step>--<token>.json`), so a
  file from an earlier run can never complete a fresh child (P03);
- `resume=True` reuses a succeeded step only when the LATEST attempt succeeded, its frozen bytes
  still exist under the state root and hash to the recorded digest, and the run's subject and
  definition are the requested ones (P04/P05);
- accepted content is checked against the step's identity (phase, string lists), not only against
  top-level types (G05);
- `roles` binds registry roles per step (opt-in; default stays freestyle).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable

import prime_workflow as pw

# `spawn` returns an admission, not a result. The recipe therefore waits for the child to settle
# (or for its artifact file to appear) before it looks — measured in a sessionless run on
# 24.09.2026, where an immediate check marked a just-started, still-running child unresolved.
RESULT_WAIT_S = float(os.environ.get("PRIME_WORKFLOW_RECIPE_WAIT_S", "240"))
COLLECT_SLICE_MS = 15_000

# The result contract every phase must satisfy: a JSON object with a statement, findings and a
# coverage list. A phase that cannot fill this in is not done — there is no "file exists" substitute.
PHASE_SCHEMA: dict[str, Any] = {
    "required": ["phase", "summary", "findings", "coverage"],
    "properties": {
        "phase": "string",
        "summary": "string",
        "findings": "array",
        "coverage": "array",
    },
    "additionalProperties": False,
}

PHASES = ("explore", "verify", "contrarian", "judge")

PROMPTS = {
    "explore": (
        "Explore {subject} read-only. Identify the code regions that matter for an audit of it. "
        "Then write EXACTLY ONE JSON object to {artifact_path} with the keys: "
        'phase="explore", summary (string), findings (array of strings), coverage (array of strings). '
        "Write nothing else to that file and do not modify any other file."
    ),
    "verify": (
        "Verify the audit of {subject} read-only against the explore result at {explore}. "
        "Check its claims against the actual code. Then write EXACTLY ONE JSON object to {artifact_path} with the keys: "
        'phase="verify", summary (string), findings (array of strings), coverage (array of strings). '
        "Write nothing else to that file and do not modify any other file."
    ),
    "contrarian": (
        "Attack the explore findings for {subject} read-only (explore result: {explore}). "
        "What is untested, what would break, what did explore miss? Then write EXACTLY ONE JSON object to {artifact_path} with the keys: "
        'phase="contrarian", summary (string), findings (array of strings), coverage (array of strings). '
        "Write nothing else to that file and do not modify any other file."
    ),
    "judge": (
        "Judge the audit of {subject}. Inputs: explore digest {explore_digest}, verify digest {verify_digest}, "
        "contrarian digest {contrarian_digest}. Then write EXACTLY ONE JSON object to {artifact_path} with the keys: "
        'phase="judge", summary (string), findings (array of strings), coverage (array of strings). '
        "Write nothing else to that file and do not modify any other file."
    ),
}

PARALLEL_ANGLE_PROMPT = (
    "Research the following angle of a question, read-only.\nQuestion: {question}\nAngle: {angle}\n"
    'Then write EXACTLY ONE JSON object to {artifact_path} with the keys: phase="{step_id}", '
    "summary (string), findings (array of strings), coverage (array of strings). "
    "Write nothing else to that file and do not modify any other file."
)
PARALLEL_SYNTH_PROMPT = (
    "Synthesize the research on: {question}\nAccepted inputs: {refs}\n"
    'Then write EXACTLY ONE JSON object to {artifact_path} with the keys: phase="synthesis", '
    "summary (string), findings (array of strings), coverage (array of strings). "
    "Write nothing else to that file and do not modify any other file."
)

ACTIVE_ATTEMPT_STATES = {"intended", "admitted", "running", "unresolved"}


def definition_digest(steps: list[dict[str, Any]]) -> str:
    """Canonical digest over the step specs — the same rule the host uses (sorted keys)."""
    return hashlib.sha256(json.dumps(steps, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _default_spawner() -> Callable[..., Awaitable[dict[str, Any]]]:
    async def spawn(run_id: str, step_id: str, prompt: str, *, artifact_path: str | None = None, **kwargs: Any) -> dict[str, Any]:
        # `artifact_path` travels inside the prompt; the real spawner has no use for the kwarg.
        return await pw.spawn(run_id, step_id, prompt, **kwargs)

    return spawn


def _artifact_path(artifacts: Path, run_id: str, step_id: str) -> Path:
    """Unique per run and attempt: an old file can never complete a fresh child (P03)."""
    path = artifacts / run_id / f"{step_id}--{uuid.uuid4().hex[:8]}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _step_state(run: dict[str, Any], step_id: str) -> str:
    """`succeeded` | `active` | `failed` | `none` — from the LATEST attempt (P05)."""
    attempts = [run.get("attempts", {}).get(aid) for aid in run.get("steps", {}).get(step_id, [])]
    attempts = [a for a in attempts if a]
    if not attempts:
        return "none"
    if any(a.get("status") in ACTIVE_ATTEMPT_STATES for a in attempts):
        return "active"
    return "succeeded" if attempts[-1].get("status") == "succeeded" else "failed"


def _frozen_ok(result_ref: str, digest: str | None) -> bool:
    """The frozen bytes still exist and hash to the recorded digest (P04)."""
    if not digest:
        return False
    try:
        data = (pw.state_root() / result_ref).read_bytes()
    except OSError:
        return False
    return hashlib.sha256(data).hexdigest() == digest


def _reuse(run: dict[str, Any], step_id: str) -> dict[str, Any] | None:
    """Reusable only when the LATEST attempt succeeded and its frozen result verifies (P05)."""
    attempts = [run.get("attempts", {}).get(aid) for aid in run.get("steps", {}).get(step_id, [])]
    attempts = [a for a in attempts if a]
    if not attempts:
        return None
    latest = attempts[-1]
    if latest.get("status") != "succeeded" or not latest.get("result_ref"):
        return None
    if not _frozen_ok(latest["result_ref"], latest.get("result_digest")):
        return None
    return {
        "result_ref": latest["result_ref"],
        "digest": latest.get("result_digest"),
        "origin": latest.get("result_origin"),
        "attempt_id": latest["attempt_id"],
        "reused": True,
    }


async def _prepare(run_id: str, step_id: str, resume: bool) -> dict[str, Any] | None:
    """On resume: a reusable result for a succeeded step, or a hard stop. Never a silent restart."""
    if not resume:
        return None
    run = await pw.run_status(run_id)
    state = _step_state(run, step_id)
    if state == "succeeded":
        reused = _reuse(run, step_id)
        if reused is None:
            raise pw.WorkflowError(
                f"step {step_id} is marked succeeded but its frozen result is missing or no longer matches; refusing to reuse it",
            )
        return reused
    if state == "active":
        raise pw.WorkflowError(
            f"step {step_id} has an active attempt; write it off explicitly (prime_workflow.abandon or recipes.abandon_blocked) before resuming",
        )
    return None


def _content_error(path: Path, step_id: str) -> str | None:
    """Identity check beyond top-level types (G05): right phase, findings/coverage as string lists."""
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return f"not readable JSON: {error}"
    if not isinstance(parsed, dict):
        return "the result must be a JSON object"
    if parsed.get("phase") != step_id:
        return f'phase is {parsed.get("phase")!r}, expected {step_id!r}'
    for key in ("findings", "coverage"):
        value = parsed.get(key)
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            return f"{key} must be a list of strings"
    if not isinstance(parsed.get("summary"), str):
        return "summary must be a string"
    return None


async def _wait_for_artifact(run_id: str, step_id: str, attempt_id: str, result_path: Path, spawned: dict[str, Any]) -> dict[str, Any]:
    """Wait, then freeze. Never treats an existing unrelated file as this child's result."""
    handle = spawned.get("handle")
    settled = False
    if handle is not None:
        deadline = time.monotonic() + RESULT_WAIT_S
        while not result_path.exists() and time.monotonic() < deadline:
            snapshots = await pw.collect([handle], timeout_ms=COLLECT_SLICE_MS) or []
            if snapshots and all(bool(getattr(s, "settled", False)) for s in snapshots):
                settled = True
                break
        if not result_path.exists():
            # A file can land between the last collect slice and this label (observed 24.09.2026);
            # one short grace saves an `unresolved` that a re-run would have to undo.
            await asyncio.sleep(1.0)
    if not result_path.exists():
        if settled:
            # The child is DETERMINATELY done and produced no artifact (measured: a child whose
            # request failed settles immediately). Terminal `failed` keeps the step retryable;
            # `unresolved` would block every retry forever.
            await pw.settle(run_id, attempt_id, "failed", note=f"child settled without writing {result_path}")
            return {"status": "failed", "result": None, "attempt_id": attempt_id}
        await pw.unresolved(run_id, attempt_id, f"no result file at {result_path} after waiting")
        return {"status": "unresolved", "result": None, "attempt_id": attempt_id}
    problem = _content_error(result_path, step_id)
    if problem:
        await pw.settle(run_id, attempt_id, "failed", note=f"result content rejected: {problem}")
        return {"status": "failed", "result": None, "attempt_id": attempt_id, "content_error": problem}
    outcome = await pw.accept_result(run_id, attempt_id, result_path, schema=PHASE_SCHEMA)
    await pw.settle(run_id, attempt_id, "succeeded", note=f"result {outcome['result_ref']}")
    return {"status": "succeeded", "result": {**outcome, "source_path": str(result_path)}, "attempt_id": attempt_id}


async def abandon_blocked(run_id: str, *, reason: str) -> list[dict[str, Any]]:
    """Write off every blocked (active) attempt of a run, each with the same stated reason.

    Covers `unresolved` and the crash leftovers `intended`/`admitted`/`running`: all four block a
    resume, and a caller that knows the children died (a crashed session, a killed parent) says so
    here — then a retry becomes legal. Reconciliation deliberately never does this by itself.
    """
    run = await pw.run_status(run_id)
    written_off: list[dict[str, Any]] = []
    for attempt in run.get("attempts", {}).values():
        if attempt.get("status") in ACTIVE_ATTEMPT_STATES:
            await pw.abandon(run_id, attempt["attempt_id"], reason=reason)
            written_off.append({"attempt_id": attempt["attempt_id"], "step_id": attempt.get("step_id")})
    return written_off


async def _ensure_run(
    *,
    run_id: str,
    resume: bool,
    steps: list[dict[str, Any]],
    specs: list[dict[str, Any]],
    subject: dict[str, Any],
) -> None:
    """Create, or continue the SAME work: subject and definition must still match (P04)."""
    digest = definition_digest(steps)
    if resume:
        try:
            run = await pw.run_status(run_id)
        except pw.WorkflowError:
            try:
                await pw.run_create(run_id, digest, subject=subject, steps=specs)
                return
            except pw.WorkflowError as error:
                # A create that races another creator (or replaces an interrupted one) answers
                # "already exists": read the run and validate it like a normal resume instead of
                # failing the whole recipe (QA round).
                if "already exists" not in str(error):
                    raise
                run = await pw.run_status(run_id)
        if (run.get("subject") or {}) != subject:
            raise pw.WorkflowError(
                f"run {run_id} belongs to a different subject: {run.get('subject')!r} != {subject!r}; refusing to resume",
            )
        if run.get("definition_digest") != digest:
            raise pw.WorkflowError(f"run {run_id} was created from a different definition; refusing to resume")
        return
    await pw.run_create(run_id, digest, subject=subject, steps=specs)


def _role_kwargs(roles: dict[str, str] | None, step_id: str) -> dict[str, Any]:
    role = (roles or {}).get(step_id)
    return {} if role is None else {"role": role}


async def run_audit_readonly(
    *,
    run_id: str,
    subject: str,
    artifacts_dir: str | Path,
    spawner: Callable[..., Awaitable[dict[str, Any]]] | None = None,
    resume: bool = False,
    roles: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run the four phases and return the accepted artifacts by phase.

    `roles` maps step ids to registry roles (e.g. `{"verify": "sota-verify"}`); without it every
    phase is freestyle `rlm.spawn`. Note: `verify` and `contrarian` run sequentially here —
    `run_standard_parallel` is the recipe that fans out concurrently.
    """
    spawn = spawner or _default_spawner()
    artifacts = Path(artifacts_dir)
    artifacts.mkdir(parents=True, exist_ok=True)

    steps = [{"id": phase, "prompt": PROMPTS[phase]} for phase in PHASES]
    specs = [
        {"id": "explore"},
        {"id": "verify", "depends_on": ["explore"]},
        {"id": "contrarian", "depends_on": ["explore"]},
        {"id": "judge", "depends_on": ["verify", "contrarian"]},
    ]
    await _ensure_run(run_id=run_id, resume=resume, steps=steps, specs=specs, subject={"project": subject})

    accepted: dict[str, dict[str, Any]] = {}

    async def phase(step_id: str, build_prompt: Callable[[Path], str], depends_on: list[str]) -> dict[str, Any]:
        reused = await _prepare(run_id, step_id, resume)
        if reused is not None:
            accepted[step_id] = {"result_ref": reused["result_ref"], "digest": reused["digest"]}
            return {"phase": step_id, "status": "succeeded", "result": reused, "attempt_id": reused["attempt_id"], "reused": True, "depends_on": depends_on}
        path = _artifact_path(artifacts, run_id, step_id)
        spawned = await spawn(run_id, step_id, build_prompt(path), artifact_path=str(path), **_role_kwargs(roles, step_id))
        attempt_id = str(spawned["attempt_id"])
        waited = await _wait_for_artifact(run_id, step_id, attempt_id, path, spawned)
        if waited["status"] == "succeeded":
            accepted[step_id] = waited["result"]
        return {"phase": step_id, "status": waited["status"], "result": waited["result"], "attempt_id": attempt_id, "depends_on": depends_on, **({} if "content_error" not in waited else {"content_error": waited["content_error"]})}

    explore = await phase("explore", lambda p: PROMPTS["explore"].format(subject=subject, artifact_path=p), [])
    if explore["status"] != "succeeded":
        # No explore result, no verification: a missing phase stays missing, it is never filled in
        # by running the next one against nothing.
        return {
            "run_id": run_id,
            "definition_digest": definition_digest(steps),
            "phases": [explore],
            "accepted": accepted,
            "status": (await pw.run_status(run_id))["status"],
        }
    explore_ref = accepted["explore"]["result_ref"]
    explore_digest = accepted["explore"]["digest"]

    verify = await phase("verify", lambda p: PROMPTS["verify"].format(subject=subject, explore=explore_ref, artifact_path=p), ["explore"])
    contrarian = await phase("contrarian", lambda p: PROMPTS["contrarian"].format(subject=subject, explore=explore_ref, artifact_path=p), ["explore"])
    if verify["status"] != "succeeded" or contrarian["status"] != "succeeded":
        return {
            "run_id": run_id,
            "definition_digest": definition_digest(steps),
            "phases": [explore, verify, contrarian],
            "accepted": accepted,
            "status": (await pw.run_status(run_id))["status"],
        }

    judge = await phase(
        "judge",
        lambda p: PROMPTS["judge"].format(
            subject=subject,
            artifact_path=p,
            explore_digest=explore_digest,
            verify_digest=accepted["verify"]["digest"],
            contrarian_digest=accepted["contrarian"]["digest"],
        ),
        ["verify", "contrarian"],
    )

    # All four succeeded: close the run. `finish` refuses `done` while any step is open, so this
    # cannot turn a partial run green. A resumed run may already be closed — then there is nothing
    # to do.
    if judge["status"] == "succeeded" and (await pw.run_status(run_id))["status"] == "running":
        await pw.finish(run_id)
    return {
        "run_id": run_id,
        "definition_digest": definition_digest(steps),
        "phases": [explore, verify, contrarian, judge],
        "accepted": accepted,
        "status": (await pw.run_status(run_id))["status"],
    }


async def run_standard_parallel(
    *,
    run_id: str,
    question: str,
    angles: list[str],
    artifacts_dir: str | Path,
    spawner: Callable[..., Awaitable[dict[str, Any]]] | None = None,
    resume: bool = False,
    parallel: bool = True,
    roles: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Fan out one research child per angle (spawned together), then synthesize.

    `parallel=False` runs the same angles strictly one after another — the same recipe, the same
    steps, so a latency/coverage comparison between the two is an A/B on the fan-out alone.
    """
    if not angles:
        raise pw.WorkflowError("standard-parallel needs at least one angle")
    spawn = spawner or _default_spawner()
    artifacts = Path(artifacts_dir)
    artifacts.mkdir(parents=True, exist_ok=True)

    angle_ids = [f"angle-{index + 1}" for index in range(len(angles))]
    # Prompts are part of the definition digest, so a different question or different angles can
    # never share this run's supposed basis (review WF-R04/R05).
    steps = [{"id": aid, "prompt": angle} for aid, angle in zip(angle_ids, angles)] + [{"id": "synthesis", "prompt": question}]
    specs = [{"id": aid} for aid in angle_ids] + [{"id": "synthesis", "depends_on": angle_ids}]
    subject = {"question": question, "angles": list(angles)}
    await _ensure_run(run_id=run_id, resume=resume, steps=steps, specs=specs, subject=subject)

    accepted: dict[str, dict[str, Any]] = {}

    async def phase(step_id: str, build_prompt: Callable[[Path], str], depends_on: list[str]) -> dict[str, Any]:
        reused = await _prepare(run_id, step_id, resume)
        if reused is not None:
            accepted[step_id] = {"result_ref": reused["result_ref"], "digest": reused["digest"]}
            return {"phase": step_id, "status": "succeeded", "result": reused, "attempt_id": reused["attempt_id"], "reused": True, "depends_on": depends_on}
        path = _artifact_path(artifacts, run_id, step_id)
        spawned = await spawn(run_id, step_id, build_prompt(path), artifact_path=str(path), **_role_kwargs(roles, step_id))
        attempt_id = str(spawned["attempt_id"])
        waited = await _wait_for_artifact(run_id, step_id, attempt_id, path, spawned)
        if waited["status"] == "succeeded":
            accepted[step_id] = waited["result"]
        return {"phase": step_id, "status": waited["status"], "result": waited["result"], "attempt_id": attempt_id, "depends_on": depends_on, **({} if "content_error" not in waited else {"content_error": waited["content_error"]})}

    if parallel:
        # Claim and spawn ALL angles before waiting for any — the fan-out is the point.
        jobs: list[tuple[str, Path]] = [(step_id, _artifact_path(artifacts, run_id, step_id)) for step_id in angle_ids]
        started: list[tuple[str, dict[str, Any] | None, Path]] = []
        for (step_id, path), angle in zip(jobs, angles):
            reused = await _prepare(run_id, step_id, resume)
            if reused is not None:
                accepted[step_id] = {"result_ref": reused["result_ref"], "digest": reused["digest"]}
                started.append((step_id, None, path))
                continue
            prompt = PARALLEL_ANGLE_PROMPT.format(question=question, angle=angle, artifact_path=path, step_id=step_id)
            spawned = await spawn(run_id, step_id, prompt, artifact_path=str(path), **_role_kwargs(roles, step_id))
            started.append((step_id, spawned, path))
        angle_results: list[dict[str, Any]] = []
        for step_id, spawned, path in started:
            if spawned is None:
                angle_results.append({"phase": step_id, "status": "succeeded", "result": {**accepted[step_id], "reused": True}, "reused": True})
                continue
            attempt_id = str(spawned["attempt_id"])
            waited = await _wait_for_artifact(run_id, step_id, attempt_id, path, spawned)
            if waited["status"] == "succeeded":
                accepted[step_id] = waited["result"]
            angle_results.append({"phase": step_id, "status": waited["status"], "result": waited["result"], "attempt_id": attempt_id})
    else:
        angle_results = []
        for step_id, angle in zip(angle_ids, angles):
            angle_results.append(
                await phase(step_id, lambda p, a=angle, s=step_id: PARALLEL_ANGLE_PROMPT.format(question=question, angle=a, artifact_path=p, step_id=s), []),
            )

    if any(result["status"] != "succeeded" for result in angle_results):
        return {
            "run_id": run_id,
            "definition_digest": definition_digest(steps),
            "phases": angle_results,
            "accepted": accepted,
            "status": (await pw.run_status(run_id))["status"],
        }

    refs = ", ".join(f"{step_id} {accepted[step_id]['digest']}" for step_id in angle_ids)
    synthesis = await phase("synthesis", lambda p: PARALLEL_SYNTH_PROMPT.format(question=question, refs=refs, artifact_path=p), angle_ids)
    if synthesis["status"] == "succeeded" and (await pw.run_status(run_id))["status"] == "running":
        await pw.finish(run_id)
    phases = [*angle_results, synthesis]
    return {
        "run_id": run_id,
        "definition_digest": definition_digest(steps),
        "phases": phases,
        "accepted": accepted,
        "status": (await pw.run_status(run_id))["status"],
    }
