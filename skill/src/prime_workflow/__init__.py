"""prime_workflow — the Python side of the workflow bridge.

RLM owns orchestration; this module owns the durable execution contract around it. You spawn with
`await prime_workflow.spawn(...)`: it books the attempt with the host service, runs `rlm.spawn`,
and books the admission — so a crash between the two is visible instead of invisible. The service
never starts anything; it records and gates.

Discovery is deterministic: the host serves `<state_root>/bridge.sock`, where the state root is
`$PRIME_WORKFLOW_STATE/workflow-state` or `<git-toplevel>/.prime/workflow-state`. Every mutating
request carries a fresh `operation_id`; a lost response can be retried without applying twice.

Only the standard library is used, so a plain 3.12 venv can run this module.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

PROTOCOL = "prime-workflow/bridge/1"
SOCKET_NAME = "bridge.sock"
# The bridge caps a reply at 1 MiB; the reader default (64 KiB) would truncate a valid one (P06).
RESPONSE_LIMIT = 4 * 1024 * 1024
BRIDGE_DEADLINE_SECONDS = 10
MUTATING = {"run.create", "attempt.claim", "attempt.admit", "attempt.settle", "attempt.unresolved", "result.accept", "reconcile.run", "steps.expand", "run.finish"}


class WorkflowError(RuntimeError):
    """The host service refused the request (state, epoch, version, or protocol)."""

    def __init__(self, message: str, *, code: str | None = None):
        super().__init__(message)
        self.code = code


def _git_toplevel(cwd: Path) -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=True,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def state_root(env: Mapping[str, str] | None = None, cwd: str | Path | None = None) -> Path:
    """Where the host keeps journal, runs and the socket — the same rule the extension uses."""
    env = os.environ if env is None else env
    override = env.get("PRIME_WORKFLOW_STATE")
    if override:
        return Path(override) / "workflow-state"
    top = _git_toplevel(Path(cwd or os.getcwd()))
    if not top:
        raise WorkflowError("no git repository here and PRIME_WORKFLOW_STATE is unset")
    return Path(top) / ".prime" / "workflow-state"


def socket_path(env: Mapping[str, str] | None = None, cwd: str | Path | None = None) -> Path:
    return state_root(env, cwd) / SOCKET_NAME


_epoch: str | None = None
_epoch_socket: Path | None = None


def _clear_epoch() -> None:
    global _epoch, _epoch_socket
    _epoch = None
    _epoch_socket = None


async def _exchange(operation: str, payload: dict[str, Any] | None, envelope: dict[str, Any]) -> Any:
    request = {"v": PROTOCOL, "operation": operation, "payload": payload or {}, **envelope}
    target = socket_path()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(str(target), limit=RESPONSE_LIMIT), BRIDGE_DEADLINE_SECONDS)
    except (OSError, TimeoutError) as error:
        raise WorkflowError(f"cannot reach the workflow bridge at {target}: {error}", code="TRANSPORT_UNKNOWN") from error
    try:
        writer.write((json.dumps(request) + "\n").encode("utf-8"))
        await asyncio.wait_for(writer.drain(), BRIDGE_DEADLINE_SECONDS)
        line = await asyncio.wait_for(reader.readline(), BRIDGE_DEADLINE_SECONDS)
    except (OSError, ValueError, TimeoutError) as error:
        # A reset, a torn reply, or a reply beyond RESPONSE_LIMIT must surface as the module's own
        # error class, not as ConnectionResetError/ValueError (review round 2).
        raise WorkflowError(f"the workflow bridge connection failed: {error}", code="TRANSPORT_UNKNOWN") from error
    finally:
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), BRIDGE_DEADLINE_SECONDS)
        except (OSError, TimeoutError):
            pass
    if not line:
        raise WorkflowError("the workflow bridge closed the connection without a reply", code="TRANSPORT_UNKNOWN")
    if not line.endswith(b"\n"):
        raise WorkflowError("the workflow bridge returned a partial reply", code="TRANSPORT_UNKNOWN")
    try:
        reply = json.loads(line.decode("utf-8"))
    except ValueError as error:
        raise WorkflowError(f"the workflow bridge replied with invalid JSON: {error}", code="TRANSPORT_UNKNOWN") from error
    if not isinstance(reply, dict):
        raise WorkflowError(f"the workflow bridge replied with {type(reply).__name__}, not an object")
    if reply.get("v") != PROTOCOL:
        raise WorkflowError("the workflow bridge replied with a different protocol version")
    if envelope.get("operation_id") and reply.get("ok") and reply.get("operation_id") != envelope["operation_id"]:
        raise WorkflowError("the workflow bridge replied for a different operation", code="TRANSPORT_UNKNOWN")
    if not reply.get("ok"):
        err = reply.get("error") or {}
        if not isinstance(err, dict):
            raise WorkflowError("the workflow bridge returned a malformed error", code="TRANSPORT_UNKNOWN")
        if err.get("code") == "STALE_DRIVER_EPOCH" and reply.get("operation_id") != envelope.get("operation_id"):
            raise WorkflowError("unbound stale epoch refusal; reconcile before retry", code="TRANSPORT_UNKNOWN")
        raise WorkflowError(f"{err.get('class', 'Error')}: {err.get('message', 'unknown bridge failure')}", code=err.get("code"))
    return reply.get("result")


async def call(
    operation: str,
    payload: dict[str, Any] | None = None,
    *,
    expected_run_version: int | None = None,
    operation_id: str | None = None,
) -> Any:
    """One request; mutating operations get an id, the cached epoch, and one stale-epoch retry."""
    global _epoch, _epoch_socket
    mutating = operation in MUTATING
    target = socket_path()
    if _epoch_socket != target:
        _clear_epoch()
    if mutating and _epoch is None:
        pong = await _exchange("ping", {}, {})
        if not isinstance(pong, dict) or "epoch" not in pong:
            raise WorkflowError(f"ping replied without an epoch: {pong!r}")
        _epoch = str(pong["epoch"])
        _epoch_socket = target
    envelope: dict[str, Any] = {}
    if mutating:
        envelope = {
            "operation_id": operation_id or uuid.uuid4().hex,
            "driver_epoch": _epoch,
            **({} if expected_run_version is None else {"expected_run_version": expected_run_version}),
        }
    try:
        return await _exchange(operation, payload, envelope)
    except WorkflowError as error:
        # The host may have restarted with a new writer epoch; refresh once, then give up loudly.
        if mutating and error.code == "STALE_DRIVER_EPOCH":
            _clear_epoch()
            pong = await _exchange("ping", {}, {})
            if not isinstance(pong, dict) or "epoch" not in pong:
                raise WorkflowError(f"ping replied without an epoch: {pong!r}") from error
            _epoch = str(pong["epoch"])
            _epoch_socket = target
            envelope["driver_epoch"] = _epoch
            return await _exchange(operation, payload, envelope)
        raise


async def ping() -> dict[str, Any]:
    global _epoch, _epoch_socket
    result = await _exchange("ping", {}, {})
    if not isinstance(result, dict) or "epoch" not in result:
        raise WorkflowError(f"ping replied without an epoch: {result!r}")
    _epoch = str(result["epoch"])
    _epoch_socket = socket_path()
    return result


async def run_create(
    run_id: str,
    definition_digest: str,
    *,
    subject: dict[str, Any] | None = None,
    limits: dict[str, Any] | None = None,
    steps: list[dict[str, Any]] | None = None,
    operation_id: str | None = None,
) -> dict[str, Any]:
    """`steps` is the persisted program (`[{id, depends_on?}]`); without it a run has no coverage
    to check, and `finish` can only fall back to "every attempt succeeded"."""
    return await call(
        "run.create",
        {
            "run_id": run_id,
            "definition_digest": definition_digest,
            "subject": subject,
            "limits": limits,
            "steps": steps,
        },
        operation_id=operation_id,
    )


async def run_status(run_id: str) -> dict[str, Any]:
    return await call("run.read", {"run_id": run_id})


async def ready(run_id: str, *, current_input_digests: dict[str, str] | None = None) -> dict[str, Any]:
    """Which persisted steps may run now — a computation; the caller decides what to spawn."""
    payload: dict[str, Any] = {"run_id": run_id}
    if current_input_digests:
        payload["current_input_digests"] = current_input_digests
    return await call("run.ready", payload)


async def abandon(run_id: str, attempt_id: str, *, reason: str) -> dict[str, Any]:
    """Explicitly write off one attempt (a crashed child, a killed session) so a retry is legal.

    This is an operator/executor act, not an inference: reconciliation never abandons on its own,
    because a missing registry entry is not proof that a child ended. Abandoning only discards what
    was never accepted; a frozen result stays where it is.
    """
    return await settle(run_id, attempt_id, "abandoned", note=reason)


async def finish(run_id: str, *, outcome: str = "done", note: str | None = None) -> dict[str, Any]:
    """Close the run; `done` is refused while any persisted step is not succeeded."""
    current = await run_status(run_id)
    payload: dict[str, Any] = {"run_id": run_id, "outcome": outcome}
    if note:
        payload["note"] = note
    return await call("run.finish", payload, expected_run_version=int(current["state_version"]))


async def expand(run_id: str, steps: list[dict[str, Any]], *, cause: str = "") -> dict[str, Any]:
    """Record newly discovered steps explicitly; resume must not re-derive a different set."""
    current = await run_status(run_id)
    return await call("steps.expand", {"run_id": run_id, "steps": steps, "cause": cause}, expected_run_version=int(current["state_version"]))


def _rlm() -> Any:
    try:
        import rlm  # type: ignore
    except ImportError as error:  # pragma: no cover - only inside a Prime kernel
        raise WorkflowError("rlm is only importable inside Prime's kernel; spawn needs it") from error
    return rlm


def _roles() -> Any:
    try:
        import roles  # type: ignore
    except ImportError as error:
        raise WorkflowError("role binding needs the `roles` skill installed in this kernel") from error
    return roles


def list_roles() -> list[str]:
    """The registry's role ids — the roster is dynamic, nothing is hardcoded here."""
    return sorted(str(role) for role in _roles().CANONICAL_ROLES)


def _child_id(handle: Any) -> str:
    # The real handle is `RLMSpawnHandle(rlm_child_id=...)` — measured in a sessionless run on
    # 24.09.2026, where the wrong tuple left a spawned child unadmitted.
    for attribute in ("rlm_child_id", "child_id", "childId", "id"):
        value = getattr(handle, attribute, None)
        if value:
            return str(value)
    raise WorkflowError(f"rlm.spawn returned a handle without a child id: {handle!r}")


async def spawn(
    run_id: str,
    step_id: str,
    prompt: str,
    *,
    child_name: str | None = None,
    model: str | None = None,
    thinking: str | None = None,
    lease_ms: int | None = None,
    role: str | None = None,
) -> dict[str, Any]:
    """Book the attempt, spawn, book the admission — in that order, on purpose.

    With `role` set the spawn goes through the `roles` skill (which verifies the role directory
    against its manifest and returns the digests); without it, `rlm.spawn` runs freestyle. Either
    way the attempt is booked BEFORE anything starts.
    """
    name = child_name or f"{run_id}--{step_id}--{uuid.uuid4().hex[:8]}"
    status = await run_status(run_id)

    binding: Any = None
    if role:
        # Bind before claim, including aliases and the installed manifest pin. Never hash an
        # unverified directory directly or accept an alias as a filesystem path.
        binding = _roles().bind_role(role)

    claim: dict[str, Any] = {"run_id": run_id, "step_id": step_id, "child_name": name}
    if lease_ms is not None:
        claim["lease_ms"] = lease_ms
    if role:
        claim["role"] = binding.role
        claim["role_digest"] = binding.role_sha256
        claim["role_binding"] = binding.wire()
    claimed = await call("attempt.claim", claim, expected_run_version=int(status["state_version"]))
    attempt_id = str(claimed["attempt_id"])

    role_spawn: Any = None
    try:
        if role:
            role_spawn = await _roles().spawn(role, prompt, binding=binding, name=name,
                                               **({} if not model else {"model": model}), **({} if not thinking else {"thinking": thinking}))
            handle = role_spawn.handle
            if role_spawn.binding != binding:
                raise WorkflowError(
                    f"role {role} changed between pinning and spawning",
                )
        else:
            kwargs: dict[str, Any] = {"name": name}
            if model:
                kwargs["model"] = model
            if thinking:
                kwargs["thinking"] = thinking
            handle = await _rlm().spawn(prompt, **kwargs)
        # Inside the try on purpose: a handle we cannot read is a spawn we cannot admit, and it must
        # end as `unresolved`, not as an `intended` attempt nobody resolves.
        child_id = _child_id(handle)
    except Exception as error:
        # The claim exists but no admission will: visible as unresolved, never as a silent retry.
        # The booking MUST carry the current run version — without it the host refuses the call and
        # the attempt stays `intended`, blocking every retry (review round 2). The original spawn
        # error stays visible even when the write-off itself fails.
        try:
            current = await run_status(run_id)
            await call(
                "attempt.unresolved",
                {"run_id": run_id, "attempt_id": attempt_id, "reason": f"spawn raised: {error}"},
                expected_run_version=int(current["state_version"]),
            )
        except Exception as bookkeeping_error:
            raise WorkflowError(f"spawn raised: {error}; writing the attempt off failed too: {bookkeeping_error}") from error
        raise
    after = await run_status(run_id)
    await call(
        "attempt.admit",
        {"run_id": run_id, "attempt_id": attempt_id, "child_id": child_id},
        expected_run_version=int(after["state_version"]),
    )
    return {
        "attempt_id": attempt_id,
        "child_id": child_id,
        "child_name": name,
        "handle": handle,
        **({} if not role else {"role": binding.role, "role_digest": binding.role_sha256,
                               "role_binding": binding.wire(), "parent_tools": role_spawn.parent_tools}),
    }


async def collect(handles: list[Any], timeout_ms: int = 0) -> list[Any]:
    """`rlm.collect` passthrough; a snapshot is a snapshot, never a result."""
    return await _rlm().collect(handles, timeout_ms=timeout_ms)


def _observe(child: Any) -> dict[str, Any]:
    return {
        "name": getattr(child, "session_name", None) or getattr(child, "name", None),
        "child_id": getattr(child, "rlm_child_id", None) or getattr(child, "child_id", None),
        "status": getattr(child, "status", None),
    }


async def reconcile(run_id: str) -> dict[str, Any]:
    """Re-label open attempts from the ledger + the kernel registry. It restarts nothing, ever."""
    observed = [_observe(child) for child in await _rlm().list_subagents()]
    current = await run_status(run_id)
    return await call("reconcile.run", {"run_id": run_id, "observed": observed}, expected_run_version=int(current["state_version"]))


async def accept_result(
    run_id: str,
    attempt_id: str,
    source_path: str | Path,
    *,
    schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate and freeze the child's result file; the digest names exactly those bytes."""
    current = await run_status(run_id)
    payload: dict[str, Any] = {"run_id": run_id, "attempt_id": attempt_id, "source_path": str(source_path)}
    if schema is not None:
        payload["schema"] = schema
    return await call("result.accept", payload, expected_run_version=int(current["state_version"]))


async def settle(run_id: str, attempt_id: str, status: str, *, note: str | None = None) -> dict[str, Any]:
    if status not in {"succeeded", "failed", "abandoned"}:
        raise WorkflowError(f"settle status must be succeeded|failed|abandoned, not {status!r}")
    current = await run_status(run_id)
    payload: dict[str, Any] = {"run_id": run_id, "attempt_id": attempt_id, "status": status}
    if note:
        payload["note"] = note
    return await call("attempt.settle", payload, expected_run_version=int(current["state_version"]))


async def unresolved(run_id: str, attempt_id: str, reason: str) -> dict[str, Any]:
    current = await run_status(run_id)
    return await call(
        "attempt.unresolved",
        {"run_id": run_id, "attempt_id": attempt_id, "reason": reason},
        expected_run_version=int(current["state_version"]),
    )


async def run(run_id: str | None = None) -> str:
    """Skill entry point: bridge info, or one run's folded state as JSON."""
    if run_id is None:
        pong = await ping()
        return f"workflow bridge at {socket_path()} (epoch {pong['epoch']}, root {pong['state_root']})"
    return json.dumps(await run_status(run_id), indent=2, sort_keys=True)
