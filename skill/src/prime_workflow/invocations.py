"""Invoke executor over the existing claim/admit/accept/settle service.

No graph engine or auto-retry. Full typed return artifacts are wrapped as JSON
objects; their native digest and the workflow's accepted digest remain distinct.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
import tempfile
import uuid
from pathlib import Path
from typing import Any

import prime_workflow as pw

RESULT_LIMIT = 262144
ENVELOPE_SCHEMA = {
    "required": ["schema_version", "present", "value", "value_encoding", "origin"],
    "properties": {"schema_version": "string", "present": "boolean", "value": "object",
                   "value_encoding": "string", "origin": "object"},
    "additionalProperties": False,
}


def _result_bytes(result: dict[str, Any]) -> bytes:
    """Read a regular producer artifact with an explicit cap and exact digest."""
    locator, expected = result.get("result_ref"), result.get("digest")
    if not isinstance(locator, str) or not Path(locator).is_absolute():
        raise pw.WorkflowError("Invoke result needs an absolute producer artifact")
    if not isinstance(expected, str) or re.fullmatch(r"[a-f0-9]{64}", expected) is None:
        raise pw.WorkflowError("Invoke result has no valid full-artifact digest")
    fd = os.open(locator, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise pw.WorkflowError("Invoke result is not a regular artifact")
        data = source.read(RESULT_LIMIT + 1)
    if len(data) > RESULT_LIMIT or hashlib.sha256(data).hexdigest() != expected:
        raise pw.WorkflowError("Invoke result exceeded limit or changed")
    return data


def _identity(result: Any, receipt: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict) or result.get("version") != 1:
        raise pw.WorkflowError("Invoke result protocol mismatch")
    for key in ("call_id", "attempt_id", "child_id"):
        if (not isinstance(receipt.get(key), str) or not receipt[key]
                or result.get(key) != receipt[key]):
            raise pw.WorkflowError("Invoke result identity mismatch")
    return result


async def _uncertain(run_id: str, attempt_id: str, reason: str) -> None:
    # An ACK may be lost after settle; never reopen a known terminal attempt.
    current = await pw.run_status(run_id)
    attempt = current.get("attempts", {}).get(attempt_id, {})
    if attempt.get("status") in {"succeeded", "failed", "abandoned"}:
        return
    await pw.unresolved(run_id, attempt_id, reason[:512])


def _invoke_api() -> Any:
    """The kernel binds the public facade as the namespace instance `rlm.rlm.invoke`
    (`rlm.rlm` is `_RLMNamespace()`); the plain module attribute `rlm.invoke` is the
    submodule and has no `start`. Prefer whichever exposes `start`."""
    module = pw._rlm()
    for candidate in (getattr(getattr(module, "rlm", None), "invoke", None),
                      getattr(module, "invoke", None)):
        if callable(getattr(candidate, "start", None)):
            return candidate
    raise pw.WorkflowError("native Invoke v1 is unavailable")


async def invoke_step(
    run_id: str, step_id: str, prompt: str, *, inputs: dict[str, Any] | None = None,
    child_name: str | None = None, model: str | None = None,
    max_requests: int = 4, deadline_ms: int = 60000,
) -> dict[str, Any]:
    """Execute one fresh step. Existing attempts require explicit reconciliation.

    Requires the native Invoke v1 start/workflow_binding contract. The adapter
    does not claim producer identity as independently host-verified evidence.
    """
    api = _invoke_api()
    current = await pw.run_status(run_id)
    if current.get("steps", {}).get(step_id):
        raise pw.WorkflowError("existing attempt requires explicit reconciliation, not respawn")
    name = child_name or f"wf-{uuid.uuid4().hex}"
    claimed = await pw.call("attempt.claim", {"run_id": run_id, "step_id": step_id,
                                            "child_name": name},
                            expected_run_version=int(current["state_version"]))
    attempt_id = str(claimed["attempt_id"])
    try:
        handle = await api.start(
            prompt, name=name, inputs=inputs, model=model,
            max_requests=max_requests, deadline_ms=deadline_ms,
            workflow_binding={"run_id": run_id, "step_id": step_id, "attempt_id": attempt_id},
        )
        receipt = getattr(handle, "receipt", None)
        if not isinstance(receipt, dict) or receipt.get("state") != "started":
            raise pw.WorkflowError("Invoke start receipt missing; reconcile before retry")
        _identity(receipt, receipt)
        current = await pw.run_status(run_id)
        await pw.call("attempt.admit", {"run_id": run_id, "attempt_id": attempt_id,
                                        "child_id": receipt["child_id"]},
                      expected_run_version=int(current["state_version"]))
        result = _identity(await handle.wait(), receipt)
        state = result.get("state")
        if state in {"failed", "cancelled"}:
            await pw.settle(run_id, attempt_id, "failed", note=f"Invoke terminal state: {state}")
            return {"state": "failed", "attempt_id": attempt_id, "invoke": result}
        if state != "returned" or result.get("present") is not True:
            raise pw.WorkflowError("Invoke has no confirmed full return; reconcile before retry")
        if result.get("durability") != "value":
            raise pw.WorkflowError("live handles are not durable workflow results")
        data = _result_bytes(result)
        value = json.loads(data)
        if not isinstance(value, dict) or "t" not in value:
            raise pw.WorkflowError("Invoke artifact is not a typed value")
        envelope = {
            "schema_version": "prime-workflow/invoke-result/1", "present": True,
            "value": value, "value_encoding": "prime-invoke-wire/1",
            "origin": {"kind": "invoke", "provenance": "adapter_checked_host_response",
                       "call_id": receipt["call_id"], "attempt_id": receipt["attempt_id"],
                       "child_id": receipt["child_id"], "native_result_digest": result["digest"]},
        }
        # The existing service owns the durable accepted copy. This temporary
        # envelope is never a second result store and is removed even on refusal.
        with tempfile.TemporaryDirectory(prefix="workflow-invoke-") as directory:
            source = Path(directory) / "result.json"
            source.write_text(json.dumps(envelope, ensure_ascii=False, allow_nan=False),
                              encoding="utf-8")
            accepted = await pw.accept_result(run_id, attempt_id, source, schema=ENVELOPE_SCHEMA)
        await pw.settle(run_id, attempt_id, "succeeded")
        return {"state": "succeeded", "attempt_id": attempt_id, "accepted": accepted,
                "invoke_call_id": receipt["call_id"], "invoke_attempt_id": receipt["attempt_id"]}
    except (Exception, asyncio.CancelledError) as error:
        try:
            await _uncertain(
                run_id, attempt_id, f"invoke adapter interrupted: {type(error).__name__}"
            )
        except Exception as bookkeeping:
            raise pw.WorkflowError(
                "Invoke outcome needs reconciliation; bookkeeping failed: "
                f"{type(bookkeeping).__name__}"
            ) from error
        raise
