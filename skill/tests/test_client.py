"""M1.3 — offline tests for the Python bridge client.

A fake host serves the same NDJSON protocol on a Unix socket; a fake `rlm` module stands in for the
kernel. No Prime process and no model call are needed, so the ordering guarantees (claim → spawn →
admit) and the epoch retry are testable in a plain Python.

Run: python3 tests/test_client.py
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import prime_workflow as pw  # noqa: E402


def install_module(name: str, **attributes: object) -> None:
    module = types.ModuleType(name)
    for attribute, value in attributes.items():
        setattr(module, attribute, value)
    sys.modules[name] = module


class FakeHost:
    def __init__(self, root: Path):
        self.root = root
        self.requests: list[dict] = []
        self.epoch = "7"
        self.state_version = 3
        self.finished = False
        self.projection: dict | None = None
        self.server: asyncio.AbstractServer | None = None

    def current_version(self) -> int:
        if self.projection is not None and isinstance(self.projection.get("state_version"), int):
            return int(self.projection["state_version"])
        return self.state_version

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        line = await reader.readline()
        request = json.loads(line.decode("utf-8"))
        self.requests.append(request)
        operation = request.get("operation")
        envelope = {"v": pw.PROTOCOL, "operation_id": request.get("operation_id", "")}

        def ok(result):
            return {**envelope, "ok": True, "result": result}

        def err(message, code=None):
            return {**envelope, "ok": False, "error": {"class": "WorkflowStateError", "message": message, "code": code}}

        reply = None
        if request.get("v") != pw.PROTOCOL:
            reply = err("unsupported protocol")
        elif operation in pw.MUTATING and operation != "run.create":
            if not isinstance(request.get("expected_run_version"), int):
                reply = err(f"operation {operation} needs an integer expected_run_version")
            elif request["expected_run_version"] != self.current_version():
                reply = err(f"stale expected_run_version {request['expected_run_version']}; the run is at {self.current_version()}")
        if reply is not None:
            pass
        elif operation == "ping":
            reply = ok({"epoch": self.epoch, "state_root": str(self.root)})
        elif operation == "run.read":
            if self.projection is not None:
                projection = dict(self.projection)
                if self.finished:
                    projection["status"] = "done"
                reply = ok(projection)
            else:
                reply = ok({"run_id": request["payload"]["run_id"], "state_version": self.state_version, "status": "done" if self.finished else "running"})
        elif operation == "attempt.claim":
            if request.get("driver_epoch") != self.epoch:
                reply = err(f"stale driver_epoch {request.get('driver_epoch')}; the current epoch is {self.epoch}", "STALE_DRIVER_EPOCH")
            else:
                self.state_version += 1
                reply = ok({"attempt_id": "a-1"})
        elif operation == "attempt.admit":
            self.state_version += 1
            reply = ok({"run_id": request["payload"]["run_id"], "state_version": self.state_version})
        elif operation == "attempt.unresolved":
            self.state_version += 1
            reply = ok({"state_version": self.state_version})
        elif operation == "result.accept":
            self.state_version += 1
            source = Path(request["payload"]["source_path"])
            data = source.read_bytes()
            step = str(request["payload"]["attempt_id"]).removeprefix("a-")
            reply = ok(
                {
                    "result_ref": f"runs/r1/results/{step}--01.json",
                    "digest": hashlib.sha256(data).hexdigest(),
                    "size_bytes": len(data),
                },
            )
        elif operation == "attempt.settle":
            self.state_version += 1
            reply = ok({"state_version": self.state_version})
        elif operation == "run.create":
            self.state_version = 1
            reply = ok({"run_id": request["payload"]["run_id"], "state_version": 1})
        elif operation == "reconcile.run":
            self.state_version += 1
            reply = ok({"actions": [], "ledger_found": True, "unknown_ledger_lines": 0, "state_version": self.state_version})
        elif operation == "run.ready":
            reply = ok({"ready": ["explore"], "blocked": [{"step_id": "verify", "reason": "dependencies not settled: explore"}]})
        elif operation == "steps.expand":
            self.state_version += 1
            reply = ok({"state_version": self.state_version, "step_specs": {}})
        elif operation == "run.finish":
            self.state_version += 1
            self.finished = True
            reply = ok({"status": "done", "state_version": self.state_version})
        else:
            reply = err(f"unknown operation {operation!r}")
        writer.write((json.dumps(reply) + "\n").encode("utf-8"))
        await writer.drain()
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    async def start(self) -> None:
        socket = pw.socket_path()
        socket.parent.mkdir(parents=True, exist_ok=True)
        self.server = await asyncio.start_unix_server(self.handle, path=str(socket))
        os.chmod(socket, 0o600)

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()


def install_fake_rlm(spawn_calls: list[dict]) -> None:
    async def spawn(prompt, *, name, model=None, thinking=None):
        spawn_calls.append({"prompt": prompt, "name": name, "model": model, "thinking": thinking})
        return types.SimpleNamespace(child_id="sub-fake-1", name=name)

    install_module("rlm", spawn=spawn)


async def test_discovery_and_ping(tmp: Path) -> None:
    os.environ["PRIME_WORKFLOW_STATE"] = str(tmp)
    assert pw.state_root() == tmp / "workflow-state", pw.state_root()
    assert pw.socket_path() == tmp / "workflow-state" / "bridge.sock"
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    try:
        pong = await pw.ping()
        assert pong["epoch"] == "7", pong
    finally:
        await host.stop()


async def test_spawn_books_claim_and_admission_in_order(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    spawn_calls: list[dict] = []
    install_fake_rlm(spawn_calls)
    pw._clear_epoch()
    try:
        result = await pw.spawn("r1", "explore", "Look around", model="openai/gpt-5-mini")
        assert result["attempt_id"] == "a-1"
        assert result["child_id"] == "sub-fake-1"
        operations = [r["operation"] for r in host.requests]
        assert operations == ["run.read", "ping", "attempt.claim", "run.read", "attempt.admit"], operations
        claim = next(r for r in host.requests if r["operation"] == "attempt.claim")
        assert claim["driver_epoch"] == "7"
        assert claim["expected_run_version"] == 3
        assert claim["operation_id"], "mutating requests carry an operation id"
        assert len(spawn_calls) == 1 and spawn_calls[0]["name"] == result["child_name"]
        assert spawn_calls[0]["model"] == "openai/gpt-5-mini"
    finally:
        await host.stop()


async def test_role_spawn_uses_the_roles_skill_and_books_the_digest(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    install_fake_rlm([])
    calls: list[dict] = []

    class FakeRoles:
        CANONICAL_ROLES = ["sota-verify", "sota-explore"]

        @staticmethod
        def bind_role(role):
            assert role == "sota-verify"
            return types.SimpleNamespace(role=role, role_sha256="a" * 64,
                wire=lambda: {"role": role, "role_sha256": "a" * 64,
                              "manifest_sha256": "b" * 64, "contract_sha256": "c" * 64})

        @staticmethod
        async def spawn(role: str, task: str, **kwargs):
            calls.append({"role": role, "task": task, **kwargs})
            return types.SimpleNamespace(handle=types.SimpleNamespace(child_id="sub-role-1"), binding=kwargs["binding"], parent_tools=("read",))

    install_module("roles", CANONICAL_ROLES=FakeRoles.CANONICAL_ROLES,
                   bind_role=FakeRoles.bind_role, spawn=FakeRoles.spawn)
    pw._clear_epoch()
    try:
        assert pw.list_roles() == ["sota-explore", "sota-verify"]
        result = await pw.spawn("r1", "verify", "Check it", role="sota-verify")
        assert result["child_id"] == "sub-role-1"
        assert result["role_digest"] == "a" * 64
        claim = next(r for r in host.requests if r["operation"] == "attempt.claim")
        assert claim["payload"]["role"] == "sota-verify"
        assert claim["payload"]["role_binding"]["manifest_sha256"] == "b" * 64
        assert calls and calls[0]["role"] == "sota-verify"
    finally:
        sys.modules.pop("roles", None)
        await host.stop()


async def test_accept_result_freezes_via_the_bridge(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()
    try:
        (tmp / "out.json").write_text('{"files": []}')
        result = await pw.accept_result("r1", "a-explore", tmp / "out.json", schema={"required": ["files"]})
        assert result["digest"] == hashlib.sha256(b'{"files": []}').hexdigest()
        assert result["result_ref"] == "runs/r1/results/explore--01.json"
        request = host.requests[-1]
        assert request["operation"] == "result.accept"
        assert request["expected_run_version"] == 3
        assert request["payload"]["schema"] == {"required": ["files"]}
    finally:
        await host.stop()


async def test_audit_readonly_recipe_runs_four_phases_and_accepts_every_artifact(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()
    seen: list[dict] = []

    async def fake_spawner(run_id: str, step_id: str, prompt: str, **kwargs):
        seen.append({"step": step_id, "prompt": prompt, "run_id": run_id})
        path = Path(kwargs["artifact_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"phase": step_id, "summary": "s", "findings": [], "coverage": []}))
        return {"attempt_id": f"a-{step_id}", "child_id": f"sub-{step_id}", "child_name": "n", "result_path": path}

    try:
        from prime_workflow import recipes

        out = await recipes.run_audit_readonly(
            run_id="audit-1",
            subject=str(tmp),
            artifacts_dir=tmp / "artifacts",
            spawner=fake_spawner,
        )
        assert [p["phase"] for p in out["phases"]] == ["explore", "verify", "contrarian", "judge"]
        assert all(p["status"] == "succeeded" for p in out["phases"]), out["phases"]
        assert set(out["accepted"]) == {"explore", "verify", "contrarian", "judge"}
        assert out["status"] == "done", "a completed run is closed, not left running"
        assert any(r["operation"] == "run.finish" for r in host.requests)
        create = next(r for r in host.requests if r["operation"] == "run.create")
        assert len(create["payload"]["steps"]) == 4, "the program is persisted with the run"
        verify_prompt = next(s["prompt"] for s in seen if s["step"] == "verify")
        contrarian_prompt = next(s["prompt"] for s in seen if s["step"] == "contrarian")
        assert "explore--01.json" in verify_prompt, "the explore artifact reference reaches verify"
        assert "explore--01.json" in contrarian_prompt, "and contrarian"
        judge_prompt = next(s["prompt"] for s in seen if s["step"] == "judge")
        assert "explore" in judge_prompt and "verify" in judge_prompt and "contrarian" in judge_prompt
        operations = [r["operation"] for r in host.requests]
        assert operations.count("result.accept") == 4
        assert operations.count("attempt.settle") == 4
    finally:
        await host.stop()


async def test_reconcile_reports_the_kernel_registry_and_restarts_nothing(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()

    class FakeRlm:
        @staticmethod
        async def list_subagents():
            return [
                types.SimpleNamespace(session_name="r1--explore--1", rlm_child_id="sub-1", status="running"),
                types.SimpleNamespace(name="other", child_id="sub-2", status="running"),
            ]

    install_module("rlm", list_subagents=FakeRlm.list_subagents)
    pw._clear_epoch()
    try:
        result = await pw.reconcile("r1")
        assert result["actions"] == []
        request = host.requests[-1]
        assert request["operation"] == "reconcile.run"
        observed = request["payload"]["observed"]
        assert {"name": "r1--explore--1", "child_id": "sub-1", "status": "running"} in observed
        assert len(observed) == 2
    finally:
        await host.stop()


async def test_spawn_reads_the_real_handle_attribute_rlm_child_id(tmp: Path) -> None:
    # Measured 24.09.2026 in a sessionless run: the handle carries `rlm_child_id`, and a lookup for
    # `child_id` left a spawned child unadmitted.
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()

    class RealShapeRlm:
        @staticmethod
        async def spawn(prompt, **kwargs):
            return types.SimpleNamespace(rlm_child_id="sub-5757be68", name=kwargs.get("name"))

    install_module("rlm", spawn=RealShapeRlm.spawn)
    try:
        result = await pw.spawn("r1", "explore", "p")
        assert result["child_id"] == "sub-5757be68"
        admit = next(r for r in host.requests if r["operation"] == "attempt.admit")
        assert admit["payload"]["child_id"] == "sub-5757be68"
    finally:
        sys.modules.pop("rlm", None)
        await host.stop()


async def test_spawn_failure_is_recorded_as_unresolved_and_the_error_surfaces(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()

    class FailingRlm:
        @staticmethod
        async def spawn(prompt, **kwargs):
            raise RuntimeError("boom")

    install_module("rlm", spawn=FailingRlm.spawn)
    pw._clear_epoch()
    try:
        try:
            await pw.spawn("r1", "explore", "p")
        except RuntimeError as error:
            assert "boom" in str(error)
        else:
            raise AssertionError("the original spawn error must surface")
        operations = [r["operation"] for r in host.requests]
        assert "attempt.unresolved" in operations, "a claim without an admission must not vanish"
        booking = next(r for r in host.requests if r["operation"] == "attempt.unresolved")
        assert isinstance(booking.get("expected_run_version"), int), "the write-off must carry the run version"
        assert "boom" in booking["payload"]["reason"], booking["payload"]["reason"]
    finally:
        sys.modules.pop("rlm", None)
        await host.stop()


async def test_collect_is_a_passthrough_and_changes_no_state(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()

    class CollectRlm:
        @staticmethod
        async def collect(handles, timeout_ms=0):
            return ["snapshot"]

    install_module("rlm", collect=CollectRlm.collect)
    try:
        assert await pw.collect(["h"], timeout_ms=5) == ["snapshot"]
        assert host.requests == [], "a snapshot is not a state transition"
    finally:
        sys.modules.pop("rlm", None)
        await host.stop()


async def test_recipe_stops_when_a_phase_has_no_result(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()

    async def missing_spawner(run_id: str, step_id: str, prompt: str, **kwargs):
        return {"attempt_id": f"a-{step_id}", "child_id": f"sub-{step_id}", "child_name": "n", "result_path": tmp / "nope.json"}

    try:
        from prime_workflow import recipes

        out = await recipes.run_audit_readonly(run_id="audit-2", subject=str(tmp), artifacts_dir=tmp / "artifacts", spawner=missing_spawner)
        assert len(out["phases"]) == 1, "no verification runs against a missing explore result"
        assert out["phases"][0]["status"] == "unresolved"
        operations = [r["operation"] for r in host.requests]
        assert "attempt.unresolved" in operations
        assert "result.accept" not in operations
    finally:
        await host.stop()


async def test_ready_computes_and_expand_records(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()
    try:
        ready = await pw.ready("r1", current_input_digests={"spec.json": "2"})
        assert ready["ready"] == ["explore"]
        assert ready["blocked"][0]["step_id"] == "verify"
        ready_request = host.requests[-1]
        assert ready_request["payload"]["current_input_digests"] == {"spec.json": "2"}
        assert "operation_id" not in ready_request, "a computation is not a mutating operation"

        expanded = await pw.expand("r1", [{"id": "deep-dive", "depends_on": ["verify"]}], cause="explore found X")
        assert expanded["state_version"] >= 2
        expand_request = host.requests[-1]
        assert expand_request["operation"] == "steps.expand"
        assert expand_request["payload"]["cause"] == "explore found X"
        assert expand_request["operation_id"], "an expansion is a mutation and carries an id"
        assert "expected_run_version" in expand_request
    finally:
        await host.stop()


async def test_recipe_waits_for_settle_before_reading_the_artifact(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()
    state = {"collects": 0, "path": None}
    handle = object()

    async def slow_spawner(run_id: str, step_id: str, prompt: str, **kwargs):
        if step_id == "explore":
            state["path"] = Path(kwargs["artifact_path"])
        return {
            "attempt_id": f"a-{step_id}",
            "child_id": f"sub-{step_id}",
            "child_name": "n",
            "handle": handle if step_id == "explore" else None,
        }

    class SlowRlm:
        @staticmethod
        async def collect(handles, timeout_ms=0):
            # The child writes its file only while collect is being awaited — the exact race the
            # immediate check lost in the sessionless run.
            state["collects"] += 1
            path = state["path"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"phase": "explore", "summary": "s", "findings": [], "coverage": []}))
            return [types.SimpleNamespace(settled=True)]

    install_module("rlm", collect=SlowRlm.collect)
    try:
        from prime_workflow import recipes

        out = await recipes.run_audit_readonly(run_id="audit-wait", subject=str(tmp), artifacts_dir=tmp / "artifacts", spawner=slow_spawner)
        assert state["collects"] >= 1, "the recipe must wait for the child, not check immediately"
        assert out["phases"][0]["status"] == "succeeded", out["phases"]
        assert out["accepted"]["explore"]["digest"]
    finally:
        sys.modules.pop("rlm", None)
        await host.stop()


async def test_recipe_marks_a_settled_child_without_artifact_as_failed(tmp: Path) -> None:
    # Measured 24.09.2026: a child whose request failed settles immediately and writes nothing.
    # `failed` is terminal and retryable; `unresolved` would block the step forever.
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()

    async def spawner(run_id: str, step_id: str, prompt: str, **kwargs):
        return {
            "attempt_id": f"a-{step_id}",
            "child_id": f"sub-{step_id}",
            "child_name": "n",
            "handle": object() if step_id == "explore" else None,
        }

    class DeadChildRlm:
        @staticmethod
        async def collect(handles, timeout_ms=0):
            return [types.SimpleNamespace(settled=True)]  # settled, no file, ever

    install_module("rlm", collect=DeadChildRlm.collect)
    try:
        from prime_workflow import recipes

        out = await recipes.run_audit_readonly(run_id="audit-dead", subject=str(tmp), artifacts_dir=tmp / "artifacts", spawner=spawner)
        assert out["phases"][0]["status"] == "failed", out["phases"]
        operations = [r["operation"] for r in host.requests]
        assert "attempt.settle" in operations, "a settled child is settled, not left unresolved"
        settle = next(r for r in host.requests if r["operation"] == "attempt.settle")
        assert settle["payload"]["status"] == "failed"
    finally:
        sys.modules.pop("rlm", None)
        await host.stop()


async def test_stale_epoch_is_refreshed_once_then_succeeds(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    install_fake_rlm([])
    pw._clear_epoch()
    try:
        await pw.ping()  # caches epoch 7
        host.epoch = "8"  # the host restarted with a new writer
        claimed = await pw.call("attempt.claim", {"run_id": "r1", "step_id": "x", "child_name": "n"}, expected_run_version=3)
        assert claimed["attempt_id"] == "a-1"
        claims = [r for r in host.requests if r["operation"] == "attempt.claim"]
        assert len(claims) == 2, "one refused with the stale epoch, one retried with the fresh one"
        assert claims[0]["driver_epoch"] == "7" and claims[1]["driver_epoch"] == "8"
    finally:
        await host.stop()


async def test_epoch_cache_follows_socket_not_previous_workspace(tmp: Path) -> None:
    first = tmp / "first"
    second = tmp / "second"
    hosts: list[FakeHost] = []
    for root in (first, second):
        os.environ["PRIME_WORKFLOW_STATE"] = str(root)
        host = FakeHost(root / "workflow-state")
        await host.start()
        hosts.append(host)
    initial, newer = hosts
    try:
        pw._clear_epoch()
        os.environ["PRIME_WORKFLOW_STATE"] = str(first)
        await pw.call("run.create", {"run_id": "r1", "definition_digest": "d"})
        newer.epoch = "other"
        os.environ["PRIME_WORKFLOW_STATE"] = str(second)
        await pw.call("run.create", {"run_id": "r2", "definition_digest": "d"})
        assert [r["operation"] for r in newer.requests] == ["ping", "run.create"]
        assert newer.requests[1]["driver_epoch"] == "other"
    finally:
        await initial.stop()
        await newer.stop()


async def test_stale_text_without_code_is_not_retried(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    async def forged(reader, writer):
        line = await reader.readline()
        req = json.loads(line)
        if req["operation"] == "attempt.claim":
            host.requests.append(req)
            writer.write((json.dumps({"v": pw.PROTOCOL, "ok": False,
                       "error": {"class": "WorkflowStateError", "message": "stale driver_epoch but mutation may have happened"}}) + "\n").encode())
            await writer.drain()
            writer.close()
        else:
            writer.write((json.dumps({"v": pw.PROTOCOL, "ok": True,
                          "result": {"epoch": host.epoch, "state_root": str(host.root)}}) + "\n").encode())
            await writer.drain()
            writer.close()

    # Replace the handler by a fresh socket at the same scratch path; no live host mutation.
    await host.stop()
    pw.socket_path().unlink()
    host.server = await asyncio.start_unix_server(forged, path=str(pw.socket_path()))
    pw._clear_epoch()
    try:
        try:
            await pw.call("attempt.claim", {"run_id": "r1"}, expected_run_version=3)
        except pw.WorkflowError as error:
            assert "stale driver_epoch" in str(error)
        else:
            raise AssertionError("no retry without an explicit pre-admission code")
        assert sum(r["operation"] == "attempt.claim" for r in host.requests) == 1
    finally:
        await host.stop()


async def test_silent_bridge_times_out_with_unknown_admission(tmp: Path) -> None:
    socket = pw.socket_path()
    socket.parent.mkdir(parents=True, exist_ok=True)

    async def silent(reader, writer):
        await reader.readline()
        await reader.read()  # wait for the client to close after its deadline
        writer.close()

    server = await asyncio.start_unix_server(silent, path=str(socket))
    before = pw.BRIDGE_DEADLINE_SECONDS
    pw.BRIDGE_DEADLINE_SECONDS = 0.05
    pw._clear_epoch()
    try:
        try:
            await pw.call("run.create", {"run_id": "r1"})
        except pw.WorkflowError as error:
            assert error.code == "TRANSPORT_UNKNOWN"
        else:
            raise AssertionError("silent bridge cannot be treated as an accepted operation")
    finally:
        pw.BRIDGE_DEADLINE_SECONDS = before
        server.close()
        await server.wait_closed()


async def test_stale_epoch_reply_for_other_operation_never_retries(tmp: Path) -> None:
    socket = pw.socket_path()
    socket.parent.mkdir(parents=True, exist_ok=True)
    requests = []

    async def serve(reader, writer):
        request = json.loads(await reader.readline())
        requests.append(request)
        if request["operation"] == "ping":
            reply = {"v": pw.PROTOCOL, "ok": True, "result": {"epoch": "old"}}
        else:
            reply = {"v": pw.PROTOCOL, "ok": False, "operation_id": "not-your-operation",
                     "error": {"code": "STALE_DRIVER_EPOCH", "message": "stale driver_epoch"}}
        writer.write((json.dumps(reply) + "\n").encode())
        await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(serve, path=str(socket))
    pw._clear_epoch()
    try:
        try:
            await pw.call("run.create", {"run_id": "r1"})
        except pw.WorkflowError as error:
            assert error.code == "TRANSPORT_UNKNOWN"
        else:
            raise AssertionError("unbound refusal must never retry")
        assert [r["operation"] for r in requests] == ["ping", "run.create"]
    finally:
        server.close()
        await server.wait_closed()


async def test_refusals_surface_as_workflow_error(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    try:
        try:
            await pw.call("graph.execute", {"run_id": "r1"})
        except pw.WorkflowError as error:
            assert "unknown operation" in str(error), error
        else:
            raise AssertionError("an unknown operation must raise WorkflowError")
    finally:
        await host.stop()


async def test_settle_validates_status_locally(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    try:
        try:
            await pw.settle("r1", "a-1", "green")
        except pw.WorkflowError as error:
            assert "succeeded|failed|abandoned" in str(error), error
        else:
            raise AssertionError("an invalid status must be refused before any request")
        assert not host.requests, "no request should have been sent"
    finally:
        await host.stop()


async def test_standard_parallel_spawns_all_angles_then_synthesizes(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()
    spawns: list[str] = []

    async def spawner(run_id: str, step_id: str, prompt: str, **kwargs):
        spawns.append(step_id)
        path = Path(kwargs["artifact_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"phase": step_id, "summary": "s", "findings": [], "coverage": []}))
        return {"attempt_id": f"a-{step_id}", "child_id": f"sub-{step_id}", "child_name": "n", "result_path": path}

    from prime_workflow import recipes

    try:
        out = await recipes.run_standard_parallel(
            run_id="par-1",
            question="How does X work?",
            angles=["docs", "issues", "source"],
            artifacts_dir=tmp / "par",
            spawner=spawner,
        )
        assert spawns == ["angle-1", "angle-2", "angle-3", "synthesis"], "every angle is spawned before any wait"
        assert out["status"] == "done"
        create = next(r for r in host.requests if r["operation"] == "run.create")
        specs = create["payload"]["steps"]
        assert [s["id"] for s in specs] == ["angle-1", "angle-2", "angle-3", "synthesis"]
        assert specs[3]["depends_on"] == ["angle-1", "angle-2", "angle-3"]
        assert sum(1 for r in host.requests if r["operation"] == "result.accept") == 4
    finally:
        await host.stop()


async def main() -> None:
    import tempfile

    tests = [
        test_discovery_and_ping,
        test_spawn_books_claim_and_admission_in_order,
        test_role_spawn_uses_the_roles_skill_and_books_the_digest,
        test_accept_result_freezes_via_the_bridge,
        test_reconcile_reports_the_kernel_registry_and_restarts_nothing,
        test_audit_readonly_recipe_runs_four_phases_and_accepts_every_artifact,
        test_spawn_reads_the_real_handle_attribute_rlm_child_id,
        test_spawn_failure_is_recorded_as_unresolved_and_the_error_surfaces,
        test_collect_is_a_passthrough_and_changes_no_state,
        test_ready_computes_and_expand_records,
        test_recipe_stops_when_a_phase_has_no_result,
        test_recipe_waits_for_settle_before_reading_the_artifact,
        test_recipe_marks_a_settled_child_without_artifact_as_failed,
        test_resume_reuses_succeeded_phases_and_requires_an_explicit_write_off,
        test_resume_with_a_different_subject_is_refused,
        test_artifact_paths_are_unique_per_run,
        test_reuse_requires_the_latest_attempt_and_verifiable_bytes,
        test_standard_parallel_spawns_all_angles_then_synthesizes,
        test_stale_epoch_is_refreshed_once_then_succeeds,
        test_epoch_cache_follows_socket_not_previous_workspace,
        test_stale_text_without_code_is_not_retried,
        test_silent_bridge_times_out_with_unknown_admission,
        test_stale_epoch_reply_for_other_operation_never_retries,
        test_refusals_surface_as_workflow_error,
        test_settle_validates_status_locally,
        test_abandon_blocked_writes_off_intended_and_unresolved,
        test_role_drift_between_pin_and_spawn_books_unresolved,
        test_broken_replies_surface_as_workflow_error,
        test_ensure_run_recovers_from_a_create_race,
    ]
    for test in tests:
        with tempfile.TemporaryDirectory(prefix="wf-py-") as tmp:
            os.environ["PRIME_WORKFLOW_STATE"] = tmp
            pw._clear_epoch()
            await test(Path(tmp))
        print(f"ok {test.__name__}")
    print(f"{len(tests)} tests passed")




async def test_resume_reuses_succeeded_phases_and_requires_an_explicit_write_off(tmp: Path) -> None:
    os.environ["PRIME_WORKFLOW_STATE"] = str(tmp)
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()

    from prime_workflow import recipes
    frozen_ref = "runs/r1/results/explore--01.json"
    frozen_path = tmp / "workflow-state" / frozen_ref
    frozen_path.parent.mkdir(parents=True, exist_ok=True)
    frozen_path.write_text('{"phase": "explore", "summary": "s", "findings": [], "coverage": []}')
    frozen_digest = hashlib.sha256(frozen_path.read_bytes()).hexdigest()
    steps = [{"id": phase, "prompt": recipes.PROMPTS[phase]} for phase in recipes.PHASES]

    def projection(verify_status: str) -> dict:
        return {
            "run_id": "r1",
            "status": "running",
            "state_version": 9,
            "subject": {"project": str(tmp)},
            "definition_digest": recipes.definition_digest(steps),
            "attempts": {
                "a-explore": {
                    "attempt_id": "a-explore",
                    "step_id": "explore",
                    "status": "succeeded",
                    "result_ref": frozen_ref,
                    "result_digest": frozen_digest,
                    "result_origin": "child_file",
                },
                "a-verify": {"attempt_id": "a-verify", "step_id": "verify", "status": verify_status, "result_ref": None, "result_digest": None},
            },
            "steps": {"explore": ["a-explore"], "verify": ["a-verify"]},
        }

    spawns: list[str] = []

    async def spawner(run_id: str, step_id: str, prompt: str, **kwargs):
        spawns.append(step_id)
        path = Path(kwargs["artifact_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"phase": step_id, "summary": "s", "findings": [], "coverage": []}))
        return {"attempt_id": f"a2-{step_id}", "child_id": f"sub-{step_id}", "child_name": "n"}

    try:
        host.projection = projection("admitted")
        try:
            await recipes.run_audit_readonly(run_id="r1", subject=str(tmp), artifacts_dir=tmp / "artifacts", spawner=spawner, resume=True)
        except pw.WorkflowError as error:
            assert "active attempt" in str(error), error
        else:
            raise AssertionError("an open attempt must stop the resume until it is written off")
        assert "explore" not in spawns, "a succeeded phase is reused, never respawned"

        host.projection = projection("unresolved")
        written = await recipes.abandon_blocked("r1", reason="children died with the crashed session")
        assert written == [{"attempt_id": "a-verify", "step_id": "verify"}], written
        settle = next(r for r in host.requests if r["operation"] == "attempt.settle")
        assert settle["payload"]["status"] == "abandoned"
        assert "crashed session" in settle["payload"]["note"]

        host.projection = projection("abandoned")
        out = await recipes.run_audit_readonly(run_id="r1", subject=str(tmp), artifacts_dir=tmp / "artifacts", spawner=spawner, resume=True)
        assert out["phases"][0]["reused"] is True, "the explore artifact of the crashed session is the input again"
        assert out["phases"][0]["result"]["result_ref"] == "runs/r1/results/explore--01.json"
        assert spawns == ["verify", "contrarian", "judge"], spawns
        assert out["status"] == "done"
    finally:
        await host.stop()


async def test_resume_with_a_different_subject_is_refused(tmp: Path) -> None:
    os.environ["PRIME_WORKFLOW_STATE"] = str(tmp)
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()

    from prime_workflow import recipes
    steps = [{"id": phase, "prompt": recipes.PROMPTS[phase]} for phase in recipes.PHASES]
    host.projection = {
        "run_id": "r1",
        "status": "running",
        "state_version": 5,
        "subject": {"project": "/somewhere/else"},
        "definition_digest": recipes.definition_digest(steps),
        "attempts": {},
        "steps": {},
    }

    async def spawner(*_args, **_kwargs):
        raise AssertionError("no child may spawn once the subject mismatch is detected")

    try:
        try:
            await recipes.run_audit_readonly(run_id="r1", subject=str(tmp), artifacts_dir=tmp / "artifacts", spawner=spawner, resume=True)
        except pw.WorkflowError as error:
            assert "different subject" in str(error), error
        else:
            raise AssertionError("resuming a run that belongs to another subject must be refused")
    finally:
        await host.stop()


async def test_artifact_paths_are_unique_per_run(tmp: Path) -> None:
    from prime_workflow.recipes import _artifact_path

    a = _artifact_path(tmp / "artifacts", "r1", "explore")
    b = _artifact_path(tmp / "artifacts", "r1", "explore")
    c = _artifact_path(tmp / "artifacts", "r2", "explore")
    assert a != b, "two attempts never share one artifact path (P03)"
    assert a.parent.name == "r1" and c.parent.name == "r2"
    assert a.name.startswith("explore--") and a.name.endswith(".json")
    assert len(a.name) == len("explore--") + 8 + len(".json")
    a.write_text("x")
    assert a.exists() and not b.exists()


async def test_reuse_requires_the_latest_attempt_and_verifiable_bytes(tmp: Path) -> None:
    os.environ["PRIME_WORKFLOW_STATE"] = str(tmp)
    from prime_workflow import recipes

    good = tmp / "workflow-state" / "runs/r1/results/explore--01.json"
    good.parent.mkdir(parents=True, exist_ok=True)
    good.write_text(json.dumps({"phase": "explore", "summary": "s", "findings": [], "coverage": []}))
    digest = hashlib.sha256(good.read_bytes()).hexdigest()

    def run_of(*attempts):
        return {
            "attempts": {a["attempt_id"]: a for a in attempts},
            "steps": {"explore": [a["attempt_id"] for a in attempts]},
        }

    ok = {"attempt_id": "a1", "step_id": "explore", "status": "succeeded", "result_ref": "runs/r1/results/explore--01.json", "result_digest": digest}
    failed_latest = {**ok, "attempt_id": "a2", "status": "failed", "result_ref": None, "result_digest": None}
    stale_bytes = {**ok, "result_digest": "0" * 64}
    gone = {**ok, "result_ref": "runs/r1/results/gone.json"}
    assert recipes._reuse(run_of(ok), "explore")["reused"] is True
    assert recipes._reuse(run_of(ok, failed_latest), "explore") is None, "the latest attempt decides (P05)"
    assert recipes._reuse(run_of(stale_bytes), "explore") is None, "the frozen bytes must hash to the digest (P04)"
    assert recipes._reuse(run_of(gone), "explore") is None, "a vanished result is not reusable"


async def test_abandon_blocked_writes_off_intended_and_unresolved(tmp: Path) -> None:
    os.environ["PRIME_WORKFLOW_STATE"] = str(tmp)
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    pw._clear_epoch()

    from prime_workflow import recipes
    host.projection = {
        "run_id": "r1",
        "status": "running",
        "state_version": 4,
        "attempts": {
            "a-intended": {"attempt_id": "a-intended", "step_id": "explore", "status": "intended"},
            "a-unresolved": {"attempt_id": "a-unresolved", "step_id": "verify", "status": "unresolved"},
            "a-succeeded": {"attempt_id": "a-succeeded", "step_id": "judge", "status": "succeeded"},
        },
        "steps": {"explore": ["a-intended"], "verify": ["a-unresolved"], "judge": ["a-succeeded"]},
    }
    try:
        written = await recipes.abandon_blocked("r1", reason="children died with the crashed session")
        assert written == [
            {"attempt_id": "a-intended", "step_id": "explore"},
            {"attempt_id": "a-unresolved", "step_id": "verify"},
        ], written
        settled = [r["payload"] for r in host.requests if r["operation"] == "attempt.settle"]
        assert [row["status"] for row in settled] == ["abandoned", "abandoned"], settled
        assert all("crashed session" in row["note"] for row in settled), settled
    finally:
        await host.stop()


async def test_role_drift_between_pin_and_spawn_books_unresolved(tmp: Path) -> None:
    host = FakeHost(tmp / "workflow-state")
    await host.start()
    install_fake_rlm([])

    class DriftingRoles:
        CANONICAL_ROLES = ["sota-verify"]

        @staticmethod
        def bind_role(role):
            return types.SimpleNamespace(role=role, role_sha256="a" * 64,
                wire=lambda: {"role": role, "role_sha256": "a" * 64,
                              "manifest_sha256": "b" * 64, "contract_sha256": "c" * 64})

        @staticmethod
        async def spawn(role: str, task: str, **kwargs):
            return types.SimpleNamespace(handle=types.SimpleNamespace(child_id="sub-1"), binding="changed-sha")

    install_module("roles", CANONICAL_ROLES=DriftingRoles.CANONICAL_ROLES,
                   bind_role=DriftingRoles.bind_role, spawn=DriftingRoles.spawn)
    pw._clear_epoch()
    try:
        try:
            await pw.spawn("r1", "verify", "Check it", role="sota-verify")
        except pw.WorkflowError as error:
            assert "changed between pinning and spawning" in str(error), error
        else:
            raise AssertionError("a drifting role must not be admitted silently")
        booking = next(r for r in host.requests if r["operation"] == "attempt.unresolved")
        assert isinstance(booking.get("expected_run_version"), int), "the write-off must carry the run version"
        assert "changed between pinning and spawning" in booking["payload"]["reason"]
    finally:
        sys.modules.pop("roles", None)
        await host.stop()


async def test_broken_replies_surface_as_workflow_error(tmp: Path) -> None:
    os.environ["PRIME_WORKFLOW_STATE"] = str(tmp)
    socket = pw.socket_path()
    socket.parent.mkdir(parents=True, exist_ok=True)

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            line = await reader.readline()
            if not line:
                return
            reply = json.loads(line.decode("utf-8"))
            if reply.get("operation") == "nothing":
                writer.write(b"null\n")
            elif reply.get("operation") == "drop":
                writer.close()
                return
            elif reply.get("operation") == "ping":
                writer.write((json.dumps({"v": pw.PROTOCOL, "ok": True, "result": {}}) + "\n").encode())
            else:
                writer.write((json.dumps({"v": pw.PROTOCOL, "ok": True, "result": {"epoch": "7", "state_root": str(tmp)}}) + "\n").encode("utf-8"))
            await writer.drain()

    server = await asyncio.start_unix_server(serve, path=str(socket))
    pw._clear_epoch()
    try:
        try:
            await pw.call("nothing", {})
        except pw.WorkflowError as error:
            assert "not an object" in str(error), error
        else:
            raise AssertionError("a null reply is not a result")
        try:
            await pw.ping()
        except pw.WorkflowError as error:
            assert "without an epoch" in str(error), error
        except Exception as error:  # noqa: BLE001 - the point is that it must NOT be a KeyError
            raise AssertionError(f"a missing epoch must surface as WorkflowError, not {type(error).__name__}") from error
        else:
            raise AssertionError("a ping without an epoch is not a valid reply")
        try:
            await pw.call("drop", {})
        except pw.WorkflowError as error:
            assert "closed the connection" in str(error) or "connection failed" in str(error), error
        except Exception as error:  # noqa: BLE001 - a dropped connection must not leak OSError
            raise AssertionError(f"a dropped connection must surface as WorkflowError, not {type(error).__name__}") from error
        else:
            raise AssertionError("a dropped connection is not a result")
    finally:
        server.close()


async def test_ensure_run_recovers_from_a_create_race(tmp: Path) -> None:
    os.environ["PRIME_WORKFLOW_STATE"] = str(tmp)
    socket = pw.socket_path()
    socket.parent.mkdir(parents=True, exist_ok=True)
    from prime_workflow import recipes

    steps = [{"id": phase, "prompt": recipes.PROMPTS[phase]} for phase in recipes.PHASES]
    digest = recipes.definition_digest(steps)
    reads = {"count": 0}

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            line = await reader.readline()
            if not line:
                return
            request = json.loads(line.decode("utf-8"))
            if request.get("operation") == "ping":
                reply = {"v": pw.PROTOCOL, "ok": True, "result": {"epoch": "7", "state_root": str(tmp)}}
            elif request.get("operation") == "run.read":
                reads["count"] += 1
                if reads["count"] == 1:
                    reply = {"v": pw.PROTOCOL, "ok": False, "error": {"class": "WorkflowStateError", "message": "no run r1"}}
                else:
                    reply = {
                        "v": pw.PROTOCOL, "ok": True,
                        "result": {
                            "run_id": "r1",
                            "state_version": 2,
                            "status": "running",
                            "subject": {"project": str(tmp)},
                            "definition_digest": digest,
                        },
                    }
            elif request.get("operation") == "run.create":
                reply = {"v": pw.PROTOCOL, "ok": False, "error": {"class": "WorkflowStateError", "message": "run r1 already exists"}}
            else:
                reply = {"v": pw.PROTOCOL, "ok": False, "error": {"class": "WorkflowStateError", "message": f"unexpected {request.get('operation')}"}}
            writer.write((json.dumps(reply) + "\n").encode("utf-8"))
            await writer.drain()

    server = await asyncio.start_unix_server(serve, path=str(socket))
    pw._clear_epoch()
    try:
        await recipes._ensure_run(run_id="r1", resume=True, steps=steps, specs=[{"id": "explore"}], subject={"project": str(tmp)})
        assert reads["count"] == 2, "the race must end in a fresh run.read, not in a failed recipe"
    finally:
        server.close()


if __name__ == "__main__":


    asyncio.run(main())
