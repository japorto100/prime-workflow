"""Adapter fault boundaries; the real native Invoke lifecycle is qualified separately."""
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prime_workflow as pw
from prime_workflow.invocations import invoke_step


class InvokeAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.events = []
        self.attempt = {}
        self.journal = {"state_version": 1, "steps": {}, "attempts": {"wf-attempt": self.attempt}}
        self.receipt = {"version": 1, "state": "started", "call_id": "native-call",
                        "attempt_id": "native-attempt", "child_id": "child"}
        self.source = Path(self.temp.name) / "native.json"
        self.source.write_text(json.dumps({"t": "value", "v": False}))
        self.result = {**self.receipt, "state": "returned", "present": True, "value": False,
                       "durability": "value", "result_ref": str(self.source),
                       "digest": hashlib.sha256(self.source.read_bytes()).hexdigest()}
        self.fault = None
        self.envelope = {}

        async def status(_run):
            return self.journal

        async def call(op, payload, **kwargs):
            self.events.append(op)
            if self.fault == op:
                raise RuntimeError("lost response")
            if op == "attempt.claim":
                self.attempt["status"] = "intended"
                self.journal["steps"]["step"] = ["wf-attempt"]
                return {"attempt_id": "wf-attempt"}
            if op == "attempt.admit":
                self.assertEqual(payload["child_id"], "child")
                self.attempt["status"] = "admitted"
            return {}

        async def start(_prompt, **kwargs):
            self.events.append("native.start")
            self.assertEqual(kwargs["workflow_binding"]["attempt_id"], "wf-attempt")
            if self.fault == "native.start":
                raise RuntimeError("admission unknown")
            return SimpleNamespace(receipt=self.receipt, wait=wait)

        async def wait():
            self.events.append("native.wait")
            self.assertEqual(self.attempt["status"], "admitted")
            return self.result

        async def accept(_run, aid, source, **kwargs):
            self.events.append("result.accept")
            self.assertEqual(aid, "wf-attempt")
            self.envelope = json.loads(Path(source).read_text())
            if self.fault == "result.accept":
                raise RuntimeError("response unknown")
            return {"digest": "workflow-digest", "result_ref": "frozen-result"}

        async def settle(_run, _attempt, state, **kwargs):
            self.events.append("attempt.settle")
            self.attempt["status"] = state
            if self.fault == "attempt.settle":
                raise RuntimeError("committed response lost")
            return {}

        async def unresolved(*args):
            self.events.append("attempt.unresolved")
            self.attempt["status"] = "unresolved"

        replacements = {"_rlm": lambda: SimpleNamespace(invoke=SimpleNamespace(start=start)),
                        "run_status": status, "call": call, "accept_result": accept,
                        "settle": settle, "unresolved": unresolved}
        for name, replacement in replacements.items():
            patcher = patch.object(pw, name, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_full_false_value_admitted_before_wait_then_accepted_once(self):
        result = await invoke_step("run", "step", "inspect")
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(self.envelope["value"], {"t": "value", "v": False})
        self.assertNotEqual(result["attempt_id"], result["invoke_attempt_id"])
        self.assertEqual(self.events, ["attempt.claim", "native.start", "attempt.admit",
                                      "native.wait", "result.accept", "attempt.settle"])

    async def test_unknown_start_or_admission_does_not_respawn_or_accept(self):
        for point in ("native.start", "attempt.admit"):
            with self.subTest(point=point):
                self.events.clear()
                self.journal["steps"] = {}
                self.fault = point
                with self.assertRaises(RuntimeError):
                    await invoke_step("run", "step", "inspect")
                self.assertEqual(self.events.count("native.start"), 1)
                self.assertNotIn("result.accept", self.events)
                self.assertEqual(self.attempt["status"], "unresolved")

    async def test_preview_wrong_identity_and_changed_artifact_never_accepted(self):
        for variation in ("preview", "identity", "digest"):
            with self.subTest(variation=variation):
                self.journal["steps"] = {}
                self.events.clear()
                original = dict(self.result)
                if variation == "preview":
                    self.result.pop("present")
                    self.result["answer_preview"] = "looks correct"
                if variation == "identity":
                    self.result["attempt_id"] = "foreign"
                if variation == "digest":
                    self.result["digest"] = "0" * 64
                with self.assertRaises(pw.WorkflowError):
                    await invoke_step("run", "step", "inspect")
                self.assertNotIn("result.accept", self.events)
                self.result = original

    async def test_existing_attempt_requires_reconciliation_without_new_claim(self):
        self.journal["steps"]["step"] = ["old"]
        with self.assertRaisesRegex(pw.WorkflowError, "reconciliation"):
            await invoke_step("run", "step", "inspect")
        self.assertEqual(self.events, [])

    async def test_settle_ack_loss_never_reopens_known_success(self):
        self.fault = "attempt.settle"
        with self.assertRaises(RuntimeError):
            await invoke_step("run", "step", "inspect")
        self.assertEqual(self.attempt["status"], "succeeded")
        self.assertNotIn("attempt.unresolved", self.events)


if __name__ == "__main__":
    unittest.main()
