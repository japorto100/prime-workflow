"""Discovery regression for the workflow Python skill (audit finding V2-R18, candidate 002).

Pins the three things the finding was about:

1. Discovery name/layout: skill directory, frontmatter name and import layout agree
   (``workflow`` -> ``src/workflow/__init__.py``), and the hatch package list ships both
   the facade and the implementation, with ``requires-python`` unchanged.
2. API identity: ``workflow`` re-exports the *same objects* as ``prime_workflow`` (no copies,
   no second state); ``prime_workflow.recipes`` remains the implementation submodule.
3. Real wheel: the built wheel contains both packages and both import from the wheel archive
   in an isolated interpreter (zipimport; no install, no network).

Run: python3 tests/test_facade_discovery.py
Exit codes: 0 = passed, 1 = failed, 2 = NOT_RUN (uv missing; a started build that fails is FAIL/1).
This is an offline staging check, not a fresh-kernel live proof.
"""

from __future__ import annotations

import inspect
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SKILL_NAME = "workflow"
IMPORT_NAME = SKILL_NAME.replace("-", "_")
EXPORTS = [
    "WorkflowError",
    "state_root",
    "socket_path",
    "call",
    "ping",
    "run_create",
    "run_status",
    "ready",
    "abandon",
    "finish",
    "expand",
    "list_roles",
    "spawn",
    "collect",
    "reconcile",
    "accept_result",
    "settle",
    "unresolved",
    "run",
]
ASYNC_EXPORTS = {
    "call",
    "ping",
    "run_create",
    "run_status",
    "ready",
    "abandon",
    "finish",
    "expand",
    "spawn",
    "collect",
    "reconcile",
    "accept_result",
    "settle",
    "unresolved",
}


def check_layout() -> list[str]:
    failures: list[str] = []
    if ROOT.name != SKILL_NAME:
        failures.append(f"skill directory is {ROOT.name!r}, expected {SKILL_NAME!r}")
    skill_md = ROOT / "SKILL.md"
    if not skill_md.exists():
        failures.append("SKILL.md is missing")
    else:
        text = skill_md.read_text()
        name_match = re.search(r"^name:\s*(\S+)\s*$", text, re.MULTILINE)
        if not name_match or name_match.group(1) != SKILL_NAME:
            found = name_match.group(1) if name_match else None
            failures.append(f"SKILL.md frontmatter name is {found!r}, expected {SKILL_NAME!r}")
        desc_match = re.search(r"^description:\s*(\S.*)$", text, re.MULTILINE)
        description = desc_match.group(1).strip() if desc_match else ""
        if not 0 < len(description) <= 1024:
            failures.append("SKILL.md frontmatter description is missing or out of range")
    init = SRC / IMPORT_NAME / "__init__.py"
    if not init.exists():
        failures.append(f"src/{IMPORT_NAME}/__init__.py (official detection layout) is missing")
    pyproject = ROOT / "pyproject.toml"
    if not pyproject.exists():
        failures.append("pyproject.toml is missing")
    else:
        data = tomllib.loads(pyproject.read_text())
        packages = (
            data.get("tool", {})
            .get("hatch", {})
            .get("build", {})
            .get("targets", {})
            .get("wheel", {})
            .get("packages", [])
        )
        for package in (f"src/{IMPORT_NAME}", "src/prime_workflow"):
            if package not in packages:
                failures.append(f"hatch wheel packages missing {package!r}: {packages!r}")
        requires = data.get("project", {}).get("requires-python")
        if requires != ">=3.12":
            failures.append(f"requires-python changed: {requires!r} (must stay >=3.12)")
    return failures


def check_source_identity() -> list[str]:
    failures: list[str] = []
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    import prime_workflow
    import prime_workflow.recipes as recipes
    import workflow

    if workflow.__name__ != SKILL_NAME:
        failures.append(f"workflow.__name__ is {workflow.__name__!r}")
    if set(getattr(workflow, "__all__", ())) != set(EXPORTS):
        failures.append("workflow.__all__ does not match the 19 audited public exports")
    public = {name for name in dir(workflow) if not name.startswith("_")}
    if public != set(EXPORTS):
        failures.append(f"facade exposes more/less than the audited API: {sorted(public ^ set(EXPORTS))}")
    for name in EXPORTS:
        if getattr(workflow, name, None) is not getattr(prime_workflow, name, None):
            failures.append(f"{name} is not the same object in workflow and prime_workflow")
    for name in sorted(ASYNC_EXPORTS):
        if not inspect.iscoroutinefunction(getattr(workflow, name)):
            failures.append(f"{name} is not a coroutine function through the facade")
    if recipes.__name__ != "prime_workflow.recipes":
        failures.append(f"implementation submodule moved: {recipes.__name__!r}")
    return failures


def check_wheel() -> tuple[str, list[str], str]:
    """Return (status, failures, detail); status is PASS, FAIL or NOT_RUN."""
    uv = shutil.which("uv")
    if uv is None:
        return "NOT_RUN", [], "uv not found on PATH"
    out = Path(tempfile.mkdtemp(prefix="wf-wheel-"))
    try:
        proc = subprocess.run(
            [uv, "build", "--offline", "--wheel", "--no-progress", "--no-create-gitignore", "-o", str(out), str(ROOT)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout).strip().splitlines()[-5:]
            return "FAIL", [f"uv build started but failed (exit {proc.returncode}): " + " | ".join(tail)], ""
        wheels = sorted(out.glob("*.whl"))
        if len(wheels) != 1:
            return "FAIL", [f"expected exactly one wheel, found {[wheel.name for wheel in wheels]}"], ""
        wheel = wheels[0]
        failures: list[str] = []
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            name_set = set(names)
            for required in ("workflow/__init__.py", "prime_workflow/__init__.py", "prime_workflow/recipes.py"):
                if required not in name_set:
                    failures.append(f"wheel misses {required}")
            if any(name.startswith(("tests/", "src/")) for name in names):
                failures.append("wheel leaks source/test paths")
            if any(name.endswith(".pyc") for name in names):
                failures.append("wheel contains .pyc files")
            metadata = next((name for name in names if name.endswith(".dist-info/METADATA")), None)
            if metadata is None:
                failures.append("wheel has no METADATA")
            elif "Requires-Python: >=3.12" not in archive.read(metadata).decode("utf-8"):
                failures.append("wheel METADATA does not pin Requires-Python: >=3.12")
        code = (
            "import sys\n"
            f"wheel = {str(wheel)!r}\n"
            "sys.path.insert(0, wheel)\n"
            "import workflow, prime_workflow\n"
            f"exports = {EXPORTS!r}\n"
            "assert workflow.__name__ == 'workflow', workflow.__name__\n"
            "assert workflow.__file__.startswith(wheel), workflow.__file__\n"
            "assert prime_workflow.__file__.startswith(wheel), prime_workflow.__file__\n"
            "for name in exports:\n"
            "    assert getattr(workflow, name) is getattr(prime_workflow, name), name\n"
            "print('isolated wheel import ok:', len(exports), 'exports, identical objects')\n"
        )
        proc = subprocess.run(
            [sys.executable, "-I", "-c", code],
            cwd=str(out),
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            last = (proc.stderr.strip().splitlines() or [""])[-1]
            failures.append("isolated wheel import failed: " + last)
        if failures:
            return "FAIL", failures, ""
        return "PASS", [], proc.stdout.strip()
    finally:
        shutil.rmtree(out, ignore_errors=True)


def main() -> int:
    failures = check_layout() + check_source_identity()
    status, wheel_failures, detail = check_wheel()
    if status == "NOT_RUN":
        print("NOT_RUN (wheel content/import): " + detail)
        for failure in failures:
            print("FAIL:", failure)
        return 2 if not failures else 1
    failures += wheel_failures
    if failures:
        for failure in failures:
            print("FAIL:", failure)
        return 1
    print(f"ok: discovery layout + API identity ({len(EXPORTS)} exports); {detail}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
