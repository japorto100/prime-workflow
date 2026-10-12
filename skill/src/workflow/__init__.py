"""Prime discovery facade; the implementation and state remain in prime_workflow.

The skill name is ``workflow`` so Prime discovers ``src/workflow/__init__.py``.
Existing callers importing ``prime_workflow`` remain supported.
"""
from prime_workflow import (
    WorkflowError,
    state_root,
    socket_path,
    call,
    ping,
    run_create,
    run_status,
    ready,
    abandon,
    finish,
    expand,
    list_roles,
    spawn,
    collect,
    reconcile,
    accept_result,
    settle,
    unresolved,
    run,
)

__all__ = ['WorkflowError', 'state_root', 'socket_path', 'call', 'ping', 'run_create', 'run_status', 'ready', 'abandon', 'finish', 'expand', 'list_roles', 'spawn', 'collect', 'reconcile', 'accept_result', 'settle', 'unresolved', 'run']
