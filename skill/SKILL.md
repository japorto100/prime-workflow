---
name: workflow
description: Named, validated orchestration programs — save/plan/plan_from_tasks/record a workflow instead of ad-hoc tool chains. Use for repeatable, multi-step procedures.
---

# workflow — benannte Arbeitsprogramme

- Programm zuerst **registrieren** (`workflow_save` / `workflow_plan`), dann ausführen, mit
  `workflow_record` fortschreiben.
- `workflow_plan_from_tasks` erzeugt Steps aus einer Aufgabenliste (Argus-`TASK_*`-Blöcke) —
  **eine Welle je Dependency-Level**.
- Es prüft die zwei Fehler, die ad-hoc-Orchestrierung macht: Abhängigkeit auf einen **nicht
  existierenden Schritt** und **Zyklen**; Acceptance-Checks, die nicht fehlschlagen können, werden
  **vor** dem Speichern abgelehnt.
- Persistenz über den Store; Gate `PRIME_WORKFLOW=on` (Legacy-Alias `PRIME_LAB_WORKFLOW`, Deprecation-Quittung).
- **Fortsetzbarer Lauf:** Steps werden beim `run_create` deklariert (Claimes nur auf deklarierte
  Steps), `succeeded` setzt ein akzeptiertes Ergebnis voraus, Ergebnisse sind einmalig; Resume
   prüft Subjekt + Definition und verwendet nur den jüngsten verifizierten Versuch.
- Python-API: `import workflow` (Facade von `prime_workflow`); `await workflow.run()`
  fragt nur die laufende Bridge ab, es startet keinen Run. Paket auf Disk ist
  noch kein Beweis für eine im frischen Kernel aktivierte Bridge.
- Bridge-Deadline: nach 10 s ohne vollständige Antwort `WorkflowError` mit
  `code="TRANSPORT_UNKNOWN"`; **kein** automatischer Claim-/Spawn-Retry. Nur eine
  explizit als `STALE_DRIVER_EPOCH` verweigerte Operation wird mit derselben
  Operation-ID und dem neu abgefragten Writer-Epoch genau einmal erneut gesendet.
- Rollen: `roles.bind_role` prüft Manifest + Rolle vor `attempt.claim`;
  `roles.spawn` muss dieselbe Bindung hostseitig bestätigt bekommen. Fehlt der
  Hostvertrag, keinen Rollen-Spawn als erfolgreich/autorisiert melden.

Faustregel: benanntes Programm statt Aktionismus — wiederholbar, prüffähig, kompaktierbar.

Details: [`references/workflow.md`](references/workflow.md).
