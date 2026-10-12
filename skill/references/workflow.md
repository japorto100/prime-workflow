# workflow — Details

- **Tools:** `workflow_save`, `workflow_plan`, `workflow_plan_from_tasks`, `workflow_record`
  (Extension `extensions/workflow/`).
- **Gate:** `PRIME_WORKFLOW=on` (off-unless; der Legacy-Alias `PRIME_LAB_WORKFLOW` oeffnet weiter und schreibt eine Deprecation-Quittung).
- **Planner-Vertrag:** `PLANNER-CONTRACT-v2.md` beim Parser — `planFromTasks()` wirft verbotene Formen
  mit Schlüssel und Zeile, bevor etwas gespeichert wird.
- **Abgrenzung:** langes *Werk* → `background` (`bg_run`); wiederholbares *Vorgehen* → `workflow`.
- **Lauf-Vertrag:** deklarierte Steps + Deps vor dem Claim, Ergebnis einmalig akzeptiert, `run.finish`
  verweigert `done` bei offenen Steps; Artefaktpfade sind pro Lauf eindeutig
  (`<artifacts>/<run_id>/<step>--<8hex>.json`); Run-Budget: ohne eigene Angabe gilt
  `max_agents_total=50` (im Journal aufgezeichnet). Vertrag + Nachfix-Evidenz: `../../WORKFLOW_OPEN_LIVE.md` §3a/§3b.
- **Offen (vorgemerkt):** `workflow` ist heute **lab-facing**; evtl. Umbenennung für labs + Neuaufbau
  als Prime-Extension — siehe `OPEN-ITEMS §22`.
- **Discovery getrennt:** `src/workflow/__init__.py` ist die installierte Prime-Facade,
  `src/prime_workflow/` bleibt die Implementierung. Offline-Wheel/Importtest ist
  nicht gleich Fresh-Kernel-Admission. Der Bridge-Client hat einen 10-s-Deadline;
  Timeout/Verbindungsverlust sind unbekannte Mutation, nie ein Retry-Signal.
