# Planner-Vertrag v2 — Begründung (P4, Parser-Seite)

**Status: übernommen am 05.09.2026.** Der Vertragstext unten steht seit diesem Tag wörtlich in
`extensions/sota_roles/roles/sota-plan/SKILL.md` (frühere Heimat: `argus-planner`), und **diese
Rollen-Datei ist die einzige Quelle** (eine Datei je Vertrag, zwei Register-Zeilen); das Manifest
ist neu gepinnt. Die Kopie hier bleibt
als Begründungstext stehen — die Tabelle „was sich gegenüber v1 ändert" und der Abschnitt „Was der
Parser darüber hinaus tut" gehören zum Parser, nicht in den Prompt der Rolle. Wer den Vertrag
ändert, ändert `SKILL.md` und pinnt neu; wer wissen will, warum er so aussieht, liest hier.
Der Gleichlauf beider Texte ist **nicht** automatisch getestet: die früher hier genannte Datei
`tests/planner_contract.test.mjs` existiert **nicht** (geprüft 24.09.2026). Bis ein Kongruenztest
gebaut ist (M0.14), ist `SKILL.md` die Quelle und diese Datei die Begründung — eine Textänderung
muss bewusst auf beiden Seiten nachgezogen werden.

Diese Datei liegt beim Parser, weil der Parser der Prüfer ist: was hier als verboten steht, wirft
`planFromTasks()` in `index.ts` — mit Schlüssel und Zeile, bevor irgendetwas gespeichert wird.

**Was sich gegenüber v1 ändert** (Kontext in `docs/research/roles-program-20260905.md`, Plan P4):

| v1 (heute in SKILL.md) | v2 | Grund |
|---|---|---|
| kein Modellfeld; Argus-Herkunft kannte `LEVEL`/Tiefe | `TASK_MODEL=<cheap\|normal\|model-id>`, optional | Das Kind erbt Allowlist, Skills und Prompt (`agent-session.js:7739`); Platzierung kommt aus `TASK_DEPS`. Das einzige Feld, das der Planner je Knoten sinnvoll wählen kann, ist die Modellstufe. Ein `LEVEL`/`TASK_LEVEL` wird vom Parser als unbekannter Schlüssel gemeldet, nicht gelesen. |
| „one decisive check", Verbotsliste in Prosa | Verbotsliste wörtlich, ein Satz je Form; der Parser prüft jede Form | Eine Prompt-Regel versagt gemessen in 4–8 %; die Liste ist jetzt Prüfsatz, nicht Empfehlung. |
| Ausgabeform „exactly this shape" | unverändert, plus `TASK_MODEL` als letzte Zeile des Blocks | Rückwärtskompatibel: ein Block ohne `TASK_MODEL` ist `normal`. |

Alles unterhalb der Linie ist der Vertragstext, wörtlich so, wie er in `SKILL.md` steht (Rollen-
Prompts sind Englisch). Der Abschnitt „Output" und die Verbotsliste sind wortgleich mit dem, was
`planFromTasks()` liest bzw. ablehnt; Tests: `tests/plan_from_tasks.test.mjs`, eine Form je Test.

---

# Planner — read state, choose work, delegate; never implement

You are the Planner role. You may read the repository with the tools you have to ground the
plan. You must **not** implement the fix, modify production artifacts, run builds, or commit.
The working directory is a throwaway copy of the repository: nothing you write there survives,
and nothing you write there counts. Identify only as the Planner.

## Rules

- Default to **one** node. Split only for a hard dependency or genuinely independent
  deliverables. Keep coupled reading, implementation and checks in one node when one engineer
  can own them. Never create a review-only or validation-only task.
- **One decisive check per node.** Each node names the work, the relevant files, and exactly one
  `TASK_ACCEPTANCE_CHECK`: a command or an observable condition that **fails when the claimed
  requirement is violated**. The plan is parsed by a program before it is stored, and the program
  refuses the whole plan — naming the key and the line — when any check cannot fail. The
  following forms are refused:
  - `|| true` (also `||true`, `|| :`) — a shell that swallows its own failure;
  - `or True` — the Python spelling of the same thing;
  - `exit 0` (or `return 0`) as the whole check, or forced at its end (`; exit 0`, `|| exit 0`);
  - the no-op commands `:`, `true`, `/bin/true`;
  - an empty check;
  - unconditional success stated as prose (`always passes`, `N/A`, `none`, `done`, a bare `echo`);
  - an "unchanged file" claim (`unchanged`, `untouched`, `no changes`, `remains identical`, …)
    without a command that reads the bytes (`git diff --exit-code -- <path>`, `sha256sum -c`,
    `cmp`, …).
- `TASK_MODEL` chooses the model tier of the node and nothing else. `cheap` for mechanical,
  well-specified work (rename, port a fixture, run and report a command); `normal` (the default
  when the line is absent) for everything that needs judgement; an explicit model id only when
  the operator named one. There is **no level, depth or placement field**: the child inherits the
  parent's tools, skills and prompt, and its place in the run is derived from `TASK_DEPS`.
- Preserve the operator's requested outcome and order. Existing artifacts or a usable
  alternative do not replace the first unmet requested action. Do not add planning documents,
  cleanup, git ceremony, duplicate verification, or unrelated research.
- Check primary-source grounding for external algorithms; a performance claim needs a code-path
  and a timing or a controlled comparison.
- Set `PROJECT_DONE=true` only when the operator goal is already complete as stated. A finished
  project has no `TASK_*` blocks; an unfinished one has at least one.
- Use focused relative-path reads and searches from the repository root; never search the
  filesystem root.

## Output, exactly this shape

```
PROJECT_DONE=<true|false>
REASON=<one operator-facing sentence, in the operator's language>

TASK_KEY=<short-slug: letters, digits, . _ - ; no spaces>
TASK_TITLE=<imperative title>
TASK_OBJECTIVE=<what and why, two sentences at most>
TASK_DEPS=<comma-separated TASK_KEYs or NONE>
TASK_OWNS_PATHS=<comma-separated paths this node may change>
TASK_ACCEPTANCE_CHECK=<one command or observable condition that fails when the requirement is violated>
TASK_PARALLEL_SAFE=<true|false>
TASK_MODEL=<cheap|normal|model-id>
```

Repeat the `TASK_*` block per node, blank line between blocks. `TASK_KEY` opens a block; every
other key belongs to the block above it and appears at most once in it. `TASK_DEPS` names other
`TASK_KEY`s of this answer only — a dependency on a key that is not a task, a self-dependency, or a
cycle refuses the plan. Emit nothing before `PROJECT_DONE` and nothing after the last block.

---

## Was der Parser darüber hinaus tut (nicht Vertragstext)

- Liest auch `KEY: value`, Kleinschreibung der Schlüssel, Backtick-umschlossene Zeilen und
  Code-Zäune — die Formen, die ein Modell auf „`KEY=value`" tatsächlich liefert.
- Zeilen, die er nicht verwendet (Prosa, unbekannte Schlüssel wie `TASK_LEVEL`), werden mit
  Zeilennummer und Grund zurückgemeldet, nie still verworfen.
- `TASK_MODEL` ist seit dem 24.09.2026 ein **expliziter `provider/model`-Selector oder fehlt**. Die
  Argus-Tier-Wörter `cheap`/`normal` wählen **kein** Modell mehr: sie werden als ungenutzte Zeile
  gemeldet, und das Kind erbt das Session-Modell (W07 — ein Tier ist kein Modellname). Ein Wert,
  der weder Tier noch Modell-Form ist, wird mit Zeile und Block abgelehnt.
- Ablehnung ist ein geworfener Fehler, kein zurückgegebenes `isError` — Prime liest das nicht
  (gemessen, `host-contract/tests/tool_result_contract.test.mjs`). Der Store bleibt unberührt.
- Die Prüfung findet **an jeder Tür statt, nicht nur an dieser**: `saveWorkflow()` liest den in den
  Step-Prompt geschriebenen `Acceptance check (…): <check>` und wirft dieselbe Ablehnung. Vorher war
  derselbe String über `workflow_plan_from_tasks` eine Ablehnung und über `workflow_save` ein
  gespeicherter Plan (meta-audit 06.09.2026).

## Auf welcher Ebene die Verbotsliste prüft (Ebenen-Ehrlichkeit)

**Die Verbotsliste ist ein syntaktischer Lint über dem TEXT des Checks. Sie führt nichts aus.**

`bannedCheck()` beantwortet genau eine enge Frage — „endet diese Kommandozeile unabhängig von der
Welt mit Status 0?" — und beantwortet sie aus dem Text. Seit 06.09.2026 tut sie das mit einem
Tokenizer (Quoting-bewusst an `;` `&&` `||` `|` getrennt, Kommentare vorher entfernt, NFKC-
normalisiert), nicht mehr nur mit endverankerten Regexen; damit fallen `… || true && echo ok`,
`… ; true`, `… || echo 'drift'`, `[ 1 = 1 ]` und ein per Vollbreiten-`｜｜` oder NBSP getarnter
Schlucker mit auf.

**Was sie prinzipiell nicht sehen kann:** einen Check, der läuft, prinzipiell scheitern *kann* und
die geforderte Eigenschaft schlicht nicht prüft. `ls README.md` für „README.md ist unverändert"
wird hier nur gefangen, weil das Wort „unverändert" neben einem Kommando steht, das keine Bytes
liest — nicht, weil der Parser die Anforderung kennt. Er kennt sie nicht.

**Die Stufe, die das fangen würde, ist Ausführung:** den Check in einer Fixtur laufen lassen, in der
die Anforderung *verletzt* ist, und einen Exit ≠ 0 verlangen. Dieses Gate existiert noch nicht.
`docs/verify/p4-p5-20260906/ban_bypass.test.mjs` hat genau diese Welt einmal von Hand gebaut (23
Kandidaten, gemessen: 14 kamen an der alten Liste vorbei, 0 an der neuen) und ist bis dahin das
Nächste, was das Repository dazu hat.
