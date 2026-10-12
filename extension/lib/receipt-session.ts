/**
 * Wer eine Quittungszeile geschrieben hat — aus dem `ctx` des schreibenden Handlers, ohne Zustand.
 *
 * **Warum diese Datei zustandslos ist, und zwar zwingend.**
 *
 * Der erste Anlauf (B-05, 09.09.2026) hielt die Session in einer modulweiten Variablen, die ein
 * `agent_start`-Handler nachführte. Das ist falsch, sobald ein RLM-Kind im selben Prozess läuft —
 * und genau das tut `roles.spawn`. Gemessen am 0.9.4-Bundle
 * (`~/.local/share/mise/installs/node/24.16.0/lib/node_modules/prime-agent/dist/bundle`):
 *
 * - `chunk-TOACIHN2.js:58734` `_createInlineRlmSubagentRuntime` gibt dem Kind
 *   `resourceLoader: this._resourceLoader` (`:58773`) — den Loader des Elternteils.
 * - `chunk-TOACIHN2.js:58409` `const extensionsResult = this._resourceLoader.getExtensions()`:
 *   die Kind-`AgentSession` bekommt damit **dieselben** Extension-Objekte.
 * - `chunk-TOACIHN2.js:38413` `loadExtensionModule` baut zwar pro Aufruf ein frisches jiti mit
 *   `moduleCache: false` (`:38416`) — aber aufgerufen wird es nur aus `resourceLoader.reload()`, und das
 *   Kind lädt nicht neu. Parent und Inline-Kind teilen also **eine** Modulinstanz.
 * - `chunk-TOACIHN2.js:58418` `new ExtensionRunner(extensionsResult.extensions, …, this.sessionManager, …)`:
 *   pro Session ein eigener Runner, aber über denselben Handler-Closures. Der Unterschied zwischen
 *   Parent und Kind steckt **ausschliesslich im `ctx`**, nirgends sonst.
 *
 * Folge des alten Entwurfs: das `agent_start` des Kindes überschreibt die Variable, und danach
 * tragen die Quittungen des Elternteils die Session-ID und die Tiefe des Kindes — genau die
 * Verwechslung, die B-05 beheben sollte, eine Ebene tiefer. Ein „aktuelle Session"-Cache kann das
 * auch grundsätzlich nicht leisten: der Parent **wartet** auf das Kind, beide Handler-Ketten sind
 * gleichzeitig offen, es gibt in diesem Moment keine einzelne aktuelle Session.
 *
 * Deshalb: kein Zustand. Wer schreibt, reicht seinen `ctx` durch.
 *
 * **`null` heisst „nicht bekannt", nie „der Elternteil" und nie „Tiefe 0".** Quittungen, die vor
 * dem ersten Handler entstehen (Konfiguration beim Laden, `*_armed`), haben strukturell keine
 * Session; sie tragen `null` und ein Leser muss sie ausschliessen, nicht raten.
 */

/** Die Herkunft einer Quittungszeile. Beide Felder `null`, wenn kein `ctx` vorlag. */
export type ReceiptSession = { id: string | null; depth: number | null };

/** Der Zustand „kein Handler-Kontext" — geteilt, damit ihn niemand als `{}` nachbaut. */
export const UNKNOWN_SESSION: ReceiptSession = Object.freeze({ id: null, depth: null }) as ReceiptSession;

/** Die Form, die wir von einem HostContext lesen. Alles optional: Prime reicht nicht überall alles durch. */
export type SessionCarrier = {
	sessionManager?: {
		getSessionId?: () => string | undefined;
		getHeader?: () => { rlmDepth?: number } | undefined;
	};
};

/**
 * Liest Session-ID und RLM-Tiefe aus dem `ctx` eines Handlers.
 *
 * **Die Tiefe kommt aus dem Session-Header, nicht aus der Umgebung** (Korrektur 09.09., sota-verify
 * F3): `PRIME_AGENT_RLM_DEPTH` existiert in Prime 0.9.4 nicht — 0 Treffer im gesamten dist. Die
 * Quelle, die zwei Geschwisterpakete im selben Repo längst benutzen, ist
 * `ctx.sessionManager.getHeader()?.rlmDepth` (`refinement-shadow/runtime.ts:118`,
 * `refinement-proposal-v2/runtime.ts:871`).
 *
 * Jeder Zugriff einzeln abgesichert: `getSessionArtifactDir` wirft nachweislich, wenn die Session
 * nicht aktiv ist, und optionales Verketten hilft gegen einen werfenden Getter nicht. Für die
 * beiden hier gelesenen Getter ist dasselbe nicht ausgeschlossen, also wird es gleich behandelt.
 * Ein Fehler ergibt `null` — geraten wird nichts.
 */
export function sessionOf(ctx: unknown): ReceiptSession {
	const carrier = (ctx && typeof ctx === "object" ? ctx : undefined) as SessionCarrier | undefined;
	let id: string | null = null;
	let depth: number | null = null;
	try {
		const raw = carrier?.sessionManager?.getSessionId?.();
		if (typeof raw === "string" && raw) id = raw;
	} catch {
		/* unbekannt bleibt null */
	}
	try {
		const raw = carrier?.sessionManager?.getHeader?.()?.rlmDepth;
		if (Number.isInteger(raw)) depth = raw as number;
	} catch {
		/* unbekannt bleibt null */
	}
	return { id, depth };
}

/**
 * Die zwei Felder, wie sie in der Quittungszeile stehen — damit alle Pakete dieselben Namen
 * benutzen und ein Leser eine Regel statt neun braucht.
 */
export function sessionFields(who: ReceiptSession = UNKNOWN_SESSION): {
	session_id: string | null;
	rlm_depth: number | null;
} {
	return { session_id: who.id ?? null, rlm_depth: who.depth ?? null };
}
