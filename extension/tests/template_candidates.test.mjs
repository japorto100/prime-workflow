import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { freezeTemplate, createRunFromTemplate } from '../lib/template-candidates.ts';
import { acquireWriter, readRun, readEvents } from '../lib/service.ts';

const candidate = () => ({ version: 1, id: 'review', revision: 'v1', applicability: 'Bounded code review',
  input_names: ['subject'], steps: [{ id: 'inspect' }, { id: 'verify', depends_on: ['inspect'] }],
  roles: ['explorer', 'verifier'], result_forms: { inspect: 'json-object', verify: 'json-object' },
  limits: { max_agents_total: 2 }, method_refs: ['method-and-resource-pin'], effects: 'read_only',
  stop_rule: 'Stop on unavailable source', retry_rule: 'No automatic retry of unresolved admission',
  evidence: [{ run_id: 'training-1', source_digest: 'a'.repeat(64), partition: 'training', outcome: 'negative' }] });
function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), 'jaz-template-'));
  const env = { PRIME_WORKFLOW_STATE: dir };
  const writer = acquireWriter(env, { session_id: 'template-test' });
  t.after(() => { writer.release(); rmSync(dir, { recursive: true, force: true }); });
  return { env, writer };
}

test('W02: instantiated definition remains fixed after candidate mutation', t => {
  const { env, writer } = fixture(t);
  const value = candidate();
  const pin = freezeTemplate(value);
  const run = createRunFromTemplate(env, writer, value, { run_id: 'r1', selected_digest: pin.digest,
    inputs: { subject: 'original' }, subject: { revision: 'original' } });
  value.steps.push({ id: 'new-phase' }); value.revision = 'v2';
  const stored = readRun(env, 'r1');
  assert.equal(stored.definition_digest, run.definition_digest);
  assert.equal(stored.definition_object, run.definition_object);
  assert.equal(JSON.parse(stored.definition_object).template.revision, 'v1');
  assert.ok(!stored.step_specs['new-phase']);
  assert.deepEqual(Object.keys(stored.attempts), []);
  assert.equal(readEvents(env).events.length, 1);
});

test('changed selection and cyclic steps fail before journal mutation', t => {
  const { env, writer } = fixture(t); const value = candidate();
  const pin = freezeTemplate(value); value.revision = 'v2';
  assert.throws(() => createRunFromTemplate(env, writer, value, { run_id: 'r1', selected_digest: pin.digest,
    inputs: { subject: 's' }, subject: {} }), /selected version changed/);
  value.steps[0].depends_on = ['verify'];
  assert.throws(() => createRunFromTemplate(env, writer, value, { run_id: 'r1', selected_digest: freezeTemplate(value).digest,
    inputs: { subject: 's' }, subject: {} }), /cycl/i);
  assert.equal(readEvents(env).events.length, 0);
});

test('training/evaluation overlap is refused by run or source identity', () => {
  for (const copy of [true, false]) {
    const value = candidate();
    value.evidence.push({ run_id: copy ? 'new-run' : 'training-1',
      source_digest: (copy ? 'a' : 'b').repeat(64), partition: 'evaluation', outcome: 'positive' });
    assert.throws(() => freezeTemplate(value), /overlap/);
  }
});

test('negative evidence and method pins survive; candidate never becomes promoted', () => {
  const value = candidate(); const frozen = freezeTemplate(value);
  assert.equal(frozen.status, 'candidate');
  assert.equal(JSON.parse(frozen.definition).evidence[0].outcome, 'negative');
  assert.deepEqual(JSON.parse(frozen.definition).method_refs, value.method_refs);
  const reordered = Object.fromEntries(Object.entries(value).reverse());
  assert.equal(freezeTemplate(reordered).digest, frozen.digest);
});

test('non-JSON and oversized definitions are rejected', () => {
  for (const bad of [NaN, Infinity, () => 1, undefined]) {
    const value = candidate(); value.result_forms.bad = bad;
    assert.throws(() => freezeTemplate(value), /finite JSON/);
  }
  const value = candidate(); value.applicability = 'x'.repeat(65537);
  assert.throws(() => freezeTemplate(value), /limit/);
});
