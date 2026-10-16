import assert from 'node:assert/strict';
import fc from 'fast-check';
import { test } from 'vitest';
import { createAttend, createAttendInternal } from '../src/attend.js';
import { memoryStore } from '../src/store/memory.js';
import { fakeClock, drain } from '../src/testing.js';
import { DEFAULT_POLICY, KINDS, type DeadLetter, type Decision, type Event, type Judge, type JudgeInput, type Judgment, type SettlePolicy, type Trace } from '../src/core/types.js';
import { PermanentError } from '../src/errors.js';
import { initialRecord } from './oracle/step.js';
import { createAttend as oracleAttend } from './oracle/attend.js';
import { memoryStore as oracleStore } from './oracle/memory.js';
import { fakeClock as oracleClock } from './oracle/testing.js';

type Step = { at: number; scope: string } & ({ kind: 'push'; events: Event[] } | { kind: 'flush' } | { kind: 'advance' });
type Mode = 'mixed' | 'wait' | 'retry' | 'permanent' | 'sinkRetry' | 'sinkDead' | 'skip' | 'session';
interface Scenario { name: string; policy: SettlePolicy; scopes: string[]; steps: Step[]; mode: Mode }
const policy: SettlePolicy = { ...DEFAULT_POLICY, closingGraceMs: 5, streamGapMs: 7, passiveQuietMs: 11, maxWindowMs: 40,
  waitHorizonMs: 17, maxWaits: 2, maxEvents: 4, seenCount: 8, recentCount: 3, retryBackoffMs: [3, 7], judgeTimeoutMs: 30, deliveryTimeoutMs: 30 };
const coverage = { scenarios: 0, steps: 0, merges: 0, horizonWaits: 0, judgeLetters: 0, sinkLetters: 0, duplicatesInBatch: 0, duplicatesAcrossRing: 0 };

function judgment(mode: Mode, input: JudgeInput, call: number): Judgment {
  const has = (id: string) => input.window.events.some((event) => event.id === id);
  if (mode === 'session') {
    if (has('f')) throw new PermanentError('refused');
    if (has('e') && call === 1) throw new Error('flaky');
    if (has('b') && !has('d') && input.routes.some((route) => route.route === 'wait')) return { route: 'wait' };
    return { route: has('d') ? 'interrupt' : has('e') ? 'note' : has('g') ? 'skip' : 'queue' };
  }
  if (mode === 'permanent') throw new PermanentError('refused');
  if (mode === 'retry' && call === 1) throw new Error('busy');
  if (mode === 'wait') return { route: input.routes.some((route) => route.route === 'wait') ? 'wait' : 'queue' };
  if (mode === 'skip') return { route: 'skip' };
  if (mode !== 'mixed') return { route: 'queue' };
  const choice = (parseInt(input.window.id.slice(-5), 16) + call) % 11;
  if (choice === 0) throw new Error('retryable');
  if (choice === 1) throw new PermanentError('permanent');
  if (choice === 2) return Reflect.get({ malformed: true }, 'value');
  const routes = ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'] as const;
  return { route: routes[(choice - 3) % routes.length]!, probabilities: { interrupt: 0.4, queue: 0.2, note: 0.1, ask: 0.1, skip: 0.1, wait: 0.1 }, predicates: { addressed: 0.7 }, confidence: 0.8 };
}

function environment(scenario: Scenario, flavor: 'oracle' | 'ratio' | 'always') {
  const clock = flavor === 'oracle' ? oracleClock() : fakeClock();
  const calls: { scope: string; id: string; call: number; offered: string[]; events: string[]; reason: string; now: number }[] = [];
  const deliveries: { scope: string; id: string; route: string; at: number; events: string[] }[] = [];
  const letters: { scope: string; id: string; stage: string; attempts: number; message: string }[] = [];
  const errors: string[] = [];
  const traces: Trace[] = [];
  const counts = new Map<string, number>();
  const attempts = new Map<string, number>();
  const committed = new Map<string, number>();
  let pending = 0;
  const guard = async <T>(work: () => Promise<T>) => { pending += 1; try { return await work(); } finally { pending -= 1; } };
  const judge: Judge = (input) => guard(async () => {
    const call = (counts.get(input.window.id) ?? 0) + 1;
    counts.set(input.window.id, call);
    calls.push({ scope: input.scope, id: input.window.id, call, offered: input.routes.map((route) => route.route), events: input.window.events.map((event) => event.id), reason: input.window.reason, now: input.now });
    if (flavor === 'ratio' && input.window.waits > 0 && !input.routes.some((route) => route.route === 'wait') && !['flush', 'forced'].includes(input.window.reason)) coverage.horizonWaits += 1;
    return judgment(scenario.mode, input, call);
  });
  const sink = { deliver: (decision: Decision) => guard(async () => {
    const attempt = (attempts.get(decision.id) ?? 0) + 1;
    attempts.set(decision.id, attempt);
    if (scenario.mode === 'sinkDead' || (scenario.mode === 'session' && decision.route === 'interrupt')
      || (scenario.mode === 'sinkRetry' && attempt === 1) || (scenario.mode === 'mixed' && parseInt(decision.id.slice(-2), 16) % 7 === 0)) throw new Error('sink down');
    deliveries.push({ scope: decision.scope, id: decision.id, route: decision.route, at: decision.at, events: decision.window.events.map((event) => event.id) });
  }) };
  const deadLetter = (letter: DeadLetter) => guard(async () => {
    letters.push({ scope: letter.scope, id: letter.id, stage: letter.stage, attempts: letter.attempts, message: letter.message });
    if (flavor === 'ratio') { if (letter.stage === 'judge') coverage.judgeLetters += 1; else coverage.sinkLetters += 1; }
  });
  const config = { clock, judge, sink, deadLetter, kind: (event: Event) => KINDS.find((kind) => kind === event.name) ?? 'consequence',
    settle: scenario.policy, observe: (trace: Trace) => { traces.push(trace); if (flavor === 'ratio' && trace.kind === 'settled' && trace.merged) coverage.merges += 1; } };
  const inner = memoryStore();
  const legacy = oracleStore();
  const attend = flavor === 'oracle' ? oracleAttend({ ...config, store: { ...legacy,
    get: (scope) => guard(() => legacy.get(scope)), put: (record, expected) => guard(() => legacy.put(record, expected)),
    due: (now, limit) => guard(() => legacy.due(now, limit)),
  } }) : createAttendInternal({ ...config, store: { ...inner,
    load: (scope) => guard(() => inner.load(scope)), due: (now, limit, after) => guard(() => inner.due(now, limit, after)),
    commit: (writes) => guard(async () => {
      const results = await inner.commit(writes);
      for (const [i, write] of writes.entries()) if (results[i] === 'ok') committed.set(write.scope, (committed.get(write.scope) ?? 0) + write.inputs.length);
      return results;
    }),
  } }, flavor);
  const controller = new AbortController();
  const done = attend.run({ signal: controller.signal, sweepMs: 1000 });
  return { attend, clock, calls, deliveries, letters, errors, committed, controller, done, flavor, pending: () => pending };
}

async function settle(env: ReturnType<typeof environment>, at: number) {
  do {
    await env.clock.set(at);
    await drain();
  } while (env.pending() > 0 || (env.clock.nextAt() !== null && env.clock.nextAt()! <= at));
}

async function run(scenario: Scenario) {
  const environments = [environment(scenario, 'oracle'), environment(scenario, 'ratio'), environment(scenario, 'always')];
  coverage.scenarios += 1;
  coverage.steps += scenario.steps.length;
  try {
    for (const step of scenario.steps) {
      for (const env of environments) {
        await settle(env, step.at);
        try {
          if (step.kind === 'push') {
            if (env.flavor === 'ratio') {
              const before = await env.attend.inspect(step.scope);
              for (const event of step.events) {
                if (before?.batch?.window.events.some((prior) => prior.id === event.id)) coverage.duplicatesInBatch += 1;
                else if (before?.seen.includes(event.id) && !before.buffer.entries.some((prior) => prior.id === event.id)) coverage.duplicatesAcrossRing += 1;
              }
            }
            await env.attend.push(step.scope, step.events);
          } else if (step.kind === 'flush') await env.attend.flush(step.scope);
        } catch (error) { env.errors.push(error instanceof Error ? error.name + ':' + error.message : String(error)); }
        await settle(env, step.at);
      }
      for (const scope of scenario.scopes) {
        for (const env of environments.slice(1)) {
          const oracle = environments[0]!;
          assert.deepStrictEqual(env.calls.filter((call) => call.scope === scope), oracle.calls.filter((call) => call.scope === scope), `${scenario.name}: calls at ${step.at}`);
          assert.deepStrictEqual(env.deliveries.filter((call) => call.scope === scope), oracle.deliveries.filter((call) => call.scope === scope), `${scenario.name}: deliveries at ${step.at}`);
          assert.deepStrictEqual(env.letters.filter((call) => call.scope === scope), oracle.letters.filter((call) => call.scope === scope), `${scenario.name}: letters at ${step.at}`);
          assert.deepStrictEqual(env.errors, oracle.errors, `${scenario.name}: intake errors`);
        }
      }
    }
    for (const scope of scenario.scopes) {
      const expected = await environments[0]!.attend.inspect(scope);
      for (const env of environments.slice(1)) {
        const record = await env.attend.inspect(scope);
        assert.equal(record?.version ?? 0, env.committed.get(scope) ?? 0, `${scenario.name}: committed input count`);
        const withoutVersion = (value: typeof record) => { const { version: _, ...rest } = value ?? initialRecord(scope); return rest; };
        assert.deepStrictEqual(withoutVersion(record), withoutVersion(expected), `${scenario.name}: final record for ${scope}`);
      }
    }
  } finally {
    for (const env of environments) env.controller.abort();
    await Promise.all(environments.map((env) => env.done));
  }
}

const ev = (id: string, name = 'closing'): Event => ({ id, at: 0, name, data: { id }, text: name });
const push = (at: number, ids: string[], name = 'closing', scope = 's'): Step => ({ at, scope, kind: 'push', events: ids.map((id) => ev(id, name)) });
const advance = (at: number): Step => ({ at, scope: 's', kind: 'advance' });
const flush = (at: number): Step => ({ at, scope: 's', kind: 'flush' });
const fixed: Scenario[] = ([
  { name: 'closing grace', mode: 'mixed', steps: [push(0, ['a']), advance(4), advance(5), advance(200)] },
  { name: 'flush event', mode: 'mixed', steps: [push(0, ['a'], 'flush'), advance(200)] },
  { name: 'flush punctuation', mode: 'wait', steps: [push(0, ['a'], 'consequence'), flush(4), advance(200)] },
  { name: 'waiting and merging', mode: 'wait', steps: [push(0, ['a']), advance(5), push(6, ['a']), push(7, ['b']), advance(12), advance(29), push(30, ['a']), advance(200)] },
  { name: 'horizon with consequence', mode: 'wait', steps: [push(0, ['a']), advance(5), push(6, ['b'], 'consequence'), advance(22), advance(200)] },
  { name: 'horizon alone', mode: 'wait', steps: [push(0, ['a']), advance(22), advance(200)] },
  { name: 'flush during wait', mode: 'wait', steps: [push(0, ['a']), advance(5), flush(6), advance(200)] },
  { name: 'judge retry', mode: 'retry', steps: [push(0, ['a']), advance(5), advance(7), advance(8), advance(200)] },
  { name: 'permanent judge failure', mode: 'permanent', steps: [push(0, ['a']), advance(200)] },
  { name: 'sink retry', mode: 'sinkRetry', steps: [push(0, ['a']), advance(200)] },
  { name: 'sink dead letter', mode: 'sinkDead', steps: [push(0, ['a']), advance(200)] },
  { name: 'outbox full', mode: 'sinkDead', steps: [push(0, ['a'], 'flush'), push(1, ['b'], 'flush'), advance(200)] , policy: { ...policy, maxOutbox: 1 } },
  { name: 'two scopes', mode: 'mixed', scopes: ['s', 't'], steps: [push(0, ['a']), push(0, ['b'], 'closing', 't'), advance(200)] },
  { name: 'duplicates and seen ring', mode: 'skip', steps: [push(0, ['a', 'a']), advance(5), push(6, ['a']), push(7, ['b', 'c', 'd', 'e', 'f']), advance(12), push(13, ['a']), advance(200)] },
  { name: 'non-act age pruning', mode: 'skip', steps: [push(0, ['a'], 'context'), push(41, ['b'], 'consequence'), flush(42), advance(200)] },
  { name: 'continuous stream', mode: 'wait', steps: [push(0, ['a'], 'continuous'), push(6, ['b'], 'continuous'), advance(12), advance(13), advance(200)] },
  { name: 'source-free cursor-less run', mode: 'skip', steps: [push(0, ['a'], 'passive'), advance(200)] },
  { name: 'crash session without a crash', mode: 'session', policy: { ...DEFAULT_POLICY, judgeTimeoutMs: 1000, deliveryTimeoutMs: 2000, retryBackoffMs: [1000, 1000, 1000] },
    steps: [push(0, ['a']), advance(600), advance(8600), push(8600, ['b'], 'passive'), advance(12700), push(12700, ['d']), advance(13300), advance(38300), push(38300, ['e'], 'flush'), advance(39800), advance(47800), push(47800, ['f']), advance(48400), advance(56400), push(56400, ['g']), advance(57000), advance(117000)] },
] satisfies (Omit<Scenario, 'policy' | 'scopes'> & Partial<Pick<Scenario, 'policy' | 'scopes'>>)[]).map((scenario) => ({ policy, scopes: ['s'], ...scenario }));

const randomScenario = fc.record({
  scopeCount: fc.integer({ min: 1, max: 3 }),
  closingGraceMs: fc.integer({ min: 0, max: 7 }), streamGapMs: fc.integer({ min: 1, max: 11 }), passiveQuietMs: fc.integer({ min: 1, max: 15 }),
  maxEvents: fc.integer({ min: 1, max: 6 }), seenCount: fc.integer({ min: 1, max: 12 }), maxWaits: fc.integer({ min: 0, max: 3 }), recentCount: fc.integer({ min: 0, max: 5 }),
  steps: fc.array(fc.record({ delta: fc.integer({ min: 0, max: 15 }), scope: fc.nat(2), operation: fc.nat(5), events: fc.array(fc.record({ id: fc.nat(8), kind: fc.nat(5) }), { minLength: 1, maxLength: 5 }) }), { minLength: 1, maxLength: 45 }),
}).map((generated): Scenario => {
  let at = 0;
  const scopes = Array.from({ length: generated.scopeCount }, (_, i) => `s${i}`);
  const steps: Step[] = generated.steps.map((step) => {
    at += step.delta;
    const scope = scopes[step.scope % scopes.length]!;
    if (step.operation === 0) return { at, scope, kind: 'flush' };
    if (step.operation === 1) return { at, scope, kind: 'advance' };
    return { at, scope, kind: 'push', events: step.events.map((event) => ev(`${scope}/${event.id}`, KINDS[event.kind]!)) };
  });
  steps.push(advance(at + 500));
  return { name: 'generated', scopes, steps, mode: 'mixed', policy: { ...policy, closingGraceMs: generated.closingGraceMs, streamGapMs: generated.streamGapMs, passiveQuietMs: generated.passiveQuietMs,
    maxEvents: generated.maxEvents, seenCount: generated.seenCount, maxWaits: generated.maxWaits, recentCount: generated.recentCount } };
});

test('fake-clock shell differential against the frozen shell and always-snapshot mode', async () => {
  for (const scenario of fixed) await run(scenario);
  const numRuns = Number(process.env.ATTEND_SHELL_DIFF_RUNS ?? 200);
  await fc.assert(fc.asyncProperty(randomScenario, run), { numRuns, seed: 0x506a });
  for (const key of ['merges', 'horizonWaits', 'judgeLetters', 'sinkLetters', 'duplicatesInBatch', 'duplicatesAcrossRing'] as const) assert.ok(coverage[key] > 0, key);
  console.log(JSON.stringify({ shellDifferential: coverage }));
}, 300000);
