import { randomUUID } from 'node:crypto';
import { createClient, RESP_TYPES } from 'redis';
import { afterAll, describe, expect, test } from 'vitest';
import { DEFAULT_POLICY, type DecisionAppend, type LoggedInput, type ScopeRecord } from '../../src/core/types.js';
import { redisStore, type RedisSend, type RedisStoreOptions } from '../../src/store/redis.js';
import type { DueKey, ScopeStore, ScopeWrite, StoredScope } from '../../src/store/types.js';
import { decisionLogConformance, scopeStoreConformance } from '../../src/testing/conformance.js';

const url = process.env.ATTEND_REDIS_URL ?? 'redis://127.0.0.1:16379';
const run = `attend-p5d:{${randomUUID().slice(0, 8)}}:`;
const closers: (() => Promise<void>)[] = [];
let prefixes = 0;
const fresh = () => `${run}${prefixes++}:`;

async function connect(resp: 2 | 3 = 3, mapping: 'default' | 'buffers' = 'default'): Promise<RedisSend> {
  const client = createClient({ url, RESP: resp, socket: { reconnectStrategy: false, connectTimeout: 5000 } });
  client.on('error', () => undefined);
  await client.connect();
  closers.push(() => client.close());
  const typeMapping = mapping === 'buffers' ? { [RESP_TYPES.BLOB_STRING]: Buffer, [RESP_TYPES.SIMPLE_STRING]: Buffer } : {};
  return (args) => client.sendCommand(args, { typeMapping });
}

let main: Promise<RedisSend> | undefined;
const shared = () => (main ??= connect());

afterAll(async () => {
  const send = await main?.catch(() => undefined);
  const keys = await send?.(['KEYS', `${run}*`]);
  if (send && Array.isArray(keys) && keys.length > 0) await send(['UNLINK', ...keys.map(String)]);
  await Promise.all(closers.map((close) => close()));
});

for (const config of [
  { name: 'RESP3 without blockingSend', resp: 3, mapping: 'default', blocking: false },
  { name: 'RESP3 with blockingSend', resp: 3, mapping: 'default', blocking: true },
  { name: 'RESP2 with Buffer replies', resp: 2, mapping: 'buffers', blocking: true },
] satisfies { name: string; resp: 2 | 3; mapping: 'default' | 'buffers'; blocking: boolean }[]) {
  describe(config.name, () => {
    const instance = async (prefix: string): Promise<ScopeStore> => {
      await shared();
      const options: RedisStoreOptions = { prefix };
      if (config.blocking) options.blockingSend = await connect(config.resp, config.mapping);
      return redisStore(await connect(config.resp, config.mapping), options);
    };
    const make = () => instance(fresh());
    const concurrent = async (): Promise<[ScopeStore, ScopeStore]> => {
      const prefix = fresh();
      return [await instance(prefix), await instance(prefix)];
    };
    for (const c of [...scopeStoreConformance(make), ...decisionLogConformance(make, { concurrent })]) test(c.name, c.run, 30_000);
  });
}

function input(id: string): LoggedInput {
  return { input: { kind: 'events', events: [{ id, at: 1000, name: 'changed', data: { id }, kind: 'closing' }] }, now: 1000 };
}

function record(scope: string, version: number): ScopeRecord {
  return {
    scope, version, cursors: {}, seen: [], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null },
    batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: null,
  };
}

function append(id: string, scope = 's', at = 1000, kind: 'decision' | 'deadLetter' = 'decision'): DecisionAppend {
  const window = { id, events: [], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: at } satisfies DecisionAppend['body']['window'];
  if (kind === 'deadLetter') return {
    kind, id, scope, at, route: null,
    body: { id, scope, at, window, stage: 'judge', decision: null, message: 'failed', permanent: false, attempts: 1 },
  };
  return {
    kind, id, scope, at, route: 'interrupt',
    body: { id, scope, at, window, route: 'interrupt', confidence: null, predicates: {}, probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 } },
  };
}

function write(scope: string, expectedVersion = 0, options: Partial<Pick<ScopeWrite, 'inputs' | 'snapshot' | 'appends' | 'due'>> = {}): ScopeWrite {
  return { scope, expectedVersion, inputs: [input(`${scope}:${expectedVersion}`)], snapshot: null, appends: [], ...options };
}

function committed(before: StoredScope | null, value: ScopeWrite): StoredScope {
  return {
    version: value.expectedVersion + value.inputs.length,
    snapshot: value.snapshot ?? before?.snapshot ?? null,
    tail: value.snapshot ? [] : [...(before?.tail ?? []), ...value.inputs],
  };
}

test('a mid-script error at any command loses no committed decision', async () => {
  const send = await shared();
  let partialCommit = false;
  let orphan = false;
  let exercised = 0;
  let resolved = false;
  for (let k = 1; k < 1000; k += 1) {
    const prefix = fresh();
    const sources = new Map<string, string>();
    let armed = false;
    const faultSend: RedisSend = async (args) => {
      if (args[0] === 'SCRIPT' && args[1] === 'LOAD') {
        const reply = await send(args);
        if (typeof reply !== 'string' || args[2] === undefined) throw new Error('missing script');
        sources.set(reply, args[2]);
        return reply;
      }
      const source = sources.get(args[1] ?? '');
      if (armed && args[0] === 'EVALSHA' && source?.includes('XADD')) {
        armed = false;
        const prelude = `local attend_real = redis\nlocal attend_calls = 0\nlocal redis = setmetatable({ call = function(...) attend_calls = attend_calls + 1 if attend_calls == ${k} then error('injected fault') end return attend_real.call(...) end }, { __index = attend_real })\n`;
        return send(['EVAL', prelude + source, ...args.slice(2)]);
      }
      return send(args);
    };
    const store = redisStore(faultSend, { prefix });
    expect(await store.commit([
      write('a', 0, { due: 1000, appends: [append('a:seed', 'a')] }),
      write('b', 0, { inputs: [input('b:0'), input('b:1'), input('b:2')], due: 2000 }),
      write('d', 0, { inputs: [input('d:0'), input('d:1')], due: 4000 }),
    ])).toEqual(['ok', 'ok', 'ok']);
    const group = [
      write('a', 1, { inputs: [input('a:1'), input('a:2')], appends: [append('a:decision', 'a'), append('a:letter', 'a', 1000, 'deadLetter')], due: 5000 }),
      write('b', 3, { inputs: [input('b:3'), input('b:4')], snapshot: { version: 5, record: record('b', 5), policy: DEFAULT_POLICY, reducer: 1 }, appends: [append('b:decision', 'b')], due: null }),
      write('c', 0, { appends: [append('c:decision', 'c')], due: 3000 }),
      write('d', 1, { appends: [append('d:conflict', 'd')], due: 9000 }),
    ];
    const before = await Promise.all(group.map((w) => store.load(w.scope)));
    const expected = group.map((w, i) => committed(before[i] ?? null, w));
    armed = true;
    let rejected = false;
    try { expect(await store.commit(group)).toEqual(['ok', 'ok', 'ok', 'conflict']); }
    catch (error) {
      if (!(error instanceof Error) || !error.message.includes('injected fault')) throw error;
      rejected = true;
    }
    exercised += 1;
    if (!rejected) {
      resolved = true;
      break;
    }
    const rows = (await store.log.read({ after: null, limit: 100 })).rows;
    expect(rows.filter((row) => row.id === 'a:seed').map((row) => row.id)).toEqual(['a:seed']);
    const due: DueKey[] = [{ due: 4000, scope: 'd' }];
    const retryOutcomes: string[] = [];
    for (let i = 0; i < 3; i += 1) {
      const w = group[i];
      if (!w) throw new Error('missing write');
      const state = await store.load(w.scope);
      const isCommitted = state?.version === expected[i]?.version;
      expect(state).toEqual(isCommitted ? expected[i] : before[i]);
      partialCommit ||= isCommitted;
      const appended = rows.filter((row) => w.appends.some((a) => a.id === row.id && a.kind === row.kind));
      expect((await store.log.read({ after: null, scope: w.scope, limit: 100 })).rows).toEqual(rows.filter((row) => row.scope === w.scope));
      for (const row of appended) expect(await store.log.find(row.kind, row.id)).toEqual(row);
      if (isCommitted) expect(appended.map((row) => row.id)).toEqual(w.appends.map((row) => row.id));
      else orphan ||= appended.length > 0;
      retryOutcomes.push(isCommitted ? 'conflict' : 'ok');
      if (w.scope === 'a') due.push({ due: isCommitted ? 5000 : 1000, scope: 'a' });
      if (w.scope === 'b' && !isCommitted) due.push({ due: 2000, scope: 'b' });
      if (w.scope === 'c' && isCommitted) due.push({ due: 3000, scope: 'c' });
    }
    expect(await store.load('d')).toEqual(before[3]);
    expect(rows.filter((row) => row.id === 'd:conflict')).toEqual([]);
    expect(await store.due(10_000, 10)).toEqual(due.sort((a, b) => a.due - b.due));
    for (let i = 0; i < 3; i += 1) {
      const w = group[i];
      if (!w) throw new Error('missing write');
      expect(await store.commit([w])).toEqual([retryOutcomes[i]]);
      expect(await store.load(w.scope)).toEqual(expected[i]);
    }
    const final = (await store.log.read({ after: null, limit: 100 })).rows;
    expect(final.map((row) => `${row.kind}:${row.id}`).sort()).toEqual([
      'decision:a:seed', 'decision:a:decision', 'deadLetter:a:letter', 'decision:b:decision', 'decision:c:decision',
    ].sort());
    expect(await store.load('d')).toEqual(before[3]);
  }
  expect(exercised).toBeLessThan(1000);
  expect(resolved).toBe(true);
  expect(partialCommit).toBe(true);
  expect(orphan).toBe(true);
  console.info(`Fault injection exercised ${exercised} K values`);
}, 30_000);

test('validation writes nothing when a group key has the wrong type', async () => {
  const send = await shared();
  const prefix = fresh();
  const store = redisStore(send, { prefix });
  await send(['SET', `${prefix}inputs:b`, 'x']);
  const group = [write('a', 0, { appends: [append('a', 'a')], due: 1000 }), write('b')];
  await expect(store.commit(group)).rejects.toThrow('wrong type');
  expect(await store.load('a')).toBe(null);
  expect(await store.log.read({ after: null, limit: 10 })).toEqual({ rows: [], checked: null, more: false });
  expect(await store.due(10_000, 10)).toEqual([]);
  await send(['DEL', `${prefix}inputs:b`]);
  expect(await store.commit(group)).toEqual(['ok', 'ok']);
}, 30_000);

test('malformed positional commit arguments reject before writes', async () => {
  const send = await shared();
  let replacement: { index: number; value: string } | undefined;
  const store = redisStore((args) => {
    if (args[0] === 'EVALSHA' && replacement) {
      const changed = [...args];
      const start = 3 + Number(args[2]);
      changed[start + replacement.index] = replacement.value;
      replacement = undefined;
      return send(changed);
    }
    return send(args);
  }, { prefix: fresh() });
  const group = [
    write('a', 0, { appends: [append('a', 'a')], due: 1000 }),
    write('b', 0, { snapshot: { version: 1, record: record('b', 1), policy: DEFAULT_POLICY, reducer: 1 } }),
  ];
  for (const [index, value] of [
    [1, '1.5'], [2, 'nan'], [3, '-1'], [3, '1.5'], [3, '3'],
    [5, '-1'], [5, '9007199254740992'], [6, '1.5'], [7, '2'], [8, 'invalid'], [9, '1.5'],
    [11, 'invalid'], [14, 'nan'], [14, 'inf'], [19, '9007199254740991'], [25, '2'], [27, 'nan'],
  ] satisfies [number, string][]) {
    replacement = { index, value };
    await expect(store.commit(group)).rejects.toThrow();
    expect(await store.load('a')).toBe(null);
    expect(await store.load('b')).toBe(null);
    expect(await store.log.bounds()).toEqual({ first: null, last: null });
    expect(await store.due(1000, 10)).toEqual([]);
  }
  expect(await store.commit(group)).toEqual(['ok', 'ok']);
}, 30_000);

test('a forgotten script is reloaded', async () => {
  const send = await shared();
  let forgotten = false;
  const loaded: string[] = [];
  const wrapper: RedisSend = async (args) => {
    if (args[0] === 'SCRIPT' && args[1] === 'LOAD' && args[2] !== undefined) loaded.push(args[2]);
    if (args[0] === 'EVALSHA' && !forgotten) {
      forgotten = true;
      throw new Error('NOSCRIPT No matching script. Please use EVAL.');
    }
    return send(args);
  };
  const store = redisStore(wrapper, { prefix: fresh() });
  expect(await store.commit([write('s')])).toEqual(['ok']);
  expect(await store.load('s')).toEqual({ version: 1, snapshot: null, tail: [input('s:0')] });
  expect(loaded.filter((source) => source.includes('XADD'))).toHaveLength(2);
}, 30_000);

test('sixteen stores on separate connections at one version give exactly one ok', async () => {
  await shared();
  const prefix = fresh();
  const stores = await Promise.all(Array.from({ length: 16 }, async () => redisStore(await connect(), { prefix })));
  const seed = write('s');
  expect(await stores[0]?.commit([seed])).toEqual(['ok']);
  const writes = stores.map((_, i) => write('s', 1, { inputs: [input(`contender:${i}`)] }));
  const outcomes = await Promise.all(stores.map((store, i) => {
    const w = writes[i];
    if (!w) throw new Error('missing write');
    return store.commit([w]);
  }));
  expect(outcomes.filter(([outcome]) => outcome === 'ok')).toEqual([['ok']]);
  const winner = outcomes.findIndex(([outcome]) => outcome === 'ok');
  expect(await stores[15]?.load('s')).toEqual({ version: 2, snapshot: null, tail: [input('s:0'), input(`contender:${winner}`)] });
}, 30_000);

test('a thousand scopes due at one instant page without skips or repeats', async () => {
  const store = redisStore(await shared(), { prefix: fresh() });
  const scopes = Array.from({ length: 1000 }, (_, i) => `s${i}`);
  expect(await store.commit(scopes.map((scope) => write(scope, 0, { due: 1000 })))).toEqual(scopes.map(() => 'ok'));
  const seen: DueKey[] = [];
  let after: DueKey | undefined;
  for (;;) {
    const page = await store.due(1000, 7, after);
    if (page.length === 0) break;
    seen.push(...page);
    after = page.at(-1);
    expect(seen.length).toBeLessThanOrEqual(1000);
  }
  expect(seen).toEqual(scopes.sort().map((scope) => ({ due: 1000, scope })));
}, 30_000);

test('due values round-trip exactly', async () => {
  const store = redisStore(await shared(), { prefix: fresh() });
  const values = [-100.5, -Number.MIN_VALUE, 0, Number.MIN_VALUE, 100.5, 1_792_140_494_126.25];
  expect(await store.commit(values.map((due, i) => write(`s${i}`, 0, { due })))).toEqual(values.map(() => 'ok'));
  for (const [i, due] of values.entries()) {
    const after = i === 0 ? undefined : { due: values[i - 1] ?? 0, scope: `s${i - 1}` };
    const below = due === 0 ? -Number.MIN_VALUE : due - Math.max(Number.MIN_VALUE, Math.abs(due) * Number.EPSILON);
    expect(await store.due(below, 1, after)).toEqual([]);
    expect(await store.due(due, 1, after)).toEqual([{ due, scope: `s${i}` }]);
  }
}, 30_000);

test('retain trims as groups commit by count and age while keeping the head', async () => {
  const send = await shared();
  const store = redisStore(send, { prefix: fresh(), retain: { maxRows: 3 } });
  for (let i = 0; i < 5; i += 1) expect(await store.commit([write(`s${i}`, 0, { appends: [append(`row${i}`, `s${i}`)] })])).toEqual(['ok']);
  expect((await store.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['row2', 'row3', 'row4']);
  expect(await store.log.find('decision', 'row0')).toBe(null);
  expect((await store.log.read({ after: null, scope: 's0', limit: 10 })).rows).toEqual([]);
  const old = Date.now() - 10 * 86_400_000;
  const aged = redisStore(send, { prefix: fresh(), retain: { olderThanMs: 60_000 } });
  expect(await aged.commit([write('old', 0, { appends: [append('old1', 'old', old), append('old2', 'old', old)] })])).toEqual(['ok']);
  expect((await aged.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['old2']);
  expect(await aged.commit([write('new', 0, { appends: [append('new', 'new', Date.now())] })])).toEqual(['ok']);
  expect((await aged.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['new']);
  expect(await aged.log.find('decision', 'old2')).toBe(null);
}, 30_000);

test('serialized pieces are bound without serializing their objects again', async () => {
  const send = await shared();
  const commands: string[][] = [];
  const prefix = fresh();
  const store = redisStore((args) => { commands.push(args); return send(args); }, { prefix });
  const first = write('s', 0, { appends: [append('first')] });
  first.serialized = { inputs: first.inputs.map((value) => JSON.stringify(value)), snapshot: null, appends: first.appends.map((row) => JSON.stringify(row.body)) };
  const expectedInput = input('s:0');
  const expectedRow = append('first');
  for (const value of [...first.inputs, ...first.appends.map((row) => row.body)]) {
    Object.defineProperty(value, 'toJSON', { value: () => { throw new Error('serialized twice'); } });
  }
  expect(await store.commit([first])).toEqual(['ok']);
  expect(commands[0]?.[2]).not.toContain('cjson');
  expect(commands[1]?.slice(3)).toEqual([
    `${prefix}due-index`, `${prefix}decisions`, `${prefix}decided`, `${prefix}scope:s`, `${prefix}inputs:s`, `${prefix}by-scope:s`,
    prefix, '-1', '', '1', 's', '0', '1', '0', 'keep', '1', JSON.stringify(expectedInput),
    'decision', 'first', 's', '1000', 'interrupt', JSON.stringify(expectedRow.body),
  ]);
  expect(await store.load('s')).toEqual({ version: 1, snapshot: null, tail: [expectedInput] });
  const row = await store.log.find('decision', 'first');
  expect(row?.body).toEqual(expectedRow.body);
  const snapshot = { version: 2, record: record('s', 2), policy: DEFAULT_POLICY, reducer: 3 };
  const second = write('s', 1, { snapshot });
  second.serialized = { inputs: second.inputs.map((value) => JSON.stringify(value)), snapshot: JSON.stringify(snapshot.record), appends: [] };
  Object.defineProperty(snapshot.record, 'toJSON', { value: () => { throw new Error('serialized twice'); } });
  commands.length = 0;
  expect(await store.commit([second])).toEqual(['ok']);
  expect(commands[0]?.slice(3)).toEqual([
    `${prefix}due-index`, `${prefix}decisions`, `${prefix}decided`, `${prefix}scope:s`, `${prefix}inputs:s`,
    prefix, '-1', '', '1', 's', '1', '1', '1', 'keep', '0', JSON.stringify(input('s:1')),
    JSON.stringify(record('s', 2)), '2', JSON.stringify(DEFAULT_POLICY), '3',
  ]);
  expect(await store.load('s')).toEqual({ version: 2, snapshot: { ...snapshot, record: record('s', 2) }, tail: [] });
  expect(store.durable).toBe(true);
  expect(Object.hasOwn(store, 'close')).toBe(false);
}, 30_000);

test('non-finite due values reject before sending anything', async () => {
  const send = await shared();
  let commands = 0;
  const store = redisStore(async (args) => { commands += 1; return send(args); }, { prefix: fresh() });
  expect(await store.commit([])).toEqual([]);
  expect(commands).toBe(0);
  for (const due of [NaN, Infinity, -Infinity]) {
    await expect(store.commit([write('a', 0, { appends: [append('a', 'a')] }), write('b', 0, { due })])).rejects.toThrow('due must be finite');
  }
  expect(commands).toBe(0);
  expect(await store.load('a')).toBe(null);
  expect(await store.log.read({ after: null, limit: 10 })).toEqual({ rows: [], checked: null, more: false });
}, 30_000);

test('a lone surrogate in event text and in a decision body round-trips', async () => {
  const store = redisStore(await shared(), { prefix: fresh() });
  for (const serialized of [false, true]) {
    const scope = String(serialized);
    const event = { id: scope, at: 1000, name: 'changed', data: { t: 'bad \ud800 text' }, kind: 'closing' } as const;
    const logged: LoggedInput = { input: { kind: 'events', events: [event] }, now: 1000 };
    const row = append(scope, scope);
    row.body.window.events.push({ ...event, seenAt: 1000 });
    const value = write(scope, 0, { inputs: [logged], appends: [row] });
    if (serialized) value.serialized = { inputs: [JSON.stringify(logged)], snapshot: null, appends: [JSON.stringify(row.body)] };
    expect(await store.commit([value])).toEqual(['ok']);
    expect(await store.load(scope)).toEqual({ version: 1, snapshot: null, tail: [logged] });
    expect((await store.log.find('decision', scope))?.body).toEqual(row.body);
  }
}, 30_000);

test('load ignores uncommitted input versions and rejects a committed gap', async () => {
  const send = await shared();
  const prefix = fresh();
  const store = redisStore(send, { prefix });
  expect(await store.commit([write('s', 0, { inputs: [input('one'), input('two')] })])).toEqual(['ok']);
  await send(['HSET', `${prefix}inputs:s`, '3', JSON.stringify(input('orphan'))]);
  expect(await store.load('s')).toEqual({ version: 2, snapshot: null, tail: [input('one'), input('two')] });
  await send(['HDEL', `${prefix}inputs:s`, '1']);
  await expect(store.load('s')).rejects.toThrow('corrupt scope');
}, 30_000);

test('trim removes multiple chunks from the stream and both indexes', async () => {
  const store = redisStore(await shared(), { prefix: fresh() });
  const rows = Array.from({ length: 600 }, (_, i) => append(`row${i}`, 's'));
  expect(await store.commit([write('s', 0, { appends: rows })])).toEqual(['ok']);
  await store.log.trim({ maxRows: 2 });
  expect((await store.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['row598', 'row599']);
  expect((await store.log.read({ after: null, scope: 's', limit: 10, scanLimit: 2 })).rows.map((row) => row.id)).toEqual(['row598', 'row599']);
  expect(await store.log.find('decision', 'row0')).toBe(null);
  expect(await store.log.find('decision', 'row597')).toBe(null);
}, 30_000);

test('wait wakes on a real blocking read and aborts while another is pending', async () => {
  const send = await shared();
  const blockingSend = await connect();
  let announce: () => void = () => {};
  const blocked = () => new Promise<void>((resolve) => { announce = resolve; });
  const store = redisStore(send, { prefix: fresh(), blockingSend: (args) => {
    const reply = blockingSend(args);
    announce();
    return reply;
  } });
  const started = blocked();
  const controller = new AbortController();
  let finished = false;
  const waiting = store.log.wait(null, 5000, controller.signal).then(() => { finished = true; });
  await started;
  expect(finished).toBe(false);
  expect(await store.commit([write('s', 0, { appends: [append('wake')] })])).toEqual(['ok']);
  await waiting;
  expect(finished).toBe(true);
  const startedAgain = blocked();
  const head = (await store.log.bounds()).last;
  finished = false;
  const pending = store.log.wait(head, 60_000, controller.signal).then(() => { finished = true; });
  await startedAgain;
  const at = Date.now();
  controller.abort();
  await pending;
  expect(finished).toBe(true);
  expect(Date.now() - at).toBeLessThan(500);
  expect((await store.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['wake']);
}, 30_000);

test('read rejects positions not produced by this log and wait still resolves', async () => {
  const store = redisStore(await shared(), { prefix: fresh() });
  for (const after of ['0-0', '1', 'not-a-position']) {
    await expect(store.log.read({ after, limit: 1 })).rejects.toThrow('not a position');
    const started = Date.now();
    await expect(store.log.wait(after, 10_000, new AbortController().signal)).resolves.toBeUndefined();
    expect(Date.now() - started).toBeLessThan(100);
  }
  expect(await store.log.read({ after: '00000009999999999999-00000000000000000000', limit: 1 })).toEqual({
    rows: [], checked: '00000009999999999999-00000000000000000000', more: false,
  });
}, 30_000);

test('commit retention bounds cleanup by rows actually appended', async () => {
  const send = await shared();
  const prefix = fresh();
  const store = redisStore(send, { prefix });
  const rows = Array.from({ length: 150 }, (_, i) => append(`row${i}`));
  expect(await store.commit([write('s', 0, { appends: rows })])).toEqual(['ok']);
  const retaining = redisStore(send, { prefix, retain: { maxRows: 1 } });
  const conflicts = Array.from({ length: 100 }, (_, i) => append(`conflict${i}`));
  expect(await retaining.commit([write('s', 0, { appends: conflicts })])).toEqual(['conflict']);
  expect((await store.log.read({ after: null, limit: 200 })).rows.map((row) => row.id)).toEqual(rows.slice(64).map((row) => row.id));
  expect(await store.log.find('decision', 'conflict0')).toBe(null);
}, 30_000);
