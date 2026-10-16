import type { DecisionAppend, DeliveredRoute, JsonObject } from '../core/types.js';
import type { Retention } from '../store/types.js';
import type { Principal } from '../server/types.js';
import type { ConformanceCase } from '../testing.js';
import { assertEqual, assertOk } from '../internal/assert.js';

/** A running server under test, seen through one MCP client, plus the log behind it. */
export interface WireTarget {
  /** One JSON-RPC request; resolves with the raw wire result, or rejects with an Error carrying code and data. */
  request(method: string, params: JsonObject): Promise<Record<string, unknown>>;
  /** Commits rows to the decision log the server reads, as the shell would. */
  commit(appends: DecisionAppend[]): Promise<void>;
  trim(retention: Retention): Promise<void>;
  stop(): Promise<void>;
}

function decision(id: string, scope = 's', route: DeliveredRoute = 'interrupt', at = Date.now()): DecisionAppend {
  return { kind: 'decision', id, scope, route, at, body: {
    id, scope, route, at, confidence: null, probabilities: { interrupt: 0.8, queue: 0.1, note: 0.05, ask: 0.05, skip: 0, wait: 0 }, predicates: { relevant: 0.9 },
    window: { id, events: [{ id: `event:${id}`, at: at - 2, name: 'changed', data: { nested: ['hello', 1] }, kind: 'closing', seenAt: at - 1 }], reason: 'closing', settledAt: at - 1, waits: 2, dropped: 3, gap: true },
  } };
}

function letter(id: string, stage: 'judge' | 'sink' = 'judge', at = Date.now()): DecisionAppend {
  const row = decision(id, 's', 'interrupt', at);
  if (row.kind !== 'decision') throw new Error('Expected decision');
  return { kind: 'deadLetter', id, scope: row.scope, route: stage === 'sink' ? row.route : null, at: row.at, body: { id, scope: row.scope, at: row.at, window: row.body.window, stage, message: 'could not deliver', permanent: true, attempts: 3, decision: stage === 'sink' ? row.body : null } };
}

function record(value: unknown): Record<string, unknown> {
  assertOk(typeof value === 'object' && value !== null && !Array.isArray(value), 'object');
  return value as Record<string, unknown>;
}

function events(result: Record<string, unknown>): Record<string, unknown>[] {
  assertOk(Array.isArray(result.events), 'events array');
  return (result.events as unknown[]).map(record);
}

function ids(result: Record<string, unknown>): unknown[] { return events(result).map((e) => e.eventId); }

async function error(run: () => Promise<unknown>, code: number, field: string, expected?: unknown): Promise<unknown> {
  try { await run(); } catch (caught) {
    const value = record(caught);
    assertEqual(value.code, code, 'error code');
    const data = record(value.data);
    if (expected === undefined) assertOk(typeof data[field] === 'string', field);
    else assertEqual(data[field], expected, field);
    return data[field];
  }
  throw new Error('Expected rejection');
}

const all: Principal = { id: 'local', scopes: [''] };
type WireCase = [string, (target: WireTarget, start: (principal: Principal) => Promise<WireTarget>) => Promise<void>, Principal?];

/** The SEP-3415 MUSTs for events/list, events/poll and the tools, as cases. `start` gives each case a fresh server and empty log, serving `principal`. */
export function eventsWireConformance(start: (principal: Principal) => Promise<WireTarget>): ConformanceCase[] {
  const checked = async (principal: Principal): Promise<WireTarget> => {
    const target = await start(principal);
    return { ...target, async request(method, params) {
      const result = await target.request(method, params);
      assertEqual(result.resultType, 'complete', `${method} resultType`);
      return result;
    } };
  };
  const poll = (target: WireTarget, params: JsonObject = {}) => target.request('events/poll', { name: 'attention.decision', ...params });
  const head = async (target: WireTarget) => (await poll(target, { cursor: null })).cursor as string;
  const cases: WireCase[] = [
    ['events/list describes attention.decision and attention.deadLetter', async (t) => {
      const result = await t.request('events/list', {});
      const descriptors = (result.events as unknown[]).map(record);
      assertEqual(descriptors.map((d) => d.name), ['attention.decision', 'attention.deadLetter'], 'names');
      for (const d of descriptors) {
        assertEqual(d.delivery, ['poll', 'push'], 'delivery');
        assertEqual(record(d.inputSchema).additionalProperties, false, 'strict arguments');
      }
      assertEqual(record(descriptors[0]!.payloadSchema).required, ['decisionId', 'scope', 'route', 'probabilities', 'predicates', 'confidence', 'reason', 'settledAt', 'decidedAt', 'eventCount', 'eventIds', 'dropped', 'gap', 'waits', 'notice'], 'required fields');
      assertOk(!('nextCursor' in result), 'one page');
      await error(() => t.request('events/list', { cursor: 'page-2' }), -32602, 'field', 'cursor');
    }],
    ['a null cursor starts from now', async (t) => {
      await t.commit([decision('old')]);
      const result = await poll(t, { cursor: null });
      assertEqual(ids(result), [], 'no replay');
      assertEqual(result.truncated, false, 'not truncated');
      assertEqual(result.hasMore, false, 'no more');
      assertOk(typeof result.nextPollMs === 'number' && result.nextPollMs >= 1000, 'poll interval');
      assertOk(typeof result.cursor === 'string', 'cursor');
      await t.commit([decision('new')]);
      assertEqual(ids(await poll(t, { cursor: result.cursor as string })), ['new'], 'new row');
      assertEqual(ids(await poll(t)), [], 'absent cursor starts now');
    }],
    ['the cursor from an empty log replays the first rows', async (t) => {
      const cursor = await head(t);
      await t.commit([decision('a'), decision('b')]);
      assertEqual(ids(await poll(t, { cursor })), ['a', 'b'], 'commit order');
    }],
    ['occurrences carry the payload fields and no per-event cursor', async (t) => {
      const cursor = await head(t);
      const rows = [decision('a'), decision('b', 's', 'queue'), letter('c', 'sink', 1000), letter('d', 'judge', 1000)];
      await t.commit(rows);
      const decisions = events(await poll(t, { cursor }));
      const letters = events(await poll(t, { cursor, name: 'attention.deadLetter' }));
      for (const [i, event] of [...decisions, ...letters].entries()) {
        const row = rows[i]!;
        const body = row.body;
        const data = record(event.data);
        assertEqual(event.eventId, row.id, 'event id');
        assertEqual(event.name, row.kind === 'decision' ? 'attention.decision' : 'attention.deadLetter', 'event name');
        assertEqual(event.timestamp, new Date(row.at).toISOString(), 'timestamp');
        assertOk(!('cursor' in event), 'no occurrence cursor');
        assertEqual(data.scope, body.scope, 'scope');
        assertEqual(data.eventIds, body.window.events.map((e) => e.id), 'event ids');
        assertEqual(data.eventCount, body.window.events.length, 'event count');
        assertEqual(data.reason, body.window.reason, 'reason');
        if (row.kind === 'decision') {
          assertEqual(data.decisionId, row.id, 'decision id');
          assertEqual(data.route, row.body.route, 'route');
          assertEqual(data.probabilities, row.body.probabilities, 'probabilities');
          assertEqual(data.predicates, row.body.predicates, 'predicates');
          assertEqual(data.settledAt, new Date(body.window.settledAt).toISOString(), 'settled at');
          assertEqual(data.decidedAt, new Date(body.at).toISOString(), 'decided at');
          assertEqual(data.confidence, null, 'confidence');
          assertEqual([data.dropped, data.gap, data.waits], [3, true, 2], 'window fields');
          assertOk(typeof data.notice === 'string' && data.notice.length > 0, 'notice');
        } else {
          assertEqual(data, row.id === 'c'
            ? { letterId: 'c', scope: 's', stage: 'sink', message: 'could not deliver', permanent: true, attempts: 3, decisionId: 'c', route: 'interrupt', reason: 'closing', eventCount: 1, eventIds: ['event:c'], at: '1970-01-01T00:00:01.000Z' }
            : { letterId: 'd', scope: 's', stage: 'judge', message: 'could not deliver', permanent: true, attempts: 3, decisionId: null, route: null, reason: 'closing', eventCount: 1, eventIds: ['event:d'], at: '1970-01-01T00:00:01.000Z' }, 'letter payload');
        }
      }
    }],
    ['a full page sets hasMore and the next poll continues after it', async (t) => {
      let cursor = await head(t);
      await t.commit(['a', 'b', 'c', 'd', 'e'].map((id) => decision(id)));
      for (const [expected, more] of [[['a', 'b'], true], [['c', 'd'], true], [['e'], false]] as const) {
        const result = await poll(t, { cursor, maxEvents: 2 });
        assertEqual(ids(result), expected, 'page');
        assertEqual(result.hasMore, more, 'hasMore');
        cursor = result.cursor as string;
      }
      await t.commit([decision('f'), decision('g')]);
      const result = await poll(t, { cursor, maxEvents: 2 });
      assertEqual(ids(result), ['f', 'g'], 'exact page');
      assertEqual(result.hasMore, false, 'exact head');
    }],
    ['a quiet filtered poll still advances the cursor', async (t) => {
      const cursor = await head(t);
      await t.commit(['a', 'b', 'c'].map((id) => decision(id, 'other')));
      const quiet = await poll(t, { cursor, arguments: { scope: 'mine' } });
      assertEqual(ids(quiet), [], 'quiet');
      assertOk(quiet.cursor !== cursor, 'advanced');
      await t.commit([decision('mine', 'mine')]);
      assertEqual(ids(await poll(t, { cursor: quiet.cursor as string, arguments: { scope: 'mine' } })), ['mine'], 'next');
    }],
    ['arguments filter by scope, scopePrefix, routes and minProbability', async (t) => {
      const cursor = await head(t);
      await t.commit([decision('a', 'a/1'), decision('b', 'a/2', 'queue'), decision('c', 'b/1', 'note')]);
      for (const [args, expected] of [[{ scope: 'a/1' }, ['a']], [{ scopePrefix: 'a/' }, ['a', 'b']], [{ routes: ['queue'] }, ['b']], [{ minProbability: 0.5 }, ['a']], [{ minProbability: 0.8 }, ['a']], [{ minProbability: 0.81 }, []], [{ routes: [] }, []]] satisfies [JsonObject, string[]][]) assertEqual(ids(await poll(t, { cursor, arguments: args })), expected, 'filter');
    }],
    ['include events carries the window entries', async (t) => {
      const cursor = await head(t);
      const row = decision('a');
      await t.commit([row]);
      assertEqual(record(events(await poll(t, { cursor, arguments: { include: 'events' } }))[0]!.data).events, row.body.window.events, 'entries');
      assertOk(!('events' in record(events(await poll(t, { cursor }))[0]!.data)), 'minimal payload');
    }],
    ['attention.deadLetter serves dead letters only, filtered by stage', async (t) => {
      const cursor = await head(t);
      await t.commit([decision('a'), letter('b'), letter('c', 'sink')]);
      assertEqual(ids(await poll(t, { cursor, name: 'attention.deadLetter' })), ['b', 'c'], 'letters');
      assertEqual(ids(await poll(t, { cursor, name: 'attention.deadLetter', arguments: { stage: 'sink' } })), ['c'], 'stage');
    }],
    ['a cursor from another log incarnation is truncated and resets to the floor', async (t) => {
      await t.commit([decision('a'), decision('b')]);
      const result = await poll(t, { cursor: 'v1.another-epoch.1' });
      assertEqual(ids(result), ['a', 'b'], 'replay');
      assertEqual(result.truncated, true, 'gap');
      assertEqual((await poll(t, { cursor: result.cursor as string })).truncated, false, 'resumed');
    }],
    ['a cursor below the retention floor is truncated', async (t) => {
      const cursor = await head(t);
      await t.commit(['a', 'b', 'c'].map((id) => decision(id)));
      const first = await poll(t, { cursor, maxEvents: 1 });
      await t.trim({ maxRows: 1 });
      const result = await poll(t, { cursor: first.cursor as string });
      assertEqual(result.truncated, true, 'retention gap');
      assertEqual(ids(result), ['c'], 'retained row');
    }],
    ['maxAgeMs skips older rows and says truncated', async (t) => {
      const cursor = await head(t);
      const now = Date.now();
      await t.commit([decision('old', 's', 'interrupt', now - 3_600_000), decision('mid', 's', 'interrupt', now), decision('older', 's', 'interrupt', now - 3_600_000), decision('new', 's', 'interrupt', now)]);
      const mid = await poll(t, { cursor, maxEvents: 2 });
      const older = await poll(t, { cursor, maxEvents: 3 });
      assertEqual(ids(mid), ['old', 'mid'], 'cursor after mid');
      assertEqual(ids(older), ['old', 'mid', 'older'], 'cursor after older');
      const result = await poll(t, { cursor, maxAgeMs: 60_000 });
      assertEqual(ids(result), ['mid', 'new'], 'age filter');
      assertEqual(result.truncated, true, 'age gap');
      const afterMid = await poll(t, { cursor: mid.cursor as string, maxAgeMs: 60_000 });
      assertEqual(ids(afterMid), ['new'], 'after mid');
      assertEqual(afterMid.truncated, true, 'old next row');
      const afterOlder = await poll(t, { cursor: older.cursor as string, maxAgeMs: 60_000 });
      assertEqual(ids(afterOlder), ['new'], 'after older');
      assertEqual(afterOlder.truncated, false, 'fresh next row');
    }],
    ['errors carry the SEP codes and typed data', async (t, fresh) => {
      await error(() => poll(t, { name: 'unknown' }), -32023, 'kind', 'event');
      for (const cursor of ['garbage', 'v1.', 'v2.x.1']) await error(() => poll(t, { cursor }), -32602, 'field', 'cursor');
      for (const args of [{ bogus: 1 }, { scope: 'a', scopePrefix: 'a' }, { routes: ['skip'] }, { minProbability: 2 }]) {
        const field = await error(() => poll(t, { arguments: args }), -32602, 'field');
        assertOk(typeof field === 'string' && field.startsWith('arguments'), 'argument field');
      }
      for (const [args, field] of [[{ routes: ['interrupt'] }, 'arguments.routes'], [{ minProbability: 0.5 }, 'arguments.minProbability'], [{ include: 'events' }, 'arguments.include']] satisfies [JsonObject, string][]) {
        await error(() => poll(t, { name: 'attention.deadLetter', arguments: args }), -32602, 'field', field);
      }
      await error(() => poll(t, { maxEvents: 0 }), -32602, 'field', 'maxEvents');
      const restricted = await fresh({ id: 'tenant-a', scopes: ['tenant-a/'] });
      try {
        for (const args of [{ scope: 'tenant-b/x' }, { scopePrefix: 'tenant-b/' }]) await error(() => poll(restricted, { arguments: args }), -32024, 'reason');
        assertEqual(ids(await poll(restricted, { arguments: { scopePrefix: 'tenant-a/x' } })), [], 'granted prefix');
      } finally { await restricted.stop(); }
    }],
    ["a malformed position under the log's own epoch is InvalidParams", async (t) => {
      const cursor = await head(t);
      const epoch = cursor.split('.')[1];
      await error(() => poll(t, { cursor: `v1.${epoch}.not a position` }), -32602, 'field', 'cursor');
    }],
    ['a principal with no grants is Forbidden', async (t) => {
      for (const params of [{}, { arguments: { scope: 'a' } }, { name: 'attention.deadLetter' }] satisfies JsonObject[]) {
        await error(() => poll(t, params), -32024, 'reason');
      }
      const result = await t.request('tools/call', { name: 'attend_wait', arguments: { cursor: null, waitMs: 0 } });
      assertEqual(result.isError, true, 'tool error');
      assertEqual(record(record(result.structuredContent).error).code, -32024, 'Forbidden');
    }, { id: 'none', scopes: [] }],
    ['a principal reads only inside its grants', async (t, fresh) => {
      const cursor = await head(t);
      await t.commit([decision('a', 'a/x'), decision('b', 'b/x'), decision('c', 'c/x')]);
      assertEqual(ids(await poll(t, { cursor })), ['a'], 'single grant');
      const multiple = await fresh({ id: 'multi', scopes: ['a/', 'b/'] });
      try {
        const before = await head(multiple);
        await multiple.commit([decision('a', 'a/x'), decision('b', 'b/x'), decision('c', 'c/x')]);
        assertEqual(ids(await poll(multiple, { cursor: before })), ['a', 'b'], 'multiple grants');
      } finally { await multiple.stop(); }
    }, { id: 'single', scopes: ['a/'] }],
    ['a long poll answers as soon as a matching row commits', async (t) => {
      const cursor = await head(t);
      const startAt = Date.now();
      const timer = setTimeout(() => { void t.commit([decision('new')]); }, 100);
      try {
        assertEqual(ids(await poll(t, { cursor, waitMs: 5000 })), ['new'], 'awakened');
        assertOk(Date.now() - startAt < 4000, 'answered early');
      } finally { clearTimeout(timer); }
    }],
    ['a row that does not match does not end a long poll', async (t) => {
      const cursor = await head(t);
      const startAt = Date.now();
      const timer = setTimeout(() => { void t.commit([decision('other', 'other')]); }, 100);
      let result: Record<string, unknown>;
      try { result = await poll(t, { cursor, arguments: { scope: 'mine' }, waitMs: 800 }); }
      finally { clearTimeout(timer); }
      assertEqual(ids(result), [], 'still quiet');
      assertOk(Date.now() - startAt >= 600, 'waited');
      assertOk(result.cursor !== cursor, 'checked other row');
      await t.commit([decision('new', 'mine')]);
      assertEqual(ids(await poll(t, { cursor: result.cursor as string })), ['new'], 'past other');
    }],
    ['attend_wait returns the poll shapes', async (t) => {
      const listed = await t.request('tools/list', {});
      const tools = (listed.tools as unknown[]).map(record);
      assertEqual(tools.map((tool) => tool.name), ['attend_wait', 'attend_window'], 'tools');
      for (const tool of tools) assertEqual(record(tool.inputSchema).type, 'object', 'schema');
      const initial = record((await t.request('tools/call', { name: 'attend_wait', arguments: { cursor: null, waitMs: 0 } })).structuredContent);
      assertEqual(Object.keys(initial).sort(), ['cursor', 'events', 'truncated'], 'shape');
      await t.commit([decision('a')]);
      const result = record((await t.request('tools/call', { name: 'attend_wait', arguments: { cursor: initial.cursor as string, waitMs: 0 } })).structuredContent);
      assertEqual(ids(result), ['a'], 'wait result');
    }],
    ['attend_window returns the full window inside the grants only', async (t) => {
      const row = decision('allowed', 'a/x');
      await t.commit([row, decision('hidden', 'b/x')]);
      const call = (decisionId: string) => t.request('tools/call', { name: 'attend_window', arguments: { decisionId } });
      assertEqual(record((await call('allowed')).structuredContent).events, row.body.window.events, 'window');
      const missing = await call('missing');
      assertEqual(missing.isError, true, 'missing');
      assertEqual(await call('hidden'), missing, 'indistinguishable');
    }, { id: 'a', scopes: ['a/'] }],
  ];
  return cases.map(([name, run, principal = all]) => ({ name, async run() {
    const target = await checked(principal);
    try { await run(target, checked); } finally { await target.stop(); }
  } }));
}
