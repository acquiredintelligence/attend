import { createServer, request, type ClientRequest, type Server } from 'node:http';
import { once } from 'node:events';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { ndjsonIngest } from '../../src/server/ingest.js';
import type { HttpRoute, NdjsonIngestOptions } from '../../src/server/types.js';
import type { Attend } from '../../src/shell/types.js';
import { BackpressureError, ConflictError, OutboxFullError } from '../../src/errors.js';
import { createAttend } from '../../src/attend.js';
import { memoryStore } from '../../src/memory.js';
import { fakeClock, scriptedJudge } from '../../src/testing.js';

const servers: Server[] = [];
const clients: ClientRequest[] = [];
const stops: (() => Promise<void>)[] = [];
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const event = (id: string, scope = 'allowed/a') => ({ scope, id, at: 0, name: 'event', data: { value: id } });
const lines = (values: unknown[]) => values.map((value) => JSON.stringify(value)).join('\n') + '\n';
const unused = () => { throw new Error('unused Attend member'); };
const fake = (overrides: Partial<Pick<Attend, 'push' | 'flush' | 'end'>> = {}): Attend => ({
  push: vi.fn(async () => undefined), flush: vi.fn(async () => undefined), end: vi.fn(async () => undefined),
  attach: unused, inspect: unused, decisions: unused, run: unused, ...overrides,
});
function deferred() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => { resolve = () => done(); });
  return { promise, resolve };
}
async function listen(route: HttpRoute) {
  const server = createServer((req, res) => { void Promise.resolve(route(req, res)).catch(() => res.destroy()); });
  servers.push(server);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('expected TCP address');
  return address.port;
}
function connect(port: number, options: { method?: string; contentType?: string } = {}) {
  type Frame = { credit?: number; ack?: number; through?: number; rejected?: number[]; errors?: string[] };
  const frames: Frame[] = [];
  const chunks: string[] = [];
  let partial = '';
  const req = request({ host: '127.0.0.1', port, path: '/v1/ingest', method: options.method ?? 'POST', headers: { 'content-type': options.contentType ?? 'application/x-ndjson' } });
  clients.push(req);
  const response = new Promise<{ status: number | undefined; headers: import('node:http').IncomingHttpHeaders; body: string }>((resolve, reject) => {
    req.on('error', reject);
    req.on('response', (res) => {
      res.setEncoding('utf8');
      res.on('data', (chunk: string) => {
        chunks.push(chunk);
        if (res.headers['content-type'] === 'application/x-ndjson') {
          partial += chunk;
          let newline: number;
          while ((newline = partial.indexOf('\n')) >= 0) {
            frames.push(JSON.parse(partial.slice(0, newline)));
            partial = partial.slice(newline + 1);
          }
        }
      });
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: chunks.join('') }));
    });
  });
  void response.catch(() => undefined);
  return { req, response, frames, chunks };
}
async function post(attend: Attend, body: string, options?: NdjsonIngestOptions) {
  const client = connect(await listen(ndjsonIngest(attend, options)));
  client.req.end(body);
  return client.response;
}
afterEach(async () => {
  for (const client of clients.splice(0)) client.destroy();
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => { server.closeAllConnections(); server.close(() => resolve()); })));
  await Promise.all(stops.splice(0).map((stop) => stop()));
});

describe('ndjsonIngest', () => {
  test('reports each line with its scope and isolates outbox and conflict rejections', async () => {
    const attend = fake({ push: vi.fn(async (scope) => {
      if (scope === 'allowed/full') throw new OutboxFullError(scope, 'decisions', 10);
      if (scope === 'allowed/conflict') throw new ConflictError(scope, 3);
    }) });
    const body = [JSON.stringify(event('a')), '', 'not json', JSON.stringify({ scope: 'allowed/b', id: 'missing' }),
      JSON.stringify({ scope: 'allowed/c', flush: true }), JSON.stringify({ scope: 'allowed/d', end: true }),
      JSON.stringify({ ...event('large'), text: 'x'.repeat(500) }), JSON.stringify(event('no', 'elsewhere')),
      JSON.stringify(event('full', 'allowed/full')), JSON.stringify(event('conflict', 'allowed/conflict')),
      JSON.stringify(event('last', 'allowed/z'))].join('\n');
    const result = await post(attend, body, { maxLineBytes: 200, principal: () => ({ id: 'test', scopes: ['allowed/'] }) });
    expect(result.status).toBe(200);
    expect(result.headers['content-type']).toBe('application/json');
    expect(JSON.parse(result.body)).toEqual({ accepted: 4, duplicates: 0, rejected: [
      { line: 1, error: 'invalid', scope: null }, { line: 2, error: 'invalid', scope: 'allowed/b' },
      { line: 5, error: 'too_large', scope: null }, { line: 6, error: 'forbidden', scope: 'elsewhere' },
      { line: 7, error: 'outbox_full', scope: 'allowed/full' }, { line: 8, error: 'conflict', scope: 'allowed/conflict' },
    ] });
    expect(attend.push).toHaveBeenCalledWith('allowed/z', [{ id: 'last', at: 0, name: 'event', data: { value: 'last' } }]);
    expect(attend.flush).toHaveBeenCalledWith('allowed/c');
    expect(attend.end).toHaveBeenCalledWith('allowed/d');
  });

  test('waits for commit before sending any request response bytes', async () => {
    const gate = deferred();
    const attend = fake({ push: vi.fn(() => gate.promise) });
    const client = connect(await listen(ndjsonIngest(attend)));
    client.req.end(lines([event('a')]));
    await expect.poll(() => vi.mocked(attend.push).mock.calls.length).toBe(1);
    await pause(100);
    expect(client.chunks).toEqual([]);
    gate.resolve();
    expect(JSON.parse((await client.response).body)).toEqual({ accepted: 1, duplicates: 0, rejected: [] });
  });
});

describe('ndjsonIngest stream', () => {
  test('sends initial credit immediately and acknowledges only committed lines', async () => {
    const gate = deferred();
    const attend = fake({ push: vi.fn(() => gate.promise) });
    const client = connect(await listen(ndjsonIngest(attend, { stream: true })));
    client.req.flushHeaders();
    await expect.poll(() => client.frames).toEqual([{ credit: 8192 }]);
    client.req.end(lines([event('a')]));
    await expect.poll(() => vi.mocked(attend.push).mock.calls.length).toBe(1);
    await pause(100);
    expect(client.frames).toEqual([{ credit: 8192 }]);
    gate.resolve();
    expect((await client.response).status).toBe(200);
    expect(client.frames).toEqual([{ credit: 8192 }, { ack: 1, through: 1 }, { credit: 1 }]);
  });

  test('acknowledges the contiguous prefix when a later write finishes first', async () => {
    const gates = [deferred(), deferred(), deferred()];
    const attend = fake({ push: vi.fn((_scope, events) => gates[Number(events[0]?.id)]!.promise) });
    const client = connect(await listen(ndjsonIngest(attend, { stream: true })));
    client.req.end(lines([event('0'), event('1'), event('2')]));
    await expect.poll(() => vi.mocked(attend.push).mock.calls.length).toBe(3);
    gates[0]!.resolve();
    await expect.poll(() => client.frames.some((frame) => frame.through === 1)).toBe(true);
    gates[2]!.resolve();
    await pause(100);
    expect(client.frames).toEqual([{ credit: 8192 }, { ack: 1, through: 1 }, { credit: 1 }]);
    gates[1]!.resolve();
    await client.response;
    expect(client.frames).toEqual([
      { credit: 8192 }, { ack: 1, through: 1 }, { credit: 1 }, { ack: 2, through: 3 }, { credit: 2 },
    ]);
  });

  test('withholds admission credit and queues later lines until the retry delay', async () => {
    let rejectedAt = 0;
    const calls: number[] = [];
    const attend = fake({ push: vi.fn(async () => {
      calls.push(Date.now());
      if (calls.length === 1) {
        rejectedAt = Date.now();
        throw new BackpressureError(200, 20000);
      }
    }) });
    const client = connect(await listen(ndjsonIngest(attend, { stream: true })));
    client.req.end(lines([event('a'), event('b')]));
    await expect.poll(() => client.frames.some((frame) => frame.through === 1), { interval: 5 }).toBe(true);
    await pause(120);
    expect(Date.now() - rejectedAt).toBeLessThan(190);
    expect(client.frames).toEqual([
      { credit: 8192 }, { ack: 1, through: 1, rejected: [0], errors: ['backpressure'] },
    ]);
    expect(attend.push).toHaveBeenCalledTimes(1);
    await client.response;
    expect(calls[1]! - rejectedAt).toBeGreaterThanOrEqual(190);
    expect(client.frames).toEqual([
      { credit: 8192 }, { ack: 1, through: 1, rejected: [0], errors: ['backpressure'] },
      { credit: 1 }, { ack: 2, through: 2 }, { credit: 1 },
    ]);
  });

  test('returns withheld credit even when the rejected line is the last line', async () => {
    const client = connect(await listen(ndjsonIngest(fake({ push: async () => { throw new BackpressureError(200, 20000); } }), { stream: true })));
    const started = Date.now();
    client.req.end(lines([event('a')]));
    await client.response;
    expect(Date.now() - started).toBeGreaterThanOrEqual(190);
    expect(client.frames).toEqual([
      { credit: 8192 }, { ack: 1, through: 1, rejected: [0], errors: ['backpressure'] }, { credit: 1 },
    ]);
  });

  test('releases concurrent admission credits on their individual retry deadlines', async () => {
    const gate = deferred();
    const attend = fake({ push: vi.fn(async (_scope, events) => {
      await gate.promise;
      throw new BackpressureError(events[0]?.id === 'a' ? 50 : 200, 20000);
    }) });
    const client = connect(await listen(ndjsonIngest(attend, { stream: true })));
    client.req.end(lines([event('a'), event('b')]));
    await expect.poll(() => vi.mocked(attend.push).mock.calls.length).toBe(2);
    gate.resolve();
    await client.response;
    expect(client.frames).toEqual([
      { credit: 8192 }, { ack: 1, through: 2, rejected: [0, 1], errors: ['backpressure', 'backpressure'] },
      { credit: 1 }, { credit: 1 },
    ]);
  });

  test('limits unacknowledged intake to twice the credit and eventually acknowledges all 40000 lines', async () => {
    const gate = deferred();
    const attend = fake({ push: vi.fn(() => gate.promise) });
    const client = connect(await listen(ndjsonIngest(attend, { stream: true })));
    let blocked = false;
    let sent = 0;
    let finishedWriting = false;
    const writing = (async () => {
      while (sent < 40000) {
        const writable = client.req.write(lines([{ ...event(String(sent++)), text: 'x'.repeat(256) }]));
        if (!writable) { blocked = true; await once(client.req, 'drain'); }
      }
      finishedWriting = true;
      client.req.end();
    })();
    void writing.catch(() => undefined);
    await expect.poll(() => vi.mocked(attend.push).mock.calls.length, { timeout: 10000 }).toBe(16384);
    await pause(100);
    expect(attend.push).toHaveBeenCalledTimes(16384);
    expect(blocked).toBe(true);
    expect(finishedWriting).toBe(false);
    expect(client.frames).toEqual([{ credit: 8192 }]);
    gate.resolve();
    await writing;
    await client.response;
    expect(attend.push).toHaveBeenCalledTimes(40000);
    expect(client.frames.filter((frame) => frame.through !== undefined).at(-1)).toMatchObject({ through: 40000 });
    expect(client.frames.reduce((sum, frame) => sum + (frame.credit ?? 0), 0)).toBe(48192);
    let through = 0;
    let acknowledged = 0;
    let sequence = 0;
    for (const frame of client.frames) {
      if (frame.through === undefined) continue;
      expect(frame.ack).toBe(++sequence);
      expect(frame.through).toBeGreaterThan(through);
      acknowledged += frame.through - through;
      through = frame.through;
    }
    expect(acknowledged).toBe(40000);
  }, 20000);

  test('discards an oversized fragmented line through its newline and preserves UTF-8 across chunks', async () => {
    const attend = fake();
    const client = connect(await listen(ndjsonIngest(attend, { stream: true, maxLineBytes: 160 })));
    client.req.write('{"scope":"oversized","text":"');
    client.req.write('x'.repeat(10000));
    await pause(30);
    expect(attend.push).toHaveBeenCalledTimes(0);
    expect(client.frames).toEqual([{ credit: 8192 }]);
    client.req.write('"}\n \t\r\n');
    const utf8 = Buffer.from(lines([{ ...event('é'), text: '🙂' }]));
    const split = utf8.indexOf(Buffer.from('🙂')) + 1;
    client.req.write(utf8.subarray(0, split));
    await pause(10);
    client.req.end(utf8.subarray(split));
    await client.response;
    expect(attend.push).toHaveBeenCalledWith('allowed/a', [{ id: 'é', at: 0, name: 'event', data: { value: 'é' }, text: '🙂' }]);
    expect(client.frames.filter((frame) => frame.rejected)).toEqual([
      { ack: 1, through: 1, rejected: [0], errors: ['too_large'] },
    ]);
    expect(client.frames.filter((frame) => frame.through !== undefined).at(-1)?.through).toBe(2);
  });
});

describe('ndjsonIngest admission and validation', () => {
  test('returns 429 with retry-after and rejects all lines after the admission failure', async () => {
    let calls = 0;
    const attend = fake({ push: vi.fn(async () => {
      if (++calls === 3) throw new BackpressureError(1500, 20000);
    }) });
    const result = await post(attend, lines([event('0'), event('1'), event('2'), event('3'), event('4')]));
    expect(result.status).toBe(429);
    expect(result.headers['retry-after']).toBe('2');
    expect(JSON.parse(result.body)).toEqual({ accepted: 2, duplicates: 0, rejected: [
      { line: 2, scope: 'allowed/a', error: 'backpressure' },
      { line: 3, scope: 'allowed/a', error: 'backpressure' },
      { line: 4, scope: 'allowed/a', error: 'backpressure' },
    ] });
    expect(attend.push).toHaveBeenCalledTimes(3);
  });

  test.each([
    { name: 'method', method: 'GET', contentType: 'application/x-ndjson', status: 405 },
    { name: 'content type', method: 'POST', contentType: 'application/json', status: 415 },
  ])('rejects an unsupported $name', async ({ method, contentType, status }) => {
    const attend = fake();
    const client = connect(await listen(ndjsonIngest(attend)), { method, contentType });
    client.req.end();
    const result = await client.response;
    expect(result.status).toBe(status);
    if (status === 405) expect(result.headers.allow).toBe('POST');
    expect(attend.push).toHaveBeenCalledTimes(0);
  });

  test('resolves the principal before method and content-type checks', async () => {
    const principal = vi.fn(async () => null);
    const client = connect(await listen(ndjsonIngest(fake(), { principal })), { method: 'GET', contentType: 'text/plain' });
    client.req.end();
    expect((await client.response).status).toBe(401);
    expect(principal).toHaveBeenCalledTimes(1);
  });

  test('rejects an oversized body without dispatching earlier valid lines', async () => {
    const attend = fake();
    const client = connect(await listen(ndjsonIngest(attend, { maxBodyBytes: 100 })));
    client.req.write(lines([event('a')]));
    client.req.end('x'.repeat(101));
    expect((await client.response).status).toBe(413);
    expect(attend.push).toHaveBeenCalledTimes(0);
  });

  test('validates objects, fields and exclusive commands with exact scopes', async () => {
    const values = [null, [], 42, { scope: '' }, { scope: 1 }, { ...event('a'), at: '1' },
      { ...event('a'), data: [] }, { ...event('a'), text: 1 }, { scope: 's', flush: false },
      { scope: 's', end: false }, { scope: 's', flush: true, end: true },
      { scope: 's', flush: true, end: false }, { ...event('a'), name: null }];
    const body = lines(values) + '{"scope":"s","id":"infinite","at":1e999,"name":"event","data":{}}\n';
    const result = await post(fake(), body);
    expect(JSON.parse(result.body)).toEqual({ accepted: 0, duplicates: 0, rejected: [
      { line: 0, scope: null, error: 'invalid' }, { line: 1, scope: null, error: 'invalid' },
      { line: 2, scope: null, error: 'invalid' }, { line: 3, scope: '', error: 'invalid' },
      { line: 4, scope: null, error: 'invalid' }, { line: 5, scope: 'allowed/a', error: 'invalid' },
      { line: 6, scope: 'allowed/a', error: 'invalid' }, { line: 7, scope: 'allowed/a', error: 'invalid' },
      { line: 8, scope: 's', error: 'invalid' }, { line: 9, scope: 's', error: 'invalid' },
      { line: 10, scope: 's', error: 'invalid' }, { line: 11, scope: 's', error: 'invalid' },
      { line: 12, scope: 'allowed/a', error: 'invalid' }, { line: 13, scope: 's', error: 'invalid' },
    ] });
  });

  test('accepts media-type parameters, grant-all prefixes, extra payload fields and a final line without newline', async () => {
    const attend = fake();
    const client = connect(await listen(ndjsonIngest(attend, { principal: () => ({ id: 'all', scopes: [''] }) })), { contentType: 'application/x-ndjson; charset=utf-8' });
    client.req.end('\n \r\n' + JSON.stringify({ ...event('a', 'anywhere'), text: 'hello', extra: 1 }));
    expect(JSON.parse((await client.response).body)).toEqual({ accepted: 1, duplicates: 0, rejected: [] });
    expect(attend.push).toHaveBeenCalledWith('anywhere', [{ id: 'a', at: 0, name: 'event', data: { value: 'a' }, text: 'hello', extra: 1 }]);
  });

  test('reports unexpected errors as a short 500 without leaking the error', async () => {
    const result = await post(fake({ push: async () => { throw new Error('secret detail'); } }), lines([event('a')]));
    expect(result.status).toBe(500);
    expect(result.body).toBe('Internal server error');
  });

  test('destroys a stream on an unexpected operation error', async () => {
    const gate = deferred();
    const attend = fake({ push: async () => { await gate.promise; throw new Error('secret detail'); } });
    const client = connect(await listen(ndjsonIngest(attend, { stream: true })));
    client.req.end(lines([event('a')]));
    await expect.poll(() => client.frames).toEqual([{ credit: 8192 }]);
    gate.resolve();
    await expect(client.response).rejects.toThrow();
    expect(client.frames).toEqual([{ credit: 8192 }]);
  });
});

describe.each([false, true])('ndjsonIngest slicing (stream=%s)', (stream) => {
  test.each([
    { count: 200, slice: undefined, bursts: [64, 64, 64, 8] },
    { count: 25, slice: 10, bursts: [10, 10, 5] },
  ])('dispatches $count lines in macrotask slices', async ({ count, slice, bursts }) => {
    const observed: number[] = [];
    let burst = 0;
    const attend = fake({ push: async () => {
      if (burst++ === 0) setImmediate(() => { observed.push(burst); burst = 0; });
    } });
    const result = await post(attend, lines(Array.from({ length: count }, (_, i) => event(String(i)))), { stream, ...(slice === undefined ? {} : { slice }) });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(result.status).toBe(200);
    expect(observed).toEqual(bursts);
  });

  test('stops dispatching further slices after a client disconnect', async () => {
    const gate = deferred();
    let client: ReturnType<typeof connect>;
    let disconnected = false;
    let callsAfterDisconnect = 0;
    const attend = fake({ push: vi.fn(() => {
      if (disconnected) callsAfterDisconnect += 1;
      client.req.destroy();
      return gate.promise;
    }) });
    const route = ndjsonIngest(attend, { stream, slice: 1 });
    client = connect(await listen((req, res) => {
      res.on('close', () => { disconnected = true; });
      return route(req, res);
    }));
    client.req.end(lines(Array.from({ length: 200 }, (_, i) => event(String(i)))));
    await expect(client.response).rejects.toThrow();
    await pause(100);
    expect(disconnected).toBe(true);
    expect(callsAfterDisconnect).toBe(0);
    expect(vi.mocked(attend.push).mock.calls.length).toBeLessThan(200);
    gate.resolve();
  });
});

describe.each([false, true])('ndjsonIngest real Attend (stream=%s)', (stream) => {
  test('commits posted events before responding', async () => {
    const attend = createAttend({ store: memoryStore(), clock: fakeClock(1000), judge: scriptedJudge([{ route: 'note' }]), kind: () => 'context', sink: 'log', deadLetter: 'log' });
    const controller = new AbortController();
    const running = attend.run({ signal: controller.signal });
    stops.push(async () => { controller.abort(); await running; });
    const body = lines([event('a'), event('b'), ...(stream ? [{ scope: 'allowed/a', flush: true }] : [])]);
    const client = connect(await listen(ndjsonIngest(attend, { stream })));
    client.req.end(body);
    const result = await client.response;
    expect(result.status).toBe(200);
    if (stream) expect(client.frames.filter((frame) => frame.through !== undefined).at(-1)?.through).toBe(3);
    else expect(JSON.parse(result.body)).toEqual({ accepted: 2, duplicates: 0, rejected: [] });
    const record = await attend.inspect('allowed/a');
    expect(record?.seen).toEqual(['a', 'b']);
    expect(record?.buffer.entries.map(({ id, data }) => ({ id, data }))).toEqual([
      { id: 'a', data: { value: 'a' } }, { id: 'b', data: { value: 'b' } },
    ]);
  });
});
