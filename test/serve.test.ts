import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createClient } from 'redis';
import { afterAll, afterEach, beforeAll, expect, test } from 'vitest';
import { createAttend } from '../src/attend.js';
import { memoryStore } from '../src/memory.js';
import { redisStore } from '../src/redis.js';
import { cli, serve } from '../src/server/serve.js';

const root = join(import.meta.dirname, '..');
const dist = join(root, 'node_modules/.cache/attend-smoke');
const stream = { 'content-type': 'application/json', accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2026-07-28', 'mcp-method': 'events/stream' };
const ndjson = (rows: object[]) => rows.map((row) => JSON.stringify(row)).join('\n') + '\n';
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const stoppers: (() => Promise<unknown>)[] = [];
afterEach(async () => { await Promise.all(stoppers.splice(0).map((stop) => stop())); });

beforeAll(() => {
  execFileSync(join(root, 'node_modules/.bin/tsc'), ['-p', 'tsconfig.build.json', '--outDir', dist], { cwd: root, timeout: 120_000 });
}, 120_000);

interface Frame { method?: string; params?: { eventId?: string; type?: string; data?: { route: string; scope: string; eventIds: string[]; notice: string }; cursor?: string } }

/** Opens events/stream and collects its SSE frames, and when each decision frame arrived; `ended` settles when the server closes it. */
async function listen(base: string, headers: Record<string, string> = {}, cursor?: string) {
  const frames: Frame[] = [];
  const arrivals: number[] = [];
  const controller = new AbortController();
  stoppers.push(async () => controller.abort());
  const response = await fetch(`${base}/mcp`, { method: 'POST', headers: { ...stream, ...headers }, signal: controller.signal, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'events/stream', params: { name: 'attention.decision', ...(cursor === undefined ? {} : { cursor }) } }) });
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const ended = (async () => {
    for (;;) {
      const { done, value } = await reader.read().catch(() => ({ done: true, value: undefined }));
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      for (let split = buffer.indexOf('\n\n'); split >= 0; split = buffer.indexOf('\n\n')) {
        const frame = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);
        if (!frame.startsWith('data: ')) continue;
        const parsed: Frame = JSON.parse(frame.slice(6));
        frames.push(parsed);
        if (parsed.params?.data?.route !== undefined) arrivals.push(performance.now());
      }
    }
  })();
  const until = async (check: () => boolean) => { for (let i = 0; i < 400 && !check(); i += 1) await pause(25); expect(check()).toBe(true); };
  await until(() => frames.some((frame) => frame.method === 'notifications/events/active'));
  return { frames, arrivals, ended, until, decisions: () => frames.filter((frame) => frame.params?.data?.route !== undefined) };
}

/** One events/poll that waits up to `waitMs` past `cursor`; `at` is when its reply arrived. */
async function poll(base: string, cursor: string, waitMs: number) {
  const response = await fetch(`${base}/mcp`, { method: 'POST', headers: { ...stream, 'mcp-method': 'events/poll' }, body: JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'events/poll', params: { name: 'attention.decision', cursor, waitMs, _meta: { 'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': { extensions: { 'io.modelcontextprotocol/events': {} } } } } }) });
  const body = await response.json() as { result?: { events: unknown[] } };
  return { at: performance.now(), events: body.result?.events };
}

async function ingest(base: string, rows: object[], headers: Record<string, string> = {}) {
  const response = await fetch(`${base}/v1/ingest`, { method: 'POST', headers: { 'content-type': 'application/x-ndjson', ...headers }, body: ndjson(rows) });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : null };
}

const pair = [
  { scope: 'app/1', id: 'e1', at: 1, name: 'build.failed', data: {}, text: 'build 12 failed' },
  { scope: 'app/1', id: 'e2', at: 2, name: 'user.said', data: {}, text: 'can you look?' },
];

function start(args: string[], env: NodeJS.ProcessEnv = {}) {
  const child = spawn(process.execPath, [join(dist, 'cli.js'), 'serve', '--port', '0', ...args], { env: { PATH: process.env.PATH, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  const exited = new Promise<number | null>((resolve) => child.once('exit', (code) => resolve(code)));
  stoppers.push(async () => { child.kill('SIGKILL'); await exited; });
  const url = new Promise<string>((resolve, reject) => {
    let text = '';
    child.stdout!.on('data', (chunk: Buffer) => {
      text += chunk;
      const match = /attend serving (http:\/\/\S+)/.exec(text);
      if (match) resolve(match[1]!);
    });
    exited.then((code) => reject(new Error(`exited ${code}`)));
  });
  return { child, url, exited };
}

test('the built CLI takes NDJSON in and puts the decision on events/stream', async () => {
  expect(execFileSync(process.execPath, [join(dist, 'cli.js'), '--help'], { encoding: 'utf8' })).toContain('usage: attend serve');
  const { child, url, exited } = start(['--judge', 'rule', '--kinds', '{"user.said":"flush"}']);
  const base = await url;
  const feed = await listen(base);
  expect(await ingest(base, pair)).toEqual({ status: 200, body: { accepted: 2, duplicates: 0, rejected: [] } });
  await feed.until(() => feed.decisions().length === 1);
  const data = feed.decisions()[0]!.params!.data!;
  expect({ route: data.route, scope: data.scope, events: data.eventIds, notice: data.notice }).toEqual({ route: 'interrupt', scope: 'app/1', events: ['e1', 'e2'], notice: 'build 12 failed\ncan you look?\nAttend to this now, even mid-task.' });
  child.kill('SIGTERM');
  expect(await exited).toBe(0);
  await feed.ended;
}, 60_000);

test('sqlite keeps the decision log across restarts', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'attend-serve-'));
  stoppers.push(async () => rmSync(dir, { recursive: true, force: true }));
  const file = join(dir, 'attend.db');
  const first = start(['--store', `sqlite:${file}`, '--kinds', '{"user.said":"flush"}']);
  const one = await first.url;
  const feed = await listen(one);
  await ingest(one, pair);
  await feed.until(() => feed.decisions().length === 1);
  first.child.kill('SIGTERM');
  expect(await first.exited).toBe(0);
  const second = start(['--store', `sqlite:${file}`]);
  const two = await second.url;
  const epoch = feed.frames.find((frame) => frame.params?.cursor)!.params!.cursor!.split('.')[1];
  const replay = await listen(two, {}, `v1.${epoch}.`);
  await replay.until(() => replay.decisions().length === 1);
  expect(replay.decisions()[0]!.params!.data).toMatchObject({ route: 'interrupt', scope: 'app/1', eventIds: ['e1', 'e2'] });
}, 60_000);

/**
 * The CLI writes under the default `attend:` prefix, so its Redis test gets a database of its own, cleared of that prefix.
 * `log` reads the same decision log through a store that blocks on a connection of its own, so its `wait` resolves when a decision lands.
 */
async function redisDatabase(db: number) {
  const url = new URL(process.env.ATTEND_REDIS_URL ?? 'redis://127.0.0.1:16379');
  url.pathname = `/${db}`;
  const client = createClient({ url: url.href, socket: { reconnectStrategy: false, connectTimeout: 5000 } });
  await client.connect();
  const blocking = client.duplicate();
  await blocking.connect();
  stoppers.push(async () => { blocking.destroy(); client.destroy(); });
  const clear = async () => { for await (const keys of client.scanIterator({ MATCH: 'attend:*', COUNT: 1000 })) if (keys.length > 0) await client.unlink(keys); };
  await clear();
  const { log } = redisStore((args) => client.sendCommand(args), { blockingSend: (args) => blocking.sendCommand(args) });
  return { url: url.href, clear, log };
}

const percentile = (values: number[], p: number) => [...values].sort((a, b) => a - b)[Math.ceil(values.length * p / 100) - 1]!;
const summary = (values: number[]) => `p50 ${percentile(values, 50).toFixed(1)} p90 ${percentile(values, 90).toFixed(1)} max ${Math.max(...values).toFixed(1)} ms`;

test('on redis, a decision wakes a waiting events/stream subscriber and events/poll caller at once, not on the next poll', async () => {
  const redis = await redisDatabase(15);
  const { child, url, exited } = start(['--store', `redis:${redis.url}`, '--kinds', '{"user.said":"flush"}']);
  const base = await url;
  const feed = await listen(base);
  const scope = `wake/${randomUUID()}`;
  const streamed: number[] = [];
  const polled: number[] = [];
  const ingestToStream: number[] = [];
  for (let i = 0; i < 40; i += 1) {
    const seen = feed.decisions().length;
    const { last } = await redis.log.bounds();
    const landed = redis.log.wait(last, 10_000, AbortSignal.timeout(10_000)).then(() => performance.now());
    const waiting = poll(base, feed.frames.findLast((frame) => frame.params?.cursor)!.params!.cursor!, 10_000);
    // Both subscribers are parked before the event goes in, at a random phase of any poll loop on the server.
    await pause(30 + Math.random() * 250);
    expect((await ingest(base, [{ scope, id: `e${i}`, at: Date.now(), name: 'user.said', data: {}, text: 'now' }])).status).toBe(200);
    const replied = performance.now();
    const answer = await waiting;
    expect(answer.events).toHaveLength(1);
    await feed.until(() => feed.decisions().length > seen);
    const committed = await landed;
    streamed.push(feed.arrivals[seen]! - committed);
    polled.push(answer.at - committed);
    ingestToStream.push(feed.arrivals[seen]! - replied);
  }
  console.log(`redis, ${streamed.length} samples. decision committed to events/stream: ${summary(streamed)}; to events/poll: ${summary(polled)}. ingest reply to events/stream: ${summary(ingestToStream)}`);
  // Measured on an idle host: p90 2 to 5 ms with a blocking connection, 210 to 226 ms on the 250 ms poll.
  // The poll puts 80% of samples above 50 ms, so 36 of 40 below it does not happen by luck.
  expect(percentile(streamed, 90)).toBeLessThan(50);
  expect(percentile(polled, 90)).toBeLessThan(50);
  child.kill('SIGTERM');
  expect(await exited).toBe(0);
  await redis.clear();
}, 60_000);

test('serve() answers once ready, is guarded by its principal, and closes with a stream open', async () => {
  const store = memoryStore();
  const attend = createAttend({ store, judge: async () => ({ route: 'queue' }), kind: (event) => (event.name === 'user.said' ? 'flush' : 'consequence'), sink: 'log', deadLetter: 'log' });
  const server = serve(attend, store, { listen: { port: 0 }, principal: (req) => (req.headers.authorization === 'Bearer t' ? { id: 'p', scopes: ['app/'] } : null) });
  expect(() => server.url()).toThrow('await ready');
  await server.ready;
  const base = server.url();
  expect(await ingest(base, pair, {})).toEqual({ status: 401, body: null });
  const auth = { authorization: 'Bearer t' };
  expect(await ingest(base, [pair[0]!, { ...pair[1]!, scope: 'other/1' }], auth)).toEqual({ status: 200, body: { accepted: 1, duplicates: 0, rejected: [{ line: 1, scope: 'other/1', error: 'forbidden' }] } });
  const feed = await listen(base, auth);
  const started = Date.now();
  await server.close();
  await server.close();
  expect(Date.now() - started).toBeLessThan(2000);
  await feed.ended;
  await expect(fetch(`${base}/v1/ingest`, { method: 'POST', body: '' })).rejects.toThrow();
}, 30_000);

test('--token answers 401 to ingest and MCP without the bearer, and accepts it with', async () => {
  const { child, url, exited } = start(['--token', 'test-token', '--kinds', '{"user.said":"flush"}']);
  const base = await url;
  expect((await ingest(base, pair)).status).toBe(401);
  const initialize = { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'test', version: '0' } } };
  const mcp = await fetch(`${base}/mcp`, { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify(initialize) });
  expect(mcp.status).toBe(401);
  expect(await ingest(base, pair, { authorization: 'Bearer test-token' })).toEqual({ status: 200, body: { accepted: 2, duplicates: 0, rejected: [] } });
  child.kill('SIGTERM');
  expect(await exited).toBe(0);
}, 60_000);

test('usage errors exit 2 and never print a key', async () => {
  const run = async (argv: string[], env: NodeJS.ProcessEnv = {}) => {
    let text = '';
    const code = await cli(argv, env, { out: (t) => void (text += t), err: (t) => void (text += t) });
    return { code, text };
  };
  const jev = await run(['serve', '--judge', 'jev']);
  expect(jev.code).toBe(2);
  expect(jev.text).toContain('TYPESAFE_API_KEY');
  expect((await run(['serve', '--judge', 'decisions'], { OPENAI_API_KEY: 'sk-secret-value' })).text).not.toContain('sk-secret-value');
  expect((await run(['serve', '--store', 'bogus'])).code).toBe(2);
  expect((await run(['serve', '--kinds', '{"x":"nope"}'])).code).toBe(2);
  expect((await run(['serve', '--act-threshold', '2'])).code).toBe(2);
  expect((await run(['serve', '--port', 'abc'])).code).toBe(2);
  expect((await run(['frobnicate'])).code).toBe(2);
  expect(await run(['--help'])).toMatchObject({ code: 0 });
});

test('a flag beats its env var, and aborting the signal shuts the command down', async () => {
  let text = '';
  const stop = new AbortController();
  const done = cli(['serve', '--port', '0', '--kinds', '{"user.said":"closing"}'], { ATTEND_KINDS: '{"user.said":"flush"}', ATTEND_STORE: 'memory' }, { out: (t) => void (text += t), signal: stop.signal });
  const base = await new Promise<string>((resolve) => { const timer = setInterval(() => { const match = /serving (http:\/\/\S+)/.exec(text); if (match) { clearInterval(timer); resolve(match[1]!); } }, 10); });
  const feed = await listen(base);
  await ingest(base, pair);
  await pause(300);
  expect(feed.decisions()).toEqual([]);
  stop.abort();
  expect(await done).toBe(0);
});

afterAll(() => { rmSync(dist, { recursive: true, force: true }); });
