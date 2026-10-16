import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, afterEach, beforeAll, expect, test } from 'vitest';
import { createAttend } from '../src/attend.js';
import { memoryStore } from '../src/memory.js';
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

/** Opens events/stream and collects its SSE frames; `ended` settles when the server closes it. */
async function listen(base: string, headers: Record<string, string> = {}, cursor?: string) {
  const frames: Frame[] = [];
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
        if (frame.startsWith('data: ')) frames.push(JSON.parse(frame.slice(6)));
      }
    }
  })();
  const until = async (check: () => boolean) => { for (let i = 0; i < 400 && !check(); i += 1) await pause(25); expect(check()).toBe(true); };
  await until(() => frames.some((frame) => frame.method === 'notifications/events/active'));
  return { frames, ended, until, decisions: () => frames.filter((frame) => frame.params?.data?.route !== undefined) };
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
