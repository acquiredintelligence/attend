import { test, expect } from 'vitest';
import { countingSqlite, countingPostgres, countingRedis, countingStatePort, countingScopePort, openStore } from '../lib/store.mjs';
import { loadBuild } from '../lib/build.mjs';
import { validateReport } from '../lib/schema.mjs';
import { cli, dist } from './helpers.mjs';

test('SQLite accounting counts only run strings and byte parameters', () => {
  const counter = { bytes: 0 };
  const db = countingSqlite({
    prepare() { return { run: (...args) => args.length, get: () => 7, all: () => [8] }; },
  }, counter);
  const statement = db.prepare('insert');
  expect(statement.get('ignored')).toBe(7);
  expect(statement.all('ignored')).toEqual([8]);
  expect(counter.bytes).toBe(0);
  expect(statement.run('abc', Buffer.from([1, 2]), new Uint8Array([3, 4, 5]), 999, null)).toBe(5);
  expect(counter.bytes).toBe(8);
});
test('Postgres accounting skips SELECT after whitespace regardless of case', async () => {
  const counter = { bytes: 0 };
  const client = countingPostgres({ query: async () => ({ rows: [], rowCount: 1 }) }, counter);
  expect(await client.query(' \n select value', ['ignored'])).toEqual({ rows: [], rowCount: 1 });
  expect(counter.bytes).toBe(0);
  await client.query('UPDATE records', ['abc', Buffer.from('de')]);
  await client.query({ text: 'INSERT INTO records', values: ['f'] });
  expect(counter.bytes).toBe(6);
});
test('Redis accounting skips every read command and script bodies and counts all write arguments', async () => {
  const counter = { bytes: 0 };
  const send = countingRedis(async () => 'OK', counter);
  for (const command of 'GET MGET HGET HMGET HGETALL EXISTS ZRANGE ZRANGEBYSCORE ZSCORE XRANGE XREVRANGE XREAD XLEN SCAN TYPE PTTL TTL CONFIG PING INFO'.split(' ')) {
    expect(await send([command.toLowerCase(), 'ignored'])).toBe('OK');
  }
  expect(counter.bytes).toBe(0);
  await send(['SET', 'abc', 'de']);
  await send(['DEL', 'f']);
  expect(counter.bytes).toBe(6);
  await send(['EVAL', 'return redis.call("SET", KEYS[1], ARGV[1])', '1', 'k', 'value']);
  await send(['evalsha', 'a'.repeat(40), '1', 'k', 'xy']);
  expect(counter.bytes).toBe(17);
});
test('State accounting records one latency per settled put including rejection', async () => {
  const counter = { bytes: 0 };
  const store = countingStatePort({
    get: async () => 7,
    put: async (value) => { if (!value) throw new Error('failed'); return 'ok'; },
  }, counter);
  expect(await store.get()).toBe(7);
  expect(await store.put({ scope: 's' }, 0)).toBe('ok');
  await expect(store.put(null, 0)).rejects.toThrow('failed');
  expect(counter.commits).toBe(2);
  expect(counter.latency.count).toBe(2);
  expect(counter.bytes).toBe(0);
});
test('Scope accounting counts a group once and preserves every other port member', async () => {
  const counter = { bytes: 0 };
  let closed = false;
  const log = { read: () => [] };
  const store = countingScopePort({
    durable: true, log, close() { closed = true; },
    load: async (scope) => scope, due: async () => ['s'], remove: async () => 'removed',
    commit: async (writes) => writes.map(() => 'ok'),
  }, counter);
  expect(await store.commit([
    { serialized: { inputs: ['123', '45'], snapshot: '6789', appends: ['abc'] } },
    { serialized: { inputs: ['d'], snapshot: null, appends: ['ef'] } }, {},
  ])).toEqual(['ok', 'ok', 'ok']);
  expect(counter.commits).toBe(1);
  expect(counter.bytes).toBe(15);
  expect(counter.latency.count).toBe(1);
  expect(store.durable).toBe(true);
  expect(store.log).toBe(log);
  expect(await store.load('s')).toBe('s');
  expect(await store.due()).toEqual(['s']);
  expect(await store.remove()).toBe('removed');
  store.close();
  expect(closed).toBe(true);
});
test.each([['WAL', 'NORMAL'], ['DELETE', 'FULL']])('SQLite reads back %s/%s from its file', async (journalMode, synchronous) => {
  const build = await loadBuild(dist);
  const opened = await openStore(build, { store: 'sqlite', journalMode, synchronous, worker: false });
  try { expect(opened.durability).toEqual({ store: 'sqlite', journalMode, synchronous, worker: false }); }
  finally { await opened.close(); }
});
test('SQLite worker mode rejects the state port', async () => {
  const build = await loadBuild(dist);
  await expect(openStore(build, { store: 'sqlite', journalMode: 'WAL', synchronous: 'NORMAL', worker: true })).rejects.toThrow('worker mode needs the scope store port');
});
test('stores CLI produces a validated SQLite report with bound record bytes', async () => {
  const result = await cli('stores', ['--dist', dist, '--store', 'sqlite', '--workloads', '5x20', '--max-windows', '50']);
  expect(result.code, result.stderr).toBe(0);
  const report = JSON.parse(result.stdout);
  expect(validateReport(report).ok).toBe(true);
  expect(report.eventsPerCommit).toBeGreaterThan(0);
  expect(report.bytesWrittenPerEvent).toBeGreaterThan(report.eventBytes);
  expect(report.scopes).toBe(5);
  expect(report.eventsPerScope).toBe(20);
});
