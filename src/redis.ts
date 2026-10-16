/**
 * `attend/redis`: a `StateStore` and an `EventLog` over Redis 6.2 or later. Takes a function
 * that sends one command and resolves with its reply, so importing this module loads no client:
 *
 *   node-redis: (args) => client.sendCommand(args)
 *   ioredis:    ([command, ...args]) => client.call(command, ...args)
 *
 * Replies are parsed whether the client hands back strings or Buffers, over RESP2 or RESP3.
 * On Redis Cluster, put a hash tag in the prefix, such as 'attend:{app}:', so that every key a
 * script touches lands in one slot.
 */
import { boundary } from './internal/adapter.js';
import type { EventLog, LogEntry, StateStore } from './types.js';

const { parseEvent, parseRecord, text } = boundary('attend/redis');

export type RedisSend = (args: string[]) => Promise<unknown>;

export interface RedisOptions {
  /** Prepended to every key. Default 'attend:'. */
  prefix?: string;
}

/** KEYS: record hash, due set. ARGV: expected version, new version, record JSON, dueAt or '', scope. */
const PUT = `
local current = redis.call('HGET', KEYS[1], 'version')
if (current and tonumber(current) or 0) ~= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[2], 'record', ARGV[3])
if ARGV[4] == '' then redis.call('ZREM', KEYS[2], ARGV[5]) else redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5]) end
return 1`;

/** KEYS: record hash, due set. ARGV: scope. */
const REMOVE = `
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1`;

/** KEYS: stream. ARGV: event JSON, in order. */
const APPEND = `
for i = 1, #ARGV do redis.call('XADD', KEYS[1], '*', 'event', ARGV[i]) end
return #ARGV`;

/**
 * Each record is a hash holding its version beside its JSON, so the CAS script compares a
 * field instead of decoding the document. A sorted set scored by `dueAt` indexes due scopes.
 */
export function redisStore(send: RedisSend, options?: RedisOptions): StateStore {
  const prefix = options?.prefix ?? 'attend:';
  const key = (scope: string) => `${prefix}record:${scope}`;
  const due = `${prefix}due`;
  return {
    async get(scope) {
      const reply = await send(['HGET', key(scope), 'record']);
      return reply === null || reply === undefined ? null : parseRecord(reply, scope);
    },
    async put(record, expectedVersion) {
      const dueAt = record.dueAt === null ? '' : String(record.dueAt);
      const reply = await send(['EVAL', PUT, '2', key(record.scope), due, String(expectedVersion), String(record.version), JSON.stringify(record), dueAt, record.scope]);
      return integer(reply) === 1 ? 'ok' : 'conflict';
    },
    async due(now, limit) {
      return list(await send(['ZRANGEBYSCORE', due, '-inf', String(now), 'LIMIT', '0', String(limit)])).map(text);
    },
    async remove(scope) {
      await send(['EVAL', REMOVE, '2', key(scope), due, scope]);
    },
  };
}

/**
 * One stream per scope. Positions are stream ids with both parts padded to twenty digits, so
 * they compare as strings in read order; a raw id puts `…-10` before `…-9`.
 *
 * `read` waits with XREAD BLOCK only when `blockingSend` is given, because a blocked command
 * holds its connection. Give each blocking reader a connection of its own. Without it, `read`
 * returns at once and `logSource` paces the loop.
 */
export function redisLog(send: RedisSend, options?: RedisOptions & { blockingSend?: RedisSend }): EventLog {
  const prefix = options?.prefix ?? 'attend:';
  const blockingSend = options?.blockingSend;
  const key = (scope: string) => `${prefix}log:${scope}`;
  return {
    async append(scope, events) {
      if (events.length === 0) return;
      await send(['EVAL', APPEND, '1', key(scope), ...events.map((event) => JSON.stringify(event))]);
    },
    async read(scope, after, { limit, blockMs }) {
      const stream = key(scope);
      const entries = parseEntries(await send(['XRANGE', stream, after === null ? '-' : `(${streamId(after)}`, '+', 'COUNT', String(limit)]));
      if (entries.length > 0 || blockingSend === undefined || blockMs === undefined || blockMs <= 0) return entries;
      const reply = await blockingSend(['XREAD', 'COUNT', String(limit), 'BLOCK', String(Math.ceil(blockMs)), 'STREAMS', stream, after === null ? '0-0' : streamId(after)]);
      return reply === null || reply === undefined ? [] : parseEntries(onlyStream(reply));
    },
  };
}

function integer(reply: unknown): number {
  if (typeof reply === 'number') return reply;
  if (typeof reply === 'bigint') return Number(reply);
  return Number(text(reply));
}

function list(reply: unknown): unknown[] {
  if (!Array.isArray(reply)) throw new Error(`attend/redis: expected an array reply, got ${typeof reply}`);
  return reply;
}

/** XREAD answers per stream: `[[key, entries]]` over RESP2, a map over RESP3, which clients decode as an object, a Map or a flat array. */
function onlyStream(reply: unknown): unknown {
  if (reply instanceof Map) return [...reply.values()][0];
  if (Array.isArray(reply)) return Array.isArray(reply[0]) ? list(reply[0])[1] : reply[1];
  if (typeof reply === 'object' && reply !== null) return Object.values(reply)[0];
  throw new Error(`attend/redis: unexpected XREAD reply, got ${typeof reply}`);
}

function parseEntries(reply: unknown): LogEntry[] {
  return list(reply).map((item) => {
    const [id, fields] = list(item);
    const [name, value] = list(fields);
    if (text(name) !== 'event') throw new Error('attend/redis: corrupt log entry');
    return { position: toPosition(text(id)), event: parseEvent(value) };
  });
}

function toPosition(id: string): string {
  const [, ms, seq] = /^(\d{1,20})-(\d{1,20})$/.exec(id) ?? [];
  if (ms === undefined || seq === undefined) throw new Error(`attend/redis: expected a stream id, got "${id}"`);
  return `${ms.padStart(20, '0')}-${seq.padStart(20, '0')}`;
}

function streamId(position: string): string {
  const [, ms, seq] = /^(\d{20})-(\d{20})$/.exec(position) ?? [];
  if (ms === undefined || seq === undefined) throw new Error(`attend/redis: not a position from this log: "${position}"`);
  return `${BigInt(ms)}-${BigInt(seq)}`;
}
