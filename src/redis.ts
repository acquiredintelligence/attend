/**
 * `attend/redis`: a ScopeStore and an EventLog over Redis 6.2 or later. Takes a function
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
import type { EventLog, LogEntry } from './types.js';
import type { RedisSend } from './store/redis.js';

export { redisStore } from './store/redis.js';
export type { RedisSend, RedisStoreOptions } from './store/redis.js';

const { parseEvent, text } = boundary('attend/redis');

export interface RedisOptions {
  /** Prepended to every key. Default 'attend:'. */
  prefix?: string;
}

/** KEYS: stream. ARGV: event JSON, in order. */
const APPEND = `
for i = 1, #ARGV do redis.call('XADD', KEYS[1], '*', 'event', ARGV[i]) end
return #ARGV`;

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
