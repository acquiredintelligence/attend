import { boundary } from '../internal/adapter.js';
import type { LoggedInput, ScopeRecord, SettlePolicy } from '../core/types.js';
import type { DecisionRow, Retention, ScopeStore, ScopeWrite } from './types.js';

const { text } = boundary('attend/redis');

export type RedisSend = (args: string[]) => Promise<unknown>;

export interface RedisStoreOptions {
  /** Must contain a hash tag on Redis Cluster, so every key one script touches is in one slot. Default 'attend:'. */
  prefix?: string;
  retain?: Retention;
  /** A dedicated connection for XREAD BLOCK on the decision stream. Without it, `log.wait` polls every 250 ms, so a subscriber hears each new decision up to 250 ms late. */
  blockingSend?: RedisSend;
}

/** Lua library prepended to scripts, providing position formatting, row fields, key type checks and retention. */
const COMMON = `
local function position(id)
  local ms, seq = string.match(id, '^(%d+)%-(%d+)$')
  return string.rep('0', 20 - #ms) .. ms .. '-' .. string.rep('0', 20 - #seq) .. seq
end
local function fields(row)
  local result = {}
  for i = 1, #row[2], 2 do result[row[2][i]] = row[2][i + 1] end
  return result
end
local function checkType(key, expected)
  local actual = redis.call('TYPE', key).ok
  if actual ~= 'none' and actual ~= expected then error('attend/redis: wrong type for ' .. key) end
end
local function trim(stream, decided, prefix, maximum, cutoff, budget)
  local length = redis.call('XLEN', stream)
  local rows = redis.call('XRANGE', stream, '-', '+', 'COUNT', budget + 1)
  local dropped = 0
  for _, row in ipairs(rows) do
    if dropped == budget or length <= 1 then break end
    local value = fields(row)
    if (maximum < 0 or length <= maximum) and (cutoff == '' or tonumber(value.at) >= tonumber(cutoff)) then break end
    redis.call('XDEL', stream, row[1])
    redis.call('HDEL', decided, value.kind .. ':' .. value.id)
    redis.call('ZREM', prefix .. 'by-scope:' .. value.scope, position(row[1]))
    length = length - 1
    dropped = dropped + 1
  end
  return dropped
end`;

/** KEYS: due index, decisions, decided, scope/input/index keys. ARGV: prefix, max rows, cutoff, write count; each write's scope, version, input count, snapshot flag, due, append count, inputs, snapshot fields, append fields. */
const COMMIT = `${COMMON}
local prefix = ARGV[1]
local function finite(value)
  local n = tonumber(value)
  if not n or n ~= n or n == math.huge or n == -math.huge then error('invalid number') end
  return n
end
local function count(value)
  local n = finite(value)
  if n < 0 or n % 1 ~= 0 or n > 9007199254740991 then error('invalid count') end
  return n
end
local maximum = finite(ARGV[2])
local cutoff = ARGV[3]
if maximum < -1 or maximum % 1 ~= 0 or maximum > 9007199254740991 or (cutoff ~= '' and not finite(cutoff)) then error('invalid retention') end
local writeCount = count(ARGV[4])
checkType(KEYS[1], 'zset')
checkType(KEYS[2], 'stream')
checkType(KEYS[3], 'hash')
local seen, outcomes = {}, {}
local proposedAppends = 0
local arg = 5
for i = 1, writeCount do
  local scope, expected = ARGV[arg], count(ARGV[arg + 1])
  local inputCount, hasSnapshot = count(ARGV[arg + 2]), ARGV[arg + 3]
  local due, appendCount = ARGV[arg + 4], count(ARGV[arg + 5])
  arg = arg + 6
  if not scope or seen[scope] then error('duplicate or invalid scope') end
  seen[scope] = true
  if expected + inputCount > 9007199254740991 then error('invalid version') end
  if hasSnapshot ~= '0' and hasSnapshot ~= '1' then error('invalid snapshot flag') end
  if not due or (due ~= 'keep' and due ~= '' and (#due ~= 16 or string.find(due, '[^0-9a-f]'))) then error('invalid due') end
  checkType(prefix .. 'scope:' .. scope, 'hash')
  checkType(prefix .. 'inputs:' .. scope, 'hash')
  arg = arg + inputCount
  if hasSnapshot == '1' then
    if count(ARGV[arg + 1]) ~= expected + inputCount then error('invalid snapshot') end
    finite(ARGV[arg + 3])
    arg = arg + 4
  end
  if arg + appendCount * 6 - 1 > #ARGV then error('missing write arguments') end
  for j = 1, appendCount do
    local kind, rowScope = ARGV[arg], ARGV[arg + 2]
    if kind ~= 'decision' and kind ~= 'deadLetter' then error('invalid append') end
    finite(ARGV[arg + 3])
    checkType(prefix .. 'by-scope:' .. rowScope, 'zset')
    arg = arg + 6
  end
  proposedAppends = proposedAppends + appendCount
  local current = redis.call('HGET', prefix .. 'scope:' .. scope, 'version')
  if current and not tonumber(current) then error('corrupt scope version') end
  outcomes[i] = (tonumber(current) or 0) == expected and 'ok' or 'conflict'
end
if arg ~= #ARGV + 1 then error('invalid write arguments') end
local head = redis.call('XREVRANGE', KEYS[2], '+', '-', 'COUNT', 1)
local repair = false
if #head > 0 then
  local row = fields(head[1])
  local pos = position(head[1][1])
  checkType(prefix .. 'by-scope:' .. row.scope, 'zset')
  if redis.call('HGET', KEYS[3], row.kind .. ':' .. row.id) ~= pos or not redis.call('ZSCORE', prefix .. 'by-scope:' .. row.scope, pos) then
    repair = {row, pos}
  end
end
if maximum >= 0 or cutoff ~= '' then
  for _, row in ipairs(redis.call('XRANGE', KEYS[2], '-', '+', 'COUNT', proposedAppends + 65)) do
    checkType(prefix .. 'by-scope:' .. fields(row).scope, 'zset')
  end
end
if repair then
  -- XADD may have survived an error before either index was written.
  redis.call('HSET', KEYS[3], repair[1].kind .. ':' .. repair[1].id, repair[2])
  redis.call('ZADD', prefix .. 'by-scope:' .. repair[1].scope, 0, repair[2])
end
local appended = 0
arg = 5
for i = 1, writeCount do
  local scope, expected = ARGV[arg], tonumber(ARGV[arg + 1])
  local inputCount, hasSnapshot = tonumber(ARGV[arg + 2]), ARGV[arg + 3] == '1'
  local due, appendCount = ARGV[arg + 4], tonumber(ARGV[arg + 5])
  arg = arg + 6
  local inputs = arg
  local snapshot = inputs + inputCount
  arg = snapshot + (hasSnapshot and 4 or 0)
  if outcomes[i] == 'ok' then
    local scopeKey = prefix .. 'scope:' .. scope
    local inputKey = prefix .. 'inputs:' .. scope
    local oldDue = redis.call('HGET', scopeKey, 'due') or ''
    if not hasSnapshot then
      for j = 1, inputCount do redis.call('HSET', inputKey, string.format('%.0f', expected + j), ARGV[inputs + j - 1]) end
    end
    for j = 1, appendCount do
      local row = arg + (j - 1) * 6
      local id = ARGV[row] .. ':' .. ARGV[row + 1]
      if redis.call('HEXISTS', KEYS[3], id) == 0 then
        local pos = position(redis.call('XADD', KEYS[2], '*', 'kind', ARGV[row], 'id', ARGV[row + 1], 'scope', ARGV[row + 2], 'at', ARGV[row + 3], 'route', ARGV[row + 4], 'body', ARGV[row + 5]))
        appended = appended + 1
        redis.call('HSET', KEYS[3], id, pos)
        redis.call('ZADD', prefix .. 'by-scope:' .. ARGV[row + 2], 0, pos)
      end
    end
    if due ~= 'keep' and due ~= '' and due ~= oldDue then redis.call('ZADD', KEYS[1], 0, due .. ':' .. scope) end
    local update = {'version', string.format('%.0f', expected + inputCount)}
    if due ~= 'keep' then table.insert(update, 'due'); table.insert(update, due) end
    if hasSnapshot then
      table.insert(update, 'snapshot'); table.insert(update, ARGV[snapshot])
      table.insert(update, 'snapshotVersion'); table.insert(update, ARGV[snapshot + 1])
      table.insert(update, 'policy'); table.insert(update, ARGV[snapshot + 2])
      table.insert(update, 'reducer'); table.insert(update, ARGV[snapshot + 3])
    end
    redis.call('HSET', scopeKey, unpack(update))
    if due ~= 'keep' and due ~= oldDue and oldDue ~= '' then redis.call('ZREM', KEYS[1], oldDue .. ':' .. scope) end
    if hasSnapshot then redis.call('DEL', inputKey) end
  end
  arg = arg + appendCount * 6
end
if maximum >= 0 or cutoff ~= '' then trim(KEYS[2], KEYS[3], prefix, maximum, cutoff, appended + 64) end
return outcomes`;

/** KEYS: scope hash, inputs hash. ARGV: none. */
const LOAD = `
local state = redis.call('HMGET', KEYS[1], 'version', 'snapshot', 'snapshotVersion', 'policy', 'reducer')
if not state[1] then return false end
local version = tonumber(state[1])
local floor = tonumber(state[3]) or 0
if not version or floor > version then return redis.error_reply('attend/redis: corrupt scope') end
local inputs, tail = redis.call('HGETALL', KEYS[2]), {}
for i = 1, #inputs, 2 do
  local n = tonumber(inputs[i])
  if n and n > floor and n <= version then table.insert(tail, {n, inputs[i + 1]}) end
end
table.sort(tail, function(a, b) return a[1] < b[1] end)
if #tail ~= version - floor then return redis.error_reply('attend/redis: corrupt scope') end
local result = {}
for i, item in ipairs(tail) do
  if item[1] ~= floor + i then return redis.error_reply('attend/redis: corrupt scope') end
  result[i] = item[2]
end
return {state[1], state[2], state[3], state[4], state[5], result}`;

/** KEYS: due index. ARGV: prefix, lower bound, upper bound, limit. */
const DUE = `
local result, after = {}, ARGV[2]
local limit = tonumber(ARGV[4])
while #result < limit do
  local rows = redis.call('ZRANGEBYLEX', KEYS[1], after, ARGV[3], 'LIMIT', 0, limit - #result)
  if #rows == 0 then break end
  for _, member in ipairs(rows) do
    local hex, scope = string.sub(member, 1, 16), string.sub(member, 18)
    if redis.call('HGET', ARGV[1] .. 'scope:' .. scope, 'due') == hex then table.insert(result, member)
    else redis.call('ZREM', KEYS[1], member) end
    after = '(' .. member
  end
end
return result`;

/** KEYS: scope hash, inputs hash, due index. ARGV: scope. */
const REMOVE = `
local due = redis.call('HGET', KEYS[1], 'due')
redis.call('DEL', KEYS[1], KEYS[2])
if due and due ~= '' then redis.call('ZREM', KEYS[3], due .. ':' .. ARGV[1]) end
return 1`;

/** KEYS: decisions, scope index. ARGV: after, scope flag/value, prefix flag/value, kind count/values, route count/values (-1 means no filter), sinceAt, limit, scanLimit. */
const READ = `${COMMON}
local q = {after = ARGV[1] ~= '' and ARGV[1] or false, scope = ARGV[2] == '1' and ARGV[3] or false, prefix = ARGV[4] == '1' and ARGV[5] or false}
local arg = 6
local function values()
  local count = tonumber(ARGV[arg])
  arg = arg + 1
  if count == -1 then return false end
  local result = {}
  for i = 1, count do result[i] = ARGV[arg]; arg = arg + 1 end
  return result
end
q.kinds = values()
q.routes = values()
q.sinceAt = ARGV[arg] ~= '' and tonumber(ARGV[arg]) or false
q.limit = tonumber(ARGV[arg + 1])
q.scanLimit = tonumber(ARGV[arg + 2])
local head = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
local checked = q.after
if #head > 0 then
  local pos = position(head[1][1])
  if checked == false or pos > checked then checked = pos end
end
local after = q.after == false and '-' or '(' .. q.after
local rows
if q.scope ~= false then
  rows = redis.call('ZRANGEBYLEX', KEYS[2], after, '+', 'LIMIT', 0, q.scanLimit + 1)
  if #head > 0 and #rows < q.scanLimit + 1 then
    local pos = position(head[1][1])
    if fields(head[1]).scope == q.scope and (q.after == false or pos > q.after) and rows[#rows] ~= pos then table.insert(rows, pos) end
  end
else rows = redis.call('XRANGE', KEYS[1], after, '+', 'COUNT', q.scanLimit + 1) end
local function contains(values, value)
  if values == false then return true end
  for _, item in ipairs(values) do if value == item then return true end end
  return false
end
local result, examined, last = {}, 0, q.after
for _, item in ipairs(rows) do
  if #result >= q.limit or examined >= q.scanLimit then return {result, last, 1} end
  examined = examined + 1
  local row = item
  if q.scope ~= false then
    last = item
    row = redis.call('XRANGE', KEYS[1], item, item)[1]
  else last = position(row[1]) end
  if row then
    local value = fields(row)
    if (q.prefix == false or string.sub(value.scope, 1, #q.prefix) == q.prefix)
      and contains(q.kinds, value.kind)
      and (q.routes == false or (value.route ~= '' and contains(q.routes, value.route)))
      and (q.sinceAt == false or tonumber(value.at) >= q.sinceAt) then table.insert(result, row) end
  end
end
return {result, checked, 0}`;

/** KEYS: decisions, decided. ARGV: kind:id. */
const FIND = `${COMMON}
local pos = redis.call('HGET', KEYS[2], ARGV[1])
if pos then return redis.call('XRANGE', KEYS[1], pos, pos)[1] or false end
local head = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
if #head > 0 then
  local value = fields(head[1])
  if value.kind .. ':' .. value.id == ARGV[1] then return head[1] end
end
return false`;

/** KEYS: decisions. ARGV: none. */
const BOUNDS = `${COMMON}
local first = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', 1)
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
return {first[1] and position(first[1][1]) or false, last[1] and position(last[1][1]) or false}`;

/** KEYS: decisions, decided. ARGV: prefix, max rows, age cutoff. */
const TRIM = `${COMMON}
return trim(KEYS[1], KEYS[2], ARGV[1], tonumber(ARGV[2]), ARGV[3], 256)`;

/** KEYS: meta hash. ARGV: candidate epoch. */
const EPOCH = `
redis.call('HSETNX', KEYS[1], 'epoch', ARGV[1])
return redis.call('HGET', KEYS[1], 'epoch')`;

/** One EVALSHA per group: validates every version first, then writes each scope with its version bumped last. */
export function redisStore(send: RedisSend, options?: RedisStoreOptions): ScopeStore {
  const prefix = options?.prefix ?? 'attend:';
  const scopeKey = (scope: string) => `${prefix}scope:${scope}`;
  const inputsKey = (scope: string) => `${prefix}inputs:${scope}`;
  const indexKey = (scope: string) => `${prefix}by-scope:${scope}`;
  const dueKey = `${prefix}due-index`;
  const stream = `${prefix}decisions`;
  const decided = `${prefix}decided`;
  const scripts = new Map<string, Promise<string>>();
  const load = (source: string) => {
    const pending = send(['SCRIPT', 'LOAD', source]).then(text);
    scripts.set(source, pending);
    void pending.catch(() => { if (scripts.get(source) === pending) scripts.delete(source); });
    return pending;
  };
  const run = async (source: string, keys: string[], args: string[] = []): Promise<unknown> => {
    const sha = await (scripts.get(source) ?? load(source));
    try {
      return await send(['EVALSHA', sha, String(keys.length), ...keys, ...args]);
    } catch (error) {
      if (!(error instanceof Error) || !error.message.includes('NOSCRIPT')) throw error;
      return send(['EVALSHA', await load(source), String(keys.length), ...keys, ...args]);
    }
  };
  const log: ScopeStore['log'] = {
    async epoch() { return text(await run(EPOCH, [`${prefix}meta`], [crypto.randomUUID()])); },
    async bounds() {
      const [first, last] = list(await run(BOUNDS, [stream]));
      return { first: nullableText(first), last: nullableText(last) };
    },
    async read(query) {
      if (query.after !== null) validatePosition(query.after);
      positiveInteger(query.limit, 'limit');
      positiveInteger(query.scanLimit ?? query.limit * 4, 'scanLimit');
      const [rows, checked, more] = list(await run(READ, [stream, indexKey(query.scope ?? '')], [
        query.after ?? '', query.scope === undefined ? '0' : '1', query.scope ?? '',
        query.scopePrefix === undefined ? '0' : '1', query.scopePrefix ?? '',
        String(query.kinds?.length ?? -1), ...query.kinds ?? [],
        String(query.routes?.length ?? -1), ...query.routes ?? [],
        query.sinceAt === undefined ? '' : String(query.sinceAt), String(query.limit), String(query.scanLimit ?? query.limit * 4),
      ]));
      return { rows: list(rows).map(parseRow), checked: nullableText(checked), more: numeric(more) === 1 };
    },
    async find(kind, id) {
      const reply = await run(FIND, [stream, decided], [`${kind}:${id}`]);
      return reply === null || reply === false ? null : parseRow(reply);
    },
    async wait(after, blockMs, signal) {
      try { if (after !== null) validatePosition(after); } catch { return; }
      if (signal.aborted || blockMs <= 0) return;
      await new Promise<void>((resolve) => {
        const deadline = Date.now() + blockMs;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let wakeSleep = () => {};
        let done = false;
        const finish = () => {
          if (done) return;
          done = true;
          clearTimeout(timer);
          clearTimeout(timeout);
          wakeSleep();
          signal.removeEventListener('abort', finish);
          resolve();
        };
        const sleep = async () => {
          if (!done) await new Promise<void>((wake) => {
            wakeSleep = wake;
            timer = setTimeout(wake, Math.max(0, Math.min(deadline - Date.now(), 250)));
          });
        };
        signal.addEventListener('abort', finish, { once: true });
        const timeout = setTimeout(finish, blockMs);
        void (async () => {
          try {
            while (!done && Date.now() < deadline) {
              try {
                const { last } = await log.bounds();
                if (done) return;
                if (last !== null && (after === null || last > after)) break;
                if (options?.blockingSend) {
                  const reply = await options.blockingSend(['XREAD', 'BLOCK', String(Math.max(1, Math.ceil(Math.min(deadline - Date.now(), 1000)))), 'COUNT', '1', 'STREAMS', stream, after ?? '0-0']);
                  if (reply !== null && reply !== undefined) break;
                } else await sleep();
              } catch { await sleep(); }
            }
          } finally { finish(); }
        })();
      });
    },
    async trim(retain) {
      const args = retention(retain);
      for (;;) {
        if (numeric(await run(TRIM, [stream, decided], [prefix, ...args])) < 256) return;
      }
    },
    compare: (a, b) => a < b ? -1 : a > b ? 1 : 0,
  };
  return {
    durable: true,
    log,
    async load(scope) {
      const reply = await run(LOAD, [scopeKey(scope), inputsKey(scope)]);
      if (reply === null || reply === false) return null;
      const [version, snapshot, snapshotVersion, policy, reducer, tail] = list(reply);
      return {
        version: numeric(version),
        snapshot: snapshot === null || snapshot === false ? null : {
          version: numeric(snapshotVersion), record: parseRecord(snapshot, scope),
          policy: parsePolicy(policy), reducer: numeric(reducer),
        },
        tail: list(tail).map(parseInput),
      };
    },
    async commit(writes) {
      if (writes.length === 0) return [];
      const prepared = writes.flatMap(prepare);
      const keys = [dueKey, stream, decided, ...writes.flatMap((w) => [scopeKey(w.scope), inputsKey(w.scope), ...w.appends.map((row) => indexKey(row.scope))])];
      return list(await run(COMMIT, keys, [prefix, ...retention(options?.retain), String(writes.length), ...prepared])).map((reply) => {
        const outcome = text(reply);
        if (outcome !== 'ok' && outcome !== 'conflict') throw new Error('attend/redis: invalid commit outcome');
        return outcome;
      });
    },
    async due(now, limit, after) {
      positiveInteger(limit, 'limit');
      const lower = after ? `(${dueHex(after.due)}:${after.scope}` : '-';
      return list(await run(DUE, [dueKey], [prefix, lower, `(${dueHex(now)};`, String(limit)])).map((reply) => {
        const member = text(reply);
        return { due: fromDueHex(member.slice(0, 16)), scope: member.slice(17) };
      });
    },
    async remove(scope) { await run(REMOVE, [scopeKey(scope), inputsKey(scope), dueKey], [scope]); },
  };
}

function prepare(write: ScopeWrite): string[] {
  if (!Number.isSafeInteger(write.expectedVersion) || write.expectedVersion < 0 || !Number.isSafeInteger(write.expectedVersion + write.inputs.length)) throw new Error('attend/redis: invalid version');
  const snapshot = write.snapshot;
  if (snapshot && snapshot.version !== write.expectedVersion + write.inputs.length) throw new Error('attend/redis: invalid snapshot version');
  if (write.serialized && (write.serialized.inputs.length !== write.inputs.length || write.serialized.appends.length !== write.appends.length)) throw new Error('attend/redis: invalid serialized write');
  const args = [
    write.scope, String(write.expectedVersion), String(write.inputs.length), snapshot === null ? '0' : '1',
    write.due === undefined ? 'keep' : write.due === null ? '' : dueHex(write.due), String(write.appends.length),
    ...write.serialized?.inputs ?? write.inputs.map((input) => JSON.stringify(input)),
  ];
  if (snapshot !== null) {
    const record = write.serialized ? write.serialized.snapshot : JSON.stringify(snapshot.record);
    if (record === null) throw new Error('attend/redis: missing serialized snapshot');
    args.push(record, String(snapshot.version), JSON.stringify(snapshot.policy), String(snapshot.reducer));
  }
  for (const [i, row] of write.appends.entries()) {
    if (!Number.isFinite(row.at)) throw new Error('attend/redis: invalid append time');
    const body = write.serialized ? write.serialized.appends[i] : JSON.stringify(row.body);
    if (body === undefined) throw new Error('attend/redis: missing serialized append');
    args.push(row.kind, row.id, row.scope, String(row.at), row.route ?? '', body);
  }
  return args;
}

function retention(value?: Retention): [string, string] {
  if (value?.maxRows !== undefined && (!Number.isSafeInteger(value.maxRows) || value.maxRows < 0)) throw new Error('attend/redis: invalid maxRows');
  if (value?.olderThanMs !== undefined && (!Number.isFinite(value.olderThanMs) || value.olderThanMs < 0)) throw new Error('attend/redis: invalid olderThanMs');
  return [String(value?.maxRows ?? -1), value?.olderThanMs === undefined ? '' : String(Date.now() - value.olderThanMs)];
}

function dueHex(value: number): string {
  if (!Number.isFinite(value)) throw new Error('attend/redis: due must be finite');
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, value === 0 ? 0 : value);
  const bits = view.getBigUint64(0);
  return ((bits & (1n << 63n)) !== 0n ? BigInt.asUintN(64, ~bits) : bits | (1n << 63n)).toString(16).padStart(16, '0');
}

function fromDueHex(hex: string): number {
  const bits = BigInt(`0x${hex}`);
  const view = new DataView(new ArrayBuffer(8));
  view.setBigUint64(0, (bits & (1n << 63n)) !== 0n ? bits & ((1n << 63n) - 1n) : BigInt.asUintN(64, ~bits));
  return view.getFloat64(0);
}

function validatePosition(position: string): void {
  if (!/^\d{20}-\d{20}$/.test(position)) throw new Error(`attend/redis: not a position from this log: "${position}"`);
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`attend/redis: invalid ${name}`);
}

function numeric(reply: unknown): number {
  return typeof reply === 'number' ? reply : typeof reply === 'bigint' ? Number(reply) : Number(text(reply));
}

function list(reply: unknown): unknown[] {
  if (!Array.isArray(reply)) throw new Error(`attend/redis: expected an array reply, got ${typeof reply}`);
  return reply;
}

function nullableText(reply: unknown): string | null {
  return reply === null || reply === false ? null : text(reply);
}

function parseRecord(reply: unknown, scope: string): ScopeRecord {
  const value: unknown = JSON.parse(text(reply));
  if (typeof value !== 'object' || value === null || Reflect.get(value, 'scope') !== scope || typeof Reflect.get(value, 'version') !== 'number') throw new Error('attend/redis: corrupt scope');
  return value as ScopeRecord;
}

function parsePolicy(reply: unknown): SettlePolicy {
  const value: unknown = JSON.parse(text(reply));
  if (typeof value !== 'object' || value === null || typeof Reflect.get(value, 'closingGraceMs') !== 'number') throw new Error('attend/redis: corrupt policy');
  return value as SettlePolicy;
}

function parseInput(reply: unknown): LoggedInput {
  const value: unknown = JSON.parse(text(reply));
  if (typeof value !== 'object' || value === null || typeof Reflect.get(value, 'now') !== 'number' || typeof Reflect.get(value, 'input') !== 'object' || Reflect.get(value, 'input') === null) throw new Error('attend/redis: corrupt input');
  return value as LoggedInput;
}

function parseRow(reply: unknown): DecisionRow {
  const [id, raw] = list(reply);
  const values = list(raw);
  const fields = new Map<string, unknown>();
  for (let i = 0; i < values.length; i += 2) fields.set(text(values[i]), values[i + 1]);
  const kind = text(fields.get('kind'));
  const body: unknown = JSON.parse(text(fields.get('body')));
  if ((kind !== 'decision' && kind !== 'deadLetter') || typeof body !== 'object' || body === null || Reflect.get(body, 'id') !== text(fields.get('id'))) throw new Error('attend/redis: corrupt decision');
  const [ms, seq] = text(id).split('-');
  if (ms === undefined || seq === undefined) throw new Error('attend/redis: corrupt position');
  return {
    kind, id: text(fields.get('id')), scope: text(fields.get('scope')), at: numeric(fields.get('at')),
    route: text(fields.get('route')) || null, body,
    position: `${ms.padStart(20, '0')}-${seq.padStart(20, '0')}`,
  } as DecisionRow;
}
