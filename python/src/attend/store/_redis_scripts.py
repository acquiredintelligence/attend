"""The Lua scripts of ``src/store/redis.ts``, verbatim, so both ports keep one layout. ``tests/test_redis.py`` checks they still match."""

from __future__ import annotations

COMMON = """
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
end"""

COMMIT = (
    COMMON
    + """
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
return outcomes"""
)

LOAD = """
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
return {state[1], state[2], state[3], state[4], state[5], result}"""

DUE = """
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
return result"""

REMOVE = """
local due = redis.call('HGET', KEYS[1], 'due')
redis.call('DEL', KEYS[1], KEYS[2])
if due and due ~= '' then redis.call('ZREM', KEYS[3], due .. ':' .. ARGV[1]) end
return 1"""

READ = (
    COMMON
    + """
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
return {result, checked, 0}"""
)

FIND = (
    COMMON
    + """
local pos = redis.call('HGET', KEYS[2], ARGV[1])
if pos then return redis.call('XRANGE', KEYS[1], pos, pos)[1] or false end
local head = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
if #head > 0 then
  local value = fields(head[1])
  if value.kind .. ':' .. value.id == ARGV[1] then return head[1] end
end
return false"""
)

BOUNDS = (
    COMMON
    + """
local first = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', 1)
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
return {first[1] and position(first[1][1]) or false, last[1] and position(last[1][1]) or false}"""
)

TRIM = (
    COMMON
    + """
return trim(KEYS[1], KEYS[2], ARGV[1], tonumber(ARGV[2]), ARGV[3], 256)"""
)

EPOCH = """
redis.call('HSETNX', KEYS[1], 'epoch', ARGV[1])
return redis.call('HGET', KEYS[1], 'epoch')"""
