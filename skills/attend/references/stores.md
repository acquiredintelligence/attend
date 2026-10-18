# Stores

A store holds each scope's latest snapshot and the short tail of inputs committed since, a due index, and the decision log. A write names the version it read and lands only if that is still the stored version, so concurrent writers never overwrite each other. Every store passes the same conformance suite and crash suite.

## Memory

```ts
import { memoryStore } from '@acquiredintelligence/attend';

export const store = memoryStore();
```

State lives in the process and is lost when it exits. Use it for tests, demos, browser apps and short-lived processes.

## SQLite

```ts
import { DatabaseSync } from 'node:sqlite';
import { sqliteStore } from '@acquiredintelligence/attend/sqlite';

export const store = sqliteStore(new DatabaseSync('attend.db'));
```

- Takes a `node:sqlite` `DatabaseSync`, a `bun:sqlite` `Database`, or any object with the same `exec` and `prepare`, so importing `@acquiredintelligence/attend/sqlite` loads no driver. The conformance and crash suites pass under Bun 1.4 on `bun:sqlite`.
- Creates its table and index on construction. A second store over the same file uses what the first created.
- `prefix` (default `attend_`) sets the table names. Letters, digits and underscores only.
- Calls are synchronous and block the event loop for their duration.
- `sqliteStore({ file: 'attend.db', worker: true })` runs the database on a worker thread so commits never block the event loop.
- Several processes on one host can share the file. Keep the file off network filesystems.

## Postgres

```ts
import pg from 'pg';
import { migrate, postgresStore } from '@acquiredintelligence/attend/postgres';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool);
export const store = postgresStore(pool);
```

- Takes any client with `query(text, values)`, such as a `pg` `Pool` or `Client`.
- Run `migrate()` once at deploy or startup, with a role that can create tables. It is idempotent and safe to run from several processes at once. The store itself runs no DDL and needs only `SELECT`, `INSERT`, `UPDATE` and `DELETE`.
- `prefix` (default `attend_`) must be lowercase letters, digits and underscores. To use another schema, set the connection's `search_path`.
- Safe for many replicas. Each runner sweeps for due scopes, and the version check lets one replica win each write. One statement commits a group of scopes together.

## Redis

```ts
import { createClient } from 'redis';
import { redisStore } from '@acquiredintelligence/attend/redis';

const client = createClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
await client.connect();
const blocking = client.duplicate();
await blocking.connect();
export const store = redisStore((args) => client.sendCommand(args), { blockingSend: (args) => blocking.sendCommand(args) });
```

- Takes a function that sends one command. With ioredis, pass `([command, ...args]) => client.call(command, ...args)`.
- `blockingSend` sends on a second connection, where the decision log waits with `XREAD BLOCK`. Pass it in a process that serves decisions or reads `attend.decisions`. Without it, subscribers hear each decision on the next 250 ms poll. `attend serve --store redis:<url>` opens that connection itself.
- Needs Redis 6.2 or later. One script validates every version in a group, then changes the scopes, the due index and the decision stream together.
- Durable only as far as the server's persistence settings (AOF or RDB).
- On Redis Cluster, every key a group touches and the shared due key must hash to one slot. Use a prefix with a hash tag: `redisStore(send, { prefix: 'attend:{app}:' })`. That puts every scope on one shard.

## Shared logs

Each store module also exports an `EventLog`: `memoryLog` from `@acquiredintelligence/attend`, `sqliteLog`, `postgresLog` and `redisLog`. Use one when producers live in other processes and you want a durable log in front of attend. Producers call `log.append(scope, events)`, and one process attaches the log:

```ts
import { createAttend, logSource, memoryLog, memoryStore, routes } from '@acquiredintelligence/attend';

const log = memoryLog();
const attend = createAttend({
  store: memoryStore(),
  kind: () => 'closing',
  judge: async () => ({ route: 'queue' }),
  sink: routes({ interrupt: async () => {}, queue: async () => {}, note: async () => {}, ask: async () => {} }),
  deadLetter: async () => {},
});

const controller = new AbortController();
await log.append('inbox:42', [{ id: 'm1', at: Date.now(), name: 'message.created', data: {}, text: 'Is the export done?' }]);
await Promise.all([attend.run({ signal: controller.signal }), attend.attach('inbox:42', logSource(log, 'inbox:42'), { signal: controller.signal })]);
```

- `redisLog` waits for new entries with `XREAD BLOCK` only when you pass `blockingSend`, a send function on its own connection. Without it, `read` returns at once and `logSource` polls every `pollMs` (default 250).
- `postgresLog` polls every `pollMs` (default 100) while a read blocks. `sqliteLog` never blocks.
- The log may deliver an event more than once. The scope dedups by `event.id`.

## Choosing between push and a log

`push` writes the scope's state directly from any process, with a version check. It needs no log, and it contends when many producers write one scope at a high rate: a writer that loses backs off a few milliseconds and tries again, and after eight losses `push` rejects with `ConflictError`. A log takes those writes off the scope, and one attached source moves them in.
