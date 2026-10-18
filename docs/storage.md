# Choose a store

One `ScopeStore` holds each scope's latest snapshot, the inputs committed since it, the due index and the decision log. attend replays the short tail of inputs over the snapshot to recover a scope, so a restart loses nothing.

| Store | Create it | Guarantees | Caveats |
|---|---|---|---|
| Memory | `memoryStore()` from `@acquiredintelligence/attend` | Atomic within one process. | State is lost when the process exits. Use it for tests, demos and browser apps. |
| SQLite | `sqliteStore(db)` from `@acquiredintelligence/attend/sqlite`, with `db` a `node:sqlite` `DatabaseSync`, a `bun:sqlite` `Database`, or any object with the same `exec` and `prepare`. `sqliteStore({ file, worker: true })` runs the database on a worker thread. | One transaction per group of writes. Durable on a file. Creates its tables on construction. | Given a handle, calls block the event loop for their duration; the worker mode does not. Processes that share the file must run on one host. |
| Postgres | `postgresStore(pool)` from `@acquiredintelligence/attend/postgres`, after `await migrate(pool)` | One statement commits a group of scopes' writes together, and decisions wake subscribers through `LISTEN` and `NOTIFY` when you pass a `listener`. Durable. Safe for many replicas. | Run `migrate()` at deploy with a role that can create tables. The store needs only `SELECT`, `INSERT`, `UPDATE` and `DELETE`. The table prefix must be lowercase. PostgreSQL 13 or later. In a process that serves decisions or reads `attend.decisions`, pass `listener`, which runs `LISTEN` on a client of its own. Without it, subscribers hear each decision on the next 100 ms poll. |
| Redis | `redisStore((args) => client.sendCommand(args))` from `@acquiredintelligence/attend/redis` | One script validates and writes a group of scopes' writes together, and keeps the decision log in a stream. Redis 6.2 or later. | Durable only as far as your Redis persistence settings. On Redis Cluster, a group's keys must hash to one slot. Put a hash tag in the prefix, such as `redisStore(send, { prefix: 'attend:{app}:' })`. In a process that serves decisions or reads `attend.decisions`, pass `blockingSend`, a send function on a second connection. Without it, subscribers hear each decision on the next 250 ms poll. |

A Postgres `listener` needs a client of its own, because a pool hands its connection back between queries and `LISTEN` belongs to the session. With `pg`:

```ts
import pg from 'pg';
import { migrate, postgresStore, type PostgresListener } from '@acquiredintelligence/attend/postgres';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool);
const listening = new pg.Client({ connectionString: process.env.DATABASE_URL });
await listening.connect();
const listener: PostgresListener = {
  async listen(channel, onNotify) {
    const notified = (message: pg.Notification) => { if (message.channel === channel) onNotify(); };
    listening.on('notification', notified);
    await listening.query(`LISTEN "${channel}"`);
    return async () => { listening.off('notification', notified); await listening.query(`UNLISTEN "${channel}"`); };
  },
};
export const store = postgresStore(pool, { listener });
```

`store.close()` stops listening. End `listening` after it.

Each store also takes `retain` (decision rows kept by count or age), so the log does not grow without bound. Every store passes the same conformance and crash suites.

Several processes can share one store. Any of them can `push`, and any number can `run` or serve the log: a write names the stored version it read, a writer that loses reloads and applies its input again, and every input is idempotent. Give every process the same `kind` function, because the process that ingests an event is the one that kinds it. A scope can take several sources at once; the record keeps one cursor per `Source.key`. Attach a given source to a scope from one process only, since two processes running the same source would overwrite each other's cursor, and a second `attend.attach` of the same key in one process throws.

Each adapter also has an `EventLog`: `memoryLog`, `sqliteLog`, `postgresLog` and `redisLog`. When producers live in other processes and you want a durable shared log in front of attend, append to the log and attach `logSource(log, scope)` instead of calling `push`.
