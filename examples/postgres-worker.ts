/**
 * A worker that keeps attend's state in Postgres and writes decisions to an inbox table the
 * agent's runtime reads. Run several replicas: each sweeps for due scopes, and compare-and-swap
 * lets one of them win each write. A producer in another process builds the same `attend` over
 * the same store and calls `push`; it never needs `run`.
 */
import pg from 'pg';
import { createAttend, defaultNotice, routes, type Decision } from 'attend';
import { migrate, postgresStore } from 'attend/store/postgres';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool);
await pool.query(`CREATE TABLE IF NOT EXISTS agent_inbox (
  scope text NOT NULL,
  decision_id text NOT NULL,
  route text NOT NULL,
  notice text NOT NULL,
  PRIMARY KEY (scope, decision_id)
)`);
await pool.query('CREATE TABLE IF NOT EXISTS agent_dead_letters (scope text NOT NULL, id text NOT NULL, letter json NOT NULL, PRIMARY KEY (scope, id))');

async function enqueue(decision: Decision): Promise<void> {
  await pool.query('INSERT INTO agent_inbox (scope, decision_id, route, notice) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING', [
    decision.scope,
    decision.id,
    decision.route,
    defaultNotice(decision),
  ]);
}

const attend = createAttend({
  store: postgresStore(pool),
  kind: (event) => (event.name === 'message.created' ? 'flush' : event.name === 'job.completed' ? 'closing' : 'consequence'),
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'queue' }),
  sink: routes({ interrupt: enqueue, queue: enqueue, note: enqueue, ask: enqueue }),
  deadLetter: async (letter) => {
    await pool.query('INSERT INTO agent_dead_letters (scope, id, letter) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [letter.scope, letter.id, JSON.stringify(letter)]);
  },
});

const controller = new AbortController();
process.once('SIGTERM', () => controller.abort());
await attend.run({ signal: controller.signal });
await pool.end();
