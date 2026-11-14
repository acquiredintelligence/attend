export async function openStore(kind, url, prefix) {
  if (kind === 'redis') {
    const { createClient } = await import('redis');
    const { redisStore } = await import('../../../dist/redis.js');
    const client = createClient({ url });
    await client.connect();
    const store = redisStore((args) => client.sendCommand(args), { prefix });
    return { store, async close() { await store.close?.(); await client.close(); } };
  }
  const pg = (await import('pg')).default;
  const { postgresStore } = await import('../../../dist/postgres.js');
  const pool = new pg.Pool({ connectionString: url, max: 2 });
  const store = postgresStore(pool, { prefix });
  return { store, async close() { await store.close?.(); await pool.end(); } };
}
