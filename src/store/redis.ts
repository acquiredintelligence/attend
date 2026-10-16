import type { Retention, ScopeStore } from './types.js';

export type RedisSend = (args: string[]) => Promise<unknown>;

export interface RedisStoreOptions {
  /** Must contain a hash tag on Redis Cluster, so every key one script touches is in one slot. Default 'attend:'. */
  prefix?: string;
  retain?: Retention;
  /** A dedicated connection for XREAD BLOCK on the decision stream. */
  blockingSend?: RedisSend;
}

/** One EVALSHA per group: validates every version first, then writes each scope with its version bumped last. */
export function redisStore(send: RedisSend, options?: RedisStoreOptions): ScopeStore {
  throw new Error('attend: redisStore is not implemented');
}
