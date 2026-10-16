export const DEFAULT_POLICY = { streamGapMs: 10 };
export function memoryStore() {
  return { durable: false, load: async () => null, commit: async (writes) => writes.map(() => 'ok'), due: async () => [], remove: async () => {}, log: {} };
}
