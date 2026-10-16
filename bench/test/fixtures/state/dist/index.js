export function memoryStore() {
  return { get: async () => null, put: async () => 'ok', due: async () => [], remove: async () => {} };
}
