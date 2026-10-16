export function memoryStore() {
  return { get: async () => null, put: async () => 'ok', due: async () => [], remove: async () => {} };
}
export function createAttend() {
  return {
    push: async () => { throw new Error('rejected'); },
    flush: async () => {},
    run: ({ signal }) => new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true })),
  };
}
