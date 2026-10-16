import { createAttend, defaultNotice, memoryStore, routes, type Decision } from '@acquiredintelligence/attend';

const tell = (decision: Decision) => console.log(`[${decision.route}]\n${defaultNotice(decision)}`);

const attend = createAttend({
  store: memoryStore(),
  kind: (event) => (event.name === 'user.message' ? 'flush' : 'consequence'),
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'note' }),
  sink: routes({ interrupt: tell, queue: tell, note: tell, ask: tell }),
  deadLetter: (letter) => console.error('dead letter', letter.id, letter.message),
});

void attend.run({ signal: AbortSignal.timeout(1000) });
await attend.push('session-1', [
  { id: 'e1', at: Date.now(), name: 'build.failed', data: {}, text: 'Build 4521 failed: 1 test' },
  { id: 'e2', at: Date.now(), name: 'user.message', data: {}, text: 'can you look?' },
]);
