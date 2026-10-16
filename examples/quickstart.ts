import { createAttend, defaultNotice } from '@acquiredintelligence/attend';

const attend = createAttend({
  kind: { 'user.message': 'flush', 'build.failed': 'closing' },
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'note' }),
  sink: (decision) => console.log(`[${decision.route}]\n${defaultNotice(decision)}`),
});

void attend.run();
await attend.push('session-1', { name: 'build.failed', text: 'Build 4521 failed: 1 test' });
await attend.push('session-1', { name: 'user.message', text: 'can you look?' });
setTimeout(() => void attend.stop(), 1000);
