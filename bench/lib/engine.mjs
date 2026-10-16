export const kindOf = (event) => {
  const kind = event.name?.startsWith('bench.') ? event.name.slice(6) : '';
  return ['closing', 'flush', 'continuous', 'passive', 'consequence', 'context'].includes(kind) ? kind : 'continuous';
};

export function startEngine(build, { store, settle, judge, onWindow, onDelivered }) {
  const attend = build.core.createAttend({
    store, kind: kindOf, settle,
    judge: async (input, signal) => {
      onWindow?.(input.window.events.length, input);
      return judge ? judge(input, signal) : { route: 'note' };
    },
    sink: { async deliver(decision) { await onDelivered?.(decision); } },
    deadLetter: () => {},
  });
  const controller = new AbortController();
  const running = attend.run({ signal: controller.signal, sweepMs: 60000 });
  running.catch(() => {});
  return { attend, async stop() { controller.abort(); await running; } };
}
