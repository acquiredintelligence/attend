export const calls = [];
export const outputs = new Set();
export function newOutput() { return { effects: [], appends: [], changed: false }; }
export function apply(config, state, logged, out) {
  if (out.effects.length || out.appends.length || out.changed) throw new Error('output was not reset');
  if (Object.keys(logged).sort().join(',') !== 'input,now') throw new Error('invalid logged input');
  if (config.decisions !== 'lane' || config.deadLetters !== 'lane' || config.traces !== false) throw new Error('invalid config');
  calls.push(logged);
  outputs.add(out);
  const kind = logged.input.kind;
  if (kind === 'tick') out.effects.push({ kind: 'judge', batchId: 'b1', attempt: 1 });
  if (kind === 'judged') out.effects.push({ kind: 'deliver', delivery: { item: { id: 'd1' }, phase: { attempt: 1 } } });
  out.appends.push('reset me');
  out.changed = true;
}
