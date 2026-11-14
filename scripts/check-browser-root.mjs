import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createContext, SourceTextModule } from 'node:vm';

// Preserve Node-free root linking and initialization; this is not an event-processing browser test.
const context = createContext({
  AbortController, AbortSignal, DOMException, TextEncoder, TextDecoder, URL,
  crypto: webcrypto, performance, structuredClone, queueMicrotask, setTimeout, clearTimeout,
});
const modules = new Map();
async function load(url) {
  const cached = modules.get(url.href);
  if (cached) return cached;
  const module = new SourceTextModule(await readFile(url, 'utf8'), { context, identifier: url.href });
  modules.set(url.href, module);
  return module;
}
const root = await load(new URL('../dist/index.js', import.meta.url));
await root.link(async (specifier, referencing) => {
  assert.ok(specifier.startsWith('.'), `core imported a nonportable dependency: ${specifier}`);
  return load(new URL(specifier, referencing.identifier));
});
await root.evaluate();

const timers = new Set();
const clock = {
  now: () => 0,
  at(at, fn) {
    const timer = { active: true };
    timers.add(timer);
    if (at <= 0) queueMicrotask(() => {
      if (!timer.active) return;
      timer.active = false;
      timers.delete(timer);
      fn();
    });
    return () => { timer.active = false; timers.delete(timer); };
  },
};
const attend = root.namespace.createAttend({
  clock, kind: { change: 'closing' }, settle: { closingGraceMs: 0 }, sink: 'log',
  judge: async () => { throw new Error('initialization must not judge'); },
});
const running = attend.run();
await attend.stop();
await running;
assert.equal(timers.size, 0);
console.log(JSON.stringify({ linkedCoreModules: modules.size, nodeImports: 0, initialized: true, remainingTimers: 0 }));
