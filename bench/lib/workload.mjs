const kinds = ['continuous', 'closing', 'consequence', 'passive', 'flush', 'context'];

export function random(seed) {
  return () => {
    let n = seed += 0x6d2b79f5;
    n = Math.imul(n ^ n >>> 15, n | 1);
    n ^= n + Math.imul(n ^ n >>> 7, n | 61);
    return ((n ^ n >>> 14) >>> 0) / 4294967296;
  };
}

export function createSchedule({ mix = 'continuous', rate = 10000, scopes = 1000, seconds = 60, warmup = 5, seed = 1, tapes }) {
  if (mix === 'tape') {
    if (!tapes?.tapes?.length) throw new Error('tape mode needs non-empty tapes');
    const lines = [];
    const end = (warmup + seconds) * 1000;
    for (let scope = 0; scope < scopes; scope += 1) {
      const tape = tapes.tapes[scope % tapes.tapes.length].events;
      if (!tape.length || tape.some((event, i) => !Number.isFinite(event.t) || event.t < 0 || !kinds.includes(event.kind) || (i > 0 && event.t < tape[i - 1].t))) throw new Error('invalid tape');
      const loop = tape.at(-1).t + 1000;
      const offset = (scope * 0.6180339887 % 1) * loop;
      for (let base = offset; base < end; base += loop) {
        for (const event of tape) {
          const time = base + event.t;
          if (time >= 0 && time < end) lines.push({ time, scope, kind: event.kind });
        }
      }
    }
    lines.sort((a, b) => a.time - b.time || a.scope - b.scope);
    const warmCount = lines.filter((line) => line.time < warmup * 1000).length;
    return { total: lines.length, warmCount, intendedMs: (i) => lines[i].time, scopeOf: (i) => lines[i].scope, kindOf: (i) => lines[i].kind, targetRate: (lines.length - warmCount) / seconds };
  }
  const total = Math.round(rate * (warmup + seconds));
  const warmCount = Math.round(rate * warmup);
  const codes = new Uint8Array(total);
  if (mix === 'realistic') {
    const rng = random(seed);
    const gap = () => 1000 - Math.log(1 - rng()) * 1000;
    const state = Array.from({ length: scopes }, () => ({ next: rng() * gap(), burst: 0, dwell: 0 }));
    for (let i = 0; i < total; i += 1) {
      const scope = state[i % scopes];
      const time = i * 1000 / rate;
      if (scope.burst > 0) { codes[i] = 2; scope.burst -= 1; }
      else if (scope.dwell > 0) { codes[i] = 3; scope.dwell -= 1; }
      else if (time >= scope.next) {
        codes[i] = 1;
        scope.next = time + gap();
        scope.burst = 2 + Math.floor(rng() * 6);
        scope.dwell = rng() < 0.3 ? 1 + Math.floor(rng() * 3) : 0;
      }
    }
  } else if (!['continuous', 'noop'].includes(mix)) throw new Error(`unknown mix: ${mix}`);
  return { total, warmCount, intendedMs: (i) => i * 1000 / rate, scopeOf: (i) => i % scopes, kindOf: (i) => kinds[codes[i]], targetRate: rate };
}

export function eventLine(i, scope, kind, atMs) {
  return '{"scope":"s' + scope + '","id":"e' + i + '","at":' + atMs + ',"name":"bench.' + kind + '","data":{"seq":' + i + ',"x":1,"y":2,"value":0.42},"text":"' + kind + ' ' + i + '"}\n';
}

export function benchEvent(i, kind, atMs) {
  return { id: `e${i}`, at: atMs, name: `bench.${kind}`, data: { seq: i, x: 1, y: 2, value: 0.42 }, text: `${kind} ${i}` };
}

export const EVENT_BYTES = JSON.stringify(benchEvent(0, 'continuous', 1760000000000)).length;
