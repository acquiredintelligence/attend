import type { Scheduler, SchedulerOptions } from './types.js';

type Entry = { at: number; seq: number; scope: string };
const before = (a: Entry, b: Entry) => a.at < b.at || (a.at === b.at && a.seq < b.seq);

/** One lazy min-heap and one clock timer, with bounded firing slices. */
export function createScheduler({ clock, sliceUs = 50, onDue }: SchedulerOptions): Scheduler {
  const heap: Entry[] = [];
  const queued = new Map<string, number>();
  let seq = 0;
  let timer: { at: number; cancel(): void } | null = null;
  let continuation: ReturnType<typeof setImmediate> | null = null;
  let lag = 0;
  let firing = false;

  function push(entry: Entry): void {
    let i = heap.length;
    heap.push(entry);
    while (i > 0) {
      const parent = (i - 1) >>> 1;
      if (!before(entry, heap[parent]!)) break;
      heap[i] = heap[parent]!;
      i = parent;
    }
    heap[i] = entry;
  }

  function pop(): Entry {
    const head = heap[0]!;
    const tail = heap.pop()!;
    if (heap.length > 0) {
      let i = 0;
      while (2 * i + 1 < heap.length) {
        let child = 2 * i + 1;
        if (child + 1 < heap.length && before(heap[child + 1]!, heap[child]!)) child += 1;
        if (!before(heap[child]!, tail)) break;
        heap[i] = heap[child]!;
        i = child;
      }
      heap[i] = tail;
    }
    return head;
  }

  function armHead(): void {
    const head = heap[0];
    if (!head || firing || continuation !== null || (timer && timer.at <= head.at)) return;
    timer?.cancel();
    timer = { at: head.at, cancel: clock.at(head.at, fire) };
  }

  function fire(): void {
    timer = null;
    continuation = null;
    firing = true;
    const started = performance.now();
    while (heap[0] && heap[0].at <= clock.now()) {
      const entry = pop();
      if (queued.get(entry.scope) === entry.at) {
        queued.delete(entry.scope);
        lag = clock.now() - entry.at;
        onDue(entry.scope);
      }
      if ((performance.now() - started) * 1000 >= sliceUs && heap[0] && heap[0].at <= clock.now()) {
        firing = false;
        continuation = setImmediate(fire);
        return;
      }
    }
    firing = false;
    armHead();
  }

  return {
    arm(scope, at) {
      if (at === null) return;
      const prior = queued.get(scope);
      if (prior !== undefined && prior <= at) return;
      queued.set(scope, at);
      push({ scope, at, seq: seq++ });
      armHead();
    },
    lagMs: () => lag,
    stop() {
      timer?.cancel();
      if (continuation !== null) clearImmediate(continuation);
      timer = null;
      continuation = null;
      heap.length = 0;
      queued.clear();
    },
  };
}
