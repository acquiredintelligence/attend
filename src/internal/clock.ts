import type { Clock } from '../types.js';

/** Resolve after `ms` on `clock`, or at once on abort. */
export function sleep(clock: Clock, ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const onAbort = () => {
      cancel();
      resolve();
    };
    const cancel = clock.at(clock.now() + ms, () => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    });
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

/** One macrotask: every microtask chain that does not wait on real I/O has drained after it. */
export function drain(): Promise<void> {
  return new Promise((resolve) => (typeof setImmediate === 'function' ? setImmediate(resolve) : setTimeout(resolve, 0)));
}

export function systemClock(): Clock {
  const MAX_DELAY = 2 ** 31 - 1;
  return {
    now: () => Date.now(),
    at(at, fn) {
      const timer = setTimeout(fn, Math.min(Math.max(0, at - Date.now()), MAX_DELAY));
      return () => clearTimeout(timer);
    },
  };
}
