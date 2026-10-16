import { randomUUID } from 'node:crypto';

export interface Sessions {
  open(): string | null;
  touch(id: string): boolean;
  hold(id: string): () => void;
  close(id: string): void;
  dispose(): void;
}

/** attend's session table, with a cap and idle expiry. */
export function createSessions(options: { max: number; idleMs: number; onClose?: (id: string) => void }): Sessions {
  const entries = new Map<string, { touched: number; holds: number }>();
  function close(id: string) { if (entries.delete(id)) options.onClose?.(id); }
  function expire() {
    const now = Date.now();
    for (const [id, entry] of entries) if (!entry.holds && now - entry.touched >= options.idleMs) close(id);
  }
  const timer = setInterval(expire, Math.max(1, Math.min(options.idleMs, 60_000)));
  timer.unref();
  return {
    open() {
      expire();
      if (entries.size >= options.max) return null;
      const id = randomUUID(); entries.set(id, { touched: Date.now(), holds: 0 }); return id;
    },
    touch(id) {
      const entry = entries.get(id);
      if (!entry) return false;
      if (!entry.holds && Date.now() - entry.touched >= options.idleMs) { close(id); return false; }
      entry.touched = Date.now(); return true;
    },
    hold(id) {
      const entry = entries.get(id);
      if (!entry) return () => undefined;
      entry.holds++;
      let released = false;
      return () => {
        if (released) return;
        released = true;
        if (--entry.holds === 0) entry.touched = Date.now();
      };
    },
    close,
    dispose() { clearInterval(timer); for (const id of entries.keys()) close(id); },
  };
}
