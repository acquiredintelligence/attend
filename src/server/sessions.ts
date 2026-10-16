import { randomUUID } from 'node:crypto';

export interface Sessions {
  open(): string | null;
  touch(id: string): boolean;
  close(id: string): void;
  dispose(): void;
}

/** attend's session table, with a cap and idle expiry. */
export function createSessions(options: { max: number; idleMs: number; onClose?: (id: string) => void }): Sessions {
  const entries = new Map<string, number>();
  function close(id: string) { if (entries.delete(id)) options.onClose?.(id); }
  function expire() {
    const now = Date.now();
    for (const [id, touched] of entries) if (now - touched >= options.idleMs) close(id);
  }
  const timer = setInterval(expire, Math.max(1, Math.min(options.idleMs, 60_000)));
  timer.unref();
  return {
    open() {
      expire();
      if (entries.size >= options.max) return null;
      const id = randomUUID(); entries.set(id, Date.now()); return id;
    },
    touch(id) {
      const touched = entries.get(id);
      if (touched === undefined) return false;
      if (Date.now() - touched >= options.idleMs) { close(id); return false; }
      entries.set(id, Date.now()); return true;
    },
    close,
    dispose() { clearInterval(timer); for (const id of entries.keys()) close(id); },
  };
}
