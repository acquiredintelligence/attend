import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import os from 'node:os';
import { rm } from 'node:fs/promises';
import { taskset } from './runner.mjs';

export function spawnTarget(options = {}) {
  const name = `ab-${process.pid}-${randomBytes(5).toString('hex')}.sock`;
  let socketPath = options.socketPath ?? join(os.tmpdir(), name);
  if (Buffer.byteLength(socketPath) > 100) socketPath = join('/tmp', name);
  const argv = taskset(options.cpus, [process.execPath, fileURLToPath(new URL('../target.mjs', import.meta.url)), JSON.stringify({ ...options, socketPath })]);
  const child = spawn(argv[0], argv.slice(1), { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
  let id = 0;
  const pending = new Map();
  let readyResolve;
  let readyReject;
  let failure;
  const ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
  const fail = (error) => {
    failure = error;
    readyReject(error);
    for (const { reject } of pending.values()) reject(error);
    pending.clear();
  };
  child.on('error', fail);
  child.on('exit', (code, signal) => fail(new Error(`target exited: ${code ?? signal}`)));
  child.on('message', (message) => {
    if (message.type === 'ready') readyResolve();
    if (message.type === 'error') fail(new Error(message.error));
    if (message.requestId !== undefined) {
      const entry = pending.get(message.requestId);
      pending.delete(message.requestId);
      entry?.resolve(message);
    }
  });
  const send = (message) => {
    if (failure) throw failure;
    child.send(message);
  };
  const exited = new Promise((resolve) => child.once('exit', resolve));
  let closing;
  return {
    ready, socketPath, send,
    request(type) {
      if (failure) return Promise.reject(failure);
      const requestId = ++id;
      return new Promise((resolve, reject) => {
        pending.set(requestId, { resolve, reject });
        send({ ...(typeof type === 'string' ? { type } : type), requestId });
      });
    },
    exit() {
      closing ??= (async () => {
        const timeout = setTimeout(() => child.kill('SIGKILL'), 5000);
        try {
          if (child.exitCode === null && child.signalCode === null) {
            if (child.connected) child.send({ type: 'exit' });
            else child.kill();
            await exited;
          }
        } finally { clearTimeout(timeout); await rm(socketPath, { force: true }); }
      })();
      return closing;
    },
  };
}
