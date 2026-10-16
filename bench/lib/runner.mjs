import os from 'node:os';
import { readFileSync } from 'node:fs';

export function parseCpus(value) {
  return [...new Set(value.trim().split(',').flatMap((part) => {
    const [a, b = a] = part.split('-').map(Number);
    if (!Number.isInteger(a) || !Number.isInteger(b) || a < 0 || b < a) throw new Error('invalid CPU list');
    return Array.from({ length: b - a + 1 }, (_, i) => a + i);
  }))].sort((a, b) => a - b);
}

export function allowedCpus() {
  if (process.platform !== 'linux') return null;
  const match = readFileSync('/proc/self/status', 'utf8').match(/^Cpus_allowed_list:\s*(.+)$/m);
  return match ? parseCpus(match[1]) : null;
}

export function runnerInfo({ cpus } = {}) {
  const available = os.cpus();
  const allowed = allowedCpus();
  let pinned = false;
  if (process.platform === 'linux' && allowed) {
    const online = parseCpus(readFileSync('/sys/devices/system/cpu/online', 'utf8'));
    const requested = cpus === undefined ? allowed : typeof cpus === 'string' ? parseCpus(cpus) : cpus;
    pinned = allowed.length < online.length && allowed.every((cpu) => online.includes(cpu)) && requested.every((cpu) => allowed.includes(cpu));
  }
  return {
    pinned,
    host: `${os.platform()}/${os.arch()} ${available[0]?.model ?? 'unknown'} x${available.length}, node ${process.version}`,
    loadAverage: Math.round(os.loadavg()[0] * 100) / 100,
  };
}

export function taskset(cpus, argv) {
  if (!cpus) return argv;
  if (process.platform !== 'linux') throw new Error('CPU pinning requires Linux');
  return ['taskset', '-c', Array.isArray(cpus) ? cpus.join(',') : cpus, ...argv];
}

export function finishRunner(start, target) {
  const end = runnerInfo();
  const own = allowedCpus();
  return {
    ...end,
    loadAverage: Math.max(start.loadAverage, end.loadAverage),
    pinned: target ? start.pinned && target.pinned && !!own && !!target.cpus && own.every((cpu) => !target.cpus.includes(cpu)) : start.pinned,
  };
}
