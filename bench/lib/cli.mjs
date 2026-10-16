import { parseArgs } from 'node:util';
import { appendFileSync, readFileSync } from 'node:fs';
import { validateReport } from './schema.mjs';

export function args(definitions, argv = process.argv.slice(2)) {
  const options = Object.fromEntries(Object.entries(definitions).map(([name, value]) => [name,
    typeof value === 'boolean' ? { type: 'boolean', default: value } : { type: 'string', ...(value === undefined ? {} : { default: String(value) }) }]));
  return parseArgs({ options, args: argv }).values;
}
export function positive(value, name, zero = false) {
  const n = Number(value);
  if (!Number.isFinite(n) || (zero ? n < 0 : n <= 0)) throw new Error(`invalid ${name}: ${value}`);
  return n;
}
export const list = (value) => value.split(',').map((item) => positive(item, 'list value', true));
export const storeOptions = { store: 'memory', journal: 'WAL', synchronous: 'NORMAL', worker: false, 'postgres-url': undefined, 'redis-url': undefined, 'synchronous-commit': undefined };
export function storeSpec(options) {
  switch (options.store) {
    case 'memory': return { store: 'memory' };
    case 'sqlite': return { store: 'sqlite', journalMode: options.journal, synchronous: options.synchronous, worker: options.worker };
    case 'postgres': return { store: 'postgres', url: options['postgres-url'], synchronousCommit: options['synchronous-commit'] };
    case 'redis': return { store: 'redis', url: options['redis-url'] };
    default: throw new Error(`unknown store: ${options.store}`);
  }
}
export function emit(report, out) {
  const result = validateReport(report);
  if (!result.ok) throw new Error(result.errors.join('\n'));
  const line = JSON.stringify(result.report) + '\n';
  process.stdout.write(line);
  if (out) appendFileSync(out, line);
}
export function readReports(files) {
  const reports = [];
  for (const file of files) {
    for (const [i, line] of readFileSync(file, 'utf8').split('\n').entries()) {
      if (!line.trim()) continue;
      let result;
      try { result = validateReport(JSON.parse(line)); }
      catch { throw new Error(`${file}:${i + 1}: invalid JSON`); }
      if (!result.ok) throw new Error(`${file}:${i + 1}: ${result.errors.join('; ')}`);
      reports.push(result.report);
    }
  }
  return reports;
}
