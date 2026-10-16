import { existsSync, readdirSync, statSync, readFileSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';

function sourceFiles(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const file = join(dir, entry.name);
    return entry.isDirectory() ? sourceFiles(file) : file.endsWith('.ts') ? [file] : [];
  });
}

export async function loadBuild(directory = 'dist') {
  const dist = resolve(directory);
  const root = resolve(dist, '..');
  const entry = join(dist, 'index.js');
  if (existsSync(join(root, 'src'))) {
    const builtAt = statSync(entry).mtimeMs;
    for (const file of sourceFiles(join(root, 'src'))) {
      if (statSync(file).mtimeMs > builtAt) throw new Error(`stale build: ${dist} is older than ${file}; run pnpm build`);
    }
  }
  let version = 'unknown';
  let sha = 'unknown';
  let dirty = '';
  try { version = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')).version ?? 'unknown'; } catch {}
  try {
    sha = execFileSync('git', ['-C', root, 'rev-parse', '--short', 'HEAD'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    dirty = execFileSync('git', ['-C', root, 'status', '--porcelain', '--', 'src'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim() ? '-dirty' : '';
  } catch {}
  const module = (name) => import(pathToFileURL(join(dist, `${name}.js`)).href);
  const core = await module('index');
  const probe = core.memoryStore();
  const port = typeof probe.load === 'function' && typeof probe.commit === 'function' ? 'scope' : 'state';
  await probe.close?.();
  let reducer = 'step';
  if (existsSync(join(dist, 'core/reducer.js'))) {
    try {
      (await module('core/reducer')).newOutput();
      reducer = 'core';
    } catch (error) {
      if (!/not implemented/.test(error.message)) throw error;
    }
  }
  return { dist, adapterVersion: `${version}+${sha}${dirty}`, port, reducer, core, module };
}
