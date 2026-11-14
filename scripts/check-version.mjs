#!/usr/bin/env node
// The release version is written in five files, and they must agree. Given a tag, the tag must name it too.
// Usage: node scripts/check-version.mjs [vX.Y.Z], from anywhere; the repo is the directory above this script.
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const read = (file) => readFileSync(join(repo, file), 'utf8');
const match = (file, pattern) => read(file).match(pattern)?.[1];

const found = {
  'package.json': JSON.parse(read('package.json')).version,
  'src/internal/version.ts': match('src/internal/version.ts', /^export const VERSION = '([^']+)';$/m),
  '.claude-plugin/plugin.json': JSON.parse(read('.claude-plugin/plugin.json')).version,
  'python/pyproject.toml': match('python/pyproject.toml', /^\[project\]\n(?:(?!\[).*\n)*?version = "([^"]+)"$/m),
  'python/uv.lock': match('python/uv.lock', /^name = "aqi-attend"\nversion = "([^"]+)"$/m),
};

const version = found['package.json'];
const problems = [];
// npm and PyPI spell a plain X.Y.Z the same way; a pre-release would need two spellings.
if (!/^\d+\.\d+\.\d+$/.test(version ?? '')) problems.push(`package.json: "${version}" is not X.Y.Z`);
for (const [file, value] of Object.entries(found)) {
  if (value !== version) problems.push(`${file}: ${value === undefined ? 'no version found' : `"${value}"`}, but package.json says "${version}"`);
}
const tag = process.argv[2];
if (tag && tag !== `v${version}`) problems.push(`tag "${tag}" does not name v${version}`);

if (problems.length > 0) {
  console.error(`check-version: ${problems.length} problem${problems.length === 1 ? '' : 's'}`);
  for (const problem of problems) console.error(`  ${problem}`);
  process.exit(1);
}
console.log(`check-version: ${version} in ${Object.keys(found).length} files${tag ? `, and tag ${tag} names it` : ''}`);
