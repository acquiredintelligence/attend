#!/usr/bin/env node
// Every ```ts block in the docs is either copied from examples/*.ts (its lines appear there in
// order, and `pnpm typecheck` covers examples/) or typechecks standalone against src/.
// Usage: node scripts/check-snippets.mjs, from anywhere; the repo is the directory above this script.
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const UNINSTALLED = [/from 'openai'/, /from 'ai'/, /from '@ai-sdk\//, /from '@google\/genai'/, /from 'ioredis'/];
const ANTHROPIC_STUB = `declare module '@anthropic-ai/sdk' {
  export default class Anthropic {
    messages: {
      create(
        body: {
          model: string;
          max_tokens: number;
          system: string;
          messages: { role: 'user'; content: ({ type: 'text'; text: string } | { type: 'image'; source: { type: 'base64'; media_type: string; data: string } })[] }[];
          output_config: { effort: 'low' | 'medium' | 'high' | 'xhigh' | 'max'; format: { type: 'json_schema'; schema: Record<string, unknown> } };
        },
        options: { signal: AbortSignal },
      ): Promise<{ stop_reason: string | null; content: ({ type: 'text'; text: string } | { type: 'thinking'; thinking: string })[] }>;
    };
  }
}
`;

const docs = ['README.md', 'skills/attend/SKILL.md'].filter((d) => existsSync(join(repo, d)));
for (const dir of ['skills/attend/references', 'docs']) {
  if (existsSync(join(repo, dir))) for (const f of readdirSync(join(repo, dir))) if (f.endsWith('.md')) docs.push(`${dir}/${f}`);
}

const examples = readdirSync(join(repo, 'examples'))
  .filter((f) => f.endsWith('.ts'))
  .map((f) => ({ name: `examples/${f}`, lines: readFileSync(join(repo, 'examples', f), 'utf8').split('\n').map((l) => l.trim()) }));

function copiedFrom(block) {
  const want = block.split('\n').map((l) => l.trim()).filter((l) => l !== '');
  for (const ex of examples) {
    let i = 0;
    for (const line of ex.lines) if (line === want[i]) i += 1;
    if (i === want.length) return ex.name;
  }
  return null;
}

const scratch = join(repo, '.snippets');
rmSync(scratch, { recursive: true, force: true });
mkdirSync(scratch);
const results = [];
const standalone = [];
for (const doc of docs) {
  const text = readFileSync(join(repo, doc), 'utf8');
  const re = /^```(?:ts|typescript)\n([\s\S]*?)^```/gm;
  for (let m = re.exec(text); m; m = re.exec(text)) {
    const line = text.slice(0, m.index).split('\n').length;
    const where = `${doc}:${line}`;
    const block = m[1];
    if (UNINSTALLED.some((r) => r.test(block))) {
      results.push([where, 'skipped (imports a package that is not installed)']);
      continue;
    }
    const from = copiedFrom(block);
    if (from) {
      results.push([where, `copied from ${from}`]);
      continue;
    }
    const file = join(scratch, `${doc.replace(/[^a-z0-9]+/gi, '_')}_${line}.ts`);
    writeFileSync(file, `${block}\nexport {};\n`);
    standalone.push([where, file]);
  }
}

let failed = false;
if (standalone.length > 0) {
  writeFileSync(join(scratch, 'anthropic-sdk.d.ts'), ANTHROPIC_STUB);
  writeFileSync(join(scratch, 'tsconfig.json'), JSON.stringify({ extends: '../tsconfig.json', compilerOptions: { noEmit: true, noUnusedLocals: false }, include: ['./*.ts'] }));
  let out = '';
  try {
    execFileSync(join(repo, 'node_modules/.bin/tsc'), ['-p', join(scratch, 'tsconfig.json')], { cwd: repo, encoding: 'utf8' });
  } catch (error) {
    out = `${error.stdout ?? ''}${error.stderr ?? ''}`;
  }
  for (const [where, file] of standalone) {
    const rel = relative(repo, file);
    const errors = out.split('\n').filter((l) => l.startsWith(rel));
    results.push([where, errors.length === 0 ? 'typechecks standalone' : `FAILS\n    ${errors.join('\n    ')}`]);
    if (errors.length > 0) failed = true;
  }
  const other = out.split('\n').filter((l) => /error TS/.test(l) && !standalone.some(([, f]) => l.startsWith(relative(repo, f))));
  if (other.length > 0) {
    failed = true;
    results.push(['tsc', `FAILS\n    ${other.join('\n    ')}`]);
  }
}
rmSync(scratch, { recursive: true, force: true });

for (const [where, status] of results) console.log(`${status.startsWith('FAILS') ? 'FAIL' : 'ok  '}  ${where}  ${status}`);
console.log(`check-snippets: ${results.length} blocks, ${failed ? 'FAILED' : 'all good'}`);
process.exit(failed ? 1 : 0);
