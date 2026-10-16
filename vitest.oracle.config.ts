import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { relative, resolve, sep } from 'node:path';
import { defineConfig } from 'vitest/config';

const root = fileURLToPath(new URL('.', import.meta.url));
const source = resolve(root, 'src') + sep;

export default defineConfig({
  plugins: [{
    name: 'frozen-oracle',
    enforce: 'pre',
    async resolveId(id, importer, options) {
      const resolved = await this.resolve(id, importer, { ...options, skipSelf: true });
      if (!resolved || !resolved.id.startsWith(source)) return resolved;
      const frozen = resolve(root, 'test/oracle', relative(source, resolved.id));
      if (!existsSync(frozen)) throw new Error(`No frozen oracle copy for ${resolved.id}`);
      return frozen;
    },
  }],
  test: {
    include: ['test/step.test.ts', 'test/attend.test.ts'],
  },
});
