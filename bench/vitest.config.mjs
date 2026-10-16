import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  root: fileURLToPath(new URL('..', import.meta.url)),
  test: { include: ['bench/test/**/*.test.mjs'], fileParallelism: false, testTimeout: 120000 },
});
