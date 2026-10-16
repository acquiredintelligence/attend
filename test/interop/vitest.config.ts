import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/interop/*.interop.ts'],
    fileParallelism: false,
    hookTimeout: 60_000,
  },
});
