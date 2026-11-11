import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';
import { z } from 'zod';
import { foldEvents } from '../src/index.js';

const item = z.object({ id: z.string(), group: z.string(), value: z.number(), continuous: z.boolean() });
const fixture = z.object({
  name: z.string(),
  items: z.array(item),
  fold: z.boolean().default(true),
  expected: z.array(z.object({ first: z.string(), last: z.string(), count: z.number(), mode: z.enum(['single', 'repeat', 'steps']) })),
});
const cases = z.array(fixture).parse(JSON.parse(readFileSync(new URL('../conformance/fixtures/fold.json', import.meta.url), 'utf8')));

describe('foldEvents', () => {
  for (const c of cases) {
    test(c.name, () => {
      const input = Object.freeze(c.items.map((item) => Object.freeze(item)));
      const before = JSON.stringify(input);
      const groups = foldEvents(input, {
        repeats: (a, b) => a.group === b.group && a.value === b.value,
        continues: (a, b) => a.continuous && b.continuous && a.group === b.group,
        fold: c.fold,
      });
      expect(groups.map((g) => ({ ...g, first: g.first.id, last: g.last.id }))).toEqual(c.expected);
      expect(groups.reduce((n, g) => n + g.count, 0)).toBe(input.length);
      expect(JSON.stringify(input)).toBe(before);
      for (const g of groups) {
        expect(input.includes(g.first)).toBe(true);
        expect(input.includes(g.last)).toBe(true);
      }
    });
  }
});
