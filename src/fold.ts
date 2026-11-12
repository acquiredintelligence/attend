/** A consecutive run. Endpoints refer to the original input values. */
export interface EventFold<T> {
  readonly first: T;
  readonly last: T;
  readonly count: number;
  readonly mode: 'single' | 'repeat' | 'steps';
}

/** Fold while every value repeats the first or every value continues its act. */
export function foldEvents<T>(
  events: readonly T[],
  options: {
    repeats: (first: T, next: T) => boolean;
    continues: (first: T, next: T) => boolean;
    fold?: boolean;
  },
): readonly EventFold<T>[] {
  if (options.fold === false) return events.map((event) => ({ first: event, last: event, count: 1, mode: 'single' }));
  const out: EventFold<T>[] = [];
  let run: EventFold<T> | undefined;
  let same = true;
  let steps = true;
  for (const event of events) {
    const repeats: boolean = run !== undefined && same && options.repeats(run.first, event);
    const continues: boolean = run !== undefined && steps && options.continues(run.first, event);
    if (run !== undefined && (repeats || continues)) {
      same = repeats;
      steps = continues;
      run = { first: run.first, last: event, count: run.count + 1, mode: same ? 'repeat' : 'steps' };
    } else {
      if (run !== undefined) out.push(run);
      run = { first: event, last: event, count: 1, mode: 'single' };
      same = true;
      steps = true;
    }
  }
  if (run !== undefined) out.push(run);
  return out;
}
