import type { Buffer, Event, Kind, OpenWindow, SettlePolicy, SettleReason } from './types.js';

/** Closing, continuous and passive entries are acts. */
export function isAct(entry: { kind: Kind }): boolean {
  throw new Error('attend: isAct is not implemented');
}

/** The open window's earliest deadline and its reason, in O(1) from the summary. The runtime uses this one. */
export function windowDeadline(policy: SettlePolicy, open: OpenWindow): { at: number; reason: SettleReason } | null {
  throw new Error('attend: windowDeadline is not implemented');
}

/** Today's record-level deadline: summarizes `buffer` once, then calls `windowDeadline`. */
export function deadline(policy: SettlePolicy, buffer: Buffer): { at: number; reason: SettleReason } | null {
  throw new Error('attend: deadline is not implemented');
}

/** The render-size estimate: the bytes of `name`, plus `text` or the JSON of `data`, plus about 40 B of framing. */
export function estimateRenderedBytes(event: Event): number {
  throw new Error('attend: estimateRenderedBytes is not implemented');
}
