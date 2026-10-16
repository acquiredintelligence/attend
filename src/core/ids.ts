import type { FnvState } from './types.js';

/** FNV-1a 64 over `len:part` for the scope and each id, as 16 hex characters. */
export function windowId(scope: string, ids: readonly string[]): string {
  const state = fnvStart(scope);
  for (const id of ids) foldId(state, id);
  return fnvHex(state);
}

/** A fresh FNV-1a 64 state with `len:scope` already folded. */
export function fnvStart(scope: string): FnvState {
  const state = { hi: 0xcbf29ce4, lo: 0x84222325 };
  foldId(state, scope);
  return state;
}

/** Folds `len:id` (UTF-8 byte length, a colon, the UTF-8 bytes) into `state` in place. */
export function foldId(state: FnvState, id: string): void {
  let hi = state.hi;
  let lo = state.lo;
  let length = utf8Length(id);
  let divisor = 1;
  while (divisor * 10 <= length) divisor *= 10;
  while (divisor >= 1) {
    const digit = Math.floor(length / divisor);
    lo = (lo ^ (48 + digit)) >>> 0;
    const product = lo * 0x1b3;
    hi = (hi * 0x1b3 + Math.floor(product / 0x100000000) + ((lo << 8) >>> 0)) >>> 0;
    lo = product >>> 0;
    length -= digit * divisor;
    divisor /= 10;
  }
  lo = (lo ^ 0x3a) >>> 0;
  const product = lo * 0x1b3;
  hi = (hi * 0x1b3 + Math.floor(product / 0x100000000) + ((lo << 8) >>> 0)) >>> 0;
  lo = product >>> 0;
  for (let i = 0; i < id.length; i += 1) {
    let code = id.charCodeAt(i);
    let packed = code;
    let count = 1;
    if (code >= 0x80) {
      if (code >= 0xd800 && code <= 0xdfff) {
        const next = id.charCodeAt(i + 1);
        if (code <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
          code = 0x10000 + ((code - 0xd800) << 10) + next - 0xdc00;
          i += 1;
        } else code = 0xfffd;
      }
      if (code < 0x800) {
        packed = (0xc0 | (code >>> 6)) | ((0x80 | (code & 0x3f)) << 8);
        count = 2;
      } else if (code < 0x10000) {
        packed = (0xe0 | (code >>> 12)) | ((0x80 | ((code >>> 6) & 0x3f)) << 8) | ((0x80 | (code & 0x3f)) << 16);
        count = 3;
      } else {
        packed = (0xf0 | (code >>> 18)) | ((0x80 | ((code >>> 12) & 0x3f)) << 8)
          | ((0x80 | ((code >>> 6) & 0x3f)) << 16) | ((0x80 | (code & 0x3f)) << 24);
        count = 4;
      }
    }
    do {
      lo = (lo ^ (packed & 0xff)) >>> 0;
      const product = lo * 0x1b3;
      hi = (hi * 0x1b3 + Math.floor(product / 0x100000000) + ((lo << 8) >>> 0)) >>> 0;
      lo = product >>> 0;
      packed >>>= 8;
      count -= 1;
    } while (count > 0);
  }
  state.hi = hi;
  state.lo = lo;
}

/** The two unsigned limbs as 16 lowercase hex characters. */
export function fnvHex(state: FnvState): string {
  return state.hi.toString(16).padStart(8, '0') + state.lo.toString(16).padStart(8, '0');
}

/** UTF-8 byte length, replacing lone surrogates with U+FFFD as TextEncoder does. */
export function utf8Length(text: string): number {
  let length = 0;
  for (let i = 0; i < text.length; i += 1) {
    const code = text.charCodeAt(i);
    if (code < 0x80) length += 1;
    else if (code < 0x800) length += 2;
    else if (code >= 0xd800 && code <= 0xdbff && text.charCodeAt(i + 1) >= 0xdc00 && text.charCodeAt(i + 1) <= 0xdfff) {
      length += 4;
      i += 1;
    } else length += 3;
  }
  return length;
}
