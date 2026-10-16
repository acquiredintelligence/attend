import { describe, expect, test } from 'vitest';
import { decodedLength, imageHeader, isStandardBase64 } from '../src/internal/images.js';
import { base64, jpeg, png } from './support/images.js';

describe('imageHeader', () => {
  test('reads width and height from a PNG IHDR and a JPEG SOF', () => {
    expect(imageHeader(png(640, 480))).toEqual({ format: 'png', width: 640, height: 480, animated: false });
    expect(imageHeader(jpeg(1568, 1176))).toEqual({ format: 'jpeg', width: 1568, height: 1176, animated: false });
    expect(imageHeader(jpeg(300, 301, { progressive: true }))).toEqual({ format: 'jpeg', width: 300, height: 301, animated: false });
  });

  test('walks past a long EXIF segment and a long text chunk to find the size', () => {
    expect(imageHeader(jpeg(400, 500, { app1Bytes: 60_000 }))).toEqual({ format: 'jpeg', width: 400, height: 500, animated: false });
    expect(imageHeader(png(400, 500, { textBeforeHeader: 9000, animated: true }))).toEqual({ format: 'png', width: 400, height: 500, animated: true });
  });

  test('is null for a WebP, a GIF, a truncated header or text', () => {
    const riff = base64(Uint8Array.from([0x52, 0x49, 0x46, 0x46, 0x10, 0, 0, 0, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38, 0x20]));
    const gif = base64(Uint8Array.from([...[...'GIF89a'].map((c) => c.charCodeAt(0)), 1, 0, 1, 0, 0, 0, 0]));
    expect([imageHeader(riff), imageHeader(gif), imageHeader(png(10, 10).slice(0, 24)), imageHeader(base64(new TextEncoder().encode('hello')))]).toEqual([null, null, null, null]);
  });
});

describe('base64 helpers', () => {
  test('decodedLength matches the bytes without decoding', () => {
    for (const n of [0, 1, 2, 3, 4, 100, 1023]) expect(decodedLength(base64(new Uint8Array(n)))).toBe(n);
  });

  test('isStandardBase64 rejects whitespace, data URLs, the url-safe alphabet and bad padding', () => {
    expect(isStandardBase64(png(300, 300))).toBe(true);
    expect(['', 'abc', 'ab cd', 'data:image/png;base64,AAAA', 'ab-d', 'abc=efgh', 'AAA'].map(isStandardBase64)).toEqual([false, false, false, false, false, false, false]);
  });
});
