/** Tiny real image files for tests: a valid PNG or JPEG header with the given size, and no pixel data worth looking at. */

function u32(n: number): number[] {
  return [(n >>> 24) & 0xff, (n >>> 16) & 0xff, (n >>> 8) & 0xff, n & 0xff];
}

function chunk(type: string, data: number[]): number[] {
  return [...u32(data.length), ...[...type].map((c) => c.charCodeAt(0)), ...data, 0, 0, 0, 0];
}

export function pngBytes(width: number, height: number, options?: { animated?: boolean; textBeforeHeader?: number }): Uint8Array {
  const ihdr = chunk('IHDR', [...u32(width), ...u32(height), 8, 6, 0, 0, 0]);
  const actl = options?.animated ? chunk('acTL', [...u32(2), ...u32(0)]) : [];
  const text = options?.textBeforeHeader ? chunk('tEXt', Array.from({ length: options.textBeforeHeader }, () => 0x41)) : [];
  return Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, ...ihdr, ...text, ...actl, ...chunk('IDAT', [0]), ...chunk('IEND', [])]);
}

export function jpegBytes(width: number, height: number, options?: { progressive?: boolean; app1Bytes?: number }): Uint8Array {
  const app0 = [0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00];
  const app1 = options?.app1Bytes ? [0xff, 0xe1, ...u32(options.app1Bytes + 2).slice(2), ...Array.from({ length: options.app1Bytes }, () => 0)] : [];
  const sof = [0xff, options?.progressive ? 0xc2 : 0xc0, 0x00, 0x0b, 0x08, ...u32(height).slice(2), ...u32(width).slice(2), 0x01, 0x01, 0x11, 0x00];
  return Uint8Array.from([0xff, 0xd8, ...app0, ...app1, ...sof, 0xff, 0xd9]);
}

export const base64 = (bytes: Uint8Array): string => Buffer.from(bytes).toString('base64');

export const png = (width: number, height: number, options?: Parameters<typeof pngBytes>[2]) => base64(pngBytes(width, height, options));
export const jpeg = (width: number, height: number, options?: Parameters<typeof jpegBytes>[2]) => base64(jpegBytes(width, height, options));
