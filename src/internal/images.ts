/**
 * Image checks for a judge that rejects a whole request over one bad image. Reads only the
 * PNG IHDR or JPEG SOF header, decoding base64 three bytes at a time, so a 16 MiB file costs a
 * few dozen decodes rather than one copy.
 */

export interface ImageHeader {
  format: 'png' | 'jpeg';
  width: number;
  height: number;
  animated: boolean;
}

// No repeated group: V8 recurses per repetition and overflows the stack on a 20 MB image.
const BASE64 = /^[A-Za-z0-9+/]*={0,2}$/;

/** True for the standard alphabet with `=` padding and no whitespace, which is what the providers accept. */
export function isStandardBase64(data: string): boolean {
  return data.length > 0 && data.length % 4 === 0 && BASE64.test(data);
}

/** The byte count a base64 string decodes to, without decoding it. */
export function decodedLength(data: string): number {
  const padding = data.endsWith('==') ? 2 : data.endsWith('=') ? 1 : 0;
  return (data.length / 4) * 3 - padding;
}

/** Width, height and format from the header of a base64 PNG or JPEG; null when it is neither or the header is cut short. */
export function imageHeader(data: string): ImageHeader | null {
  const at = (i: number): number | null => {
    const group = Math.floor(i / 3);
    const decoded = atob(data.slice(group * 4, group * 4 + 4));
    const code = decoded.charCodeAt(i - group * 3);
    return Number.isNaN(code) ? null : code;
  };
  const u16 = (i: number): number | null => {
    const [a, b] = [at(i), at(i + 1)];
    return a === null || b === null ? null : (a << 8) | b;
  };
  const u32 = (i: number): number | null => {
    const [a, b] = [u16(i), u16(i + 2)];
    return a === null || b === null ? null : a * 65_536 + b;
  };
  const tag = (i: number): string | null => {
    const bytes = [at(i), at(i + 1), at(i + 2), at(i + 3)];
    return bytes.some((b) => b === null) ? null : String.fromCharCode(...bytes.map((b) => b ?? 0));
  };

  if (u32(0) === 0x89504e47 && u32(4) === 0x0d0a1a0a && tag(12) === 'IHDR') {
    const [width, height] = [u32(16), u32(20)];
    if (width === null || height === null) return null;
    let animated = false;
    for (let offset = 8; ; ) {
      const [length, type] = [u32(offset), tag(offset + 4)];
      if (length === null || type === null || type === 'IDAT' || type === 'IEND') break;
      if (type === 'acTL') animated = true;
      offset += 12 + length;
    }
    return { format: 'png', width, height, animated };
  }

  if (u16(0) === 0xffd8) {
    for (let offset = 2; ; ) {
      const [marker, code] = [at(offset), at(offset + 1)];
      if (marker !== 0xff || code === null) return null;
      if (code === 0xff) {
        offset += 1;
        continue;
      }
      const standalone = code === 0x01 || (code >= 0xd0 && code <= 0xd9);
      const frame = code >= 0xc0 && code <= 0xcf && code !== 0xc4 && code !== 0xc8 && code !== 0xcc;
      if (frame) {
        const [height, width] = [u16(offset + 5), u16(offset + 7)];
        return height === null || width === null ? null : { format: 'jpeg', width, height, animated: false };
      }
      const length = standalone ? 0 : u16(offset + 2);
      if (length === null) return null;
      offset += 2 + length;
    }
  }

  return null;
}
