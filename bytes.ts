import { encode as _encodeHex } from 'https://deno.land/std@0.201.0/encoding/hex.ts';

export function checkEnd(bytes: Uint8Array, pos: number) {
    const extra = bytes.length - pos;
    if (extra > 0) throw new Error(`Unexpected trailing bytes: ${extra}`);
}

export function encodeHex(arr: Uint8Array): string {
    return decoder.decode(_encodeHex(arr));
}

//

const decoder = new TextDecoder();
