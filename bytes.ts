import { encode as _encodeHex } from 'https://deno.land/std@0.201.0/encoding/hex.ts';

export function checkEnd(bytes: Uint8Array, pos: number) {
    const extra = bytes.length - pos;
    if (extra > 0) throw new Error(`Unexpected trailing bytes: ${extra}`);
}

export function encodeHex(arr: Uint8Array): string {
    return decoder.decode(_encodeHex(arr));
}

export function equalBytes(lhs: Uint8Array, rhs: Uint8Array) {
    if (lhs.length !== rhs.length) return false;
    for (let i = 0; i < lhs.length; i++) {
        if (lhs[i] !== rhs[i]) return false;
    }
    return true;
}

//

const decoder = new TextDecoder();
