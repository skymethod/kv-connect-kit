import { encode as _encodeHex, decode as _decodeHex } from 'https://deno.land/std@0.201.0/encoding/hex.ts';

export function checkEnd(bytes: Uint8Array, pos: number) {
    const extra = bytes.length - pos;
    if (extra > 0) throw new Error(`Unexpected trailing bytes: ${extra}`);
}

export function encodeHex(arr: Uint8Array): string {
    return decoder.decode(_encodeHex(arr));
}

export function decodeHex(str: string): Uint8Array {
    return _decodeHex(new TextEncoder().encode(str));
}

export function equalBytes(lhs: Uint8Array, rhs: Uint8Array) {
    if (lhs.length !== rhs.length) return false;
    for (let i = 0; i < lhs.length; i++) {
        if (lhs[i] !== rhs[i]) return false;
    }
    return true;
}

export function computeBigintMinimumNumberOfBytes(val: bigint): number {
    let n = 0;
    while (val !== 0n) {
        val >>= 8n;
        n++;
    }
    return n;
}

//

const decoder = new TextDecoder();
