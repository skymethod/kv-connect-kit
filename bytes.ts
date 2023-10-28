export { encodeHex, decodeHex } from 'https://deno.land/std@0.204.0/encoding/hex.ts';

export function checkEnd(bytes: Uint8Array, pos: number) {
    const extra = bytes.length - pos;
    if (extra > 0) throw new Error(`Unexpected trailing bytes: ${extra}`);
}

export function equalBytes(lhs: Uint8Array, rhs: Uint8Array): boolean {
    if (lhs.length !== rhs.length) return false;
    for (let i = 0; i < lhs.length; i++) {
        if (lhs[i] !== rhs[i]) return false;
    }
    return true;
}

export function flipBytes(arr: Uint8Array | number[], start = 0): void {
    for (let i = start; i < arr.length; i++) {
        arr[i] = 0xff - arr[i];
    }
}

export function computeBigintMinimumNumberOfBytes(val: bigint): number {
    let n = 0;
    while (val !== 0n) {
        val >>= 8n;
        n++;
    }
    return n;
}
