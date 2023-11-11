export { encodeHex, decodeHex } from 'https://deno.land/std@0.206.0/encoding/hex.ts';

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

export function compareBytes(lhs: Uint8Array, rhs: Uint8Array): number {
    if (lhs === rhs) return 0;

    let x = lhs.length, y = rhs.length;
    const len = Math.min(x, y);

    for (let i = 0; i < len; i++) {
        if (lhs[i] !== rhs[i]) {
            x = lhs[i];
            y = rhs[i];
            break;
        }
    }
    
    return x < y ? -1 : y < x ? 1 : 0;
}
