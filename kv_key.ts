import { computeBigintMinimumNumberOfBytes, checkEnd } from './bytes.ts';
import { KvKey, KvKeyPart } from './kv_types.ts';

// https://github.com/apple/foundationdb/blob/main/design/tuple.md
// limited to Uint8Array | string | number | bigint | boolean

// https://github.com/denoland/deno/blob/main/ext/kv/codec.rs

export function packKey(kvKey: KvKey): Uint8Array {
    return new Uint8Array(kvKey.flatMap(v => [...packKeyPart(v)]));
}

export function packKeyPart(kvKeyPart: KvKeyPart): Uint8Array {
    if (kvKeyPart instanceof Uint8Array) return new Uint8Array([ Typecode.ByteString, ...encodeZeroWithZeroFF(kvKeyPart), 0 ]);
    if (typeof kvKeyPart === 'string') return new Uint8Array([ Typecode.UnicodeString, ...encodeZeroWithZeroFF(new TextEncoder().encode(kvKeyPart)), 0]);
    if (typeof kvKeyPart === 'number') {
        const sub = new Uint8Array(8);
        new DataView(sub.buffer).setFloat64(0, -kvKeyPart, false);
        return new Uint8Array([ 33, ...sub]);
    }
    if (kvKeyPart === false) return new Uint8Array([ Typecode.False ]);
    if (kvKeyPart === true) return new Uint8Array([ Typecode.True ]);
    if (typeof kvKeyPart === 'bigint') {
        const neg = kvKeyPart < 0;
        const abs = neg ? -kvKeyPart : kvKeyPart;
        const numBytes = BigInt(computeBigintMinimumNumberOfBytes(abs));
       
        const typecode = neg ? (numBytes <= 8n ? (Typecode.IntegerOneByteNegative - Number(numBytes) + 1) : Typecode.IntegerArbitraryByteNegative)
            : (numBytes <= 8n ? (Typecode.IntegerOneBytePositive + Number(numBytes) - 1) : Typecode.IntegerArbitraryBytePositive);
        const bytes: number[] = [ typecode ];
        if (numBytes > 8n) bytes.push(Number(neg ? 0xffn - numBytes : numBytes));
        for (let i = 0n; i < numBytes; i++) {
            const mask = 0xffn << 8n * (numBytes - i - 1n);
            let byte = Number((abs & mask) >> (8n * (numBytes - i - 1n)));
            if (neg) byte = 0xff - byte;
            bytes.push(byte);
        }
        return new Uint8Array(bytes);
    }
    throw new Error(`implement keyPart: ${typeof kvKeyPart} ${kvKeyPart}`);
}

export function unpackKey(bytes: Uint8Array): KvKey {
    const rt: KvKeyPart[] = [];
    let pos = 0;
    while (pos < bytes.length) {
        const typecode = bytes[pos++];
        if (typecode === Typecode.ByteString || typecode === Typecode.UnicodeString) {
            // Uint8Array or string
            const newBytes: number[] = [];
            while (pos < bytes.length) {
                const byte = bytes[pos++];
                if (byte === 0 && bytes[pos] === 0xff) {
                    pos++;
                } else if (byte === 0) {
                    break;
                }
                newBytes.push(byte);
            }
            rt.push(typecode === Typecode.UnicodeString ? decoder.decode(new Uint8Array(newBytes)) : new Uint8Array(newBytes));
        } else if (typecode >= Typecode.IntegerArbitraryByteNegative && typecode <= Typecode.IntegerArbitraryBytePositive) {
            const neg = typecode < Typecode.IntegerZero;
            const numBytes = BigInt((typecode === Typecode.IntegerArbitraryBytePositive || typecode === Typecode.IntegerArbitraryByteNegative) ? (neg ? (0xff - bytes[pos++]) : bytes[pos++]) : Math.abs(typecode - Typecode.IntegerZero));
            let val = 0n;
            for (let i = 0n; i < numBytes; i++) {
                let byte = bytes[pos++];
                if (neg) byte = 0xff - byte;
                val += (BigInt(byte) << ((numBytes - i - 1n) * 8n));
            }
            rt.push(neg ? -val : val);
        } else if (typecode === 33) {
            // number
            // [ 192, 105, 0, 0, 0, 0, 0, 0] => 200
            // [ 192,  94, 221,  47, 26, 159, 190, 119] => 123.456
            // [ 63, 141,  63, 255, 255, 255, 255, 255 ] => -300 // TODO negative numbers not working
            const sub = new Uint8Array(bytes.subarray(pos, pos + 8));
            const num = -new DataView(sub.buffer).getFloat64(0, false); 
            pos += 8;
            rt.push(num);
        } else if (typecode === Typecode.False) {
            // boolean false
            rt.push(false);
        } else if (typecode === Typecode.True) {
            // boolean true
            rt.push(true);
        }  else {
            throw new Error(`Unsupported typecode: ${typecode} in key: [${bytes.join(', ')}] after ${rt.join(', ')}`);
        }
    }
    checkEnd(bytes, pos);
    return rt;
}

//

const decoder = new TextDecoder();

const enum Typecode {
    ByteString = 0x01,
    UnicodeString = 0x02,
    IntegerArbitraryByteNegative = 0x0b,
    IntegerEightByteNegative = 0x0c,
    IntegerSevenByteNegative = 0x0d,
    IntegerSixByteNegative = 0x0e,
    IntegerFiveByteNegative = 0x0f,
    IntegerFourByteNegative = 0x10,
    IntegerThreeByteNegative = 0x11,
    IntegerTwoByteNegative = 0x12,
    IntegerOneByteNegative = 0x13,
    IntegerZero = 0x14,
    IntegerOneBytePositive = 0x15,
    IntegerTwoBytePositive = 0x16,
    IntegerThreeBytePositive = 0x17,
    IntegerFourBytePositive = 0x18,
    IntegerFiveBytePositive = 0x19,
    IntegerSixBytePositive = 0x1a,
    IntegerSevenBytePositive = 0x1b,
    IntegerEightBytePositive = 0x1c,
    IntegerArbitraryBytePositive = 0x1d,
    False = 0x26,
    True = 0x27,
}

function encodeZeroWithZeroFF(bytes: Uint8Array): Uint8Array {
    const index = bytes.indexOf(0);
    return index < 0 ? bytes : new Uint8Array([...bytes].flatMap(v => v === 0 ? [ 0, 0xff ] : [v]));
}
