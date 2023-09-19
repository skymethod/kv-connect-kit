import { checkEnd } from './bytes.ts';
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
    if (kvKeyPart === 0n) return new Uint8Array([ Typecode.IntegerZero ]);
    if (typeof kvKeyPart === 'bigint' && kvKeyPart < 0) return new Uint8Array([ 19, 0xff - Number(-kvKeyPart) ]);
    if (typeof kvKeyPart === 'bigint' && kvKeyPart > 0) {
        for (let numBytes = 1n; numBytes <= 9n; numBytes++) {
            const maxValue = Math.pow(2, 8 * Number(numBytes)) - 1;
            if (kvKeyPart <= maxValue) {
                const bytes: number[] = [];
                if (numBytes === 9n) bytes.push(9);
                for (let i = 0n; i < numBytes; i++) {
                    const mask = 0xffn << 8n * (numBytes - i - 1n);
                    const byteN = (kvKeyPart & mask) >> (8n * (numBytes - i - 1n));
                    bytes.push(Number(byteN));
                }
                return new Uint8Array([ Typecode.IntegerOneBytePositive + Number(numBytes) - 1, ...bytes ]);
            }
        }
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
        } else if (typecode === 19) {
            // bigint < 0
            const val = 0xff - bytes[pos++];
            rt.push(-BigInt(val));
        } else if (typecode === Typecode.IntegerZero) {
            // bigint 0
            rt.push(0n);
        } else if (typecode >= Typecode.IntegerOneBytePositive && typecode <= Typecode.IntegerArbitraryBytePositive) {
            const numBytes = BigInt(typecode === Typecode.IntegerArbitraryBytePositive ? bytes[pos++] : typecode - Typecode.IntegerOneBytePositive + 1);
            let val = 0n;
            for (let i = 0n; i < numBytes; i++) {
                const byte = BigInt(bytes[pos++]);
                val += (byte << ((numBytes - i - 1n) * 8n))
            }
            rt.push(val);
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
