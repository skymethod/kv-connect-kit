import { checkEnd } from './bytes.ts';
import { KvKey, KvKeyPart } from './kv_types.ts';

// https://github.com/apple/foundationdb/blob/main/design/tuple.md
// limited to Uint8Array | string | number | bigint | boolean

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
    if (typeof kvKeyPart === 'bigint' && kvKeyPart > 0 && kvKeyPart <= 0xff) return new Uint8Array([ Typecode.IntegerOneBytePositive, Number(kvKeyPart) ]);
    if (typeof kvKeyPart === 'bigint' && kvKeyPart > 0 && kvKeyPart <= 0xffff) {
        const n = Number(kvKeyPart);
        const byte1 = (n & 0xff00) >> 8;
        const byte2 = n & 0x00ff;
        return new Uint8Array([ Typecode.IntegerTwoBytePositive, byte1, byte2 ]);
    }
    if (typeof kvKeyPart === 'bigint' && kvKeyPart > 0 && kvKeyPart <= 0xffffff) {
        const n = Number(kvKeyPart);
        const byte1 = (n & 0xff0000) >> 16;
        const byte2 = (n & 0x00ff00) >> 8;
        const byte3 = n & 0x0000ff;
        return new Uint8Array([ Typecode.IntegerThreeBytePositive, byte1, byte2, byte3 ]);
    }
    if (typeof kvKeyPart === 'bigint' && kvKeyPart > 0 && kvKeyPart <= 0xffffffff) {
        const n = Number(kvKeyPart);
        const byte1 = (n & 0xff000000) >> 24;
        const byte2 = (n & 0x00ff0000) >> 16;
        const byte3 = (n & 0x0000ff00) >> 8;
        const byte4 = n & 0x000000ff;
        return new Uint8Array([ Typecode.IntegerFourBytePositive, byte1, byte2, byte3, byte4 ]);
    }
    throw new Error(`implement keyPart: ${JSON.stringify(kvKeyPart)}`);
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
        } else if (typecode === Typecode.IntegerOneBytePositive) {
            // bigint > 0 <= 0xff
            const val = bytes[pos++];
            rt.push(BigInt(val));
        } else if (typecode === Typecode.IntegerTwoBytePositive) {
            // bigint > 0xff <= 0xffff
            const byte1 = bytes[pos++];
            const byte2 = bytes[pos++];
            rt.push(BigInt((byte1 << 8) + byte2));
        } else if (typecode === Typecode.IntegerThreeBytePositive) {
            // bigint > 0xffff <= 0xffffff
            const byte1 = bytes[pos++];
            const byte2 = bytes[pos++];
            const byte3 = bytes[pos++];
            const n = (byte1 << 16) + (byte2 << 8) + byte3;
            rt.push(BigInt(n));
        } else if (typecode === Typecode.IntegerFourBytePositive) {
            // bigint > 0xffffff <= 0xffffffff
            const byte1 = bytes[pos++];
            const byte2 = bytes[pos++];
            const byte3 = bytes[pos++];
            const byte4 = bytes[pos++];
            const n = (byte1 << 24) + (byte2 << 16) + (byte3 << 8) + byte4;
            rt.push(BigInt(n));
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
    False = 0x26,
    True = 0x27,
}

function encodeZeroWithZeroFF(bytes: Uint8Array): Uint8Array {
    const index = bytes.indexOf(0);
    return index < 0 ? bytes : new Uint8Array([...bytes].flatMap(v => v === 0 ? [ 0, 0xff ] : [v]));
}
