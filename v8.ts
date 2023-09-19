// https://chromium.googlesource.com/v8/v8/+/refs/heads/main/src/objects/value-serializer.cc

import { checkEnd } from './bytes.ts';

export function decodeV8(bytes: Uint8Array, { wrapUnknownValues = false }: { wrapUnknownValues?: boolean } = {}): unknown {
    if (bytes.length === 0) throw new Error(`decode error: empty input`);
    let pos = 0;
    const kVersion = bytes[pos++];
    if (kVersion !== SerializationTag.kVersion) throw new Error(`decode error: Unsupported kVersion ${kVersion}`);
    const version = bytes[pos++];
    if (version !== kLatestVersion) throw new Error(`decode error: Unsupported version ${version}`);
    const tag = bytes[pos++];
    if (tag === SerializationTag.kOneByteString) {
        const len = bytes[pos++];
        const arr = bytes.subarray(pos, pos + len);
        const rt = new TextDecoder().decode(arr);
        pos += len;
        checkEnd(bytes, pos);
        return rt;
    } else if (tag === SerializationTag.kTwoByteString) {
        const len = bytes[pos++];
        const arr = bytes.subarray(pos, pos + len);
        const rt = new TextDecoder('utf-16').decode(arr);
        pos += len;
        checkEnd(bytes, pos);
        return rt;
    } else if (tag === SerializationTag.kNull) {
        checkEnd(bytes, pos);
        return null;
    } else if (tag === SerializationTag.kUndefined) {
        checkEnd(bytes, pos);
        return undefined;
    } else if (wrapUnknownValues) {
        return new UnknownV8(bytes);
    } else {
        throw new Error(`decodeV8 error: Unsupported tag ${tag} ('${String.fromCharCode(tag)}') at ${pos} in [${bytes.join(', ')}]`);
    }
}

export function encodeV8(value: unknown): Uint8Array {
    if (typeof value === 'string') {
        const chars = [...value];
        if (chars.every(isOneByteChar)) {
            const charCodes = chars.map(v => v.charCodeAt(0));
            return new Uint8Array([ SerializationTag.kVersion, kLatestVersion, SerializationTag.kOneByteString, charCodes.length, ...charCodes ]);
        }
        const bytes: number[] = [];
        for (let i = 0; i < value.length; i++) {
            const charCode = value.charCodeAt(i);
            const msb = (charCode & 0xff00) >> 8;
            const lsb = charCode & 0x00ff;
            bytes.push(lsb);
            bytes.push(msb);
        }
        return new Uint8Array([ SerializationTag.kVersion, kLatestVersion, SerializationTag.kTwoByteString, value.length * 2, ...bytes ]);
    } else if (value === null) {
        return new Uint8Array([ SerializationTag.kVersion, kLatestVersion, SerializationTag.kNull ]);
    } else if (value === undefined) {
        return new Uint8Array([ SerializationTag.kVersion, kLatestVersion, SerializationTag.kUndefined ]);
    }
    throw new Error(`encodeV8 error: Unsupported value ${JSON.stringify(value)}`);
}

//

const kLatestVersion = 15;

enum SerializationTag {
    kVersion = 0xff,
    kOneByteString = '"'.charCodeAt(0),
    kTwoByteString = 'c'.charCodeAt(0),
    kNull = '0'.charCodeAt(0),
    kUndefined = '_'.charCodeAt(0),
}

function isOneByteChar(char: string): boolean {
    const cp = char.codePointAt(0)!;
    return cp >= 0 && cp <= 0xff;
}

//

export class UnknownV8 {
    public readonly bytes: Uint8Array;

    constructor(bytes: Uint8Array) {
        this.bytes = bytes;
    }

}
