// https://chromium.googlesource.com/v8/v8/+/refs/heads/main/src/objects/value-serializer.cc

export function decodeV8(bytes: Uint8Array): string {
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
        return rt;
    } else {
        throw new Error(`decode error: Unsupported tag ${tag}`);
    }
}

export function encodeV8(value: unknown): Uint8Array {
    if (typeof value === 'string') {
        const byteChars: number[] = [];
        for (const char of value) {
            const cp = char.codePointAt(0)!;
            if (cp < 0 || cp > 0xff) throw new Error(`encode error: Unsupported codepoint: ${cp}`);
            byteChars.push(cp);
        }
        return new Uint8Array([ SerializationTag.kVersion, kLatestVersion, SerializationTag.kOneByteString, byteChars.length, ...byteChars ]);
    }
    throw new Error(`decode error: Unsupported value ${JSON.stringify(value)}`);
}

//

const kLatestVersion = 15;

enum SerializationTag {
    kVersion = 0xff,
    kOneByteString = '"'.charCodeAt(0),
}
