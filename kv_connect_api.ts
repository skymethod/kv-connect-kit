import { encodeBinary as encodeAtomicWrite } from './gen/messages/datapath/AtomicWrite.ts';
import { encodeBinary as encodeSnapshotRead } from './gen/messages/datapath/SnapshotRead.ts';
import { decodeBinary as decodeSnapshotReadOutput } from './gen/messages/datapath/SnapshotReadOutput.ts';
import { decodeBinary as decodeAtomicWriteOutput } from './gen/messages/datapath/AtomicWriteOutput.ts';
import { AtomicWrite, AtomicWriteOutput, SnapshotRead, SnapshotReadOutput } from './gen/messages/datapath/index.ts';
import { KvKey, KvKeyPart } from './kv_types.ts';
import { checkEnd } from './bytes.ts';

export async function fetchDatabaseMetadata(url: string, accessToken: string): Promise<DatabaseMetadata> {
    const res = await fetch(url, { method: 'POST', headers: { authorization: `Bearer ${accessToken}` } });
    if (res.status !== 200) throw new Error(`Unexpected response status: ${res.status} ${await res.text()}`);
    const contentType = res.headers.get('content-type') ?? undefined;
    if (contentType !== 'application/json') throw new Error(`Unexpected response content-type: ${contentType} ${await res.text()}`);
    const metadata = await res.json();
    if (!isDatabaseMetadata(metadata)) throw new Error(`Bad DatabaseMetadata: ${JSON.stringify(metadata)}`);
    return metadata;
}

export async function fetchSnapshotRead(url: string, accessToken: string, databaseId: string, req: SnapshotRead): Promise<SnapshotReadOutput> {
    return decodeSnapshotReadOutput(await fetchProtobuf(url, accessToken, databaseId, encodeSnapshotRead(req)));
}

export async function fetchAtomicWrite(url: string, accessToken: string, databaseId: string, write: AtomicWrite): Promise<AtomicWriteOutput> {
    return decodeAtomicWriteOutput(await fetchProtobuf(url, accessToken, databaseId,  encodeAtomicWrite(write)));
}

export function packKey(kvKey: KvKey): Uint8Array {
    return new Uint8Array(kvKey.flatMap(v => [...packKeyPart(v)]));
}

export function packKeyPart(kvKeyPart: KvKeyPart): Uint8Array {
    if (typeof kvKeyPart === 'string') return new Uint8Array([ 2, ...new TextEncoder().encode(kvKeyPart), 0]);
    if (typeof kvKeyPart === 'number') {
        const sub = new Uint8Array(8);
        new DataView(sub.buffer).setFloat64(0, -kvKeyPart, false);
        return new Uint8Array([ 33, ...sub]);
    }
    if (kvKeyPart instanceof Uint8Array) return new Uint8Array([ 1, ...kvKeyPart, 0 ]);
    if (kvKeyPart === false) return new Uint8Array([ 38 ]);
    if (kvKeyPart === true) return new Uint8Array([ 39 ]);
    if (kvKeyPart === 0n) return new Uint8Array([ 20 ]);
    if (typeof kvKeyPart === 'bigint' && kvKeyPart < 0) return new Uint8Array([ 19, 0xff - Number(-kvKeyPart) ]);
    if (typeof kvKeyPart === 'bigint' && kvKeyPart > 0) return new Uint8Array([ 21, Number(kvKeyPart) ]);
    throw new Error(`implement keyPart: ${JSON.stringify(kvKeyPart)}`);
}

export function unpackKey(bytes: Uint8Array): KvKey {
    const rt: KvKeyPart[] = [];
    let pos = 0;
    while (pos < bytes.length) {
        const tag = bytes[pos++];
        if (tag === 1) {
            // Uint8Array
            const start = pos;
            while (bytes[pos] !== 0) pos++;
            rt.push(bytes.subarray(start, pos));
            pos++;
        } else if (tag === 2) {
            // string
            const start = pos;
            while (bytes[pos] !== 0) pos++;
            rt.push(new TextDecoder().decode(bytes.subarray(start, pos)));
            pos++;
        } else if (tag === 19) {
            // bigint < 0
            const val = 0xff - bytes[pos++];
            rt.push(-BigInt(val));
        } else if (tag === 20) {
            // bigint 0
            rt.push(0n);
        } else if (tag === 21) {
            // bigint > 0
            const val = bytes[pos++];
            rt.push(BigInt(val));
        } else if (tag === 33) {
            // number
            // [ 192, 105, 0, 0, 0, 0, 0, 0] => 200
            // [ 192,  94, 221,  47, 26, 159, 190, 119] => 123.456
            // [ 63, 141,  63, 255, 255, 255, 255, 255 ] => -300 // TODO negative numbers not working
            const sub = new Uint8Array(bytes.subarray(pos, pos + 8));
            const num = -new DataView(sub.buffer).getFloat64(0, false); 
            pos += 8;
            rt.push(num);
        } else if (tag === 38) {
            // boolean false
            rt.push(false);
        }else if (tag === 39) {
            // boolean true
            rt.push(true);
        }  else {
            throw new Error(`Unsupported tag: ${tag} in key: [${bytes.join(', ')}] after ${rt.join(', ')}`);
        }
    }
    checkEnd(bytes, pos);
    return rt;
}

//

async function fetchProtobuf(url: string, accessToken: string, databaseId: string, body: Uint8Array): Promise<Uint8Array> {
    const res = await fetch(url, { method: 'POST', body, headers: { 'x-transaction-domain-id': databaseId , authorization: `Bearer ${accessToken}` } });
    if (res.status !== 200) throw new Error(`Unexpected response status: ${res.status} ${await res.text()}`);
    const contentType = res.headers.get('content-type') ?? undefined;
    if (contentType !== 'application/x-protobuf') throw new Error(`Unexpected response content-type: ${contentType} ${await res.text()}`);
    return new Uint8Array(await res.arrayBuffer());
}

function isEndpointInfo(obj: unknown): obj is EndpointInfo {
    if (!isRecord(obj)) return false;
    const { url, consistency, ...rest } = obj;
    return typeof url === 'string' && typeof consistency === 'string' && Object.keys(rest).length === 0;
}

function isDatabaseMetadata(obj: unknown): obj is DatabaseMetadata {
    if (!isRecord(obj)) return false;
    const { version, databaseId, endpoints, token, expiresAt, ...rest } = obj;
    return typeof version === 'number' && typeof databaseId === 'string' && Array.isArray(endpoints) && endpoints.every(isEndpointInfo) && typeof token === 'string' && typeof expiresAt === 'string' && Object.keys(rest).length === 0;
}

function isRecord(obj: unknown): obj is Record<string, unknown> {
    return typeof obj === 'object' && obj !== null && !Array.isArray(obj) && obj.constructor === Object;
}

//

export interface DatabaseMetadata {
    readonly version: number; // 1
    readonly databaseId: string; // uuid v4
    readonly endpoints: EndpointInfo[];
    readonly token: string;
    readonly expiresAt: string; // 2023-09-17T16:39:10Z
}

export interface EndpointInfo {
    readonly url: string; // https://us-east4.txnproxy.deno-gcp.net
    readonly consistency: string; // strong
}
