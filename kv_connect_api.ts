import { encodeBinary as encodeAtomicWrite } from './proto/messages/datapath/AtomicWrite.ts';
import { encodeBinary as encodeSnapshotRead } from './proto/messages/datapath/SnapshotRead.ts';
import { decodeBinary as decodeSnapshotReadOutput } from './proto/messages/datapath/SnapshotReadOutput.ts';
import { decodeBinary as decodeAtomicWriteOutput } from './proto/messages/datapath/AtomicWriteOutput.ts';
import { AtomicWrite, AtomicWriteOutput, SnapshotRead, SnapshotReadOutput } from './proto/messages/datapath/index.ts';
import { isRecord } from './check.ts';

// https://github.com/denoland/deno/tree/main/ext/kv#kv-connect
// https://github.com/denoland/deno/blob/main/cli/schemas/kv-metadata-exchange-response.v1.json
// https://github.com/denoland/deno/blob/main/ext/kv/proto/datapath.proto

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
