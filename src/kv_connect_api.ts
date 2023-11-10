import { encodeBinary as encodeAtomicWrite } from './proto/messages/com/deno/kv/datapath/AtomicWrite.ts';
import { encodeBinary as encodeSnapshotRead } from './proto/messages/com/deno/kv/datapath/SnapshotRead.ts';
import { decodeBinary as decodeSnapshotReadOutput } from './proto/messages/com/deno/kv/datapath/SnapshotReadOutput.ts';
import { decodeBinary as decodeAtomicWriteOutput } from './proto/messages/com/deno/kv/datapath/AtomicWriteOutput.ts';
import { AtomicWrite, AtomicWriteOutput, SnapshotRead, SnapshotReadOutput } from './proto/messages/com/deno/kv/datapath/index.ts';
import { isDateTime, isRecord } from './check.ts';

// VERSION 1
// https://github.com/denoland/deno/tree/092555c611ebab87ad570b4dcb73d54288dccdd9/ext/kv#kv-connect
// https://github.com/denoland/deno/blob/092555c611ebab87ad570b4dcb73d54288dccdd9/cli/schemas/kv-metadata-exchange-response.v1.json
// https://github.com/denoland/deno/blob/092555c611ebab87ad570b4dcb73d54288dccdd9/ext/kv/proto/datapath.proto

// VERSION 2
// https://github.com/denoland/denokv/blob/main/proto/kv-connect.md
// https://github.com/denoland/denokv/blob/main/proto/schema/datapath.proto
// https://github.com/denoland/denokv/blob/main/proto/schema/kv-metadata-exchange-response.v2.json

export type KvConnectProtocolVersion = 1 | 2;

export async function fetchDatabaseMetadata(url: string, accessToken: string, fetcher: typeof fetch = fetch, supportedVersions: KvConnectProtocolVersion[]): Promise<{ metadata: DatabaseMetadata, responseUrl: string }> {
    const res = await fetcher(url, { method: 'POST', headers: { authorization: `Bearer ${accessToken}`, 'content-type': 'application/json' }, body: JSON.stringify({ supportedVersions }) });
    if (res.status !== 200) throw new Error(`Unexpected response status: ${res.status} ${await res.text()}`);
    const contentType = res.headers.get('content-type') ?? undefined;
    if (contentType !== 'application/json') throw new Error(`Unexpected response content-type: ${contentType} ${await res.text()}`);
    const metadata = await res.json();
    if (!isDatabaseMetadata(metadata)) throw new Error(`Bad DatabaseMetadata: ${JSON.stringify(metadata)}`);
    return { metadata, responseUrl: res.url };
}

export async function fetchSnapshotRead(url: string, accessToken: string, databaseId: string, req: SnapshotRead, fetcher: typeof fetch = fetch, version: KvConnectProtocolVersion): Promise<SnapshotReadOutput> {
    return decodeSnapshotReadOutput(await fetchProtobuf(url, accessToken, databaseId, encodeSnapshotRead(req), fetcher, version));
}

export async function fetchAtomicWrite(url: string, accessToken: string, databaseId: string, write: AtomicWrite, fetcher: typeof fetch = fetch, version: KvConnectProtocolVersion): Promise<AtomicWriteOutput> {
    return decodeAtomicWriteOutput(await fetchProtobuf(url, accessToken, databaseId,  encodeAtomicWrite(write), fetcher, version));
}

//

async function fetchProtobuf(url: string, accessToken: string, databaseId: string, body: Uint8Array, fetcher: typeof fetch = fetch, version: KvConnectProtocolVersion): Promise<Uint8Array> {
    const headers = { authorization: `Bearer ${accessToken}`, ...(version === 1 ? { 'x-transaction-domain-id': databaseId } : { 'x-denokv-version': '2', 'x-denokv-database-id': databaseId })  };
    const res = await fetcher(url, { method: 'POST', body, headers });
    if (res.status !== 200) throw new Error(`Unexpected response status: ${res.status} ${await res.text()}`);
    const contentType = res.headers.get('content-type') ?? undefined;
    const expectedContentType = new URL(url).hostname === 'localhost' ? 'application/protobuf' : 'application/x-protobuf'; // TODO until fixed upstream
    if (contentType !== expectedContentType) throw new Error(`Unexpected response content-type: ${contentType} ${await res.text()}`);
    return new Uint8Array(await res.arrayBuffer());
}

function isValidEndpointUrl(url: string): boolean {
    try {
        const { protocol, pathname, search, hash } = new URL(url, 'https://example.com');
        return /^https?:$/.test(protocol) && (pathname === '/' || !pathname.endsWith('/') && search === '' && hash === ''); // must not end in "/" (except no path), no qp/hash implied since the spec simply appends "/action"
    } catch {
        return false;
    }
}

function isEndpointInfo(obj: unknown): obj is EndpointInfo {
    if (!isRecord(obj)) return false;
    const { url, consistency, ...rest } = obj;
    return typeof url === 'string' && isValidEndpointUrl(url)
        && (consistency === 'strong' || consistency === 'eventual')
        && Object.keys(rest).length === 0;
}

function isDatabaseMetadata(obj: unknown): obj is DatabaseMetadata {
    if (!isRecord(obj)) return false;
    const { version, databaseId, endpoints, token, expiresAt, ...rest } = obj;
    return (version === 1 || version === 2) 
        && typeof databaseId === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(databaseId)
        && Array.isArray(endpoints) && endpoints.every(isEndpointInfo)
        && typeof token === 'string'
        && typeof expiresAt === 'string' && isDateTime(expiresAt)
        && Object.keys(rest).length === 0;
}

//

export interface DatabaseMetadata {
    readonly version: 1 | 2;
    readonly databaseId: string; // uuid
    readonly endpoints: EndpointInfo[];
    readonly token: string;
    readonly expiresAt: string; // 2023-09-17T16:39:10Z
}

export interface EndpointInfo {
    /** A fully qualified URL, or a URL relative to the metadata URL. The path of the URL must not end with a slash. 
     * e.g. https://data.example.com/v1, /v1, ./v1 */
    readonly url: string; // https://us-east4.txnproxy.deno-gcp.net

    readonly consistency: 'strong' | 'eventual';
}
