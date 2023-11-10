import { decodeHex, encodeHex, equalBytes } from './bytes.ts';
import { DatabaseMetadata, EndpointInfo, fetchAtomicWrite, fetchDatabaseMetadata, fetchSnapshotRead } from './kv_connect_api.ts';
import { packKey, unpackKey } from './kv_key.ts';
import { AtomicCheck, KvCommitError, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListOptions, KvListSelector, KvMutation, KvService, KvU64 } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { BaseKv, CursorHolder, DecodeV8, EncodeV8, Enqueue, packCursor, packKvValue, readValue, unpackCursor } from './kv_util.ts';
import { encodeJson as encodeJsonAtomicWrite } from './proto/messages/datapath/AtomicWrite.ts';
import { encodeJson as encodeJsonSnapshotRead } from './proto/messages/datapath/SnapshotRead.ts';
import { AtomicWrite, AtomicWriteOutput, Enqueue as EnqueueMessage, KvCheck, KvMutation as KvMutationMessage, ReadRange, SnapshotRead, SnapshotReadOutput } from './proto/messages/datapath/index.ts';
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from './v8.ts';
export { UnknownV8 } from './v8.ts';
export * as SnapshotReadProto from './proto/messages/datapath/SnapshotRead.ts';
export * as SnapshotReadOutputProto from './proto/messages/datapath/SnapshotReadOutput.ts';
export * as AtomicWriteProto from './proto/messages/datapath/AtomicWrite.ts';
export * as AtomicWriteOutputProto from './proto/messages/datapath/AtomicWriteOutput.ts';

type Fetcher = typeof fetch;

export interface RemoteServiceOptions {
    /** Access token used to authenticate to the remote service */
    readonly accessToken: string;

    /** Wrap unsupported V8 payloads to instances of UnknownV8 instead of failing.
     * 
     * Only applicable when using the default serializer. */
    readonly wrapUnknownValues?: boolean;

    /** Enable some console logging */
    readonly debug?: boolean;

    /** Custom serializer to use when serializing v8-encoded KV values.
     * 
     * When you are running on Node 18+, pass the 'serialize' function in Node's 'v8' module. */
    readonly encodeV8?: EncodeV8;

    /** Custom deserializer to use when deserializing v8-encoded KV values.
     * 
     * When you are running on Node 18+, pass the 'deserialize' function in Node's 'v8' module. */
    readonly decodeV8?: DecodeV8;

    /** Custom fetcher to use for the underlying http calls.
     * 
     * Defaults to global 'fetch'`
     */
    readonly fetcher?: Fetcher;
}

/**
 * Creates a new KvService instance that can be used to open a remote KV database.
 */
export function makeRemoteService(opts: RemoteServiceOptions): KvService {
    return {
        openKv: async (url) => await RemoteKv.of(url, opts),
        newKvU64: value => new _KvU64(value),
        isKvU64: (obj: unknown): obj is KvU64 => obj instanceof _KvU64,
    }
}

/**
 * Creates a new KvService instance that can be used to access Deno's native implementation (only works in the Deno runtime!)
 * 
 * Requires the --unstable flag to `deno run` and any applicable --allow-read/allow-write/allow-net flags
 */
export function makeNativeService(): KvService {
    if ('Deno' in globalThis) {
        // deno-lint-ignore no-explicit-any
        const { openKv, KvU64 } = (globalThis as any).Deno;
        if (typeof openKv === 'function' && typeof KvU64 === 'function') {
            return {
                // deno-lint-ignore no-explicit-any
                openKv: openKv as any,
                newKvU64: value => new KvU64(value),
                isKvU64: (obj: unknown): obj is KvU64 => obj instanceof KvU64,
            }
        }
    }
    throw new Error(`Global 'Deno.openKv' or 'Deno.KvU64' not found`);
}

//

const emptyVersionstamp = new Uint8Array(10);

function computeReadRangeForKey(packedKey: Uint8Array): ReadRange {
    return {
        start: packedKey,
        end: new Uint8Array([ 0xff ]),
        limit: 1,
        reverse: false,
    }
}

function computeKvCheckMessage({ key, versionstamp }: AtomicCheck): KvCheck {
    return {
        key: packKey(key),
        versionstamp: (versionstamp === null || versionstamp === undefined) ? emptyVersionstamp : decodeHex(versionstamp),
    }
}

function computeKvMutationMessage(mut: KvMutation, encodeV8: EncodeV8): KvMutationMessage {
    const { key, type } = mut;
    return {
        key: packKey(key),
        mutationType: type === 'delete' ? 'M_CLEAR' : type === 'max' ? 'M_MAX' : type === 'min' ? 'M_MIN' : type == 'set' ? 'M_SET' : type === 'sum' ? 'M_SUM' : 'M_UNSPECIFIED',
        value: mut.type === 'delete' ? undefined : packKvValue(mut.value, encodeV8),
        expireAtMs: mut.type === 'set' && typeof mut.expireIn === 'number' ? (Date.now() + mut.expireIn).toString() : '0',
    }
}

function computeEnqueueMessage(value: unknown, encodeV8: EncodeV8, { delay = 0, keysIfUndelivered = [] }: { delay?: number, keysIfUndelivered?: KvKey[] } = {}): EnqueueMessage {
    return {
        backoffSchedule: [ 100, 200, 400, 800 ],
        deadlineMs: `${Date.now() + delay}`,
        kvKeysIfUndelivered: keysIfUndelivered.map(packKey),
        payload: encodeV8(value),
    }
}

async function fetchNewDatabaseMetadata(url: string, accessToken: string, debug: boolean, fetcher: Fetcher): Promise<DatabaseMetadata> {
    if (debug) console.log('Fetching database metadata...');
    const metadata = await fetchDatabaseMetadata(url, accessToken, fetcher);
    const { version, endpoints, token } = metadata;
    if (version !== 1) throw new Error(`Unsupported version: ${version}`);
    if (typeof token !== 'string' || token === '') throw new Error(`Unsupported token: ${token}`);
    if (endpoints.length === 0) throw new Error(`No endpoints`);
    if (debug) endpoints.forEach(({ url, consistency }) => console.log(`${url} (${consistency})`));
    const expiresMillis = computeExpiresInMillis(metadata);
    if (debug) console.log(`Expires in ${Math.round((expiresMillis / 1000 / 60))} minutes`); // expect 60 minutes
    return metadata;
}

function computeExpiresInMillis({ expiresAt }: DatabaseMetadata): number {
    const expiresTime = new Date(expiresAt).getTime();
    return expiresTime - Date.now();
}

function isValidHttpUrl(url: string): boolean {
    try {
        const { protocol } = new URL(url);
        return protocol === 'http:' || protocol === 'https:';
    } catch {
        return false;
    }
}

function snapshotReadToString(req: SnapshotRead): string {
    return JSON.stringify(encodeJsonSnapshotRead(req));
}

function atomicWriteToString(req: AtomicWrite): string {
    return JSON.stringify(encodeJsonAtomicWrite(req));
}

//

class RemoteKv extends BaseKv {

    private readonly url: string;
    private readonly accessToken: string;
    private readonly encodeV8: EncodeV8;
    private readonly decodeV8: DecodeV8;
    private readonly fetcher: Fetcher;

    private metadata: DatabaseMetadata;

    private constructor(url: string, accessToken: string, debug: boolean, encodeV8: EncodeV8, decodeV8: DecodeV8, fetcher: Fetcher, metadata: DatabaseMetadata) {
        super({ debug });
        this.url = url;
        this.accessToken = accessToken;
        this.encodeV8 = encodeV8;
        this.decodeV8 = decodeV8;
        this.fetcher = fetcher;
        this.metadata = metadata;
    }

    static async of(url: string | undefined, opts: RemoteServiceOptions) {
        const { accessToken, wrapUnknownValues = false, debug = false, fetcher = fetch } = opts;
        if (url === undefined || !isValidHttpUrl(url)) throw new Error(`'path' must be an http(s) url`);
        const metadata = await fetchNewDatabaseMetadata(url, accessToken, debug, fetcher);
        
        const encodeV8: EncodeV8 = opts.encodeV8 ?? _encodeV8;
        const decodeV8: DecodeV8 = opts.decodeV8 ?? (v => _decodeV8(v, { wrapUnknownValues }));

        return new RemoteKv(url, accessToken, debug, encodeV8, decodeV8, fetcher, metadata);
    }

    protected async get_<T = unknown>(key: KvKey, consistency?: KvConsistencyLevel): Promise<KvEntryMaybe<T>> {
        const { decodeV8 } = this;
        const packedKey = packKey(key);
        const req: SnapshotRead = {
            ranges: [ computeReadRangeForKey(packedKey) ]
        }
        const res = await this.snapshotRead(req, consistency);
        for (const range of res.ranges) {
            for (const item of range.values) {
                if (equalBytes(item.key, packedKey)) return { key, value: readValue(item.value, item.encoding, decodeV8) as T, versionstamp: encodeHex(item.versionstamp) };
            }
        }
        return { key, value: null, versionstamp: null };
    }

    protected async getMany_(keys: readonly KvKey[], consistency?: KvConsistencyLevel): Promise<KvEntryMaybe<unknown>[]> {
        const { decodeV8 } = this;
        const packedKeys = keys.map(packKey);
        const packedKeysHex = packedKeys.map(encodeHex);
        const req: SnapshotRead = {
            ranges: packedKeys.map(computeReadRangeForKey)
        }
        const res = await this.snapshotRead(req, consistency);
        const rowMap = new Map<string, { key: KvKey, value: unknown, versionstamp: string }>();
        for (const range of res.ranges) {
            for (const { key, value, encoding, versionstamp } of range.values) {
                rowMap.set(encodeHex(key), { key: unpackKey(key), value: readValue(value, encoding, decodeV8), versionstamp: encodeHex(versionstamp) })
            }
        }
        return keys.map((key, i) => {
            const row = rowMap.get(packedKeysHex[i]);
            return row ? { key, value: row.value, versionstamp: row.versionstamp } : { key, value: null, versionstamp: null };
        });
    }

    listenQueue_(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        throw new Error(`'listenQueue' is not possible over KV Connect`);
    }

    protected async commit(checks: AtomicCheck[], mutations: KvMutation[], enqueues: Enqueue[]): Promise<KvCommitResult | KvCommitError> {
        const write: AtomicWrite = {
            kvChecks: checks.map(computeKvCheckMessage),
            kvMutations: mutations.map(v => computeKvMutationMessage(v, this.encodeV8)),
            enqueues: enqueues.map(({ value, opts }) => computeEnqueueMessage(value, this.encodeV8, opts)),
        };
        const { status, primaryIfWriteDisabled, versionstamp } = await this.atomicWrite(write);
        if (status === 'AW_CHECK_FAILURE') return { ok: false };
        if (status !== 'AW_SUCCESS') throw new Error(`commit failed with status: ${status}${ primaryIfWriteDisabled.length > 0 ? ` primaryIfWriteDisabled=${primaryIfWriteDisabled}` : ''}`);
        return { ok: true, versionstamp: encodeHex(versionstamp) };
    }

    protected close_(): void {
        // no persistent resources yet
    }

    protected async * listStream<T>(outCursor: CursorHolder, selector: KvListSelector, { batchSize, consistency, cursor: cursorOpt, limit, reverse = false }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
        const { decodeV8 } = this;
        let yielded = 0;
        if (typeof limit === 'number' && yielded >= limit) return;
        const cursor = typeof cursorOpt === 'string' ? unpackCursor(cursorOpt) : undefined;
        let lastYieldedKeyBytes = cursor?.lastYieldedKeyBytes;
        let pass = 0;
        const prefixBytes = 'prefix' in selector ? packKey(selector.prefix) : undefined;
        while (true) {
            pass++;
            // console.log({ pass });
            const req: SnapshotRead = { ranges: [] };
            let start: Uint8Array | undefined;
            let end: Uint8Array | undefined;
            if ('prefix' in selector) {
                start = 'start' in selector ? packKey(selector.start) : prefixBytes;
                end = 'end' in selector ? packKey(selector.end) : new Uint8Array([ ...prefixBytes!, 0xff ]);
            } else {
                start = packKey(selector.start);
                end = packKey(selector.end);
            }
            if (reverse) {
                end = lastYieldedKeyBytes ?? end;
            } else {
                start = lastYieldedKeyBytes ?? start;
            }
           
            if (start === undefined || end === undefined) throw new Error();
            const batchLimit = Math.min(batchSize ?? 100, 500, limit ?? Number.MAX_SAFE_INTEGER) + (lastYieldedKeyBytes ? 1 : 0);
            req.ranges.push({ start, end, limit: batchLimit, reverse });

            const res = await this.snapshotRead(req, consistency);
            let entries = 0;
            for (const range of res.ranges) {
                for (const entry of range.values) {
                    if (entries++ === 0 && (lastYieldedKeyBytes && equalBytes(lastYieldedKeyBytes, entry.key) || prefixBytes && equalBytes(prefixBytes, entry.key))) continue;
                    const key = unpackKey(entry.key);
                    const value = readValue(entry.value, entry.encoding, decodeV8) as T;
                    const versionstamp = encodeHex(entry.versionstamp);
                    lastYieldedKeyBytes = entry.key;
                    outCursor.set(packCursor({ lastYieldedKeyBytes })); // cursor needs to be set before yield
                    yield { key, value, versionstamp };
                    yielded++;
                    // console.log({ yielded, entries, limit });
                    if (typeof limit === 'number' && yielded >= limit) return;
                }
            }
            if (entries < batchLimit) return;
        }
    }

    //

    private async locateEndpoint(consistency: KvConsistencyLevel): Promise<EndpointInfo> {
        const { url, accessToken, debug, fetcher } = this;
        if (computeExpiresInMillis(this.metadata) < 1000 * 60 * 5) {
            this.metadata = await fetchNewDatabaseMetadata(url, accessToken, debug, fetcher);
        }
        const { metadata } = this;
        const firstStrong = metadata.endpoints.filter(v => v.consistency === 'strong')[0];
        const firstNonStrong = metadata.endpoints.filter(v => v.consistency !== 'strong')[0];
        const endpoint = consistency === 'strong' ? firstStrong : (firstNonStrong ?? firstStrong);
        if (endpoint === undefined) throw new Error(`Unable to find endpoint for: ${consistency}`);
        return endpoint;
    }

    private async snapshotRead(req: SnapshotRead, consistency: KvConsistencyLevel = 'strong'): Promise<SnapshotReadOutput> {
        const { metadata, debug, fetcher } = this;
        const endpoint = await this.locateEndpoint(consistency);
        const snapshotReadUrl = new URL('/snapshot_read', endpoint.url).toString();
        const accessToken = metadata.token;
        if (debug) console.log(`fetchSnapshotRead: ${snapshotReadToString(req)}`);
        return await fetchSnapshotRead(snapshotReadUrl, accessToken, metadata.databaseId, req, fetcher);
    }

    private async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
        const { metadata, debug, fetcher } = this;
        const endpoint = await this.locateEndpoint('strong');
        const atomicWriteUrl = new URL('/atomic_write', endpoint.url).toString();
        const accessToken = metadata.token;
        if (debug) console.log(`fetchAtomicWrite: ${atomicWriteToString(req)}`);
        return await fetchAtomicWrite(atomicWriteUrl, accessToken, metadata.databaseId, req, fetcher);
    }
    
}
