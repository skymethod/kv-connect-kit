import { decodeHex, encodeHex, equalBytes } from './bytes.ts';
import { encodeJson as encodeJsonAtomicWrite } from './proto/messages/datapath/AtomicWrite.ts';
import { encodeJson as encodeJsonSnapshotRead } from './proto/messages/datapath/SnapshotRead.ts';
import { AtomicWrite, AtomicWriteOutput, KvCheck, ReadRange, SnapshotRead, SnapshotReadOutput, KvMutation as KvMutationMessage, Enqueue, KvValueEncoding, KvValue } from './proto/messages/datapath/index.ts';
import { encode as encodeBase64, decode as decodeBase64 } from './proto/runtime/base64.ts';
import { DatabaseMetadata, EndpointInfo, fetchAtomicWrite, fetchDatabaseMetadata, fetchSnapshotRead } from './kv_connect_api.ts';
import { packKey, unpackKey } from './kv_key.ts';
import { AtomicCheck, AtomicOperation, Kv, KvCommitError, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListIterator, KvListOptions, KvListSelector, KvMutation, KvService, KvU64 } from './kv_types.ts';
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from './v8.ts';
import { isRecord } from './check.ts';
export { UnknownV8 } from './v8.ts';

type EncodeV8 = (value: unknown) => Uint8Array;
type DecodeV8 = (bytes: Uint8Array) => unknown;
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
            }
        }
    }
    throw new Error(`Global 'Deno.openKv' or 'Deno.KvU64' not found`);
}

//

type Cursor = { lastYieldedKeyBytes: Uint8Array }

function packCursor({ lastYieldedKeyBytes }: Cursor): string {
    return encodeBase64(JSON.stringify({ lastYieldedKeyBytes: encodeHex(lastYieldedKeyBytes) }));
}

function unpackCursor(str: string): Cursor {
    try {
        const { lastYieldedKeyBytes } = JSON.parse(new TextDecoder().decode(decodeBase64(str)));
        if (typeof lastYieldedKeyBytes === 'string') return { lastYieldedKeyBytes: decodeHex(lastYieldedKeyBytes) };
    } catch {
        // noop
    }
    throw new Error(`Invalid cursor`);
}

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
        versionstamp: versionstamp === null ? new Uint8Array() : decodeHex(versionstamp),
    }
}

function computeKvMutationMessage(mut: KvMutation, encodeV8: EncodeV8): KvMutationMessage {
    const { key, type } = mut;
    return {
        key: packKey(key),
        mutationType: type === 'delete' ? 'M_CLEAR' : type === 'max' ? 'M_MAX' : type === 'min' ? 'M_MIN' : type == 'set' ? 'M_SET' : type === 'sum' ? 'M_SUM' : 'M_UNSPECIFIED',
        value: mut.type === 'delete' ? undefined : packKvValue(mut.value, encodeV8),
    }
}

function computeEnqueueMessage(value: unknown, encodeV8: EncodeV8, { delay = 0, keysIfUndelivered = [] }: { delay?: number, keysIfUndelivered?: KvKey[] } = {}): Enqueue {
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

function checkKeyNotEmpty(key: KvKey): void {
    if (key.length === 0) throw new Error(`Key cannot be empty`);
}

function checkExpireIn(expireIn: number | undefined): void {
    if (typeof expireIn === 'number') throw new Error(`'expireIn' not supported over KV Connect`); // https://github.com/denoland/deno/issues/20560
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

function unpackKvu(bytes: Uint8Array): _KvU64 {
    if (bytes.length !== 8) throw new Error();
    if (bytes.buffer.byteLength !== 8) bytes = new Uint8Array(bytes);
    const rt = new DataView(bytes.buffer).getBigUint64(0, true);
    return new _KvU64(rt);
}

function packKvu(value: _KvU64): Uint8Array {
    const rt = new Uint8Array(8);
    new DataView(rt.buffer).setBigUint64(0, value.value, true);
    return rt;
}

function readValue(bytes: Uint8Array, encoding: KvValueEncoding, decodeV8: DecodeV8) {
    if (encoding === 'VE_V8') return decodeV8(bytes);
    if (encoding === 'VE_LE64') return unpackKvu(bytes);
    if (encoding === 'VE_BYTES') return bytes;
    throw new Error(`Unsupported encoding: ${encoding} [${[...bytes].join(', ')}]`);
}

function packKvValue(value: unknown, encodeV8: EncodeV8): KvValue {
    if (value instanceof _KvU64) return { encoding: 'VE_LE64', data: packKvu(value) };
    if (value instanceof Uint8Array) return { encoding: 'VE_BYTES', data: value };
    return { encoding: 'VE_V8',  data: encodeV8(value) };
}

//

class RemoteKv implements Kv {

    private readonly url: string;
    private readonly accessToken: string;
    private readonly debug: boolean;
    private readonly encodeV8: EncodeV8;
    private readonly decodeV8: DecodeV8;
    private readonly fetcher: Fetcher;

    private metadata: DatabaseMetadata;

    private constructor(url: string, accessToken: string, debug: boolean, encodeV8: EncodeV8, decodeV8: DecodeV8, fetcher: Fetcher, metadata: DatabaseMetadata) {
        this.url = url;
        this.accessToken = accessToken;
        this.debug = debug;
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

    async get<T = unknown>(key: KvKey, { consistency }: { consistency?: KvConsistencyLevel } = {}): Promise<KvEntryMaybe<T>> {
        checkKeyNotEmpty(key);
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

    // deno-lint-ignore no-explicit-any
    async getMany<T>(keys: readonly KvKey[], { consistency }: { consistency?: KvConsistencyLevel } = {}): Promise<any> {
        const { decodeV8 } = this;
        keys.forEach(checkKeyNotEmpty);
        const packedKeys = keys.map(packKey);
        const packedKeysHex = packedKeys.map(encodeHex);
        const rt: KvEntryMaybe<T>[] = keys.map(v => ({ key: v, value: null, versionstamp: null }));
        const req: SnapshotRead = {
            ranges: packedKeys.map(computeReadRangeForKey)
        }
        const res = await this.snapshotRead(req, consistency);
        for (const range of res.ranges) {
            for (const item of range.values) {
                const itemKeyHex = encodeHex(item.key);
                const i = packedKeysHex.indexOf(itemKeyHex);
                if (i >= 0) rt[i] = { key: keys[i], value: readValue(item.value, item.encoding, decodeV8) as T, versionstamp: encodeHex(item.versionstamp) };
            }
        }
        return rt;
    }

    async set(key: KvKey, value: unknown, { expireIn }: { expireIn?: number } = {}): Promise<KvCommitResult> {
        checkExpireIn(expireIn);
        checkKeyNotEmpty(key);
        const { encodeV8 } = this;
        const req: AtomicWrite = {
            enqueues: [],
            kvChecks: [],
            kvMutations: [
                {
                    key: packKey(key),
                    mutationType: 'M_SET',
                    value: packKvValue(value, encodeV8),
                }
            ],
        };
        const { status, primaryIfWriteDisabled, versionstamp } = await this.atomicWrite(req);
        if (status !== 'AW_SUCCESS') throw new Error(`set failed with status: ${status}${ primaryIfWriteDisabled.length > 0 ? ` primaryIfWriteDisabled=${primaryIfWriteDisabled}` : ''}`);
        return { ok: true, versionstamp: encodeHex(versionstamp) };
    }

    async delete(key: KvKey): Promise<void> {
        const req: AtomicWrite = {
            enqueues: [],
            kvChecks: [],
            kvMutations: [
                {
                    key: packKey(key),
                    mutationType: 'M_CLEAR',
                }
            ],
        };
        const res = await this.atomicWrite(req);
        const { status, primaryIfWriteDisabled } = res;
        if (status !== 'AW_SUCCESS') throw new Error(`set failed with status: ${status}${ primaryIfWriteDisabled.length > 0 ? ` primaryIfWriteDisabled=${primaryIfWriteDisabled}` : ''}`);
    }

    list<T = unknown>(selector: KvListSelector, options?: KvListOptions): KvListIterator<T> {
        if (!isRecord(selector)) throw new Error(`Bad selector: ${JSON.stringify(selector)}`);
        const outCursor: [ string ] = [ '' ];
        const generator: AsyncGenerator<KvEntry<T>> = this.listStream(outCursor, selector, options);
        return new RemoteKvListIterator<T>(generator, () => outCursor[0]);
    }

    async enqueue(value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] }): Promise<KvCommitResult> {
        const { encodeV8 } = this;
        const req: AtomicWrite = {
            enqueues: [
                computeEnqueueMessage(value, encodeV8, opts),
            ],
            kvChecks: [],
            kvMutations: [],
        };
        const { status, primaryIfWriteDisabled, versionstamp } = await this.atomicWrite(req);
        if (status !== 'AW_SUCCESS') throw new Error(`enqueue failed with status: ${status}${ primaryIfWriteDisabled.length > 0 ? ` primaryIfWriteDisabled=${primaryIfWriteDisabled}` : ''}`);
        return { ok: true, versionstamp: encodeHex(versionstamp) };
    }

    listenQueue(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        throw new Error(`'listenQueue' is not possible over KV Connect`);
    }

    atomic(): AtomicOperation {
        let commitCalled = false;
        return new RemoteAtomicOperation(this.encodeV8, async req => {
            if (commitCalled) throw new Error(`'commit' already called for this atomic`);
            const { status, primaryIfWriteDisabled, versionstamp } = await this.atomicWrite(req);
            commitCalled = true;
            if (status === 'AW_CHECK_FAILURE') return { ok: false };
            if (status !== 'AW_SUCCESS') throw new Error(`enqueue failed with status: ${status}${ primaryIfWriteDisabled.length > 0 ? ` primaryIfWriteDisabled=${primaryIfWriteDisabled}` : ''}`);
            return { ok: true, versionstamp: encodeHex(versionstamp) };
        });
    }

    close(): void {
        // no persistent resources yet
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

    async * listStream<T>(outCursor: [ string ], selector: KvListSelector, { batchSize, consistency, cursor: cursorOpt, limit, reverse = false }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
        const { decodeV8 } = this;
        let yielded = 0;
        if (typeof limit === 'number' && yielded >= limit) return;
        const cursor = typeof cursorOpt === 'string' ? unpackCursor(cursorOpt) : undefined;
        let lastYieldedKeyBytes = cursor?.lastYieldedKeyBytes;
        let pass = 0;
        while (true) {
            pass++;
            // console.log({ pass });
            const req: SnapshotRead = { ranges: [] };
            let start: Uint8Array | undefined;
            let end: Uint8Array | undefined;
            if ('prefix' in selector) {
                const prefix = packKey(selector.prefix);
                start = 'start' in selector ? packKey(selector.start) : prefix;
                end = 'end' in selector ? packKey(selector.end) : new Uint8Array([ ...prefix, 0xff ]);
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
                    if (entries++ === 0 && lastYieldedKeyBytes && equalBytes(lastYieldedKeyBytes, entry.key)) continue;
                    const key = unpackKey(entry.key);
                    if (entry.encoding !== 'VE_V8') throw new Error(`Unsupported entry encoding: ${entry.encoding}`);
                    const value = readValue(entry.value, entry.encoding, decodeV8) as T;
                    const versionstamp = encodeHex(entry.versionstamp);
                    lastYieldedKeyBytes = entry.key;
                    outCursor[0] = packCursor({ lastYieldedKeyBytes }); // cursor needs to be set before yield
                    yield { key, value, versionstamp };
                    yielded++;
                    // console.log({ yielded, entries, limit });
                    if (typeof limit === 'number' && yielded >= limit) return;
                }
            }
            if (entries < batchLimit) return;
        }
    }
}

class RemoteKvListIterator<T> implements KvListIterator<T> {
    private readonly generator: AsyncGenerator<KvEntry<T>>;
    private readonly _cursor: () => string;

    constructor(generator: AsyncGenerator<KvEntry<T>>, cursor: () => string) {
        this.generator = generator;
        this._cursor = cursor;
    }

    get cursor(): string {
        return this._cursor();
    }

    next(): Promise<IteratorResult<KvEntry<T>, undefined>> {
        return this.generator.next();
    }

    [Symbol.asyncIterator](): AsyncIterableIterator<KvEntry<T>> {
        return this.generator[Symbol.asyncIterator]();
    }

    // deno-lint-ignore no-explicit-any
    return?(value?: any): Promise<IteratorResult<KvEntry<T>, any>> {
        return this.generator.return(value);
    }

    // deno-lint-ignore no-explicit-any
    throw?(e?: any): Promise<IteratorResult<KvEntry<T>, any>> {
        return this.generator.throw(e);
    }

}

class RemoteAtomicOperation implements AtomicOperation {

    private readonly encodeV8: EncodeV8;
    private readonly _commit: (write: AtomicWrite) => Promise<KvCommitResult | KvCommitError>;
    private readonly write: AtomicWrite = { enqueues: [], kvChecks: [], kvMutations: [] };

    constructor(encodeV8: EncodeV8, commit: (write: AtomicWrite) => Promise<KvCommitResult | KvCommitError>) {
        this.encodeV8 = encodeV8;
        this._commit = commit;
    }

    check(...checks: AtomicCheck[]): this {
        this.write.kvChecks.push(...checks.map(computeKvCheckMessage));
        return this;
    }

    mutate(...mutations: KvMutation[]): this {
        mutations.map(v => v.key).forEach(checkKeyNotEmpty);
        this.write.kvMutations.push(...mutations.map(v => computeKvMutationMessage(v, this.encodeV8)));
        return this;
    }

    sum(key: KvKey, n: bigint): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'sum', key, value: new _KvU64(n) });
    }

    min(key: KvKey, n: bigint): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'min', key, value: new _KvU64(n) });
    }

    max(key: KvKey, n: bigint): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'max', key, value: new _KvU64(n) });
    }

    set(key: KvKey, value: unknown, { expireIn }: { expireIn?: number } = {}): this {
        checkExpireIn(expireIn);
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'set', key, value });
    }

    delete(key: KvKey): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'delete', key });
    }

    enqueue(value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] }): this {
        this.write.enqueues.push(computeEnqueueMessage(value, this.encodeV8, opts));
        return this;
    }

    commit(): Promise<KvCommitResult | KvCommitError> {
        return this._commit(this.write);
    }

}

class _KvU64 implements KvU64 {
    readonly value: bigint;

    constructor(value: bigint) {
        this.value = value;
    }

}
