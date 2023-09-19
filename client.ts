import { decodeHex, encodeHex, equalBytes } from './bytes.ts';
import { encodeJson as encodeJsonAtomicWrite } from './gen/messages/datapath/AtomicWrite.ts';
import { encodeJson as encodeJsonSnapshotRead } from './gen/messages/datapath/SnapshotRead.ts';
import { AtomicWrite, AtomicWriteOutput, KvCheck, ReadRange, SnapshotRead, SnapshotReadOutput, KvMutation as KvMutationMessage, Enqueue } from './gen/messages/datapath/index.ts';
import { encode as encodeBase64, decode as decodeBase64 } from './gen/runtime/base64.ts';
import { DatabaseMetadata, EndpointInfo, fetchAtomicWrite, fetchDatabaseMetadata, fetchSnapshotRead } from './kv_connect_api.ts';
import { packKey, unpackKey } from './kv_key.ts';
import { AtomicCheck, AtomicOperation, Kv, KvCommitError, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListIterator, KvListOptions, KvListSelector, KvMutation, KvService, KvU64 } from './kv_types.ts';
import { decodeV8, encodeV8 } from './v8.ts';

export function newRemoteService({ accessToken, wrapUnknownValues, debug }: { accessToken: string, wrapUnknownValues?: boolean, debug?: boolean }): KvService {
    return {
        openKv: async (url) => await RemoteKv.of(url, { accessToken, wrapUnknownValues, debug }),
        newU64: value => new RemoteKvU64(value),
    }
}

//

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

function computeKvMutationMessage(mut: KvMutation): KvMutationMessage {
    const { key, type } = mut;
    return {
        key: packKey(key),
        mutationType: type === 'delete' ? 'M_CLEAR' : type === 'max' ? 'M_MAX' : type === 'min' ? 'M_MIN' : type == 'set' ? 'M_SET' : type === 'sum' ? 'M_SUM' : 'M_UNSPECIFIED',
        value: mut.type === 'delete' ? undefined : { data: encodeV8(mut.value), encoding: 'VE_V8' },
    }
}

function computeEnqueueMessage(value: unknown, { delay = 0, keysIfUndelivered = [] }: { delay?: number, keysIfUndelivered?: KvKey[] } = {}): Enqueue {
    return {
        backoffSchedule: [ 100, 200, 400, 800 ],
        deadlineMs: `${Date.now() + delay}`,
        kvKeysIfUndelivered: keysIfUndelivered.map(packKey),
        payload: encodeV8(value),
    }
}

async function fetchNewDatabaseMetadata(url: string, accessToken: string, debug: boolean): Promise<DatabaseMetadata> {
    if (debug) console.log('Fetching database metadata...');
    const metadata = await fetchDatabaseMetadata(url, accessToken);
    const { version, endpoints, token } = metadata;
    if (version !== 1) throw new Error(`Unsupported version: ${version}`);
    if (typeof token !== 'string' || token === '') throw new Error(`Unsupported token: ${token}`);
    if (endpoints.length === 0) throw new Error(`No endpoints`);
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

//

class RemoteKv implements Kv {

    private readonly url: string;
    private readonly accessToken: string;
    private readonly wrapUnknownValues: boolean;
    private readonly debug: boolean;

    private metadata: DatabaseMetadata;

    private constructor(url: string, accessToken: string, wrapUnknownValues: boolean, debug: boolean, metadata: DatabaseMetadata) {
        this.url = url;
        this.accessToken = accessToken;
        this.wrapUnknownValues = wrapUnknownValues;
        this.debug = debug;
        this.metadata = metadata;
    }

    static async of(url: string | undefined, { accessToken, wrapUnknownValues = false, debug = false }: { accessToken: string, wrapUnknownValues?: boolean, debug?: boolean }) {
        if (url === undefined || !isValidHttpUrl(url)) throw new Error(`'path' must be an http(s) url`);
        const metadata = await fetchNewDatabaseMetadata(url, accessToken, debug);
        return new RemoteKv(url, accessToken, wrapUnknownValues, debug, metadata);
    }


    async get<T = unknown>(key: KvKey, { consistency }: { consistency?: KvConsistencyLevel } = {}): Promise<KvEntryMaybe<T>> {
        checkKeyNotEmpty(key);
        const { wrapUnknownValues } = this;
        const packedKey = packKey(key);
        const req: SnapshotRead = {
            ranges: [ computeReadRangeForKey(packedKey) ]
        }
        const res = await this.snapshotRead(req, consistency);
        for (const range of res.ranges) {
            for (const item of range.values) {
                if (equalBytes(item.key, packedKey)) return { key, value: decodeV8(item.value, { wrapUnknownValues }) as T, versionstamp: encodeHex(item.versionstamp) };
            }
        }
        return { key, value: null, versionstamp: null };
    }

    // deno-lint-ignore no-explicit-any
    async getMany<T>(keys: readonly KvKey[], { consistency }: { consistency?: KvConsistencyLevel } = {}): Promise<any> {
        const { wrapUnknownValues } = this;
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
                if (i >= 0) rt[i] = { key: keys[i], value: decodeV8(item.value, { wrapUnknownValues }) as T, versionstamp: encodeHex(item.versionstamp) };
            }
        }
        return rt;
    }

    async set(key: KvKey, value: unknown, { expireIn }: { expireIn?: number } = {}): Promise<KvCommitResult> {
        checkExpireIn(expireIn);
        checkKeyNotEmpty(key);
        const req: AtomicWrite = {
            enqueues: [],
            kvChecks: [],
            kvMutations: [
                {
                    key: packKey(key),
                    mutationType: 'M_SET',
                    value: {
                        data: encodeV8(value),
                        encoding: 'VE_V8',
                    }
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
        const outCursor: [ string ] = [ '' ];
        const generator: AsyncGenerator<KvEntry<T>> = this.listStream(outCursor, selector, options);
        return new RemoteKvListIterator<T>(generator, () => outCursor[0]);
    }

    async enqueue(value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] }): Promise<KvCommitResult> {
        const req: AtomicWrite = {
            enqueues: [
                computeEnqueueMessage(value, opts),
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
        return new RemoteAtomicOperation(async req => {
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
        const { url, accessToken, debug } = this;
        if (computeExpiresInMillis(this.metadata) < 1000 * 60 * 5) {
            this.metadata = await fetchNewDatabaseMetadata(url, accessToken, debug);
        }
        const { metadata } = this;
        const firstStrong = metadata.endpoints.filter(v => v.consistency === 'strong').at(0);
        const firstNonStrong = metadata.endpoints.filter(v => v.consistency !== 'strong').at(0);
        const endpoint = consistency === 'strong' ? firstStrong : (firstNonStrong ?? firstStrong);
        if (endpoint === undefined) throw new Error(`Unable to find endpoint for: ${consistency}`);
        return endpoint;
    }

    private async snapshotRead(req: SnapshotRead, consistency: KvConsistencyLevel = 'strong'): Promise<SnapshotReadOutput> {
        const { metadata, debug } = this;
        const endpoint = await this.locateEndpoint(consistency);
        const snapshotReadUrl = new URL('/snapshot_read', endpoint.url).toString();
        const accessToken = metadata.token;
        if (debug) console.log(`fetchSnapshotRead: ${snapshotReadToString(req)}`);
        return await fetchSnapshotRead(snapshotReadUrl, accessToken, metadata.databaseId, req);
    }

    private async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
        const { metadata, debug } = this;
        const endpoint = await this.locateEndpoint('strong');
        const atomicWriteUrl = new URL('/atomic_write', endpoint.url).toString();
        const accessToken = metadata.token;
        if (debug) console.log(`fetchAtomicWrite: ${atomicWriteToString(req)}`);
        return await fetchAtomicWrite(atomicWriteUrl, accessToken, metadata.databaseId, req);
    }

    async * listStream<T>(outCursor: [ string ], selector: KvListSelector, { batchSize, consistency, cursor: cursorOpt, limit, reverse: reverseOpt }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
        const { wrapUnknownValues } = this;
        let yielded = 0;
        if (typeof limit === 'number' && yielded >= limit) return;
        const cursor = typeof cursorOpt === 'string' ? unpackCursor(cursorOpt) : undefined;
        let lastYieldedKeyBytes = cursor?.lastYieldedKeyBytes;
        while (true) {
            const req: SnapshotRead = { ranges: [] };
            if ('prefix' in selector) {
                throw new Error(`implement prefix-based`);
            } else {
                const start = lastYieldedKeyBytes ?? packKey(selector.start);
                const end = packKey(selector.end);
                const reverse = reverseOpt ?? false;
                const batchLimit = Math.min(batchSize ?? 100, 500, limit ?? Number.MAX_SAFE_INTEGER) + (lastYieldedKeyBytes ? 1 : 0);
                req.ranges.push({ start, end, limit: batchLimit, reverse });
            }

            const res = await this.snapshotRead(req, consistency);
            let entries = 0;
            for (const range of res.ranges) {
                for (const entry of range.values) {
                    if (entries === 0 && lastYieldedKeyBytes && equalBytes(lastYieldedKeyBytes, entry.key)) continue;
                    const key = unpackKey(entry.key);
                    if (entry.encoding !== 'VE_V8') throw new Error(`Unsupported entry encoding: ${entry.encoding}`);
                    const value = decodeV8(entry.value, { wrapUnknownValues }) as T;
                    const versionstamp = encodeHex(entry.versionstamp);
                    lastYieldedKeyBytes = entry.key;
                    outCursor[0] = packCursor({ lastYieldedKeyBytes }); // cursor needs to be set before yield
                    yield { key, value, versionstamp };
                    yielded++;
                    entries++;
                    // console.log({ yielded, entries, limit });
                    if (typeof limit === 'number' && yielded >= limit) return;
                }
            }
            if (entries === 0) return;
        }
    }
}

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

class RemoteKvU64 implements KvU64 {
    readonly value: bigint;

    constructor(value: bigint) {
        this.value = value;
    }

}

class RemoteAtomicOperation implements AtomicOperation {

    private readonly _commit: (write: AtomicWrite) => Promise<KvCommitResult | KvCommitError>;
    private readonly write: AtomicWrite = { enqueues: [], kvChecks: [], kvMutations: [] };

    constructor(commit: (write: AtomicWrite) => Promise<KvCommitResult | KvCommitError>) {
        this._commit = commit;
    }

    check(...checks: AtomicCheck[]): this {
        this.write.kvChecks.push(...checks.map(computeKvCheckMessage));
        return this;
    }

    mutate(...mutations: KvMutation[]): this {
        mutations.map(v => v.key).forEach(checkKeyNotEmpty);
        this.write.kvMutations.push(...mutations.map(computeKvMutationMessage));
        return this;
    }

    sum(key: KvKey, n: bigint): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'sum', key, value: new RemoteKvU64(n) });
    }

    min(key: KvKey, n: bigint): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'min', key, value: new RemoteKvU64(n) });
    }

    max(key: KvKey, n: bigint): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'max', key, value: new RemoteKvU64(n) });
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
        this.write.enqueues.push(computeEnqueueMessage(value, opts));
        return this;
    }

    commit(): Promise<KvCommitResult | KvCommitError> {
        return this._commit(this.write);
    }

}
