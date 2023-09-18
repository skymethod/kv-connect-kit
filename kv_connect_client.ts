// https://github.com/denoland/deno/tree/main/ext/kv#kv-connect
// https://github.com/denoland/deno/blob/main/cli/schemas/kv-metadata-exchange-response.v1.json
// https://github.com/denoland/deno/blob/main/ext/kv/proto/datapath.proto

import { encodeHex, equalBytes } from './bytes.ts';
import { AtomicWrite, AtomicWriteOutput, ReadRange, SnapshotRead, SnapshotReadOutput } from './gen/messages/datapath/index.ts';
import { DatabaseMetadata, EndpointInfo, fetchAtomicWrite, fetchDatabaseMetadata, fetchSnapshotRead, packKey, unpackKey } from './kv_connect_api.ts';
import { AtomicOperation, Kv, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListIterator, KvListOptions, KvListSelector } from './kv_types.ts';
import { decodeV8, encodeV8 } from './v8.ts';

export async function openKv(url: string, { accessToken }: { accessToken: string }): Promise<Kv> {    
    return await RemoteKv.of(url, { accessToken });
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

//

class RemoteKv implements Kv {

    private readonly url: string;
    private readonly accessToken: string;
    private readonly wrapUnknownValues: boolean;

    private metadata: DatabaseMetadata;

    private constructor(url: string, accessToken: string, wrapUnknownValues: boolean, metadata: DatabaseMetadata) {
        this.url = url;
        this.accessToken = accessToken;
        this.wrapUnknownValues = wrapUnknownValues;
        this.metadata = metadata;
    }

    static async of(url: string, { accessToken, wrapUnknownValues = false }: { accessToken: string, wrapUnknownValues?: boolean }) {
        const metadata = await fetchDatabaseMetadata(url, accessToken);
        const { version, endpoints, token, expiresAt } = metadata;
        if (version !== 1) throw new Error(`Unsupported version: ${version}`);
        if (typeof token !== 'string' || token === '') throw new Error(`Unsupported token: ${token}`);
        if (endpoints.length === 0) throw new Error(`No endpoints`);
        const expiresTime = new Date(expiresAt).getTime();
        console.log(`Expires in ${expiresTime - Date.now()}ms`);
        return new RemoteKv(url, accessToken, wrapUnknownValues, metadata);
    }


    async get<T = unknown>(key: KvKey, { consistency }: { consistency?: KvConsistencyLevel } = {}): Promise<KvEntryMaybe<T>> {
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
        if (typeof expireIn === 'number') throw new Error(`'expireIn' not supported over KV Connect`);
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

    async enqueue(value: unknown, { delay, keysIfUndelivered = [] }: { delay?: number, keysIfUndelivered?: KvKey[] }  = {}): Promise<KvCommitResult> {
        if (typeof delay === 'number') throw new Error(`'delay' not supported over KV Connect`);
        const req: AtomicWrite = {
            enqueues: [
                {
                    backoffSchedule: [], // TODO ???
                    deadlineMs: '10000', // TODO ???
                    kvKeysIfUndelivered: keysIfUndelivered.map(packKey),
                    payload: encodeV8(value),
                }
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
        throw new Error(`implement: RemoteKv.atomic()`);
    }

    close(): void {
        // no persistent resources yet
    }

    //

    private locateEndpoint(consistency: KvConsistencyLevel): EndpointInfo {
        const { metadata } = this;
        // TODO refresh metadata if necessary
        const endpoint = consistency === 'strong' ? metadata.endpoints.find(v => v.consistency === 'strong') : metadata.endpoints[0];
        if (endpoint === undefined) throw new Error(`Unable to find endpoint for: ${consistency}`);
        return endpoint;
    }

    private async snapshotRead(req: SnapshotRead, consistency: KvConsistencyLevel = 'strong'): Promise<SnapshotReadOutput> {
        const { metadata } = this;
        const endpoint = this.locateEndpoint(consistency);
        const snapshotReadUrl = new URL('/snapshot_read', endpoint.url).toString();
        const accessToken = metadata.token;
        return await fetchSnapshotRead(snapshotReadUrl, accessToken, metadata.databaseId, req);
    }

    private async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
        const { metadata } = this;
        const endpoint = this.locateEndpoint('strong');
        const atomicWriteUrl = new URL('/atomic_write', endpoint.url).toString();
        const accessToken = metadata.token;
        return await fetchAtomicWrite(atomicWriteUrl, accessToken, metadata.databaseId, req);
    }

    async * listStream<T>(outCursor: [ string ], selector: KvListSelector, { batchSize, consistency, cursor, limit: limitOpt, reverse: reverseOpt }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
        const { wrapUnknownValues } = this;
        const req: SnapshotRead = { ranges: [] };
        if ('prefix' in selector) {
            throw new Error(`implement prefix-based`);
        } else {
            // TODO do this properly
            const start = packKey([]);
            const end = packKey([ 'z' ]);
            const reverse = reverseOpt ?? false;
            const limit = Math.min(limitOpt ?? 100, 500);
            req.ranges.push({ start, end, limit, reverse });
        }

        const res = await this.snapshotRead(req, consistency);
        for (const range of res.ranges) {
            for (const entry of range.values) {
                const key = unpackKey(entry.key);
                if (entry.encoding !== 'VE_V8') throw new Error(`Unsupported entry encoding: ${entry.encoding}`);
                const value = decodeV8(entry.value, { wrapUnknownValues }) as T;
                const versionstamp = encodeHex(entry.versionstamp);
                yield { key, value, versionstamp };
            }
           
        }
        outCursor[0] = 'TODO';
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
