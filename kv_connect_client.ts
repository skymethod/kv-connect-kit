// https://github.com/denoland/deno/tree/main/ext/kv#kv-connect
// https://github.com/denoland/deno/blob/main/cli/schemas/kv-metadata-exchange-response.v1.json
// https://github.com/denoland/deno/blob/main/ext/kv/proto/datapath.proto

import { encodeHex } from './bytes.ts';
import { SnapshotRead, SnapshotReadOutput } from './gen/messages/datapath/index.ts';
import { DatabaseMetadata, fetchDatabaseMetadata, fetchSnapshotRead, packKey, unpackKey } from './kv_connect_api.ts';
import { AtomicOperation, Kv, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListIterator, KvListOptions, KvListSelector } from './kv_types.ts';
import { decodeV8 } from './v8.ts';

export async function openKv(url: string, { accessToken }: { accessToken: string }): Promise<Kv> {    
    return await RemoteKv.of(url, { accessToken });
}

//

class RemoteKv implements Kv {

    private readonly url: string;
    private readonly accessToken: string;

    private metadata: DatabaseMetadata;

    private constructor(url: string, accessToken: string, metadata: DatabaseMetadata) {
        this.url = url;
        this.accessToken = accessToken;
        this.metadata = metadata;
    }

    static async of(url: string, { accessToken }: { accessToken: string }) {
        const metadata = await fetchDatabaseMetadata(url, accessToken);
        const { version, endpoints, token, expiresAt } = metadata;
        if (version !== 1) throw new Error(`Unsupported version: ${version}`);
        if (typeof token !== 'string' || token === '') throw new Error(`Unsupported token: ${token}`);
        if (endpoints.length === 0) throw new Error(`No endpoints`);
        const expiresTime = new Date(expiresAt).getTime();
        console.log(`Expires in ${expiresTime - Date.now()}ms`);
        return new RemoteKv(url, accessToken, metadata);
    }

    get<T = unknown>(key: KvKey, options?: { consistency?: KvConsistencyLevel | undefined; } | undefined): Promise<KvEntryMaybe<T>> {
        throw new Error(`implement: RemoteKv.get(${JSON.stringify({ key, options })})`);
    }

    // deno-lint-ignore no-explicit-any
    getMany<T>(keys: readonly unknown[], options?: { consistency?: KvConsistencyLevel }): Promise<any> {
        throw new Error(`implement: RemoteKv.getMany(${JSON.stringify({ keys, options })})`);
    }

    set(key: KvKey, value: unknown, options?: { expireIn?: number | undefined; } | undefined): Promise<KvCommitResult> {
        throw new Error(`implement: RemoteKv.set(${JSON.stringify({ key, value, options })})`);
    }

    delete(key: KvKey): Promise<void> {
        throw new Error(`implement: RemoteKv.delete(${JSON.stringify({ key })})`);
    }

    list<T = unknown>(selector: KvListSelector, options?: KvListOptions): KvListIterator<T> {
        const outCursor: [ string ] = [ '' ];
        const generator: AsyncGenerator<KvEntry<T>> = this.listStream(outCursor, selector, options);
        return new RemoteKvListIterator<T>(generator, () => outCursor[0]);
    }

    enqueue(value: unknown, options?: { delay?: number | undefined; keysIfUndelivered?: KvKey[] | undefined; } | undefined): Promise<KvCommitResult> {
        throw new Error(`implement: RemoteKv.enqueue(${JSON.stringify({ value, options })})`);
    }

    listenQueue(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        throw new Error(`'listenQueue' is not possible over KV Connect`);
    }

    atomic(): AtomicOperation {
        throw new Error(`implement: RemoteKv.atomic()`);
    }

    close(): void {
        throw new Error(`implement: RemoteKv.close()`);
    }

    //

    private async snapshotRead(req: SnapshotRead): Promise<SnapshotReadOutput> {
        const { metadata } = this;
        // TODO better endpoint selection
        // TODO refresh metadata if necessary
        const endpointUrl = metadata.endpoints[0].url;
        const snapshotReadUrl = new URL('/snapshot_read', endpointUrl).toString();
        const accessToken = metadata.token;
        return await fetchSnapshotRead(snapshotReadUrl, accessToken, metadata.databaseId, req);
    }

    async * listStream<T>(outCursor: [ string ], selector: KvListSelector, { batchSize, consistency, cursor, limit: limitOpt, reverse: reverseOpt }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
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

        const res = await this.snapshotRead(req);
        for (const range of res.ranges) {
            for (const entry of range.values) {
                const key = unpackKey(entry.key);
                if (entry.encoding !== 'VE_V8') throw new Error(`Unsupported entry encoding: ${entry.encoding}`);
                const value = decodeV8(entry.value, { wrapUnknownValues: true }) as T;
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
