import { checkExpireIn, checkKeyNotEmpty, isRecord } from './check.ts';
import { AtomicOperation, Kv, KvCommitResult, KvConsistencyLevel, KvEntryMaybe, KvKey, KvListIterator, KvListOptions, KvListSelector, KvService, KvU64 } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from './v8.ts';
import { DB, SqliteOptions } from 'https://deno.land/x/sqlite@v3.8/mod.ts';

type EncodeV8 = (value: unknown) => Uint8Array;
type DecodeV8 = (bytes: Uint8Array) => unknown;

export interface SqliteServiceOptions {

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

    /** Custom options to use when initializing the sqlite db */
    readonly sqliteOptions?: SqliteOptions;
}

/**
 * Creates a new KvService instance backed by a local sqlite db.
 */
export function makeSqliteService(opts?: SqliteServiceOptions): KvService {
    return {
        openKv: (url) => Promise.resolve(SqliteKv.of(url, opts)),
        newKvU64: value => new _KvU64(value),
        isKvU64: (obj: unknown): obj is KvU64 => obj instanceof _KvU64,
    }
}


class SqliteKv implements Kv {

    private readonly db: DB;
    private readonly debug: boolean;
    private readonly encodeV8: EncodeV8;
    private readonly decodeV8: DecodeV8;

    private constructor(db: DB, debug: boolean, encodeV8: EncodeV8, decodeV8: DecodeV8) {
        this.db = db;
        this.debug = debug;
        this.encodeV8 = encodeV8;
        this.decodeV8 = decodeV8;
    }

    static of(url: string | undefined, opts: SqliteServiceOptions = {}): Kv {
        const { sqliteOptions, wrapUnknownValues, debug = false } = opts;
        const db = new DB(url, sqliteOptions);

        const encodeV8: EncodeV8 = opts.encodeV8 ?? _encodeV8;
        const decodeV8: DecodeV8 = opts.decodeV8 ?? (v => _decodeV8(v, { wrapUnknownValues }));

        return new SqliteKv(db, debug, encodeV8, decodeV8 );
    }

    async get<T = unknown>(key: KvKey, { consistency }: { consistency?: KvConsistencyLevel } = {}): Promise<KvEntryMaybe<T>> {
        checkKeyNotEmpty(key);
        throw new Error(`get(${JSON.stringify({ key, opts: { consistency } })}) not implemented`);
    }

    // deno-lint-ignore no-explicit-any
    async getMany<T>(keys: readonly KvKey[], { consistency }: { consistency?: KvConsistencyLevel } = {}): Promise<any> {
        keys.forEach(checkKeyNotEmpty);
        throw new Error(`getMany(${JSON.stringify({ keys, opts: { consistency } })}) not implemented`);
    }

    async set(key: KvKey, value: unknown, { expireIn }: { expireIn?: number } = {}): Promise<KvCommitResult> {
        checkExpireIn(expireIn);
        checkKeyNotEmpty(key);
        throw new Error(`set(${JSON.stringify({ key, value, opts: { expireIn } })}) not implemented`);
    }

    async delete(key: KvKey): Promise<void> {
        throw new Error(`delete(${JSON.stringify({ key })}) not implemented`);
    }

    list<T = unknown>(selector: KvListSelector, options?: KvListOptions): KvListIterator<T> {
        if (!isRecord(selector)) throw new Error(`Bad selector: ${JSON.stringify(selector)}`);
        throw new Error(`list(${JSON.stringify({ selector, options })}) not implemented`);
    }

    async enqueue(value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] }): Promise<KvCommitResult> {
        throw new Error(`enqueue(${JSON.stringify({ value, opts })}) not implemented`);
    }

    listenQueue(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        throw new Error(`listenQueue() not implemented`);
    }

    atomic(): AtomicOperation {
        throw new Error(`atomic() not implemented`);
    }

    close(): void {
        this.db.close();
    }

}
