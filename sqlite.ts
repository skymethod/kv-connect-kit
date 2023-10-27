import { encodeHex } from 'https://deno.land/std@0.204.0/encoding/hex.ts';
import { DB, SqliteOptions } from 'https://deno.land/x/sqlite@v3.8/mod.ts';
import { checkKeyNotEmpty, checkMatches, isRecord } from './check.ts';
import { packKey } from './kv_key.ts';
import { AtomicOperation, Kv, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListIterator, KvListOptions, KvListSelector, KvService, KvU64 } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { GenericAtomicOperation, GenericKvListIterator } from './kv_util.ts';
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from './v8.ts';

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

//

const packVersionstamp = (version: number) => `${version.toString().padStart(16, '0')}0000`;

const unpackVersionstamp = (versionstamp: string) => parseInt(checkMatches('versionstamp', versionstamp, /^(\d{16})0000$/)[1]);

//

class SqliteKv implements Kv {

    private readonly db: DB;
    private readonly debug: boolean;
    private readonly encodeV8: EncodeV8;
    private readonly decodeV8: DecodeV8;

    private closed = false;
    private version = 0;

    private constructor(db: DB, debug: boolean, encodeV8: EncodeV8, decodeV8: DecodeV8) {
        this.db = db;
        this.debug = debug;
        this.encodeV8 = encodeV8;
        this.decodeV8 = decodeV8;
        db.transaction(() => {
            db.execute(`create table if not exists prop(name text primary key, value text not null)`);
            db.execute(`create table if not exists kv(key blob primary key, value blob not null, versionstamp text not null)`);

            const [ row ] = db.query<[ string ]>(`select value from prop where name = 'versionstamp'`);
            if (row) this.version = unpackVersionstamp(row[0]);
        });
        if (debug) console.log({ version: this.version });
    }

    static of(url: string | undefined, opts: SqliteServiceOptions = {}): Kv {
        const { sqliteOptions, wrapUnknownValues, debug = false } = opts;
        const db = new DB(url, sqliteOptions);

        const encodeV8: EncodeV8 = opts.encodeV8 ?? _encodeV8;
        const decodeV8: DecodeV8 = opts.decodeV8 ?? (v => _decodeV8(v, { wrapUnknownValues }));

        return new SqliteKv(db, debug, encodeV8, decodeV8 );
    }

    async get<T = unknown>(key: KvKey, { consistency: _ }: { consistency?: KvConsistencyLevel } = {}): Promise<KvEntryMaybe<T>> {
        this.checkOpen('get');
        checkKeyNotEmpty(key);
        const keyArr = packKey(key);
        await Promise.resolve();
        const { db, decodeV8 } = this;
        const [ row ] = db.query<[ Uint8Array, string ]>('select value, versionstamp from kv where key = ?', [ keyArr ]);
        if (!row) return { key, value: null, versionstamp: null };
        const [ encoded, versionstamp ] = row;
        const value = decodeV8(encoded) as T;
        return { key, value, versionstamp };
    }

    // deno-lint-ignore no-explicit-any
    async getMany<T>(keys: readonly KvKey[], { consistency: _ }: { consistency?: KvConsistencyLevel } = {}): Promise<any> {
        this.checkOpen('getMany');
        keys.forEach(checkKeyNotEmpty);
        await Promise.resolve();
        if (keys.length === 0) return [];
        const { db, decodeV8 } = this;
        const keyArrs = keys.map(packKey);
        const keyHexes = keyArrs.map(encodeHex);
        const placeholders = new Array(keyArrs.length).fill('?').join(', ');
        const rows = db.query<[ Uint8Array, Uint8Array, string ]>(`select key, value, versionstamp from kv where key in (${placeholders})`, keyArrs);
        const rowMap = new Map(rows.map(([ keyArr, valueArr, versionstamp ]) => [ encodeHex(keyArr), ({ value: decodeV8(valueArr), versionstamp })]));
        return keys.map((key, i) => {
            const row = rowMap.get(keyHexes[i]);
            return { key, value: row?.value ?? null, versionstamp: row?.versionstamp ?? null };
        });
    }

    async set(key: KvKey, value: unknown, { expireIn }: { expireIn?: number } = {}): Promise<KvCommitResult> {
        this.checkOpen('set');
        const result = await this.atomic().set(key, value, { expireIn }).commit();
        if (!result.ok) throw new Error(`set failed`); // should never happen, there are no checks
        return result;
    }

    async delete(key: KvKey): Promise<void> {
        this.checkOpen('delete');
        const result = await this.atomic().delete(key).commit();
        if (!result.ok) throw new Error(`delete failed`); // should never happen, there are no checks
    }

    list<T = unknown>(selector: KvListSelector, options: KvListOptions = {}): KvListIterator<T> {
        this.checkOpen('list');
        if (!isRecord(selector)) throw new Error(`Bad selector: ${JSON.stringify(selector)}`);
        if (!isRecord(options)) throw new Error(`Bad options: ${JSON.stringify(options)}`);
        const outCursor: [ string ] = [ '' ];
        const generator: AsyncGenerator<KvEntry<T>> = this.listStream(outCursor, selector, options);
        return new GenericKvListIterator<T>(generator, () => outCursor[0]);
    }

    async enqueue(value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] }): Promise<KvCommitResult> {
        this.checkOpen('enqueue');
        const result = await this.atomic().enqueue(value, opts).commit();
        if (!result.ok) throw new Error(`enqueue failed`); // should never happen, there are no checks
        return result;
    }

    listenQueue(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        this.checkOpen('listenQueue');
        throw new Error(`listenQueue() not implemented`);
    }

    atomic(): AtomicOperation {
        return new GenericAtomicOperation((checks, mutations, enqueues) => {
            this.checkOpen('commit');
            const notImplemented = () => new Error(`commit(${JSON.stringify({ checks, mutations, enqueues })}) not implemented`);
            if (checks.length === 0 && enqueues.length === 0) {
                const { db, encodeV8 } = this;
                const newVersionstamp = packVersionstamp(++this.version);
                db.transaction(() => {
                    for (const mutation of mutations) {
                        const { key } = mutation;
                        const keyArr = packKey(key);
                        if (mutation.type === 'set') {
                            const { value, expireIn } = mutation;
                            if (typeof expireIn === 'number') throw notImplemented();
                            const valueArr = encodeV8(value);
                            db.query(`insert into kv(key, value, versionstamp) values (?, ?, ?) on conflict(key) do update set value=excluded.value, versionstamp=excluded.versionstamp`, [ keyArr, valueArr, newVersionstamp ]);
                        } else if (mutation.type === 'delete') {
                            db.query(`delete from kv where key = ?`, [ keyArr ]);
                        } else {
                            throw notImplemented();
                        }
                    }
                    db.query(`update prop set value = ? where name = 'version'`, [ newVersionstamp ]);
                });
                return Promise.resolve({ ok: true, versionstamp: newVersionstamp });
            }
            throw notImplemented();
        });
    }

    close(): void {
        this.checkOpen('close');
        this.closed = true;
        this.db.close();
    }

    //

    private checkOpen(method: string) {
        if (this.closed) throw new Error(`Cannot call '.${method}' after '.close' is called`);
    }

    // deno-lint-ignore require-yield
    private async * listStream<T>(_outCursor: [ string ], selector: KvListSelector, options: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
        if ('prefix' in selector) {
            return;
        }

        throw new Error(`list(${JSON.stringify({ selector, options })}) not implemented`);
    }

}
