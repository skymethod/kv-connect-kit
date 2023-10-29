import { assertInstanceOf } from 'https://deno.land/std@0.204.0/assert/assert_instance_of.ts';
import { AssertionError } from 'https://deno.land/std@0.204.0/assert/assertion_error.ts';
import { encodeHex, equalBytes } from './bytes.ts';
import { DB, SqliteOptions } from 'https://deno.land/x/sqlite@v3.8/mod.ts';
import { checkKeyNotEmpty, checkMatches } from './check.ts';
import { packKey, unpackKey } from './kv_key.ts';
import { AtomicOperation, Kv, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListIterator, KvListOptions, KvListSelector, KvService, KvU64 } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { DecodeV8, EncodeV8, GenericAtomicOperation, GenericKvListIterator, KvValueEncoding, checkListOptions, checkListSelector, packCursor, packKvValue, readValue, unpackCursor } from './kv_util.ts';
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from './v8.ts';

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

const isValidVersionstamp = (versionstamp: string) => /^(\d{16})0000$/.test(versionstamp);

const replacer = (_this: unknown, v: unknown) => typeof v === 'bigint' ? v.toString() : v;

function querySingleValue<T>(key: KvKey, db: DB, decodeV8: DecodeV8): { value: T, versionstamp: string } | undefined {
    const keyBytes = packKey(key);
    const [ row ] = db.query<[ Uint8Array, KvValueEncoding, string ]>('select bytes, encoding, versionstamp from kv where key = ?', [ keyBytes ]);
    if (!row) return undefined;
    const [ bytes, encoding, versionstamp ] = row;
    const value = readValue(bytes, encoding, decodeV8) as T;
    return { value, versionstamp };
}

function runExpirerTransaction(db: DB, debug: boolean): number | undefined {
    return db.transaction(() => {
        db.query(`delete from kv where expires <= ?`, [ Date.now() ]);
        if (debug) console.log(`runExpirerTransaction: expirer deleted ${db.changes}`);
        const [ row ] = db.query<[ number ]>(`select min(expires) from kv`);
        return row?.at(0) ?? undefined;
    });
}

//

class SqliteKv implements Kv {

    private readonly db: DB;
    private readonly debug: boolean;
    private readonly encodeV8: EncodeV8;
    private readonly decodeV8: DecodeV8;

    private closed = false;
    private version = 0;
    private minExpires: number | undefined;
    private expirerTimeout = 0;

    private constructor(db: DB, debug: boolean, encodeV8: EncodeV8, decodeV8: DecodeV8) {
        this.db = db;
        this.debug = debug;
        this.encodeV8 = encodeV8;
        this.decodeV8 = decodeV8;
        db.transaction(() => {
            db.execute(`create table if not exists prop(name text primary key, value text not null)`);
            db.execute(`create table if not exists kv(key blob primary key, bytes blob not null, encoding text not null, versionstamp text not null, expires int)`);
            this.minExpires = runExpirerTransaction(db, debug);

            const [ row ] = db.query<[ string ]>(`select value from prop where name = 'versionstamp'`);
            if (row) this.version = unpackVersionstamp(row[0]);
        });
        if (this.minExpires !== undefined) this.rescheduleExpirer(this.minExpires);
        if (debug) console.log(`new SqliteKV: version=${this.version}`);
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
        await Promise.resolve();
        const { db, decodeV8 } = this;
        const result = querySingleValue<T>(key, db, decodeV8);
        return result === undefined ? { key, value: null, versionstamp: null } : { key, value: result.value, versionstamp: result.versionstamp };
    }

    // deno-lint-ignore no-explicit-any
    async getMany<T>(keys: readonly KvKey[], { consistency: _ }: { consistency?: KvConsistencyLevel } = {}): Promise<any> {
        this.checkOpen('getMany');
        keys.forEach(checkKeyNotEmpty);
        await Promise.resolve();
        if (keys.length === 0) return [];
        const { db, decodeV8 } = this;
        const keyBytesArr = keys.map(packKey);
        const keyHexes = keyBytesArr.map(encodeHex);
        const placeholders = new Array(keyBytesArr.length).fill('?').join(', ');
        const rows = db.query<[ Uint8Array, Uint8Array, KvValueEncoding, string ]>(`select key, bytes, encoding, versionstamp from kv where key in (${placeholders})`, keyBytesArr);
        const rowMap = new Map(rows.map(([ keyBytes, bytes, encoding , versionstamp ]) => [ encodeHex(keyBytes), ({ value: readValue(bytes, encoding, decodeV8), versionstamp })]));
        return keys.map((key, i) => {
            const row = rowMap.get(keyHexes[i]);
            return { key, value: row?.value ?? null, versionstamp: row?.versionstamp ?? null };
        });
    }

    async set(key: KvKey, value: unknown, { expireIn }: { expireIn?: number } = {}): Promise<KvCommitResult> {
        this.checkOpen('set');
        const result = await this.atomic().set(key, value, { expireIn }).commit();
        if (!result.ok) throw new AssertionError(`set failed`); // should never happen, there are no checks
        return result;
    }

    async delete(key: KvKey): Promise<void> {
        this.checkOpen('delete');
        const result = await this.atomic().delete(key).commit();
        if (!result.ok) throw new AssertionError(`delete failed`); // should never happen, there are no checks
    }

    list<T = unknown>(selector: KvListSelector, options: KvListOptions = {}): KvListIterator<T> {
        this.checkOpen('list');
        checkListSelector(selector);
        options = checkListOptions(options);
        const outCursor: [ string ] = [ '' ];
        const generator: AsyncGenerator<KvEntry<T>> = this.listStream(outCursor, selector, options);
        return new GenericKvListIterator<T>(generator, () => outCursor[0]);
    }

    async enqueue(value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] }): Promise<KvCommitResult> {
        this.checkOpen('enqueue');
        const result = await this.atomic().enqueue(value, opts).commit();
        if (!result.ok) throw new AssertionError(`enqueue failed`); // should never happen, there are no checks
        return result;
    }

    listenQueue(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        this.checkOpen('listenQueue');
        throw new Error(`listenQueue() not implemented`);
    }

    atomic(): AtomicOperation {
        return new GenericAtomicOperation(async (checks, mutations, enqueues) => {
            this.checkOpen('commit');
            const notImplemented = () => new Error(`commit(${JSON.stringify({ checks, mutations, enqueues }, replacer)}) not implemented`);
            if (enqueues.length > 0) throw notImplemented();
            const { db, encodeV8, decodeV8 } = this;
            await Promise.resolve();
            return db.transaction(() => {
                let minExpires: number | undefined;
                for (const { key, versionstamp } of checks) {
                    if (!(versionstamp === null || typeof versionstamp === 'string' && isValidVersionstamp(versionstamp))) throw new AssertionError(`Bad 'versionstamp': ${versionstamp}`);
                    const existing = querySingleValue(key, db, decodeV8);
                    if (versionstamp === null && existing) return { ok: false };
                    if (typeof versionstamp === 'string' && existing?.versionstamp !== versionstamp) return { ok: false };
                }
                const newVersionstamp = packVersionstamp(++this.version);
                for (const mutation of mutations) {
                    const { key } = mutation;
                    const keyBytes = packKey(key);
                    if (mutation.type === 'set') {
                        const { value, expireIn } = mutation;
                        const expires = typeof expireIn === 'number' ? Date.now() + Math.round(expireIn) : undefined;
                        if (expires !== undefined) minExpires = Math.min(expires, minExpires ?? Number.MAX_SAFE_INTEGER);
                        const { data: bytes, encoding } = packKvValue(value, encodeV8);
                        db.query(`insert into kv(key, bytes, encoding, versionstamp, expires) values (?, ?, ?, ?, ?) on conflict(key) do update set bytes = excluded.bytes, encoding = excluded.encoding, versionstamp = excluded.versionstamp, expires = excluded.expires`,
                            [ keyBytes, bytes, encoding, newVersionstamp, expires ]);
                    } else if (mutation.type === 'delete') {
                        db.query(`delete from kv where key = ?`, [ keyBytes ]);
                    } else if (mutation.type === 'sum' || mutation.type === 'min' || mutation.type === 'max') {
                        const existing = querySingleValue<_KvU64>(key, db, decodeV8);
                        if (existing === undefined) {
                            const { data: bytes, encoding } = packKvValue(mutation.value, encodeV8);
                            db.query(`insert into kv(key, bytes, encoding, versionstamp) values (?, ?, ?, ?)`, [ keyBytes, bytes, encoding, newVersionstamp ]);
                        } else {
                            assertInstanceOf(existing.value, _KvU64, `Can only '${mutation.type}' on KvU64`);
                            const result = mutation.type === 'min' ? existing.value.min(mutation.value)
                                : mutation.type === 'max' ? existing.value.max(mutation.value)
                                : existing.value.sum(mutation.value);
                            const { data: bytes, encoding } = packKvValue(result, encodeV8);
                            db.query(`update kv set bytes = ?, encoding = ?, versionstamp = ? where key = ?`, [ bytes, encoding, newVersionstamp, keyBytes ]);
                        }
                    } else {
                        throw notImplemented();
                    }
                }
                db.query(`update prop set value = ? where name = 'version'`, [ newVersionstamp ]);
                if (minExpires !== undefined) this.rescheduleExpirer(minExpires);
                return { ok: true, versionstamp: newVersionstamp };
            });
        });
    }

    close(): void {
        clearTimeout(this.expirerTimeout);
        this.checkOpen('close');
        this.closed = true;
        this.db.close();
    }

    //

    private checkOpen(method: string) {
        if (this.closed) throw new AssertionError(`Cannot call '.${method}' after '.close' is called`);
    }

    private rescheduleExpirer(expires: number) {
        const { minExpires, debug, expirerTimeout } = this;
        if (minExpires !== undefined && minExpires < expires) return;
        this.minExpires = expires;
        clearTimeout(expirerTimeout);
        const delay = expires - Date.now();
        if (debug) console.log(`rescheduleExpirer: run in ${delay}ms`);
        this.expirerTimeout = setTimeout(() => this.runExpirer(), delay);
    }

    private runExpirer() {
        const { db, debug } = this;
        const newMinExpires = runExpirerTransaction(db, debug);
        this.minExpires = newMinExpires;
        if (newMinExpires !== undefined) {
            this.rescheduleExpirer(newMinExpires);
        } else {
            clearTimeout(this.expirerTimeout);
        }
    }

    private async * listStream<T>(outCursor: [ string ], selector: KvListSelector, { batchSize, consistency: _, cursor: cursorOpt, limit, reverse = false }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
        const { db, decodeV8 } = this;
        let yielded = 0;
        if (typeof limit === 'number' && yielded >= limit) return;
        const cursor = typeof cursorOpt === 'string' ? unpackCursor(cursorOpt) : undefined;
        let lastYieldedKeyBytes = cursor?.lastYieldedKeyBytes;
        let pass = 0;
        const prefixBytes = 'prefix' in selector ? packKey(selector.prefix) : undefined;
        while (true) {
            pass++;
            // console.log({ pass });
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

            const rows = db.query<[ Uint8Array, Uint8Array, KvValueEncoding, string, ]>(`select key, bytes, encoding, versionstamp from kv where key >= ? and key < ? order by key ${reverse ? 'desc' : 'asc'} limit ?`, [ start, end, batchLimit ]);

            let entries = 0;
            for (const [ keyBytes, bytes, encoding, versionstamp ] of rows) {
                if (entries++ === 0 && (lastYieldedKeyBytes && equalBytes(lastYieldedKeyBytes, keyBytes) || prefixBytes && equalBytes(prefixBytes, keyBytes))) continue;
                const key = unpackKey(keyBytes);
                const value = readValue(bytes, encoding, decodeV8) as T;
                lastYieldedKeyBytes = keyBytes;
                outCursor[0] = packCursor({ lastYieldedKeyBytes }); // cursor needs to be set before yield
                yield { key, value, versionstamp };
                yielded++;
                // console.log({ yielded, entries, limit });
                if (typeof limit === 'number' && yielded >= limit) return;
            }
            if (entries < batchLimit) return;
        }
    }

}
