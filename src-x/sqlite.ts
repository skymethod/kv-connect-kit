import { assertInstanceOf } from 'https://deno.land/std@0.207.0/assert/assert_instance_of.ts';
import { AssertionError } from 'https://deno.land/std@0.207.0/assert/assertion_error.ts';
import { encodeHex, equalBytes } from '../src/bytes.ts';
import { packKey, unpackKey } from '../src/kv_key.ts';
import { AtomicCheck, Kv, KvCommitError, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListOptions, KvListSelector, KvMutation, KvService, KvU64 } from '../src/kv_types.ts';
import { _KvU64 } from '../src/kv_u64.ts';
import { BaseKv, CursorHolder, DecodeV8, EncodeV8, Enqueue, Expirer, isValidVersionstamp, KvValueEncoding, packCursor, packKvValue, packVersionstamp, QueueHandler, QueueWorker, readValue, replacer, unpackCursor, unpackVersionstamp } from '../src/kv_util.ts';
import { SqliteDb, SqliteDriver, SqlitePreparedStatement, SqliteQueryParam } from './sqlite_driver.ts';
import { SqliteWasmDriver } from './sqlite_wasm_driver.ts';
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from '../src/v8.ts';

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

    /** Sqlite implementation to use. Defaults to cross-platform wasm implementation. */
    readonly driver?: SqliteDriver;

    /** Maximum number of attempts to deliver a failing queue message before giving up. Defaults to 10. */
    readonly maxQueueAttempts?: number;
}

/**
 * Creates a new KvService instance backed by a local sqlite db.
 */
export function makeSqliteService(opts?: SqliteServiceOptions): KvService {
    return {
        openKv: (url) => Promise.resolve(SqliteKv.of(url ?? ':memory:', opts)),
        newKvU64: value => new _KvU64(value),
        isKvU64: (obj: unknown): obj is KvU64 => obj instanceof _KvU64,
    }
}

//

const SCHEMA = 1;

function querySingleValue<T>(key: KvKey, statements: Statements, decodeV8: DecodeV8): { value: T, versionstamp: string } | undefined {
    const keyBytes = packKey(key);
    const [ row ] = statements.query<[ Uint8Array, KvValueEncoding, string ]>('select bytes, encoding, versionstamp from kv where key = ?', [ keyBytes ]);
    if (!row) return undefined;
    const [ bytes, encoding, versionstamp ] = row;
    const value = readValue(bytes, encoding, decodeV8) as T;
    return { value, versionstamp };
}

function runExpirerTransaction(db: SqliteDb, statements: Statements, debug: boolean): number | undefined {
    return db.transaction(() => {
        statements.query(`delete from kv where expires <= ?`, [ Date.now() ]);
        if (debug) console.log(`runExpirerTransaction: expirer deleted ${db.changes}`);
        const [ row ] = statements.query<[ number ]>(`select min(expires) from kv`);
        return row?.at(0) ?? undefined;
    });
}

function tryParseInt(str: string): number | undefined {
    try {
        return parseInt(str);
    } catch {
        return undefined;
    }
}

//

class SqliteKv extends BaseKv {

    private readonly db: SqliteDb;
    private readonly encodeV8: EncodeV8;
    private readonly decodeV8: DecodeV8;
    private readonly maxQueueAttempts: number;
    private readonly statements: Statements;
    private readonly expirer: Expirer;
    private readonly queueWorker: QueueWorker;

    private version = 0;

    private constructor(db: SqliteDb, debug: boolean, encodeV8: EncodeV8, decodeV8: DecodeV8, maxQueueAttempts: number) {
        super({ debug });
        this.db = db;
        this.encodeV8 = encodeV8;
        this.decodeV8 = decodeV8;
        this.statements = new Statements(db, debug);
        this.expirer = new Expirer(debug, () => runExpirerTransaction(db, this.statements, debug));
        this.queueWorker = new QueueWorker(queueHandler => this.runWorker(queueHandler));
        this.maxQueueAttempts = maxQueueAttempts;

        // initialize database
        db.transaction(() => {
            // create prop table, initialize schema and run migrations (in the future)
            db.execute(`create table if not exists prop(name text primary key, value text not null) without rowid`);
            {
                const existingSchema = db.query<[ string ]>(`select value from prop where name = 'schema'`).at(0)?.at(0);
                if (existingSchema === undefined) {
                    if (debug) console.log(`SqliteKV(): no existing schema, initializing to ${SCHEMA}`);
                    db.query(`insert into prop(name, value) values ('schema', ?)`, [ SCHEMA ]);
                } else {
                    const existingSchemaInt = tryParseInt(existingSchema);
                    if (!(typeof existingSchemaInt === 'number' && Number.isSafeInteger(existingSchemaInt) && existingSchemaInt > 0)) throw new Error(`Bad existing schema: ${existingSchema}`);
                    if (existingSchemaInt > 1) throw new Error(`Unknown existing schema: ${existingSchemaInt}`);
                    if (debug) console.log(`SqliteKV(): existing schema ${existingSchemaInt}`);
                }
            }

            // create the rest of the tables
            db.execute(`create table if not exists kv(key blob primary key, bytes blob not null, encoding text not null, versionstamp text not null, expires integer) without rowid`);
            db.execute(`create table if not exists queue(id integer primary key autoincrement, bytes blob not null, encoding text not null, failures integer not null, enqueued integer not null, available integer not null, locked integer)`);
            db.execute(`create table if not exists queue_keys_if_undelivered(id integer, key blob, primary key (id, key)) without rowid`);

            // expire old keys
            const minExpires = runExpirerTransaction(db, this.statements, debug);
            this.expirer.init(minExpires);

            // unlock any locked queue items
            db.execute(`update queue set locked = null where locked is not null`);

            // load initial versionstamp
            {
                const [ row ] = db.query<[ string ]>(`select value from prop where name = 'versionstamp'`);
                if (row) this.version = unpackVersionstamp(row[0]);
                if (debug) console.log(`SqliteKV(): version=${this.version}`);
            }
        });
    }

    static of(path: string, opts: SqliteServiceOptions = {}): Kv {
        const { driver = new SqliteWasmDriver(), wrapUnknownValues, debug = false, maxQueueAttempts = 10 } = opts;
        if (!(typeof maxQueueAttempts === 'number' && Number.isSafeInteger(maxQueueAttempts) && maxQueueAttempts > 0)) throw new Error(`'maxQueueAttempts' must be a positive integer`);
        const db = driver.newDb(path);

        const encodeV8: EncodeV8 = opts.encodeV8 ?? _encodeV8;
        const decodeV8: DecodeV8 = opts.decodeV8 ?? (v => _decodeV8(v, { wrapUnknownValues }));

        return new SqliteKv(db, debug, encodeV8, decodeV8, maxQueueAttempts );
    }

    protected async get_<T = unknown>(key: KvKey, _consistency: KvConsistencyLevel | undefined): Promise<KvEntryMaybe<T>> {
        await Promise.resolve();
        const { statements, decodeV8 } = this;
        const result = querySingleValue<T>(key, statements, decodeV8);
        return result === undefined ? { key, value: null, versionstamp: null } : { key, value: result.value, versionstamp: result.versionstamp };
    }

    protected async getMany_(keys: readonly KvKey[], _consistency: KvConsistencyLevel | undefined): Promise<KvEntryMaybe<unknown>[]> {
        await Promise.resolve();
        const { statements, decodeV8 } = this;
        const keyBytesArr = keys.map(packKey);
        const keyHexes = keyBytesArr.map(encodeHex);
        const placeholders = new Array(keyBytesArr.length).fill('?').join(', ');
        const rows = statements.query<[ Uint8Array, Uint8Array, KvValueEncoding, string ]>(`select key, bytes, encoding, versionstamp from kv where key in (${placeholders})`, keyBytesArr);
        const rowMap = new Map(rows.map(([ keyBytes, bytes, encoding , versionstamp ]) => [ encodeHex(keyBytes), ({ value: readValue(bytes, encoding, decodeV8), versionstamp })]));
        return keys.map((key, i) => {
            const row = rowMap.get(keyHexes[i]);
            return row ? { key, value: row.value, versionstamp: row.versionstamp } : { key, value: null, versionstamp: null };
        });
    }

    protected listenQueue_(handler: (value: unknown) => void | Promise<void>): Promise<void> {
        return this.queueWorker.listen(handler);
    }

    protected watch_(_keys: readonly KvKey[], _raw: boolean | undefined): ReadableStream<KvEntryMaybe<unknown>[]> {
        throw new Error(`TODO implement watch for SqliteKv`);
    }

    protected async commit(checks: AtomicCheck[], mutations: KvMutation[], enqueues: Enqueue[], additionalWork?: () => void): Promise<KvCommitResult | KvCommitError> {
        const { db, statements, encodeV8, decodeV8 } = this;
        await Promise.resolve();
        return db.transaction(() => {
            for (const { key, versionstamp } of checks) {
                if (!(versionstamp === null || typeof versionstamp === 'string' && isValidVersionstamp(versionstamp))) throw new AssertionError(`Bad 'versionstamp': ${versionstamp}`);
                const existing = querySingleValue(key, statements, decodeV8);
                if (versionstamp === null && existing) return { ok: false };
                if (typeof versionstamp === 'string' && existing?.versionstamp !== versionstamp) return { ok: false };
            }
            let minExpires: number | undefined;
            let minEnqueued: number | undefined;
            const newVersionstamp = packVersionstamp(++this.version);
            for (const { value, opts = {} } of enqueues) {
                const { delay = 0, keysIfUndelivered = [] } = opts;
                const enqueued = Date.now();
                const available = enqueued + delay;
                const { data: bytes, encoding } = packKvValue(value, encodeV8);
                const [ row ] = statements.query<[ number ]>(`insert into queue(bytes, encoding, failures, enqueued, available) values (?, ?, ?, ?, ?) returning last_insert_rowid()`, [ bytes, encoding, 0, enqueued, available ]);
                const id = row[0];
                const keyMap = Object.fromEntries(keysIfUndelivered.map(packKey).map(v => [ encodeHex(v), v ]));
                for (const key of Object.values(keyMap)) {
                    statements.query(`insert into queue_keys_if_undelivered(id, key) values (?, ?)`, [ id, key ]);
                }
                minEnqueued = Math.min(enqueued, minEnqueued ?? Number.MAX_SAFE_INTEGER);
            }
            for (const mutation of mutations) {
                const { key } = mutation;
                const keyBytes = packKey(key);
                if (mutation.type === 'set') {
                    const { value, expireIn } = mutation;
                    const expires = typeof expireIn === 'number' ? Date.now() + Math.round(expireIn) : null;
                    if (expires !== null) minExpires = Math.min(expires, minExpires ?? Number.MAX_SAFE_INTEGER);
                    const { data: bytes, encoding } = packKvValue(value, encodeV8);
                    statements.query(`insert into kv(key, bytes, encoding, versionstamp, expires) values (?, ?, ?, ?, ?) on conflict(key) do update set bytes = excluded.bytes, encoding = excluded.encoding, versionstamp = excluded.versionstamp, expires = excluded.expires`,
                        [ keyBytes, bytes, encoding, newVersionstamp, expires ]);
                } else if (mutation.type === 'delete') {
                    statements.query(`delete from kv where key = ?`, [ keyBytes ]);
                } else if (mutation.type === 'sum' || mutation.type === 'min' || mutation.type === 'max') {
                    const existing = querySingleValue<_KvU64>(key, statements, decodeV8);
                    if (existing === undefined) {
                        const { data: bytes, encoding } = packKvValue(mutation.value, encodeV8);
                        statements.query(`insert into kv(key, bytes, encoding, versionstamp) values (?, ?, ?, ?)`, [ keyBytes, bytes, encoding, newVersionstamp ]);
                    } else {
                        assertInstanceOf(existing.value, _KvU64, `Can only '${mutation.type}' on KvU64`);
                        const result = mutation.type === 'min' ? existing.value.min(mutation.value)
                            : mutation.type === 'max' ? existing.value.max(mutation.value)
                            : existing.value.sum(mutation.value);
                        const { data: bytes, encoding } = packKvValue(result, encodeV8);
                        statements.query(`update kv set bytes = ?, encoding = ?, versionstamp = ? where key = ?`, [ bytes, encoding, newVersionstamp, keyBytes ]);
                    }
                } else {
                    throw new Error(`commit(${JSON.stringify({ checks, mutations, enqueues }, replacer)}) not implemented`);
                }
            }
            if (additionalWork) additionalWork();
            statements.query(`insert into prop(name, value) values ('versionstamp', ?) on conflict(name) do update set value = excluded.value`, [ newVersionstamp ]);
            if (minExpires !== undefined) this.expirer.rescheduleExpirer(minExpires);
            if (minEnqueued !== undefined) this.queueWorker.rescheduleWorker();
            return { ok: true, versionstamp: newVersionstamp };
        });
    }

    protected close_(): void {
        this.expirer.finalize();
        this.queueWorker.finalize();
        this.statements.finalize();
        this.db.close();
    }

    protected async * listStream<T>(outCursor: CursorHolder, selector: KvListSelector, { batchSize, consistency: _, cursor: cursorOpt, limit, reverse = false }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
        const { statements, decodeV8 } = this;
        let yielded = 0;
        if (typeof limit === 'number' && yielded >= limit) return;
        const cursor = typeof cursorOpt === 'string' ? unpackCursor(cursorOpt) : undefined;
        let lastYieldedKeyBytes = cursor?.lastYieldedKeyBytes;
        let pass = 0;
        const prefixBytes = 'prefix' in selector ? packKey(selector.prefix) : undefined;
        while (true) {
            pass++;
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
            if (start.length === 0) start = new Uint8Array([ 0 ]);
            const batchLimit = Math.min(batchSize ?? 100, 500, limit ?? Number.MAX_SAFE_INTEGER) + (lastYieldedKeyBytes ? 1 : 0);

            const rows = statements.query<[ Uint8Array, Uint8Array, KvValueEncoding, string, ]>(`select key, bytes, encoding, versionstamp from kv where key >= ? and key < ? order by key ${reverse ? 'desc' : 'asc'} limit ?`, [ start, end, batchLimit ]);
            let entries = 0;
            for (const [ keyBytes, bytes, encoding, versionstamp ] of rows) {
                if (entries++ === 0 && (lastYieldedKeyBytes && equalBytes(lastYieldedKeyBytes, keyBytes) || prefixBytes && equalBytes(prefixBytes, keyBytes))) continue;
                const key = unpackKey(keyBytes);
                const value = readValue(bytes, encoding, decodeV8) as T;
                lastYieldedKeyBytes = keyBytes;
                outCursor.set(packCursor({ lastYieldedKeyBytes })); // cursor needs to be set before yield
                yield { key, value, versionstamp };
                yielded++;
                // console.log({ yielded, entries, limit });
                if (typeof limit === 'number' && yielded >= limit) return;
            }
            if (entries < batchLimit) return;
        }
    }

    //

    private async runWorker(queueHandler?: QueueHandler) {
        const { db, statements, decodeV8, debug } = this;
        if (!queueHandler) {
            if (debug) console.log(`runWorker: no queueHandler`);
            return;
        }
        const time = Date.now();
        const [ row ] = statements.query<[ number, Uint8Array, KvValueEncoding, number ]>(`update queue set locked = ? where id = (select min(id) from queue where available <= ? and locked is null) returning id, bytes, encoding, failures`, [ time, time ]);
        if (!row) {
            const nextAvailable = (statements.query<[ number ]>(`select min(available) from queue where locked is null`)).at(0)?.at(0);
            if (typeof nextAvailable === 'number') {
                const nextAvailableIn = nextAvailable - Date.now();
                if (debug) console.log(`runWorker: no work (nextAvailableIn=${nextAvailableIn}ms)`);
                this.queueWorker.rescheduleWorker(nextAvailableIn);
            } else {
                if (debug) console.log('runWorker: no work');
            }
            return;
        }
        const [ id, bytes, encoding, failures ] = row;
        const value = readValue(bytes, encoding, decodeV8);
        const deleteQueueRecords = () => {
            statements.query(`delete from queue_keys_if_undelivered where id = ?`, [ id ]);
            statements.query(`delete from queue where id = ?`, [ id ]);
        };
        try {
            if (debug) console.log(`runWorker: dispatching ${id}: ${value}`);
            await Promise.resolve(queueHandler(value));
            if (debug) console.log(`runWorker: ${id} succeeded, clearing`);
            db.transaction(deleteQueueRecords);
        } catch (e) {
            const totalFailures = failures + 1;
            if (debug) console.log(`runWorker: ${id} failed (totalFailures=${totalFailures}): ${e.stack || e}`);
            if (totalFailures >= this.maxQueueAttempts) {
                const rows = statements.query<[ Uint8Array ]>(`select key from queue_keys_if_undelivered where id = ?`, [ id ]);
                const keys = rows.map(v => unpackKey(v[0]));
                let atomic = this.atomic(deleteQueueRecords);
                for (const key of keys) atomic = atomic.set(key, value);
                await atomic.commit();
                if (debug) console.log(`runWorker: give up on ${id}, keys=${keys.length}`);
            } else {
                const available = Date.now() + 1000 * totalFailures;
                statements.query(`update queue set locked = null, failures = ?, available = ? where id = ?`, [ totalFailures, available, id ]);
            }
        }
        this.queueWorker.rescheduleWorker();
    }

}

class Statements {
    private readonly db: SqliteDb;
    private readonly debug: boolean;
    private readonly cache: Record<string, SqlitePreparedStatement<unknown[]>> = {};

    constructor(db: SqliteDb, debug: boolean) {
        this.db = db;
        this.debug = debug;
    }

    query<Row extends unknown[]>(sql: string, params?: SqliteQueryParam[]): Row[] {
        const { db, debug, cache } = this;
        let statement = cache[sql];
        if (!statement) {
            statement = db.prepareStatement<Row>(sql);
            cache[sql] = statement;
            if (debug) console.log(`statements: added new statement, size=${Object.keys(cache).length}`);
        }
        return statement.query(params) as Row[];
    }

    finalize() {
        const { cache, debug } = this;
        Object.values(cache).forEach(v => v.finalize());
        if (debug) console.log(`statements: finalized ${Object.keys(cache).length}`);
    }

}
