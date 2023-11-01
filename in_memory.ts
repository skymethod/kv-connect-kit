import { assertInstanceOf } from 'https://deno.land/std@0.204.0/assert/assert_instance_of.ts';
import { RedBlackTree } from 'https://deno.land/std@0.204.0/collections/unstable/red_black_tree.ts';
import { Mutex } from 'https://deno.land/x/semaphore@v1.1.2/mutex.ts';
import { compareBytes, equalBytes } from './bytes.ts';
import { packKey, unpackKey } from './kv_key.ts';
import { AtomicCheck, KvCommitError, KvCommitResult, KvConsistencyLevel, KvEntry, KvEntryMaybe, KvKey, KvListOptions, KvListSelector, KvMutation, KvService, KvU64 } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { BaseKv, CursorHolder, Enqueue, isValidVersionstamp, packCursor, packVersionstamp, replacer, unpackCursor } from './kv_util.ts';

export interface InMemoryServiceOptions {

    /** Enable some console logging */
    readonly debug?: boolean;

}

/**
 * Creates a new KvService that creates in-memory kv instances
 */
export function makeInMemoryService(opts: InMemoryServiceOptions = {}): KvService {
    const { debug = false } = opts;
    const instances = new Map<string, InMemoryKv>();
    return {
        openKv: (url = '') => {
            let kv = instances.get(url);
            if (!kv) {
                if (debug) console.log(`makeInMemoryService: new kv(${url})`);
                kv = new InMemoryKv(debug);
                instances.set(url, kv);
            }
            return Promise.resolve(kv);
        },
        newKvU64: value => new _KvU64(value),
        isKvU64: (obj: unknown): obj is KvU64 => obj instanceof _KvU64,
    }
}

//

type KVRow = [ keyBytes: Uint8Array, value: unknown, versionstamp: string, expires: number | undefined ];

const keyRow = (keyBytes: Uint8Array): KVRow => [ keyBytes, undefined, '', undefined ];

class InMemoryKv extends BaseKv {

    private readonly rows = new RedBlackTree<KVRow>((a, b) => compareBytes(a[0], b[0])); // keep sorted by keyBytes
    private readonly mutex = new Mutex();

    private version = 0;

    constructor(debug: boolean) {
        super({ debug });
    }

    protected async get_<T = unknown>(key: KvKey, _consistency: KvConsistencyLevel | undefined): Promise<KvEntryMaybe<T>> {
        return await this.mutex.use(() => Promise.resolve(this.getLocked(key)));
    }

    protected async getMany_(keys: readonly KvKey[], _consistency: KvConsistencyLevel | undefined): Promise<KvEntryMaybe<unknown>[]> {
        return await this.mutex.use(() => Promise.resolve(keys.map(v => this.getLocked(v))));
    }

    private getLocked<T>(key: KvKey): KvEntryMaybe<T> {
        const row = this.rows.find(keyRow(packKey(key)));
        return row ? { key, value: row[1] as T, versionstamp: row[2] } : { key, value: null, versionstamp: null };
    }

    protected async * listStream<T>(outCursor: CursorHolder, selector: KvListSelector, { batchSize, consistency: _, cursor: cursorOpt, limit, reverse = false }: KvListOptions = {}): AsyncGenerator<KvEntry<T>> {
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

            const rows = this.listRows({ start, end, reverse, batchLimit });
            let entries = 0;
            for (const [ keyBytes, value, versionstamp ] of rows) {
                if (entries++ === 0 && (lastYieldedKeyBytes && equalBytes(lastYieldedKeyBytes, keyBytes) || prefixBytes && equalBytes(prefixBytes, keyBytes))) continue;
                const key = unpackKey(keyBytes);
                lastYieldedKeyBytes = keyBytes;
                outCursor.set(packCursor({ lastYieldedKeyBytes })); // cursor needs to be set before yield
                yield { key, value: value as T, versionstamp };
                yielded++;
                // console.log({ yielded, entries, limit });
                if (typeof limit === 'number' && yielded >= limit) return;
            }
            if (entries < batchLimit) return;
        }
    }

    protected listenQueue_(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        // TODO listen
        throw new Error('Method not implemented.');
    }

    protected async commit(checks: AtomicCheck[], mutations: KvMutation[], enqueues: Enqueue[], additionalWork?: (() => void) | undefined): Promise<KvCommitResult | KvCommitError> {
        const { rows, mutex } = this;
        return await mutex.use(async () => {
            await Promise.resolve();
            for (const { key, versionstamp } of checks) {
                if (!(versionstamp === null || typeof versionstamp === 'string' && isValidVersionstamp(versionstamp))) throw new Error(`Bad 'versionstamp': ${versionstamp}`);
                const existing = rows.find(keyRow(packKey(key)));
                if (versionstamp === null && existing) return { ok: false };
                if (typeof versionstamp === 'string' && existing?.at(2) !== versionstamp) return { ok: false };
            }
            let minExpires: number | undefined;
            let minEnqueued: number | undefined;
            const newVersionstamp = packVersionstamp(++this.version);
            for (const { value, opts = {} } of enqueues) {
                // TODO implement enqueues/worker
                throw new Error(`enqueues not implemented`);
                // const { delay = 0, keysIfUndelivered = [] } = opts;
                // const enqueued = Date.now();
                // const available = enqueued + delay;
                // const { data: bytes, encoding } = packKvValue(value, encodeV8);
                // const [ row ] = statements.query<[ number ]>(`insert into queue(bytes, encoding, failures, enqueued, available) values (?, ?, ?, ?, ?) returning last_insert_rowid()`, [ bytes, encoding, 0, enqueued, available ]);
                // const id = row[0];
                // const keyMap = Object.fromEntries(keysIfUndelivered.map(packKey).map(v => [ encodeHex(v), v ]));
                // for (const key of Object.values(keyMap)) {
                //     statements.query(`insert into queue_keys_if_undelivered(id, key) values (?, ?)`, [ id, key ]);
                // }
                // minEnqueued = Math.min(enqueued, minEnqueued ?? Number.MAX_SAFE_INTEGER);
            }
            for (const mutation of mutations) {
                const { key } = mutation;
                const keyBytes = packKey(key);
                if (mutation.type === 'set') {
                    const { value, expireIn } = mutation;
                    const expires = typeof expireIn === 'number' ? Date.now() + Math.round(expireIn) : undefined;
                    if (expires !== undefined) minExpires = Math.min(expires, minExpires ?? Number.MAX_SAFE_INTEGER);
                    rows.remove(keyRow(keyBytes));
                    rows.insert([ keyBytes, structuredClone(value), newVersionstamp, expires ]);
                } else if (mutation.type === 'delete') {
                    rows.remove(keyRow(keyBytes));
                } else if (mutation.type === 'sum' || mutation.type === 'min' || mutation.type === 'max') {
                    const existing = rows.find(keyRow(keyBytes));
                    if (!existing) {
                        rows.insert([ keyBytes, mutation.value, newVersionstamp, undefined ]);
                    } else {
                        const existingValue = existing[1];
                        assertInstanceOf(existingValue, _KvU64, `Can only '${mutation.type}' on KvU64`);
                        const result = mutation.type === 'min' ? existingValue.min(mutation.value)
                            : mutation.type === 'max' ? existingValue.max(mutation.value)
                            : existingValue.sum(mutation.value);
                        rows.remove(keyRow(keyBytes));
                        rows.insert([ keyBytes, result, newVersionstamp, undefined ]);
                    }
                } else {
                    throw new Error(`commit(${JSON.stringify({ checks, mutations, enqueues }, replacer)}) not implemented`);
                }
            }
            if (additionalWork) additionalWork();
            // TODO expiry
            // if (minExpires !== undefined) this.rescheduleExpirer(minExpires);
            // if (minEnqueued !== undefined) this.rescheduleWorker();

            return { ok: true, versionstamp: newVersionstamp };
        });
    }

    protected close_(): void {
        throw new Error('Method not implemented.');
    }

    //

    listRows(opts: { start: Uint8Array, end: Uint8Array, reverse: boolean, batchLimit: number }): KVRow[] {
        return [];
    }

}
