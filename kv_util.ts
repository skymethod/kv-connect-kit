import { decodeHex, encodeHex } from './bytes.ts';
import { checkKeyNotEmpty, checkExpireIn } from './check.ts';
import { AtomicCheck, AtomicOperation, KvCommitError, KvCommitResult, KvEntry, KvKey, KvListIterator, KvMutation } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { decode as decodeBase64, encode as encodeBase64 } from './proto/runtime/base64.ts';

export type EncodeV8 = (value: unknown) => Uint8Array;
export type DecodeV8 = (bytes: Uint8Array) => unknown;

export type KvValueEncoding =
| 'VE_UNSPECIFIED'
| 'VE_V8'
| 'VE_LE64'
| 'VE_BYTES';

export type KvValue = {
    data: Uint8Array;
    encoding: KvValueEncoding;
}

export function unpackKvu(bytes: Uint8Array): _KvU64 {
    if (bytes.length !== 8) throw new Error();
    if (bytes.buffer.byteLength !== 8) bytes = new Uint8Array(bytes);
    const rt = new DataView(bytes.buffer).getBigUint64(0, true);
    return new _KvU64(rt);
}

export function packKvu(value: _KvU64): Uint8Array {
    const rt = new Uint8Array(8);
    new DataView(rt.buffer).setBigUint64(0, value.value, true);
    return rt;
}

export function readValue(bytes: Uint8Array, encoding: KvValueEncoding, decodeV8: DecodeV8) {
    if (encoding === 'VE_V8') return decodeV8(bytes);
    if (encoding === 'VE_LE64') return unpackKvu(bytes);
    if (encoding === 'VE_BYTES') return bytes;
    throw new Error(`Unsupported encoding: ${encoding} [${[...bytes].join(', ')}]`);
}

export function packKvValue(value: unknown, encodeV8: EncodeV8): KvValue {
    if (value instanceof _KvU64) return { encoding: 'VE_LE64', data: packKvu(value) };
    if (value instanceof Uint8Array) return { encoding: 'VE_BYTES', data: value };
    return { encoding: 'VE_V8',  data: encodeV8(value) };
}

export type Cursor = { lastYieldedKeyBytes: Uint8Array }

export function packCursor({ lastYieldedKeyBytes }: Cursor): string {
    return encodeBase64(JSON.stringify({ lastYieldedKeyBytes: encodeHex(lastYieldedKeyBytes) }));
}

export function unpackCursor(str: string): Cursor {
    try {
        const { lastYieldedKeyBytes } = JSON.parse(new TextDecoder().decode(decodeBase64(str)));
        if (typeof lastYieldedKeyBytes === 'string') return { lastYieldedKeyBytes: decodeHex(lastYieldedKeyBytes) };
    } catch {
        // noop
    }
    throw new Error(`Invalid cursor`);
}

export class GenericKvListIterator<T> implements KvListIterator<T> {
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

type Enqueue = { value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] } };
type CommitFn = (checks: AtomicCheck[], mutations: KvMutation[], enqueues: Enqueue[]) => Promise<KvCommitResult | KvCommitError>;

export class GenericAtomicOperation implements AtomicOperation {

    private readonly commitFn: CommitFn;

    private readonly checks: AtomicCheck[] = [];
    private readonly mutations: KvMutation[] = [];
    private readonly enqueues: Enqueue[] = [];

    constructor(commit: CommitFn) {
        this.commitFn = commit;
    }

    check(...checks: AtomicCheck[]): this {
        this.checks.push(...checks);
        return this;
    }

    mutate(...mutations: KvMutation[]): this {
        mutations.map(v => v.key).forEach(checkKeyNotEmpty);
        mutations.forEach(v => v.type === 'set' && checkExpireIn(v.expireIn));
        this.mutations.push(...mutations);
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
        return this.mutate({ type: 'set', key, value, expireIn });
    }

    delete(key: KvKey): this {
        checkKeyNotEmpty(key);
        return this.mutate({ type: 'delete', key });
    }

    enqueue(value: unknown, opts?: { delay?: number, keysIfUndelivered?: KvKey[] }): this {
        this.enqueues.push({ value, opts });
        return this;
    }

    async commit(): Promise<KvCommitResult | KvCommitError> {
        return await this.commitFn(this.checks, this.mutations, this.enqueues);
    }

}
