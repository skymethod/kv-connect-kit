import { checkKeyNotEmpty, checkExpireIn } from './check.ts';
import { AtomicCheck, AtomicOperation, KvCommitError, KvCommitResult, KvEntry, KvKey, KvListIterator, KvMutation } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';

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
