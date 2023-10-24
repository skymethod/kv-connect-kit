import { KvU64 } from './kv_types.ts';

export class _KvU64 implements KvU64 {
    readonly value: bigint;

    constructor(value: bigint) {
        if (value < 0n) throw new Error('value must be a positive bigint');
        this.value = value;
    }

}
