const max = (1n << 64n) - 1n;

export class _KvU64 {
    readonly value: bigint;

    constructor(value: bigint) {
        if (typeof value !== 'bigint') throw new TypeError('value must be a bigint');
        if (value < 0n) throw new Error('value must be a positive bigint');
        if (value > max) throw new Error('value must fit in a 64-bit unsigned integer');
        this.value = value;
    }

    sum(other: { readonly value: bigint }): _KvU64 {
        return new _KvU64((this.value + other.value) % (1n << 64n));
    }

    min(other: { readonly value: bigint }): _KvU64 {
        return other.value < this.value ? new _KvU64(other.value) : this;
    }

    max(other: { readonly value: bigint }): _KvU64 {
        return other.value > this.value ? new _KvU64(other.value) : this;
    }

}
