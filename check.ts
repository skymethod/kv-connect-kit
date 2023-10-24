import { KvKey } from './kv_types.ts';

export function isRecord(obj: unknown): obj is Record<string, unknown> {
    return typeof obj === 'object' && obj !== null && !Array.isArray(obj) && obj.constructor === Object;
}

export function checkKeyNotEmpty(key: KvKey): void {
    if (key.length === 0) throw new Error(`Key cannot be empty`);
}

export function checkExpireIn(expireIn: number | undefined): void {
    const valid = expireIn === undefined || typeof expireIn === 'number' && expireIn > 0 && Number.isSafeInteger(expireIn);
    if (!valid) throw new Error(`Bad 'expireIn', expected optional positive integer, found ${expireIn}`);
}
