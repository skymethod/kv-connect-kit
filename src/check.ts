import { AssertionError } from 'https://deno.land/std@0.208.0/assert/assertion_error.ts';
import { KvKey } from './kv_types.ts';

export function isRecord(obj: unknown): obj is Record<string, unknown> {
    return typeof obj === 'object' && obj !== null && !Array.isArray(obj) && obj.constructor === Object;
}

export function isDateTime(value: string): boolean {
    return typeof value === 'string' && /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?Z$/.test(value);
}

export function checkKeyNotEmpty(key: KvKey): void {
    if (key.length === 0) throw new AssertionError(`Key cannot be empty`);
}

export function checkExpireIn(expireIn: number | undefined): void {
    const valid = expireIn === undefined || typeof expireIn === 'number' && expireIn > 0 && Number.isSafeInteger(expireIn);
    if (!valid) throw new AssertionError(`Bad 'expireIn', expected optional positive integer, found ${expireIn}`);
}

export function checkMatches(name: string, value: string, pattern: RegExp): RegExpMatchArray {
    const m = pattern.exec(value);
    if (!m) throw new AssertionError(`Bad '${name}': ${value}`);
    return m;
}

export function checkString(name: string, value: unknown): asserts value is string {
    if (typeof value !== 'string') throw new AssertionError(`Bad '${name}': expected string, found ${typeof value}`);
}
