import { KvService, KvU64 } from './kv_types.ts';

/**
 * Creates a new KvService instance that can be used to access Deno's native implementation (only works in the Deno runtime!)
 * 
 * Requires the --unstable flag to `deno run` and any applicable --allow-read/allow-write/allow-net flags
 */
export function makeNativeService(): KvService {
    if ('Deno' in globalThis) {
        // deno-lint-ignore no-explicit-any
        const { openKv, KvU64 } = (globalThis as any).Deno;
        if (typeof openKv === 'function' && typeof KvU64 === 'function') {
            return {
                // deno-lint-ignore no-explicit-any
                openKv: openKv as any,
                newKvU64: value => new KvU64(value),
                isKvU64: (obj: unknown): obj is KvU64 => obj instanceof KvU64,
            }
        }
    }
    throw new Error(`Global 'Deno.openKv' or 'Deno.KvU64' not found`);
}
