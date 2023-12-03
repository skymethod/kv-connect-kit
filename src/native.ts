import { KvService } from './kv_types.ts';
import { DecodeV8, EncodeV8 } from "./kv_util.ts";

/**
 * Creates a new KvService instance that can be used to access Deno's native implementation (only works in the Deno runtime!)
 * 
 * Requires the --unstable flag to `deno run` and any applicable --allow-read/allow-write/allow-net flags
 */
export function makeNativeService(): KvService {
    if ('Deno' in globalThis) {
        // deno-lint-ignore no-explicit-any
        const { openKv } = (globalThis as any).Deno;
        if (typeof openKv === 'function') {
            return {
                // deno-lint-ignore no-explicit-any
                openKv: openKv as any,
            }
        }
    }
    throw new Error(`Global 'Deno.openKv' not found`);
}

/**
 * Returns the V8 serializer used by Deno (unstable, internal api), if available
 */
export function tryReturnNativeInternalV8Serializers(): { encodeV8: EncodeV8, decodeV8: DecodeV8 } | undefined {
    if ('Deno' in globalThis) {
        // deno-lint-ignore no-explicit-any
        const deno = (globalThis as any).Deno;
        if ('internal' in deno) {
            const internals = deno[deno.internal];
            if (typeof internals === 'object') {
                const { core } = internals;
                if (typeof core === 'object') {
                    const { serialize, deserialize } = core;
                    if (typeof serialize === 'function' && typeof deserialize === 'function') {
                        return { encodeV8: serialize, decodeV8: deserialize };
                    }
                }
            }
        }
    }
}
