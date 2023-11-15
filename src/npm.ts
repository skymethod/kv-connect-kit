import { makeInMemoryService } from './in_memory.ts';
import { DecodeV8, EncodeV8 } from './kv_util.ts';
import { isNapiInterface, makeNapiBasedService } from './napi_based.ts';
import { makeRemoteService } from './remote.ts';

export * from './napi_based.ts';
export * from './remote.ts';
export * from './in_memory.ts';
export * from './kv_types.ts';
export { UnknownV8 } from './v8.ts';

export async function openKv(path?: string, opts: Record<string, unknown> & { debug?: boolean } = {}) {
    const debug = opts.debug === true;

    if (path === undefined || path === '') return await makeInMemoryService({ debug }).openKv(path);
    
    const { encodeV8, decodeV8 } = await (async () => {
        const { encodeV8, decodeV8 } = opts;
        const defined = [ encodeV8, decodeV8 ].filter(v => v !== undefined).length;
        if (defined === 1) throw new Error(`Provide both 'encodeV8' or 'decodeV8', or neither`);
        if (defined > 0) {
            if (typeof encodeV8 !== 'function') throw new Error(`Unexpected 'encodeV8': ${encodeV8}`);
            if (typeof decodeV8 !== 'function') throw new Error(`Unexpected 'decodeV8': ${decodeV8}`);
            return { encodeV8: encodeV8 as EncodeV8, decodeV8: decodeV8 as DecodeV8 };
        }
        const v8 = await import(`${'v8'}`);
        if (!v8) throw new Error(`Unable to import the v8 module`);
        const { serialize, deserialize } = v8;
        if (typeof serialize !== 'function') throw new Error(`Unexpected 'serialize': ${serialize}`);
        if (typeof deserialize !== 'function') throw new Error(`Unexpected 'deserialize': ${deserialize}`);
        return { encodeV8: serialize as EncodeV8, decodeV8: deserialize as DecodeV8 };
    })();

    if (/^https?:\/\//i.test(path)) {
        // deno-lint-ignore no-explicit-any
        const accessToken = (globalThis as any)?.process?.env?.DENO_KV_ACCESS_TOKEN;
        if (typeof accessToken !== 'string') throw new Error(`Set the DENO_KV_ACCESS_TOKEN to your access token`);
        return await makeRemoteService({ debug, accessToken, encodeV8, decodeV8 }).openKv(path);
    }
    const { napi } = opts;
    if (!isNapiInterface(napi)) throw new Error(`Provide the napi interface for sqlite`);
    return await makeNapiBasedService({ debug, encodeV8, decodeV8, napi }).openKv(path);
}
