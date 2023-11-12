import { makeInMemoryService } from './in_memory.ts';
import { DecodeV8, EncodeV8 } from './kv_util.ts';
import { isNapiInterface, makeNapiBasedService } from './napi_based.ts';
import { makeRemoteService } from './remote.ts';

export * from './napi_based.ts';
export * from './remote.ts';
export { UnknownV8 } from './v8.ts';

export async function openKv(path?: string, opts: Record<string, unknown> & { debug?: boolean } = {}) {
    const debug = opts.debug === true;
    if (path === undefined || path === '') return await makeInMemoryService({ debug }).openKv(path);
    
    const v8 = await import(`${'v8'}`);
    if (!v8) throw new Error(`Unable to import the v8 module`);
    const encodeV8: EncodeV8 = v8.serialize;
    const decodeV8: DecodeV8 = v8.deserialize;
    if (/^https?:\/\//i.test(path)) {
        // deno-lint-ignore no-explicit-any
        const accessToken = (globalThis as any)?.process?.env?.DENO_KV_ACCESS_TOKEN;
        if (typeof accessToken !== 'string') throw new Error(`Set the DENO_KV_ACCESS_TOKEN to your access token`);
        return await makeRemoteService({ debug, accessToken }).openKv(path);
    }
    const { napi } = opts;
    if (!isNapiInterface(napi)) throw new Error(`Provide the napi interface for sqlite`);
    return await makeNapiBasedService({ debug, encodeV8, decodeV8, napi }).openKv(path);
}
