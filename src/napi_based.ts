import { isRecord } from './check.ts';
import { decodeAtomicWriteOutput, decodeSnapshotReadOutput, encodeAtomicWrite, encodeSnapshotRead } from './kv_connect_api.ts';
import { KvConsistencyLevel, KvService, KvU64 } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { DecodeV8, EncodeV8 } from './kv_util.ts';
import { AtomicWrite, AtomicWriteOutput, SnapshotRead, SnapshotReadOutput } from './proto/messages/com/deno/kv/datapath/index.ts';
import { ProtoBasedKv } from './proto_based.ts';
import { encodeV8 } from './v8.ts';

export interface NapiBasedServiceOptions {

    /** Enable some console logging */
    readonly debug?: boolean;

    readonly napi: NapiInterface;

    readonly decodeV8: DecodeV8;
    readonly encodeV8: EncodeV8;

}

export function makeNapiBasedService(opts: NapiBasedServiceOptions): KvService {
    return {
        openKv: v => Promise.resolve(NapiBasedKv.of(v, opts)),
        newKvU64: value => new _KvU64(value),
        isKvU64: (obj: unknown): obj is KvU64 => obj instanceof _KvU64,
    }
}

export interface NapiInterface {
    open(path: string, debug: boolean): number;
    close(db: number, debug: boolean): void;
    snapshotRead(dbId: number, snapshotReadBytes: Uint8Array, debug: boolean): Promise<Uint8Array>;
    atomicWrite(dbId: number, atomicWriteBytes: Uint8Array, debug: boolean): Promise<Uint8Array>;
    dequeueNextMessage(dbId: number, debug: boolean): Promise<{ bytes: Uint8Array, messageId: number } | undefined>;
    finishMessage(dbId: number, messageId: number, success: boolean, debug: boolean): Promise<void>;
}

export function isNapiInterface(obj: unknown): obj is NapiInterface {
    return isRecord(obj) 
        && typeof obj.open === 'function'
        && typeof obj.close === 'function'
        && typeof obj.snapshotRead === 'function'
        && typeof obj.atomicWrite === 'function'
        ;
}

//

class NapiBasedKv extends ProtoBasedKv {

    private readonly napi: NapiInterface;
    private readonly dbId: number;

    constructor(debug: boolean, napi: NapiInterface, dbId: number, decodeV8: DecodeV8, encodeV8: EncodeV8) {
        super(debug, decodeV8, encodeV8);
        this.napi = napi;
        this.dbId = dbId;
    }

    static of(url: string | undefined, opts: NapiBasedServiceOptions): NapiBasedKv {
        const { debug = false, napi, decodeV8 } = opts;
        if (typeof url !== 'string' || /^https?:\/\//i.test(url)) throw new Error(`Invalid path: ${url}`);
        const dbId = napi.open(url, debug);
        return new NapiBasedKv(debug, napi, dbId, decodeV8, encodeV8);
    }

    protected async listenQueue_(handler: (value: unknown) => void | Promise<void>): Promise<void> {
        const { napi, dbId, decodeV8, debug } = this;
        while (true) {
            if (debug) console.log(`listenQueue_: before dequeueNextMessage`);
            const result = await napi.dequeueNextMessage(dbId, debug);
            if (result === undefined) return;
            const { bytes, messageId } = result;
            const value = decodeV8(bytes);
            if (debug) console.log(`listenQueue_: after value ${value}`);

            try {
                await Promise.resolve(handler(value));
                await napi.finishMessage(dbId, messageId, true, debug);
            } catch (e) {
                if (debug) console.log(`listenQueue_: handler failed ${e.stack || e}`);
                await napi.finishMessage(dbId, messageId, false, debug);
            }
        }
    }

    protected close_(): void {
        const { napi, dbId, debug } = this;
        napi.close(dbId, debug);
    }

    protected async snapshotRead(req: SnapshotRead, _consistency?: KvConsistencyLevel): Promise<SnapshotReadOutput> {
        const { napi, dbId, debug } = this;
        const res = await napi.snapshotRead(dbId, encodeSnapshotRead(req), debug);
        return decodeSnapshotReadOutput(res);
    }

    protected async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
        const { napi, dbId, debug } = this;
        const res = await napi.atomicWrite(dbId, encodeAtomicWrite(req), debug);
        return decodeAtomicWriteOutput(res);
    }

}
