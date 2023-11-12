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
    open(path: string): number;
    close(db: number): void;
    snapshotRead(dbId: number, snapshotReadBytes: Uint8Array): Promise<Uint8Array>;
    atomicWrite(dbId: number, atomicWriteBytes: Uint8Array): Promise<Uint8Array>;
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
        const dbId = napi.open(url);
        return new NapiBasedKv(debug, napi, dbId, decodeV8, encodeV8);
    }

    protected listenQueue_(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        throw new Error(`'listenQueue' is not implemented over napi TODO`);
    }

    protected close_(): void {
        const { napi, dbId } = this;
        napi.close(dbId);
    }

    protected async snapshotRead(req: SnapshotRead, _consistency?: KvConsistencyLevel): Promise<SnapshotReadOutput> {
        const { napi, dbId } = this;
        const res = await napi.snapshotRead(dbId, encodeSnapshotRead(req));
        return decodeSnapshotReadOutput(res);
    }

    protected async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
        const { napi, dbId } = this;
        const res = await napi.atomicWrite(dbId, encodeAtomicWrite(req));
        return decodeAtomicWriteOutput(res);
    }

}
