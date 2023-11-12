import { DatabaseMetadata, KvConnectProtocolVersion, fetchAtomicWrite, fetchDatabaseMetadata, fetchSnapshotRead } from './kv_connect_api.ts';
import { KvConsistencyLevel, KvService, KvU64 } from './kv_types.ts';
import { _KvU64 } from './kv_u64.ts';
import { DecodeV8, EncodeV8 } from './kv_util.ts';
import { encodeJson as encodeJsonAtomicWrite } from './proto/messages/com/deno/kv/datapath/AtomicWrite.ts';
import { encodeJson as encodeJsonSnapshotRead } from './proto/messages/com/deno/kv/datapath/SnapshotRead.ts';
import { AtomicWrite, AtomicWriteOutput, SnapshotRead, SnapshotReadOutput } from './proto/messages/com/deno/kv/datapath/index.ts';
import { ProtoBasedKv } from './proto_based.ts';
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from './v8.ts';

type Fetcher = typeof fetch;

export interface RemoteServiceOptions {
    /** Access token used to authenticate to the remote service */
    readonly accessToken: string;

    /** Wrap unsupported V8 payloads to instances of UnknownV8 instead of failing.
     * 
     * Only applicable when using the default serializer. */
    readonly wrapUnknownValues?: boolean;

    /** Enable some console logging */
    readonly debug?: boolean;

    /** Custom serializer to use when serializing v8-encoded KV values.
     * 
     * When you are running on Node 18+, pass the 'serialize' function in Node's 'v8' module. */
    readonly encodeV8?: EncodeV8;

    /** Custom deserializer to use when deserializing v8-encoded KV values.
     * 
     * When you are running on Node 18+, pass the 'deserialize' function in Node's 'v8' module. */
    readonly decodeV8?: DecodeV8;

    /** Custom fetcher to use for the underlying http calls.
     * 
     * Defaults to global 'fetch'`
     */
    readonly fetcher?: Fetcher;

    /** Max number of times to attempt to retry certain fetch errors (like 5xx) */
    readonly maxRetries?: number;

    /** Limit to specific KV Connect protocol versions */
    readonly supportedVersions?: KvConnectProtocolVersion[];
}

/**
 * Creates a new KvService instance that can be used to open a remote KV database.
 */
export function makeRemoteService(opts: RemoteServiceOptions): KvService {
    return {
        openKv: async (url) => await RemoteKv.of(url, opts),
        newKvU64: value => new _KvU64(value),
        isKvU64: (obj: unknown): obj is KvU64 => obj instanceof _KvU64,
    }
}

//

function resolveEndpointUrl(url: string, responseUrl: string): string {
    const u = new URL(url, responseUrl);
    const str = u.toString();
    return u.pathname === '/' ? str.substring(0, str.length - 1) : str;
}

async function fetchNewDatabaseMetadata(url: string, accessToken: string, debug: boolean, fetcher: Fetcher, maxRetries: number, supportedVersions: KvConnectProtocolVersion[]): Promise<DatabaseMetadata> {
    if (debug) console.log(`fetchNewDatabaseMetadata: Fetching ${url}...`);
    const { metadata, responseUrl } = await fetchDatabaseMetadata(url, accessToken, fetcher, maxRetries, supportedVersions);
    const { version, endpoints, token } = metadata;
    if (version !== 1 && version !== 2 || !supportedVersions.includes(version)) throw new Error(`Unsupported version: ${version}`);
    if (debug) console.log(`fetchNewDatabaseMetadata: Using protocol version ${version}`);
    if (typeof token !== 'string' || token === '') throw new Error(`Unsupported token: ${token}`);
    if (endpoints.length === 0) throw new Error(`No endpoints`);
    const expiresMillis = computeExpiresInMillis(metadata);
    if (debug) console.log(`fetchNewDatabaseMetadata: Expires in ${Math.round((expiresMillis / 1000 / 60))} minutes`); // expect 60 minutes
    const responseEndpoints = endpoints.map(({ url, consistency }) => ({ url: resolveEndpointUrl(url, responseUrl), consistency })); // metadata url might have been redirected
    if (debug) responseEndpoints.forEach(({ url, consistency }) => console.log(`fetchNewDatabaseMetadata: ${url} (${consistency})`));
    return { ...metadata, endpoints: responseEndpoints };
}

function computeExpiresInMillis({ expiresAt }: DatabaseMetadata): number {
    const expiresTime = new Date(expiresAt).getTime();
    return expiresTime - Date.now();
}

function isValidHttpUrl(url: string): boolean {
    try {
        const { protocol } = new URL(url);
        return protocol === 'http:' || protocol === 'https:';
    } catch {
        return false;
    }
}

function snapshotReadToString(req: SnapshotRead): string {
    return JSON.stringify(encodeJsonSnapshotRead(req));
}

function atomicWriteToString(req: AtomicWrite): string {
    return JSON.stringify(encodeJsonAtomicWrite(req));
}

//

class RemoteKv extends ProtoBasedKv {

    private readonly url: string;
    private readonly accessToken: string;
    private readonly fetcher: Fetcher;
    private readonly maxRetries: number;
    private readonly supportedVersions: KvConnectProtocolVersion[];

    private metadata: DatabaseMetadata;

    private constructor(url: string, accessToken: string, debug: boolean, encodeV8: EncodeV8, decodeV8: DecodeV8, fetcher: Fetcher, maxRetries: number, supportedVersions: KvConnectProtocolVersion[], metadata: DatabaseMetadata) {
        super(debug, decodeV8, encodeV8);
        this.url = url;
        this.accessToken = accessToken;
        this.fetcher = fetcher;
        this.maxRetries = maxRetries;
        this.supportedVersions = supportedVersions;
        this.metadata = metadata;
    }

    static async of(url: string | undefined, opts: RemoteServiceOptions) {
        const { accessToken, wrapUnknownValues = false, debug = false, fetcher = fetch, maxRetries = 10, supportedVersions = [ 1, 2 ] } = opts;
        if (url === undefined || !isValidHttpUrl(url)) throw new Error(`'path' must be an http(s) url`);
        const metadata = await fetchNewDatabaseMetadata(url, accessToken, debug, fetcher, maxRetries, supportedVersions);
        
        const encodeV8: EncodeV8 = opts.encodeV8 ?? _encodeV8;
        const decodeV8: DecodeV8 = opts.decodeV8 ?? (v => _decodeV8(v, { wrapUnknownValues }));

        return new RemoteKv(url, accessToken, debug, encodeV8, decodeV8, fetcher, maxRetries, supportedVersions, metadata);
    }

    protected listenQueue_(_handler: (value: unknown) => void | Promise<void>): Promise<void> {
        throw new Error(`'listenQueue' is not possible over KV Connect`);
    }

    protected close_(): void {
        // no persistent resources yet
    }

    protected async snapshotRead(req: SnapshotRead, consistency: KvConsistencyLevel = 'strong'): Promise<SnapshotReadOutput> {
        const { url, accessToken, metadata, debug, fetcher, maxRetries, supportedVersions } = this;
        const read = async () => {
            const endpointUrl = await this.locateEndpointUrl(consistency);
            const snapshotReadUrl = `${endpointUrl}/snapshot_read`;
            const accessToken = metadata.token;
            if (debug) console.log(`snapshotRead: ${snapshotReadToString(req)}`);
            return await fetchSnapshotRead(snapshotReadUrl, accessToken, metadata.databaseId, req, fetcher, maxRetries, metadata.version);
        }
        const responseCheck = (res: SnapshotReadOutput) => !(this.metadata.version > 1 && (res.readDisabled || consistency === 'strong' && !res.readIsStronglyConsistent));
        const res = await read();
        if (!responseCheck(res)) {
            if (debug) if (debug) console.log(`snapshotRead: response checks failed, refresh metadata and retry`);
            this.metadata = await fetchNewDatabaseMetadata(url, accessToken, debug, fetcher, maxRetries, supportedVersions);
            const res = await read();
            if (!responseCheck(res)) {
                const { readDisabled, readIsStronglyConsistent } = res;
                throw new Error(`snapshotRead: response checks failed after retry: ${JSON.stringify({ readDisabled, readIsStronglyConsistent })}`);
            }
            return res;
        } else {
            return res;
        }
    }

    protected async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
        const { metadata, debug, fetcher, maxRetries } = this;
        const endpointUrl = await this.locateEndpointUrl('strong');
        const atomicWriteUrl = `${endpointUrl}/atomic_write`;
        const accessToken = metadata.token;
        if (debug) console.log(`fetchAtomicWrite: ${atomicWriteToString(req)}`);
        return await fetchAtomicWrite(atomicWriteUrl, accessToken, metadata.databaseId, req, fetcher, maxRetries, metadata.version);
    }

    //

    private async locateEndpointUrl(consistency: KvConsistencyLevel): Promise<string> {
        const { url, accessToken, debug, fetcher, maxRetries, supportedVersions } = this;
        if (computeExpiresInMillis(this.metadata) < 1000 * 60 * 5) {
            this.metadata = await fetchNewDatabaseMetadata(url, accessToken, debug, fetcher, maxRetries, supportedVersions);
        }
        const { metadata } = this;
        const firstStrong = metadata.endpoints.filter(v => v.consistency === 'strong')[0];
        const firstNonStrong = metadata.endpoints.filter(v => v.consistency !== 'strong')[0];
        const endpoint = consistency === 'strong' ? firstStrong : (firstNonStrong ?? firstStrong);
        if (endpoint === undefined) throw new Error(`Unable to find endpoint for: ${consistency}`);
        return endpoint.url; // guaranteed not to end in "/"
    }
    
}
