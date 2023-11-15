# kv-connect-kit

Minimal Typescript client implementing the [KV Connect protocol](https://github.com/denoland/deno/tree/main/ext/kv#kv-connect). Access [Deno KV](https://deno.com/kv) remotely from any Javascript environment like Node, Cloudflare Workers, Bun, Deno, or the browser. 

- Use `makeRemoteService` to provide your access token and configure optional setup params
- Use the returned service to call `openKv` (equiv to [`Deno.openKv`](https://deno.land/api?s=Deno.openKv&unstable)) and `newKvU64` (equiv to [`new Deno.KvU64`](https://deno.land/api?s=Deno.KvU64&unstable))
- The `KV` instance returned can be interacted with via the standard [KV api](https://deno.land/api?s=Deno.Kv&unstable) in any Javascript environment.

### Quick start - Deno

```ts
import { makeRemoteService } from 'https://raw.githubusercontent.com/skymethod/kv-connect-kit/v0.0.5/src/remote.ts';

const accessToken = Deno.env.get('DENO_KV_ACCESS_TOKEN');
if (accessToken === undefined) throw new Error(`Set your personal access token: https://dash.deno.com/account#access-tokens`);

// make a local openKv function, optionally wrap unsupported serialized byte values as UnknownV8
const { openKv } = makeRemoteService({ accessToken, wrapUnknownValues: true });

// open the database connection, use the url from project dashboard: https://dash.deno.com/projects/YOUR_PROJECT/kv
const kv = await openKv('https://api.deno.com/databases/YOUR_DATABASE_ID/connect');

// do anything using the kv api: https://deno.land/api?s=Deno.Kv&unstable
const result = await kv.set([ 'from-client' ], 'hello!');
console.log(result);

// close the database connection
kv.close();
```

### Quick start - Bun
_Use the [NPM package](https://www.npmjs.com/package/kv-connect-kit)_ 👉 `bun install kv-connect-kit`
```ts
import { makeRemoteService } from 'kv-connect-kit';

const accessToken = Bun.env['DENO_KV_ACCESS_TOKEN'];
if (accessToken === undefined) throw new Error(`Set your personal access token: https://dash.deno.com/account#access-tokens`);

// make a local openKv function, optionally wrap unsupported serialized byte values as UnknownV8
const { openKv } = makeRemoteService({ accessToken, wrapUnknownValues: true });

// open the database connection, use the url from project dashboard: https://dash.deno.com/projects/YOUR_PROJECT/kv
const kv = await openKv('https://api.deno.com/databases/YOUR_DATABASE_ID/connect');

// do anything using the kv api: https://deno.land/api?s=Deno.Kv&unstable
const result = await kv.set([ 'from-client' ], 'hello!');
console.log(result);

// close the database connection
kv.close();
```

### Quick start - Node 18+
_Use the [NPM package](https://www.npmjs.com/package/kv-connect-kit)_ 👉 `npm install kv-connect-kit`
```ts
import { makeRemoteService } from 'kv-connect-kit';
import { serialize, deserialize } from 'v8';

const accessToken = process.env['DENO_KV_ACCESS_TOKEN'];
if (accessToken === undefined) throw new Error(`Set your personal access token: https://dash.deno.com/account#access-tokens`);

// make a local openKv function, on Node 18+ optionally pass full-fidelity V8 serializers to support all KV values
const { openKv } = makeRemoteService({ accessToken, encodeV8: serialize, decodeV8: deserialize });

// open the database connection, use the url from project dashboard: https://dash.deno.com/projects/YOUR_PROJECT/kv
const kv = await openKv('https://api.deno.com/databases/YOUR_DATABASE_ID/connect');

// do anything using the kv api: https://deno.land/api?s=Deno.Kv&unstable
const result = await kv.set([ 'from-client' ], 'hello!');
console.log(result);

// close the database connection
kv.close();
```


### Quick start - Node 14
_Use the [NPM package](https://www.npmjs.com/package/kv-connect-kit)_ 👉 `npm install kv-connect-kit node-fetch`
```ts
import { makeRemoteService } from 'kv-connect-kit'; // or: require('kv-connect-kit')
import fetch from 'node-fetch';

const accessToken = process.env['DENO_KV_ACCESS_TOKEN'];
if (accessToken === undefined) throw new Error(`Set your personal access token: https://dash.deno.com/account#access-tokens`);

// make a local openKv function, Node 14 does not provide a global `fetch` function, so pass a custom implementation like node-fetch
const { openKv } = makeRemoteService({ accessToken, fetcher: fetch });

// open the database connection, use the url from project dashboard: https://dash.deno.com/projects/YOUR_PROJECT/kv
const kv = await openKv('https://api.deno.com/databases/YOUR_DATABASE_ID/connect');

// do anything using the kv api: https://deno.land/api?s=Deno.Kv&unstable
const result = await kv.set([ 'from-client' ], 'hello!');
console.log(result);

// close the database connection
kv.close();
```

### Credits

- Protobuf code generated with [pb](https://deno.land/x/pbkit/cli/pb/README.md)
- NPM package generated with [dnt](https://github.com/denoland/dnt)


---


### Options to `makeRemoteService`

```ts
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
```

### A note about V8 serialization

_All `KVKey` types are fully supported, as they don't use V8 serialization._

Deno KV _values_ support any structured-serializable JavaScript values, using V8's internal serializer.  This makes client interaction from pure Javascript environment without access to V8 internals... tricky. 

A default V8 serializer is included that supports a limited subset of values (see below), but can optionally treat the values as opaque bytes using the `wrapUnknownValues` option.  This is often good enough for listing/migrating/logic that behaves on keys only.

We may be able to leverage [denoland/v8_valueserializer](https://github.com/denoland/v8_valueserializer) for a WASM implementation of V8's serializer in the future!

KV value types supported by the default serializer:

| Type      | Supported |
| ----------- | ----------- |
| `null`      | ✅       |
| `undefined`   | ✅        |
| `true`/`false`   | ✅        |
| `Uint8Array`   | ✅        |
| `string`   | ✅        |

PRs to [v8.ts](https://github.com/skymethod/kv-connect-kit/blob/master/v8.ts) welcome - this would benefit everyone using this library!

If you have access to the V8 serializer on your runtime (like the `serialize`/`deserialize` in Node's `v8` module), you can pass it into the `encodeV8` and `decodeV8` options to get full value support.  Note you must be using the version that the Deno KV service uses, so this works on Node 18+ only.
