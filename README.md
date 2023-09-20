# kv-connect-kit

Minimal Typescript client implementing the [KV Connect protocol](https://github.com/denoland/deno/tree/main/ext/kv#kv-connect). Access [Deno KV](https://deno.com/kv) remotely from any Javascript environment like Node, Cloudflare Workers, Bun, Deno, or the browser. 

### Quick start - Deno

```ts
import { makeRemoteService } from 'https://raw.githubusercontent.com/skymethod/kv-connect-kit/103c961cf9163ad4eeccc99d83dd765cfbc2ac4d/client.ts';

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
