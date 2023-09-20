# kv-connect-kit

Minimal Typescript client implementing the [KV Connect protocol](https://github.com/denoland/deno/tree/main/ext/kv#kv-connect). Access [Deno KV](https://deno.com/kv) remotely from any Javascript environment like Node, Cloudflare Workers, Bun, Deno, or the browser. 

### Quick start - Deno

```ts
import { makeRemoteService } from 'https://raw.githubusercontent.com/skymethod/kv-connect-kit/103c961cf9163ad4eeccc99d83dd765cfbc2ac4d/client.ts';

const accessToken = Deno.env.get('DENO_KV_ACCESS_TOKEN');
if (accessToken === undefined) throw new Error(`Set your personal access token: https://dash.deno.com/account#access-tokens`);

// make a local openKv function, optionally wrap unsupported serialized byte values as UnknownV8
const { openKv } = makeRemoteService({ accessToken, wrapUnknownValues: true });

// open the database connection
const kv = await openKv(url);

// do anything using the kv api
const result = await kv.set([ 'from-client' ], 'hello!');
console.log(result);

// close the database connection
kv.close();
```

### Credits

- Protobuf code generated with [pb](https://deno.land/x/pbkit/cli/pb/README.md)
- NPM package generated with [dnt](https://github.com/denoland/dnt)
