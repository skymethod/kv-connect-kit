import { chunk } from 'https://deno.land/std@0.204.0/collections/chunk.ts';
import { parse as parseFlags } from 'https://deno.land/std@0.204.0/flags/mod.ts';
import { makeNativeService, makeRemoteService } from './client.ts';
import { endToEnd } from './e2e.ts';
import { makeInMemoryService } from './in_memory.ts';
import { KvKey, KvService } from './kv_types.ts';

const flags = parseFlags(Deno.args);
const debug = !!flags.debug;

Deno.test({
    name: 'e2e-kck-in-memory',
    only: false,
    fn: async () => {
        await endToEnd(makeInMemoryService({ debug, maxQueueAttempts: 1 }), { type: 'kck', path: ':memory:' });
    }
});

Deno.test({
    name: 'e2e-deno-memory',
    only: false,
    fn: async () => {
        await endToEnd(makeNativeService(), { type: 'deno', path: ':memory:' });
    }
});

Deno.test({
    name: 'e2e-deno-disk',
    only: false,
    fn: async () => {
        const path = await Deno.makeTempFile({ prefix: 'kck-e2e-tests-', suffix: '.db' });
        try {
            await endToEnd(makeNativeService(), { type: 'deno', path });
        } finally {
            await Deno.remove(path);
        }
    }
});

//

async function clear(service: KvService, path: string) {
    const kv = await service.openKv(path);
    const keys: KvKey[] = [];
    for await (const { key } of kv.list({ prefix: [] })) {
        keys.push(key);
    }
    for (const batch of chunk(keys, 1000)) {
        let tx = kv.atomic();
        for (const key of batch) {
            tx = tx.delete(key);
        }
        await tx.commit();
    }
    kv.close();
}

const denoKvAccessToken = (await Deno.permissions.query({ name: 'env', variable: 'DENO_KV_ACCESS_TOKEN' })).state === 'granted' && Deno.env.get('DENO_KV_ACCESS_TOKEN');
const denoKvDatabaseId = (await Deno.permissions.query({ name: 'env', variable: 'DENO_KV_DATABASE_ID' })).state === 'granted' && Deno.env.get('DENO_KV_DATABASE_ID');

if (typeof denoKvAccessToken === 'string' && denoKvDatabaseId) {
    Deno.test({
        name: 'e2e-deno-remote',
        fn: async () => {
            const path = `https://api.deno.com/databases/${denoKvDatabaseId}/connect`;
            const service = makeNativeService();
            await clear(service, path);
            try {
                await endToEnd(service, { type: 'deno', path });
            } finally {
                await clear(service, path);
            }
        },
    });

    Deno.test({
        name: 'e2e-kck-remote',
        fn: async () => {
            const path = `https://api.deno.com/databases/${denoKvDatabaseId}/connect`;
            const service = makeRemoteService({ accessToken: denoKvAccessToken, debug });
            await clear(service, path);
            try {
                await endToEnd(service, { type: 'kck', path });
            } finally {
                await clear(service, path);
            }
        },
    });
}

const localKvAccessToken = (await Deno.permissions.query({ name: 'env', variable: 'LOCAL_KV_ACCESS_TOKEN' })).state === 'granted' && Deno.env.get('LOCAL_KV_ACCESS_TOKEN');
const localKvUrl = (await Deno.permissions.query({ name: 'env', variable: 'LOCAL_KV_URL' })).state === 'granted' && Deno.env.get('LOCAL_KV_URL');

if (typeof localKvAccessToken === 'string' && localKvUrl) {
    Deno.test({
        only: false,
        ignore: true, // TODO won't work pre 1.38
        name: 'e2e-deno-localkv',
        fn: async () => {
            const path = localKvUrl;
            const service = makeNativeService();
            await clear(service, path);
            try {
                await endToEnd(service, { type: 'deno', path });
            } finally {
                await clear(service, path);
            }
        },
    });

    Deno.test({
        only: false,
        name: 'e2e-kck-localkv',
        fn: async () => {
            const path = localKvUrl;
            const service = makeRemoteService({ accessToken: localKvAccessToken, debug });
            await clear(service, path);
            try {
                await endToEnd(service, { type: 'kck', path });
            } finally {
                await clear(service, path);
            }
        },
    });
}
