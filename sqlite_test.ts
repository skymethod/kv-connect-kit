import { assert } from 'https://deno.land/std@0.204.0/assert/assert.ts';
import { assertEquals } from 'https://deno.land/std@0.204.0/assert/assert_equals.ts';
import { assertExists } from 'https://deno.land/std@0.204.0/assert/assert_exists.ts';
import { assertFalse } from 'https://deno.land/std@0.204.0/assert/assert_false.ts';
import { assertMatch } from 'https://deno.land/std@0.204.0/assert/assert_match.ts';
import { assertNotEquals } from 'https://deno.land/std@0.204.0/assert/assert_not_equals.ts';
import { assertRejects } from 'https://deno.land/std@0.204.0/assert/assert_rejects.ts';
import { assertThrows } from 'https://deno.land/std@0.204.0/assert/assert_throws.ts';
import { parse as parseFlags } from 'https://deno.land/std@0.204.0/flags/mod.ts';
import { chunk } from 'https://deno.land/std@0.204.0/collections/chunk.ts';
import { checkString } from './check.ts';
import { makeNativeService } from './client.ts';
import { KvKey, KvListOptions, KvListSelector, KvService } from './kv_types.ts';
import { makeSqliteService } from './sqlite.ts';

const flags = parseFlags(Deno.args);
const debug = !!flags.debug;

Deno.test({
    name: 'e2e-native-memory',
    fn: async () => {
        await endToEnd(makeNativeService(), { type: 'native', path: ':memory:' });
    }
});

Deno.test({
    name: 'e2e-sqlite-memory',
    fn: async () => {
        await endToEnd(makeSqliteService({ debug }), { type: 'sqlite', path: ':memory:' });
    }
});

//

const denoKvAccessToken = (await Deno.permissions.query({ name: 'env', variable: 'DENO_KV_ACCESS_TOKEN' })).state === 'granted' && Deno.env.get('DENO_KV_ACCESS_TOKEN');
const denoKvDatabaseId = (await Deno.permissions.query({ name: 'env', variable: 'DENO_KV_DATABASE_ID' })).state === 'granted' && Deno.env.get('DENO_KV_DATABASE_ID');

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

if (typeof denoKvAccessToken === 'string' && denoKvDatabaseId) {
    Deno.test({
        name: 'e2e-native-remote',
        fn: async () => {
            const path = `https://api.deno.com/databases/${denoKvDatabaseId}/connect`;
            const service = makeNativeService();
            await clear(service, path);
            try {
                await endToEnd(service, { type: 'native', path });
            } finally {
                await clear(service, path);
            }
        },

    });
}

//

async function endToEnd(service: KvService, { type, path }: { type: 'native' | 'sqlite', path: string }) {

    const kv = await service.openKv(path);

    {
        const items = await toArray(kv.list({ prefix: [] }));
        assertEquals(items.length, 0);
    }

    let versionstamp1: string
    {
        const result = await kv.atomic().commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);
        versionstamp1 = result.versionstamp;
    }

    {
        const result = await kv.atomic().commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);
        assertNotEquals(result.versionstamp, versionstamp1);
    }

    {
        const result = await kv.set([ 'a' ], 'a');
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);
    }

    {
        const result = await kv.get([ 'a' ]);
        assertEquals(result.key, [ 'a' ]);
        assertEquals(result.value, 'a');
        checkString('versionstamp', result.versionstamp);
        assertMatch(result.versionstamp, /^.+$/);
    }

    {
        const result = await kv.set([ 'a' ], 'a2');
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);
    }

    {
        const result = await kv.get([ 'a' ]);
        assertEquals(result.key, [ 'a' ]);
        assertEquals(result.value, 'a2');
        checkString('versionstamp', result.versionstamp);
        assertMatch(result.versionstamp, /^.+$/);
    }

    {
        const result = await kv.getMany([]);
        assertEquals(result.length, 0);
    }

    {
        const result = await kv.getMany([ [ 'a' ] ]);
        assertEquals(result.length, 1);
        const [ first ] = result;
        assertEquals(first.key, [ 'a' ]);
        assertEquals(first.value, 'a2');
        checkString('versionstamp', first.versionstamp);
        assertMatch(first.versionstamp, /^.+$/);
    }

    {
        const result = await kv.set([ 'b' ], 'b');
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);
    }

    {
        const result = await kv.getMany([ [ 'a' ], [ 'b' ] ]);
        assertEquals(result.length, 2);
        const [ first, second ] = result;
        assertEquals(first.key, [ 'a' ]);
        assertEquals(first.value, 'a2');
        checkString('versionstamp', first.versionstamp);
        assertMatch(first.versionstamp, /^.+$/);

        assertEquals(second.key, [ 'b' ]);
        assertEquals(second.value, 'b');
        checkString('versionstamp', second.versionstamp);
        assertMatch(second.versionstamp, /^.+$/);
    }

    {
        const result = await kv.getMany([ [ 'a' ], [ 'a' ] ]);
        assertEquals(result.length, 2);
        const [ first, second ] = result;
        assertEquals(first.key, [ 'a' ]);
        assertEquals(first.value, 'a2');
        checkString('versionstamp', first.versionstamp);
        assertMatch(first.versionstamp, /^.+$/);

        assertEquals(second.key, [ 'a' ]);
        assertEquals(second.value, 'a2');
        checkString('versionstamp', second.versionstamp);
        assertMatch(second.versionstamp, /^.+$/);
    }

    {
        await kv.delete([ 'b' ]);
        const result = await kv.get([ 'b' ]);
        assertEquals(result.key, [ 'b' ]);
        assertEquals(result.value, null);
        assertEquals(result.versionstamp, null);
    }

    {
        await kv.delete([ 'b' ]);
        const result = await kv.get([ 'b' ]);
        assertEquals(result.key, [ 'b' ]);
        assertEquals(result.value, null);
        assertEquals(result.versionstamp, null);
    }

    {
        const result = await kv.atomic().sum([ 'u1' ], 0n).commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);

        assertRejects(() => kv.atomic().sum([ 'a' ], 0n).commit());
    }

    {
        await kv.atomic().sum([ 'u1' ], 1n).commit();
        await kv.atomic().sum([ 'u1' ], 2n).commit();
        assertEquals((await kv.get([ 'u1' ])).value, service.newKvU64(3n));
    }

    {
        const result = await kv.atomic().max([ 'u1' ], 4n).commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);

        assertEquals((await kv.get([ 'u1' ])).value, service.newKvU64(4n));
    }

    {
        const result = await kv.atomic().max([ 'u1' ], 3n).commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);

        assertEquals((await kv.get([ 'u1' ])).value, service.newKvU64(4n));
    }

    {
        const result = await kv.atomic().min([ 'u1' ], 2n).commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);

        assertEquals((await kv.get([ 'u1' ])).value, service.newKvU64(2n));
    }

    {
        const result = await kv.atomic().min([ 'u1' ], 4n).commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);

        assertEquals((await kv.get([ 'u1' ])).value, service.newKvU64(2n));
    }

    {
        const { versionstamp: existingVersionstamp } = await kv.get([ 'a' ]);
        assertExists(existingVersionstamp);
        assertFalse((await kv.atomic().check({ key: [ 'a' ], versionstamp: null }).commit()).ok);
        assertFalse((await kv.atomic().check({ key: [ 'a' ], versionstamp: '1' + existingVersionstamp.substring(1) }).commit()).ok);
        assert((await kv.atomic().check({ key: [ 'a' ], versionstamp: existingVersionstamp }).commit()).ok);
    }

    {
        // await kv.set([ 'e' ], 'e', { expireIn: -1000 });
        // assertEquals((await kv.get([ 'e' ])).value, null); // native persists the value, probably shouldn't: https://github.com/denoland/deno/issues/21009

        if (type !== 'native') { // native doesn't do timely-enough expiration
            assertRejects(async () => await kv.set([ 'be' ], 'be', { expireIn: 0 }));
            assertRejects(async () => await kv.set([ 'be' ], 'be', { expireIn: -1000 }));
            await kv.set([ 'e' ], 'e', { expireIn: 100 });
            await kv.set([ 'ne1' ], 'ne1', { expireIn: undefined });
            await kv.set([ 'ne2' ], 'ne2');
            assertEquals((await kv.get([ 'e' ])).value, 'e');
            assertEquals((await kv.get([ 'ne1' ])).value, 'ne1');
            assertEquals((await kv.get([ 'ne2' ])).value, 'ne2');
            await sleep(200);
            assertEquals((await kv.get([ 'e' ])).value, null);
            assertEquals((await kv.get([ 'ne1' ])).value, 'ne1');
            assertEquals((await kv.get([ 'ne2' ])).value, 'ne2');
        }
    }

    await kv.atomic().delete([ 'a' ]).delete([ 'u1' ]).delete([ 'ne1' ]).delete([ 'ne2' ]).commit();

    const assertList = async (selector: KvListSelector, options: KvListOptions, expected: Record<string, unknown>) => {
        const items = await toArray(kv.list(selector, options));
        const itemArr = items.map(v => [ v.key, v.value ]);
        const expectedArr = Object.entries(expected).map(v => [ v[0].split('_'), v[1] ]);
        assertEquals(itemArr, expectedArr);
    }

    {
        await assertList({ prefix: [] }, {}, {});
        await kv.set([ 'a' ], 'a');
        await assertList({ prefix: [] }, {}, { a: 'a' });
        await assertList({ prefix: [ 'a' ] }, {}, {});
    }

    kv.close();

    // post-close assertions
    kv.atomic(); // does not throw!
    assertRejects(async () => await logAndRethrow(() => kv.atomic().set([ 'a' ], 'a').commit()) );
    assertThrows(() => kv.close()); // BadResource: Bad resource ID
    assertRejects(async () => await logAndRethrow(() => kv.delete([ 'a' ])));
    assertRejects(async () => await logAndRethrow(() => kv.enqueue('a')));
    assertRejects(async () => await logAndRethrow(() => kv.get([ 'a' ])));
    assertRejects(async () => await logAndRethrow(() => kv.getMany([ [ 'a' ] ])));
    assertRejects(async () => await logAndRethrow(() => kv.set([ 'a' ], 'a')));
    if (type !== 'native') assertRejects(async () => await logAndRethrow(() => kv.listenQueue(() => {}))); // doesn't throw, but probably should: https://github.com/denoland/deno/issues/20991
    assertRejects(async () => await logAndRethrow(() => toArray(kv.list({ prefix: [] }))));
   
}

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function toArray<T>(iter: AsyncIterableIterator<T>): Promise<T[]> {
    const rt: T[] = [];
    for await (const item of iter) {
        rt.push(item);
    }
    return rt;
}

async function logAndRethrow<T>(fn: () => Promise<T>): Promise<T> {
    try {
        return await fn();
    } catch (e) {
        // console.error(e);
        throw e;
    }
}
