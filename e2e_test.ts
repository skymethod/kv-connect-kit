// deno-lint-ignore-file no-explicit-any
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
import { makeNativeService, makeRemoteService } from './client.ts';
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
    name: 'e2e-kck-memory',
    fn: async () => {
        await endToEnd(makeSqliteService({ debug, maxQueueAttempts: 1 }), { type: 'kck', path: ':memory:' });
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

//

async function endToEnd(service: KvService, { type, path }: { type: 'native' | 'kck', path: string }) {

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

        await assertRejects(() => kv.atomic().sum([ 'a' ], 0n).commit());
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

        if (type === 'kck' && !path.startsWith('https://')) { // native sqlite doesn't do timely-enough expiration, neither does deno deploy via native or kck
            await assertRejects(async () => await kv.set([ 'be' ], 'be', { expireIn: 0 }));
            await assertRejects(async () => await kv.set([ 'be' ], 'be', { expireIn: -1000 }));
            await kv.set([ 'e' ], 'e', { expireIn: 100 });
            await kv.set([ 'ne1' ], 'ne1', { expireIn: undefined });
            await kv.set([ 'ne2' ], 'ne2');
            assertEquals((await kv.get([ 'e' ])).value, 'e');
            assertEquals((await kv.get([ 'ne1' ])).value, 'ne1');
            assertEquals((await kv.get([ 'ne2' ])).value, 'ne2');
            await sleep(150);
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
        // list selectors
        await assertList({ prefix: [] }, {}, {});
        await kv.set([ 'a' ], 'a');
        await assertList({ prefix: [] }, {}, { a: 'a' });
        await assertList({ prefix: [ 'a' ] }, {}, {});
        await kv.set([ 'a', 'a' ], 'a_a');
        await assertList({ prefix: [ 'a' ] }, {}, { a_a: 'a_a' });
        await kv.set([ 'a', 'b' ], 'a_b');
        await assertList({ prefix: [ 'a' ], start: [ 'a', '0' ] }, {}, { a_a: 'a_a', a_b: 'a_b' });
        await assertRejects(async () => await toArray(kv.list({ prefix: [ 'a' ], start: [ 'a', '0' ], end: [ 'a', '1' ] }, {})));
        await assertList({ prefix: [ 'a' ], start: [ 'a', 'a' ] }, {}, { a_a: 'a_a', a_b: 'a_b' });
        await assertList({ prefix: [ 'a' ], start: [ 'a', 'b' ] }, {}, {  a_b: 'a_b' });
        await assertList({ prefix: [ 'a' ], start: [ 'a', 'c' ] }, {}, {});
        await assertList({ prefix: [ 'a' ], end: [ 'a', 'b' ] }, {}, { a_a: 'a_a' });
        await assertList({ prefix: [ 'a' ], end: [ 'a', '1' ] }, {}, { });
        await assertList({ prefix: [ 'b' ] }, { }, {});
        await kv.set([ 'b' ], 'b');
        await assertList({ start: [ 'a', 'a' ], end: [ 'c' ] }, {}, { a_a: 'a_a', a_b: 'a_b', b: 'b' });

        // limit
        await Promise.all([0, -1, 0.6, '', {}].map(v => assertRejects(async () => await toArray(kv.list({ prefix: [] }, { limit: v as any })))));
        await assertList({ prefix: [] }, { limit: 1 }, { a: 'a' });

        // reverse
        await assertList({ prefix: [] }, { limit: 1, reverse: true }, { b: 'b' });
        await Promise.all([ 1, 'true', {}, [], 0 ].map(v => assertList({ prefix: [] }, { limit: 1, reverse: v as any }, { a: 'a' })));

        // consistency
        await Promise.all([ 'foo', '', {}, false ].map(v => assertRejects(async () => await toArray(kv.list({ prefix: [] }, { consistency: v as any })))));

        // batchSize
        await Promise.all([ 'foo', '', {}, false, -1, 0, 1001 ].map(v => assertRejects(async () => await toArray(kv.list({ prefix: [] }, { batchSize: v as any })))));
        if (type === 'kck') await Promise.all([ 1.2, 2.3 ].map(v => assertRejects(async () => await toArray(kv.list({ prefix: [] }, { batchSize: v as any }))))); // native should probably throw: https://github.com/denoland/deno/issues/21013

        // cursor
        const iter = kv.list({ prefix: [] });
        assertThrows(() => iter.cursor);
        await iter.next();
        const cursor1 = iter.cursor;
        await iter.next();
        const cursor2 = iter.cursor;
        await toArray(iter);
        
        await assertList({ prefix: [] }, { cursor: cursor1 }, { a_a: 'a_a', a_b: 'a_b', b: 'b' });
        await assertList({ prefix: [] }, { cursor: cursor2 }, { a_b: 'a_b', b: 'b' });
        await Promise.all([ [], {}, '.', 123n ].map(v => assertRejects(async () => await toArray(kv.list({ prefix: [] }, { cursor: v as any })), `${v}`)));
        if (type === 'kck') await Promise.all([ true, -1, 0, 'asdf', null ].map(v => assertRejects(async () => await toArray(kv.list({ prefix: [] }, { cursor: v as any })), `cursor: ${v}`)));

    }

    if (type === 'kck' && !path.startsWith('https://')) {
        const records: Record<string, { sent: number, received?: number }> = {};
        kv.listenQueue(v => {
            records[v as string].received = Date.now();
            if (v === 'q3') throw new Error();
        });

        {
            records['q1'] = { sent: Date.now() };
            const result = await kv.enqueue('q1');
            assert(result.ok);
            assertMatch(result.versionstamp, /^.+$/);
        }

        {
            records['q2'] = { sent: Date.now() };
            const result = await kv.enqueue('q2', { delay: 20 });
            assert(result.ok);
            assertMatch(result.versionstamp, /^.+$/);
        }

        {
            records['q3'] = { sent: Date.now() };
            const result = await kv.enqueue('q3', { keysIfUndelivered: [ [ 'q3' ] ] });
            assert(result.ok);
            assertMatch(result.versionstamp, /^.+$/);
        }

        await sleep(50);
        assertEquals(Object.entries(records).toSorted((a, b) => a[1].received! - b[1].received!).map(v => v[0]), [ 'q1', 'q3', 'q2' ]);
        assert((records.q1.received! - records.q1.sent) <= 10);
        assert((records.q2.received! - records.q2.sent) >= 20);
        assert((records.q3.received! - records.q3.sent) <= 10);
        assertEquals((await kv.get([ 'q3' ])).value, 'q3');
    }

    kv.close();

    // post-close assertions
    kv.atomic(); // does not throw!
    await assertRejects(async () => await logAndRethrow(() => kv.atomic().set([ 'a' ], 'a').commit()) );
    assertThrows(() => kv.close()); // BadResource: Bad resource ID
    await assertRejects(async () => await logAndRethrow(() => kv.delete([ 'a' ])));
    await assertRejects(async () => await logAndRethrow(() => kv.enqueue('a')));
    await assertRejects(async () => await logAndRethrow(() => kv.get([ 'a' ])));
    await assertRejects(async () => await logAndRethrow(() => kv.getMany([ [ 'a' ] ])));
    await assertRejects(async () => await logAndRethrow(() => kv.set([ 'a' ], 'a')));
    if (type !== 'native') await assertRejects(async () => await logAndRethrow(() => kv.listenQueue(() => {}))); // doesn't throw, but probably should: https://github.com/denoland/deno/issues/20991
    await assertRejects(async () => await logAndRethrow(() => toArray(kv.list({ prefix: [] }))));
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