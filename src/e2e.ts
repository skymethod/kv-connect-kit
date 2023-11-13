// deno-lint-ignore-file no-explicit-any
import { assert } from 'https://deno.land/std@0.206.0/assert/assert.ts';
import { assertEquals } from 'https://deno.land/std@0.206.0/assert/assert_equals.ts';
import { assertExists } from 'https://deno.land/std@0.206.0/assert/assert_exists.ts';
import { assertFalse } from 'https://deno.land/std@0.206.0/assert/assert_false.ts';
import { assertMatch } from 'https://deno.land/std@0.206.0/assert/assert_match.ts';
import { assertNotEquals } from 'https://deno.land/std@0.206.0/assert/assert_not_equals.ts';
import { assertRejects } from 'https://deno.land/std@0.206.0/assert/assert_rejects.ts';
import { assertThrows } from 'https://deno.land/std@0.206.0/assert/assert_throws.ts';
import { checkString } from './check.ts';
import { KvListOptions, KvListSelector, KvService } from './kv_types.ts';
import { sleep } from './sleep.ts';

export async function endToEnd(service: KvService, { type, subtype, path }: { type: 'deno' | 'kck', subtype?: 'in-memory' | 'napi' | 'remote' | 'sqlite', path: string }) {

    const pathType = path === ':memory:' ? 'memory' : /^https?:\/\//.test(path) ? 'remote' : 'disk';
    const napi = subtype === 'napi';

    const kv = await service.openKv(path);

    // basic get/set, trivial list, noop commit
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

    // getMany
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

    // delete
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

    // sum
    {
        const result = await kv.atomic().sum([ 'u1' ], 0n).commit();
        assert(result.ok);
        assertMatch(result.versionstamp, /^.+$/);

        if (!(type === 'deno' && pathType === 'remote') && path.includes('/localhost')) {
            // https://github.com/denoland/denokv/issues/32
            await assertRejects(() => kv.atomic().sum([ 'a' ], 0n).commit());
        }
    }

    {
        await kv.atomic().sum([ 'u1' ], 1n).commit();
        await kv.atomic().sum([ 'u1' ], 2n).commit();
        assertEquals((await kv.get([ 'u1' ])).value, service.newKvU64(3n));
    }

    // max
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

    // min
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

    // atomic check
    {
        const { versionstamp: existingVersionstamp } = await kv.get([ 'a' ]);
        assertExists(existingVersionstamp);
        assertFalse((await kv.atomic().check({ key: [ 'a' ], versionstamp: null }).commit()).ok);
        assertFalse((await kv.atomic().check({ key: [ 'a' ], versionstamp: '1' + existingVersionstamp.substring(1) }).commit()).ok);
        assert((await kv.atomic().check({ key: [ 'a' ], versionstamp: existingVersionstamp }).commit()).ok);
    }

    // set expireIn
    {
        // await kv.set([ 'e' ], 'e', { expireIn: -1000 });
        // assertEquals((await kv.get([ 'e' ])).value, null); // native persists the value, probably shouldn't: https://github.com/denoland/deno/issues/21009

        if (type === 'kck' && pathType !== 'remote' && !napi) { // native sqlite doesn't do timely-enough expiration, neither does deno deploy via native or kck, nor napi
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

    // clear all data
    await kv.atomic().delete([ 'a' ]).delete([ 'u1' ]).delete([ 'ne1' ]).delete([ 'ne2' ]).commit();

    {
        const assertList = async (selector: KvListSelector, options: KvListOptions, expected: Record<string, unknown>) => {
            const items = await toArray(kv.list(selector, options));
            const itemArr = items.map(v => [ v.key, v.value ]);
            const expectedArr = Object.entries(expected).map(v => [ v[0].split('_'), v[1] ]);
            assertEquals(itemArr, expectedArr);
        }

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

    // enqueue/listenQueue
    if (type === 'kck' && pathType !== 'remote') {
        const delay = napi ? 1000 : pathType === 'disk' ? 200 : 20;
        const records: Record<string, { sent: number, received?: number }> = {};
        kv.listenQueue(v => {
            if (typeof v !== 'string') throw new Error(`Expected string value, received: ${JSON.stringify(v)}`);
            const record = records[v];
            if (!record) throw new Error(`Unexpected callback value '${v}', expected: ${JSON.stringify([...Object.keys(records)])}`);
            record.received = Date.now();
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
            const result = await kv.enqueue('q2', { delay });
            assert(result.ok);
            assertMatch(result.versionstamp, /^.+$/);
        }

        {
            records['q3'] = { sent: Date.now() };
            const result = await kv.enqueue('q3', { keysIfUndelivered: [ [ 'q3' ] ] });
            assert(result.ok);
            assertMatch(result.versionstamp, /^.+$/);
        }

        await sleep(delay * 5 / 2);
        assertEquals(toSorted(Object.entries(records), (a, b) => a[1].received! - b[1].received!).map(v => v[0]), napi ? [ 'q1', 'q2', 'q3' ] : [ 'q1', 'q3', 'q2' ]);
        assert((records.q1.received! - records.q1.sent) <= delay / 2);
        assert((records.q2.received! - records.q2.sent) >= delay);
        if (!napi) assert((records.q3.received! - records.q3.sent) <= delay / 2);
        assertEquals((await kv.get([ 'q3' ])).value, 'q3');
    }

    // multiple mutations on the same key
    {
        const k1 = [ 'k1' ];
        await kv.atomic().set(k1, 'a').set(k1, 'b').commit();
        assertEquals((await kv.get(k1)).value, 'b');
        await kv.atomic().set(k1, 'a').delete(k1).commit();
        assertEquals((await kv.get(k1)).value, null);
        await kv.atomic().delete(k1).set(k1, 'a').commit();
        assertEquals((await kv.get(k1)).value, 'a');

        await kv.atomic().set(k1, service.newKvU64(1n)).commit();
        assertEquals((await kv.get(k1)).value, service.newKvU64(1n));

        await kv.atomic().set(k1, service.newKvU64(1n)).sum(k1, 2n).commit();
        assertEquals((await kv.get(k1)).value, service.newKvU64(3n));

        await kv.atomic().set(k1, service.newKvU64(1n)).sum(k1, 2n).sum(k1, 3n).commit();
        assertEquals((await kv.get(k1)).value, service.newKvU64(6n));

        await kv.atomic().set(k1, service.newKvU64(1n)).sum(k1, 2n).set(k1, service.newKvU64(5n)).commit();
        assertEquals((await kv.get(k1)).value, service.newKvU64(5n));

        await kv.atomic().max(k1, 100n).max(k1, 50n).max(k1, 150n).max(k1, 75n).commit();
        assertEquals((await kv.get(k1)).value, service.newKvU64(150n));

        await kv.atomic().min(k1, 100n).min(k1, 50n).min(k1, 150n).min(k1, 75n).commit();
        assertEquals((await kv.get(k1)).value, service.newKvU64(50n));
    }

    // close
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
    if (type !== 'deno') await assertRejects(async () => await logAndRethrow(() => kv.listenQueue(() => {}))); // doesn't throw, but probably should: https://github.com/denoland/deno/issues/20991
    await assertRejects(async () => await logAndRethrow(() => toArray(kv.list({ prefix: [] }))));

    // disk-only tests
    if (pathType === 'disk') {
        const k1 = [ 'k1' ];

        let versionstamp1: string;
        {
            const kv = await service.openKv(path);
            const result = await kv.set(k1, 'v1');
            versionstamp1 = result.versionstamp;
            kv.close();
        }
        {
            const kv = await service.openKv(path);
            const { value, versionstamp } = await kv.get(k1);
            assertEquals(value, 'v1');
            assertEquals(versionstamp, versionstamp1);
            const { versionstamp: versionstamp2 } = await kv.set(k1, 'v2');
            assert(versionstamp2 > versionstamp1, `${versionstamp2} > ${versionstamp1}`);

            await kv.enqueue('later');

            kv.close();
        }
        {
            const kv = await service.openKv(path);
            const received: unknown[] = [];
            kv.listenQueue(v => { received.push(v); });
            await sleep(napi ? 1000 : type === 'deno' ? 100 : 50);
            assertEquals(received, [ 'later' ]);

            kv.close();
        }
    }
}

//

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

function toSorted<T>(arr: T[], compareFn?: (a: T, b: T) => number): T[] { // avoid newer .toSorted to support older es environments
    const rt = [ ...arr ];
    return rt.sort(compareFn);
}