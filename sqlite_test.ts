import { makeNativeService } from './client.ts';
import { KvService } from './kv_types.ts';
import { makeSqliteService } from './sqlite.ts';
import { assertEquals } from 'https://deno.land/std@0.204.0/assert/assert_equals.ts';
import { assertThrows } from 'https://deno.land/std@0.204.0/assert/assert_throws.ts';
import { assertRejects } from 'https://deno.land/std@0.204.0/assert/assert_rejects.ts';
import { assertMatch } from 'https://deno.land/std@0.204.0/assert/assert_match.ts';
import { assert } from 'https://deno.land/std@0.204.0/assert/assert.ts';
import { assertNotEquals } from 'https://deno.land/std@0.204.0/assert/assert_not_equals.ts';

Deno.test({
    name: 'native-e2e',
    fn: async () => {
        await endToEnd(makeNativeService())
    }
});

Deno.test({
    name: 'sqlite-e2e',
    fn: async () => {
        await endToEnd(makeSqliteService())
    }
});

async function endToEnd(service: KvService) {

    const kv = await service.openKv(':memory:');

    const items = await toArray(kv.list({ prefix: [] }));
    assertEquals(items.length, 0);

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
    // assertRejects(async () => await logAndRethrow(() => kv.listenQueue(() => {}))); // doesn't throw, but probably should: https://github.com/denoland/deno/issues/20991
    assertRejects(async () => await logAndRethrow(() => toArray(kv.list({ prefix: [] }))));
   
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
