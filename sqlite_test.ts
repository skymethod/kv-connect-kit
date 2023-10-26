import { makeSqliteService } from './sqlite.ts';
import { assertEquals } from 'https://deno.land/std@0.204.0/assert/assert_equals.ts';

Deno.test({
    name: 'sqlite',
    fn: async () => {
        const { openKv } = makeSqliteService();
        const kv = await openKv();
        kv.close();
    }
});
