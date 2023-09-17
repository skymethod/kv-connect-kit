import { encodeV8, decodeV8 } from './v8.ts';
import { assertEquals } from 'https://deno.land/std@0.201.0/assert/assert_equals.ts';

Deno.test({
    name: 'encode/decode',
    fn: () => {
        const bytes = encodeV8('bar');
        assertEquals(bytes, new Uint8Array([ 255, 15,  34, 3, 98, 97, 114 ]));
        assertEquals(decodeV8(bytes), 'bar');
    }
});
