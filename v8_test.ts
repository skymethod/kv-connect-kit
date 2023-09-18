import { encodeV8, decodeV8 } from './v8.ts';
import { assertEquals } from 'https://deno.land/std@0.201.0/assert/assert_equals.ts';

Deno.test({
    name: 'encode/decode',
    fn: () => {
        const tests: [unknown, number[]][] = [
            [ 'bar', [ 255, 15,  34, 3, 98, 97, 114 ] ],
            [ null, [ 255, 15, 48 ] ],
            [ undefined, [ 255, 15, 95 ] ],
            [ 'a😮b', [ 255, 15,  99,  8,  97, 0, 61, 216, 46, 222, 98,  0 ] ],
        ]
        for (const [ value, expected ] of tests) {
            const encoded = encodeV8(value);
            assertEquals(encoded, new Uint8Array(expected));
            assertEquals(decodeV8(encoded), value);
        }
       
    }
});
