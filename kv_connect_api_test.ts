import { KvKey } from './kv_types.ts';
import { packKey, unpackKey } from './kv_connect_api.ts';
import { assertEquals } from 'https://deno.land/std@0.201.0/assert/assert_equals.ts';

Deno.test({
    name: 'packKey/unpackKey',
    fn: () => {
        const tests: [ KvKey, number[] ][] = [
            [ [ 'foo' ], [ 2, 102, 111, 111, 0 ] ],
            [ [ 123.456 ], [ 33, 192,  94, 221,  47, 26, 159, 190, 119 ] ],
            // [ [ -300 ], [ 33,  63, 141,  63, 255, 255, 255, 255, 255 ] ],  // TODO not working yet
            [ [ 200 ], [ 33, 192, 105,   0,   0,   0,  0, 0,  0 ] ],
            [ [ new TextEncoder().encode('foo') ], [ 1, 102, 111, 111,   0 ] ],
            [ [ false ], [ 38 ] ],
            [ [ true ], [ 39 ] ],
            [ [ 0n ], [ 20 ] ],
            [ [ -200n ], [ 19,  55 ] ],
            [ [ 200n ], [ 21, 200 ] ],
        ]
        for (const [ key, expected ] of tests) {
            const encoded = packKey(key);
            assertEquals(encoded, new Uint8Array(expected), key.join('/'));
            assertEquals(unpackKey(encoded), key);
        }
       
    }
});
