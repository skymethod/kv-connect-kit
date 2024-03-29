import { KvKey } from './kv_types.ts';
import { packKey, unpackKey } from './kv_key.ts';
import { assertEquals } from 'https://deno.land/std@0.212.0/assert/assert_equals.ts';

Deno.test({
    name: 'packKey/unpackKey',
    fn: () => {
        const tests: [ KvKey, number[] ][] = [
            [ [ 'foo' ], [ 2, 102, 111, 111, 0 ] ],
            [ [ 'f\0o' ], [ 2, 102, 0, 0xff, 111, 0 ] ],
            [ [ 0 ], [ 33, 128,   0,   0,   0,   0,  0, 0,  0 ] ],
            [ [ 123.456 ], [ 33, 192,  94, 221,  47, 26, 159, 190, 119 ] ],
            [ [ -300 ], [ 33,  63, 141,  63, 255, 255, 255, 255, 255 ] ],
            [ [ 200 ], [ 33, 192, 105,   0,   0,   0,  0, 0,  0 ] ],
            [ [ new TextEncoder().encode('foo') ], [ 1, 102, 111, 111,   0 ] ],
            [ [ new Uint8Array([ 4, 0, 4 ]) ], [ 1, 4, 0, 255, 4, 0 ] ],
            [ [ false ], [ 38 ] ],
            [ [ true ], [ 39 ] ],
            [ [ 0n ], [ 20 ] ],
            [ [ -200n ], [ 19,  55 ] ],
            [ [ 200n ], [ 21, 200 ] ],
            [ [ 600n ], [ 22, 2, 88 ] ],
            [ [ 70000n ], [ 23, 1, 17, 112 ] ],
            [ [ 305419896n ], [ 24, 18, 52, 86, 120 ] ],
            [ [ 78187493520n ], [ 25, 18, 52, 86, 120, 144 ] ],
            [ [ 0x112233445566n ], [ 26, 17, 34, 51, 68, 85, 102 ] ],
            [ [ 0x11223344556677n ], [ 27, 17, 34, 51, 68, 85, 102, 119 ] ],
            [ [ 0x1122334455667788n ], [ 28, 17, 34, 51, 68, 85, 102, 119, 136 ] ],
            [ [ 0x112233445566778899n ], [ 29, 9, 17, 34, 51, 68, 85, 102, 119, 136, 153 ] ],
            [ [ 0x1122334455667788990011223344556677889900n ], [ 29,  20,  17, 34, 51,  68,  85, 102, 119, 136, 153, 0, 17, 34,  51, 68,  85, 102, 119, 136, 153,  0 ] ],
            [ [ -0x1122334455667788990011223344556677889900n ], [ 11, 235, 238, 221, 204, 187, 170, 153, 136, 119, 102, 255, 238, 221, 204, 187, 170, 153, 136, 119, 102,  255 ] ],
        ]
        for (const [ key, expected ] of tests) {
            const encoded = packKey(key);
            assertEquals(encoded, new Uint8Array(expected), key.join('/'));
            assertEquals(unpackKey(encoded), key);
        }
    }
});
