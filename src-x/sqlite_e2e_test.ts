import { parseArgs as parseFlags } from 'https://deno.land/std@0.208.0/cli/parse_args.ts';
import { endToEnd } from '../src/e2e.ts';
import { makeSqliteService } from './sqlite.ts';

const SqliteNativeDriver = await (async () => {
    if ((await Deno.permissions.query({ name: 'env', variable: 'DENO_DIR' })).state !== 'granted') return undefined;
    const { SqliteNativeDriver } = await import('./sqlite_native_driver.ts');
    return SqliteNativeDriver;
})();

const flags = parseFlags(Deno.args);
const debug = !!flags.debug;

Deno.test({
    name: 'e2e-kck-wasm-memory',
    only: false,
    fn: async () => {
        await endToEnd(makeSqliteService({ debug, maxQueueAttempts: 1 }), { type: 'kck', subtype: 'sqlite', path: ':memory:' });
    }
});

Deno.test({
    name: 'e2e-kck-native-memory',
    only: false,
    ignore: SqliteNativeDriver === undefined,
    fn: async () => {
        await endToEnd(makeSqliteService({ debug, maxQueueAttempts: 1, driver: new SqliteNativeDriver!() }), { type: 'kck', subtype: 'sqlite', path: ':memory:' });
    }
});

Deno.test({
    name: 'e2e-kck-wasm-disk',
    only: false,
    fn: async () => {
        const path = await Deno.makeTempFile({ prefix: 'kck-e2e-tests-', suffix: '.db' });
        try {
            await endToEnd(makeSqliteService({ debug, maxQueueAttempts: 1 }), { type: 'kck', subtype: 'sqlite', path });
        } finally {
            await Deno.remove(path);
        }
    }
});

Deno.test({
    name: 'e2e-kck-native-disk',
    only: false,
    ignore: SqliteNativeDriver === undefined,
    fn: async () => {
        const path = await Deno.makeTempFile({ prefix: 'kck-e2e-tests-', suffix: '.db' });
        try {
            await endToEnd(makeSqliteService({ debug, maxQueueAttempts: 1, driver: new SqliteNativeDriver!() }), { type: 'kck', subtype: 'sqlite', path });
        } finally {
            await Deno.remove(path);
        }
    }
});
