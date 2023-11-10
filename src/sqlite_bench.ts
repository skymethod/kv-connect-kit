import { makeNativeService } from './client.ts';
import { makeInMemoryService } from './in_memory.ts';
import { KvService } from './kv_types.ts';
import { makeSqliteService } from './sqlite.ts';
import { SqliteNativeDriver } from './sqlite_native_driver.ts';

Deno.bench('kck-wasm-sqlite-disk', { group: 'sqlite-disk', baseline: false, only: false }, async b => {
    const path = await Deno.makeTempFile({ prefix: 'kck-bench-', suffix: '.db' });
    try {
        b.start();
        await runBenchmarks(makeSqliteService({ debug: false }), path, 10);
        b.end();
    } finally {
        await Deno.remove(path);
    }
});

Deno.bench('kck-native-sqlite-disk', { group: 'sqlite-disk', baseline: false, only: false }, async b => {
    const path = await Deno.makeTempFile({ prefix: 'kck-bench-', suffix: '.db' });
    try {
        b.start();
        await runBenchmarks(makeSqliteService({ debug: false, driver: new SqliteNativeDriver() }), path, 10);
        b.end();
    } finally {
        await Deno.remove(path);
    }
});

Deno.bench('deno-sqlite-disk', { group: 'sqlite-disk', baseline: true, only: false }, async b => {
    const path = await Deno.makeTempFile({ prefix: 'kck-bench-', suffix: '.db' });
    try {
        b.start();
        await runBenchmarks(makeNativeService(), path, 10);
        b.end();
    } finally {
        await Deno.remove(path);
    }
});

Deno.bench('kck-in-memory', { group: 'sqlite-memory', baseline: false, only: false }, async () => {
    await runBenchmarks(makeInMemoryService({ debug: false }), ':memory:', 5000);
});

Deno.bench('kck-wasm-sqlite-memory', { group: 'sqlite-memory', baseline: false, only: false }, async () => {
    await runBenchmarks(makeSqliteService({ debug: false, driver: new SqliteNativeDriver() }), ':memory:', 5000);
});

Deno.bench('kck-native-sqlite-memory', { group: 'sqlite-memory', baseline: false, only: false }, async () => {
    await runBenchmarks(makeSqliteService({ debug: false }), ':memory:', 5000);
});


Deno.bench('deno-sqlite-memory', { group: 'sqlite-memory', baseline: true, only: false }, async () => {
    await runBenchmarks(makeNativeService(), ':memory:', 5000);
});

async function runBenchmarks(service: KvService, path: string, n: number) {
    const kv = await service.openKv(path);

    for (let i = 0; i < n; i++) {
        const k = [ `k${i}` ];
        await kv.atomic().set(k, `v${i}`).commit();
        await kv.atomic().delete(k).commit();
    }

    kv.close();
}
