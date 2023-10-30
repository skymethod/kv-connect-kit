import { makeNativeService } from './client.ts';
import { KvService } from './kv_types.ts';
import { makeSqliteService } from './sqlite.ts';

Deno.bench('kck-sqlite-disk', { group: 'sqlite-disk', baseline: true, only: false }, async b => {
    const path = await Deno.makeTempFile({ prefix: 'kck-bench-', suffix: '.db' });
    try {
        b.start();
        await runBenchmarks(makeSqliteService({ debug: false }), path, 10);
        b.end();
    } finally {
        await Deno.remove(path);
    }
});

Deno.bench('native-sqlite-disk', { group: 'sqlite-disk', baseline: false, only: false }, async b => {
    const path = await Deno.makeTempFile({ prefix: 'kck-bench-', suffix: '.db' });
    try {
        b.start();
        await runBenchmarks(makeNativeService(), path, 10);
        b.end();
    } finally {
        await Deno.remove(path);
    }
});

Deno.bench('kck-sqlite-memory', { group: 'sqlite-memory', baseline: true, only: false }, async () => {
    await runBenchmarks(makeSqliteService({ debug: false }), ':memory:', 5000);
});

Deno.bench('native-sqlite-memory', { group: 'sqlite-memory', baseline: false, only: false }, async () => {
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
