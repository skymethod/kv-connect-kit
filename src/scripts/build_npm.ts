import { join } from 'https://deno.land/std@0.207.0/path/join.ts';
import { LibName, build, emptyDir } from 'https://deno.land/x/dnt@0.39.0/mod.ts';
import { parseArgs as parseFlags } from 'https://deno.land/std@0.207.0/cli/parse_args.ts';

const flags = parseFlags(Deno.args);
const tests = !!flags.tests;
if (tests) console.log('including tests!');

const outDir = await Deno.makeTempDir({ prefix: 'kck-npm-'});
await emptyDir(outDir);

await build({
    entryPoints: [ './src/npm.ts', ...(tests ? [{ name: './tests', path: './src/e2e.ts' }] : [])  ],
    outDir,
    test: false,
    shims: {
        // none!
    },
    compilerOptions: {
        // let's try to support Node 18+
        lib: [ 'ES2020', 'DOM', 'DOM.Iterable', 'ESNext.Disposable', ...(tests ? [ 'ES2021.WeakRef' as LibName ] : []) ],
        target: 'ES2020',
    },
    package: {
        // package.json properties
        name: 'kv-connect-kit',
        version: Deno.args[0],
        description: 'Minimal Typescript client implementing the KV Connect protocol. Connect to Deno KV remotely from Node, Cloudflare Workers, Bun, Deno, and the browser.',
        license: 'MIT',
        repository: {
            type: 'git',
            url: 'https://github.com/skymethod/kv-connect-kit.git',
        },
        bugs: {
            url: 'https://github.com/skymethod/kv-connect-kit/issues',
        },
        homepage: 'https://github.com/skymethod/kv-connect-kit',
      
    },
    postBuild() {
        // steps to run after building and before running the tests
        Deno.copyFileSync('LICENSE', join(outDir, 'LICENSE'));
        Deno.copyFileSync('README.md', join(outDir, 'README.md'));
    },
});

console.log(outDir);
