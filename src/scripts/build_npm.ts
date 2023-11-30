import { join } from 'https://deno.land/std@0.208.0/path/join.ts';
import { LibName, build, emptyDir } from 'https://deno.land/x/dnt@0.39.0/mod.ts';
import { parseArgs as parseFlags } from 'https://deno.land/std@0.208.0/cli/parse_args.ts';
import { generateNapiIndex } from './generate_napi_index.ts';

const flags = parseFlags(Deno.args);
const tests = !!flags.tests;
if (tests) console.log('including tests!');
const napi = typeof flags.napi === 'string' ? { packageName: '@skymethod/kv-connect-kit-napi', packageVersion: flags.napi, artifactName: 'kv-connect-kit-napi' } : undefined;

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
        ...(napi ? { optionalDependencies: Object.fromEntries(['win32-x64-msvc', 'darwin-x64','linux-x64-gnu','darwin-arm64'].map(v => [ `${napi.packageName}-${v}`, napi.packageVersion])) } : {})
      
    },
    async postBuild() {
        // steps to run after building and before running the tests
        await Deno.copyFile('LICENSE', join(outDir, 'LICENSE'));
        await Deno.copyFile('README.md', join(outDir, 'README.md'));
        if (napi) {
            const napiIndexJs = generateNapiIndex({ napiPackageName: napi.packageName, napiArtifactName: napi.artifactName });
            for (const subdir of [ 'script', 'esm' ]) {
                console.log(`writing ${join(subdir, '_napi_index.js')}`);
                await Deno.writeTextFile(join(outDir, subdir, '_napi_index.js'), napiIndexJs);

                console.log(`tweaking ${join(subdir, 'napi_based.js')}`);
                const oldContents = await Deno.readTextFile(join(outDir, subdir, 'napi_based.js'));
                const insertion = subdir === 'esm' ? `import('._napi_index.js')` : `require("./_napi_index.js")`;
                const newContents = oldContents.replace(`const DEFAULT_NAPI_INTERFACE = undefined;`, `const DEFAULT_NAPI_INTERFACE = ${insertion};`);
                await Deno.writeTextFile(join(outDir, subdir, 'napi_based.js'), newContents);
            }
        }
    },
});

console.log(outDir);
