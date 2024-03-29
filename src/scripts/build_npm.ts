import { join } from 'https://deno.land/std@0.212.0/path/join.ts';
import { LibName, build, emptyDir } from 'https://deno.land/x/dnt@0.39.0/mod.ts';
import { parseArgs as parseFlags } from 'https://deno.land/std@0.212.0/cli/parse_args.ts';
import { generateNapiIndex } from './generate_napi_index.ts';
import { run } from './process.ts';

const flags = parseFlags(Deno.args);
const tests = !!flags.tests;
if (tests) console.log('including tests!');
const publish = typeof flags.publish === 'string' ? flags.publish : undefined;
const dryrun = !!flags['dry-run'];
if (publish) console.log(`publish${dryrun ? ` (dryrun)`: ''} after build! (npm=${publish})`);
const stripLeadingV = (version: string) => version.replace(/^v/, '');
const napi = typeof flags.napi === 'string' ? { packageName: '@skymethod/kv-connect-kit-napi', packageVersion: stripLeadingV(flags.napi), artifactName: 'kv-connect-kit-napi' } : undefined;
if (napi) console.log(`napi: ${JSON.stringify(napi)}`);
const version = typeof Deno.args[0] === 'string' ? stripLeadingV(Deno.args[0]) : Deno.args[0];
if (typeof version !== 'string' || !/^[a-z0-9.-]+$/.test(version)) throw new Error(`Unexpected version: ${version}`);
console.log(`version=${version}`);

const outDir = await Deno.makeTempDir({ prefix: 'userland-npm-'});
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
        name: napi?.packageName ?? 'kv-connect-kit',
        version,
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
        await Deno.copyFile(napi ? 'napi/README.md' : 'README.md', join(outDir, 'README.md'));
        if (napi) {
            const napiIndexJs = generateNapiIndex({ napiPackageName: napi.packageName, napiArtifactName: napi.artifactName });
            for (const subdir of [ 'script', 'esm' ]) {
                const name = '_napi_index.cjs'; // cjs to ensure 'require' works in esm mode
                console.log(`writing ${join(subdir, name)}`);
                await Deno.writeTextFile(join(outDir, subdir, name), napiIndexJs);

                console.log(`tweaking ${join(subdir, 'napi_based.js')}`);
                const oldContents = await Deno.readTextFile(join(outDir, subdir, 'napi_based.js'));
                const insertion = subdir === 'esm' ? `await import('./${name}')` : `require('./${name}')`;
                const newContents = oldContents.replace(`const DEFAULT_NAPI_INTERFACE = undefined;`, `const DEFAULT_NAPI_INTERFACE = ${insertion};`);
                await Deno.writeTextFile(join(outDir, subdir, 'napi_based.js'), newContents);
            }
        }
    },
});

if (publish) {
    const updatePackageJsonVersion = async (path: string, version: string) => {
        console.log(`Updating ${path} version to ${version}`);
        const packageJson = await Deno.readTextFile(path);
        const newPackageJson = packageJson.replace(/("version"\s*:\s*")[0-9a-z.-]+"/, `$1${version}"`);
        if (packageJson === newPackageJson) throw new Error(`Unable to replace version!`);
        await Deno.writeTextFile(path, newPackageJson);
    }
    const npmPublish = async (path: string) => {
        const next = !/^[0-9]+\.[0-9]+\.[0-9]+$/.test(version);
        const out = await run({
            command: publish,
            args: [ 'publish', '--access', 'public', ...(next ? [ '--tag', 'next' ] : []), ...(dryrun ? [ '--dry-run' ] : []), path ],
        });
        console.log(out);
    }

    if (napi) {
        // first, publish the native subpackages
        for (const { name: subdir } of (await Array.fromAsync(Deno.readDir('napi/npm'))).filter(v => v.isDirectory)) {
            const path = join('napi', 'npm', subdir, 'package.json');
            await updatePackageJsonVersion(path, version);
            await npmPublish(join('napi', 'npm', subdir));
        }
    }
    // finally, publish the root package
    await npmPublish(outDir);
}

console.log(outDir);
