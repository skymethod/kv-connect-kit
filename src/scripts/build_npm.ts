import { join } from 'https://deno.land/std@0.206.0/path/mod.ts';
import { build, emptyDir } from 'https://deno.land/x/dnt@0.38.1/mod.ts';

const outDir = await Deno.makeTempDir({ prefix: 'kck-npm-'});
await emptyDir(outDir);

await build({
    entryPoints: [ './src/npm.ts' ],
    outDir,
    test: false,
    shims: {
        // none!
    },
    compilerOptions: {
        // let's try to support Node 14+
        lib: [ 'ES2020', 'DOM','DOM.Iterable' ],
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
            url: 'git+https://github.com/skymethod/kv-connect-kit.git',
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
