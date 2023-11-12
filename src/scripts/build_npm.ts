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
        // let's try to support Node 18+
        lib: [ 'ES2020', 'DOM', 'DOM.Iterable' ],
        target: 'ES2020',
    },
    filterDiagnostic: v => {
        const { code, category, file, messageText } = v;
        const txt = typeof messageText === 'string' ? messageText : messageText.messageText;
        // Symbole.dispose is too new for dnt type-checking: https://github.com/denoland/dnt/issues/345
        if (code === 1169 && file?.fileName.endsWith('/kv_types.ts') && txt.includes('unique symbol')) return false;
        if (code === 2339 && /\/kv_(types|util)\.ts$/.test(file?.fileName ?? '') && txt.includes(`Property 'dispose' does not exist on type 'SymbolConstructor'`)) return false;
        console.log(JSON.stringify({ code, category, file: file?.fileName, messageText }, undefined, 2));
        return true;
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
