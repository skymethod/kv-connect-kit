# `@skymethod/kv-connect-kit-napi`

![https://github.com/skymethod/kv-connect-kit/actions](https://github.com/skymethod/kv-connect-kit/workflows/CI/badge.svg)

> Experimental [kv-connect-kit](https://github.com/skymethod/kv-connect-kit) napi sqlite backend leveraging [denoland/denokv](https://github.com/denoland/denokv).

## Support matrix

### Operating Systems

|                  | node18 | node20 |
| ---------------- | ------ | ------ |
| Windows x64      | ✓      | ✓      |
| Windows x32      | ✓      | ✓      |
| Windows arm64    | ✓      | ✓      |
| macOS x64        | ✓      | ✓      |
| macOS arm64      | ✓      | ✓      |
| Linux x64 gnu    | ✓      | ✓      |
| Linux x64 musl   | ✓      | ✓      |
| Linux arm gnu    | ✓      | ✓      |
| Linux arm64 gnu  | ✓      | ✓      |
| Linux arm64 musl | ✓      | ✓      |
| FreeBSD x64      | ✓      | ✓      |


# Development

- Install the latest `Rust`
- Install `Node.js@18+` which fully supported `Node-API`
- Install `yarn@1.x`

## Test in local

- yarn
- yarn build
- yarn test

Should see something like:

```bash
$ ava --verbose

  ✔ something
  ✔ something (201ms)
  ─

  2 tests passed
✨  Done in 1.12s.
```

## Release package

Ensure **NPM_TOKEN** is set as a project secret.

To release:

```
npm version [<newversion> | major | minor | patch | premajor | preminor | prepatch | prerelease [--preid=<prerelease-id>] | from-git]

git push
```

GitHub Actions will do the rest!
