# Lark-owned Firebase SDK tests

End-to-end tests that drive the real Firebase JS SDK against a live Lark
stack, exercising the full wire path (WebSocket → edge → server → broadcast
→ edge → client). These cover wire-level compatibility behavior the
upstream `database-compat` suite doesn't directly test.

## How to run

```sh
./test/run-firebase-sdk.sh                          # full upstream suite + ours
./test/run-firebase-sdk.sh lark-multi-path-update   # one file
```

The runner script (`test/run-firebase-sdk.sh`) copies every `lark-*.test.ts`
in this directory into `$SDK_DIR/packages/database-compat/test/` before
invoking mocha, and removes them on exit. Tests written here have access to
the same helpers the upstream suite uses (`getRandomNode`, `pause`,
`EventAccumulator`, …) via relative imports.

## Conventions

- **Filename**: `lark-*.test.ts`. The prefix is how the runner identifies
  files to copy in and clean up — don't drop it.
- **Imports**: use the relative paths the upstream tests use
  (`'./helpers/util'`, `'../src/api/Reference'`, `'../../database/...'`).
  Files run from `$SDK_DIR/packages/database-compat/test/` after copy.
- **Isolation**: prefer `getRandomNode()` for a fresh root rather than
  reusing project roots across tests.
- **Scope**: only add a test here when the upstream suite genuinely
  doesn't cover the case. If something *should* be in `database-compat`
  upstream but isn't, that's the bar.
