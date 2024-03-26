# ZinniUP

w3up blooms

## What is this?

This is a proof-of-concept of running w3up on a WASM-adjacent runtime. 

Zinnia is "a sandboxed and resource-limited runtime for distributed workers" that
provides an easy way to run w3up on an execution platform that supports WASM. 

This proof-of-concept shows that it is possible to run w3up in "a sandboxed
and resource-limited runtime" which may be useful if we'd like to enable many different
entities to run w3up instances.

## Installation

First, `pnpm install` in the same directory as this README.

Next, follow Zinnia's instructions to install the `zinnia` CLI:

https://github.com/filecoin-station/zinnia/blob/main/cli/README.md#installation

Finally, run `pnpm build` in the same directory as this README.

## Running

If you added `zinnia` to your shell `PATH`, `pnpm start` should run the
rollup-bundled script at `dist/main.js` which, as of 3/21/2024, will create
a w3up server, print a trivial log line, and exit.