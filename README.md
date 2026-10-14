# tiny-batch-file-copier

Copies multiple files concurrently with a configurable concurrency limit and per-file progress callbacks. Pure Node.js, ESM, zero dependencies.

## Usage

```js
import { copyFiles, copyFilesBatched } from 'tiny-batch-file-copier';

const tasks = [
  { src: '/data/a.bin', dest: '/backup/a.bin' },
  { src: '/data/b.bin', dest: '/backup/b.bin' }
];

// Batch copy with progress and error accumulation
const { copied, errors } = await copyFiles(tasks, {
  concurrency: 4,
  bufferSize: 64 * 1024,
  onProgress: ({ dest, bytes, total }) => {
    console.log(dest, bytes, '/', total);
  }
});

// Streaming variant — yields results as each file finishes
for await (const r of copyFilesBatched(tasks, { concurrency: 2 })) {
  if (r.ok) console.log('done', r.result.dest, r.result.bytes);
  else console.error('failed', r.task.src, r.error.message);
}
```

## Why this exists

`fsPromises.copyFile` is fast but opaque: there is no way to report how far through a large file you are, and running many in parallel means unbounded I/O. This library copies in a manual read/write loop so progress can be emitted per chunk, and bounds the number of in-flight copies with a simple concurrency limit. The trade-off is speed: `copyFile` can use platform copy-on-write or sendfile; this library cannot, because those syscalls don't surface intermediate progress. If you don't need progress, use `fsPromises.copyFile`.

The progress callback fires once per read chunk — for 64 KiB chunks on a 1 GiB file that is ~16k callbacks. Throttle on your side if that is too chatty; this library deliberately does not impose a cadence.

## Edge cases

- Errors are accumulated, not thrown. If three of ten files fail, `copyFiles` returns `{ copied: [...7...], errors: [...3...] }` and does not reject. Inspect `errors` after awaiting.
- `concurrency` values below 1 are clamped to 1. Non-finite values become 1. We don't throw — callers computing concurrency from a live workload may legitimately hit zero.
- `total` in the progress callback comes from `stat().size`. For files where stat fails (e.g. some special files) `total` is `null` and only `bytes` (copied so far) is reliable.
- `copyFilesBatched` yields results in completion order, not input order. If you need input order, sort by task after the fact.

## API

- `copyFiles(tasks, opts?) → Promise<{ copied: Array<{dest, bytes}>, errors: Array<{task, error}> }>`
- `copyFilesBatched(tasks, opts?) → AsyncGenerator<{ ok: true, result } | { ok: false, task, error }>`
- `opts.concurrency` (default 4)
- `opts.bufferSize` (default 65536)
- `opts.onProgress({ dest, bytes, total })`

## Running the tests

```
node --test
```
