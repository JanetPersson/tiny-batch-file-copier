import { open } from 'node:fs/promises';
import { basename } from 'node:path';

/**
 * Default read/write buffer size. 64 KiB matches Node's default high-water mark for
 * fs streams and is a reasonable trade between syscall count and memory. Using a
 * single fixed size keeps the observed progress increments stable and predictable.
 */
const DEFAULT_BUFFER_SIZE = 64 * 1024;

/**
 * Minimum concurrency Node allows in a single event loop tick. Math.max(1, n)
 * guards against accidental zero or negative values without throwing, since a
 * caller who computes concurrency from a live workload may reasonably hit zero.
 */
function normalizeConcurrency(concurrency) {
  const c = Math.max(1, Math.floor(concurrency));
  return Number.isFinite(c) ? c : 1;
}

/**
 * Copy a single file from src to dest, invoking onProgress periodically.
 *
 * We copy in a manual read/write loop rather than fsPromises.copyFile so we can
 * report byte-level progress. Each progress callback fires with a monotonically
 * increasing bytes figure; to avoid flooding the event loop the caller will
 * typically throttle on their side (this library does not impose throttling —
 * it reports every chunk so callers can choose their own cadence).
 *
 * @param {{ src: string, dest: string }} task
 * @param {object} [opts]
 * @param {number} [opts.bufferSize=65536]
 * @param {(ctx: { dest: string, bytes: number, total: number|null }) => void} [opts.onProgress]
 * @returns {Promise<{ dest: string, bytes: number }>}
 */
export async function copyOne(task, opts = {}) {
  const bufferSize = Math.max(1, opts.bufferSize || DEFAULT_BUFFER_SIZE);
  const onProgress = opts.onProgress || null;

  if (!task || typeof task !== 'object' || !task.src || !task.dest) {
    throw new Error('copyOne: task must have string src and dest');
  }

  const src = task.src;
  const dest = task.dest;

  const srcHandle = await open(src, 'r');
  let total = null;
  try {
    const stat = await srcHandle.stat();
    total = stat.size;
  } catch {
    // If stat fails (e.g. special file or transient issue) we proceed with
    // total = null; progress callbacks still report bytes copied.
    total = null;
  }

  const destHandle = await open(dest, 'w', 0o644);
  const buf = Buffer.allocUnsafe(bufferSize);
  let copied = 0;

  try {
    let result;
    while (true) {
      result = await srcHandle.read(buf, 0, bufferSize, null);
      const bytesRead = result.bytesRead;
      if (bytesRead === 0) break;
      const toWrite = bytesRead === bufferSize ? buf : buf.subarray(0, bytesRead);
      await destHandle.write(toWrite, 0, bytesRead, null);
      copied += bytesRead;
      if (onProgress) {
        onProgress({ dest, bytes: copied, total });
      }
    }
  } finally {
    let writeErr = null;
    let readErr = null;
    try { await destHandle.close(); } catch (e) { writeErr = e; }
    try { await srcHandle.close(); } catch (e) { readErr = e; }
    if (writeErr && !readErr) throw writeErr;
    if (readErr && !writeErr) throw readErr;
    if (readErr && writeErr) { readErr.cause = writeErr; throw readErr; }
  }

  return { dest, bytes: copied };
}

/**
 * Copy multiple files concurrently with a bounded concurrency limit.
 *
 * Tasks are started as slots free up rather than chunked into waves, so we get
 * smooth scheduling even when individual files differ wildly in size.
 *
 * Errors are accumulated, not thrown mid-run: every queued task gets a chance to
 * run before we report the aggregated outcome. This makes the function safe to
 * use for batch jobs where partial progress must be preserved.
 *
 * @param {Array<{src:string,dest:string}>} tasks
 * @param {object} [opts]
 * @param {number} [opts.concurrency=4]
 * @param {number} [opts.bufferSize=65536]
 * @param {(ctx:{dest:string,bytes:number,total:number|null})=>void} [opts.onProgress]
 * @returns {Promise<{copied:Array<{dest:string,bytes:number}>,errors:Array<{task:{src:string,dest:string},error:Error}>}>}
 */
export async function copyFiles(tasks, opts = {}) {
  const concurrency = normalizeConcurrency(opts.concurrency ?? 4);
  const bufferSize = opts.bufferSize || DEFAULT_BUFFER_SIZE;
  const onProgress = opts.onProgress || null;

  const queue = Array.isArray(tasks) ? tasks.slice() : [];
  const copied = [];
  const errors = [];
  let cursor = 0;

  async function worker() {
    while (cursor < queue.length) {
      const i = cursor++;
      const task = queue[i];
      try {
        const result = await copyOne(task, { bufferSize, onProgress });
        copied.push(result);
      } catch (err) {
        errors.push({ task, error: err instanceof Error ? err : new Error(String(err)) });
      }
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(concurrency, queue.length); i++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  return { copied, errors };
}

/**
 * Variant of copyFiles that streams results as they complete, for callers who
 * want to react to per-file completion without awaiting the whole batch.
 *
 * Yields objects of shape { ok: true, result } on success or { ok: false, task,
 * error } on failure. We use async iteration so the concurrency is driven by
 * the consumer's pull rate — one result is produced each time next() is called,
 * while internally we keep concurrency slots filled.
 *
 * @param {Array<{src:string,dest:string}>} tasks
 * @param {object} [opts]
 * @returns {AsyncGenerator<{ok:boolean,result?:{dest:string,bytes:number},task?:{src:string,dest:string},error?:Error}>}
 */
export async function* copyFilesBatched(tasks, opts = {}) {
  const concurrency = normalizeConcurrency(opts.concurrency ?? 4);
  const bufferSize = opts.bufferSize || DEFAULT_BUFFER_SIZE;
  const onProgress = opts.onProgress || null;

  const queue = Array.isArray(tasks) ? tasks.slice() : [];
  const pending = [];
  let cursor = 0;

  function spawnNext() {
    if (cursor >= queue.length) return null;
    const idx = cursor++;
    const task = queue[idx];
    const p = (async () => {
      try {
        const result = await copyOne(task, { bufferSize, onProgress });
        return { ok: true, result };
      } catch (err) {
        return { ok: false, task, error: err instanceof Error ? err : new Error(String(err)) };
      }
    })();
    pending.push(p);
    return p;
  }

  for (let i = 0; i < Math.min(concurrency, queue.length); i++) {
    spawnNext();
  }

  while (pending.length > 0) {
    const settled = await Promise.race(pending.map(async (p, i) => {
      const v = await p;
      return { idx: i, value: v };
    }));
    pending.splice(settled.idx, 1);
    spawnNext();
    yield settled.value;
  }
}
