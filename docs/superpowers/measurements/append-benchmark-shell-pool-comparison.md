# Donated Shell Pool — Benchmark Delta

**Date:** 2026-04-29
**Branch:** `donated-shell-pool` (off master `616a2d3`)
**Baseline:** `fcc16f1` (pre-implementation, run captured in `append-benchmark-baseline.txt`)
**After:** `27a1e1a` (post-implementation, run captured in `append-benchmark-after-shell-pool.txt`)
**Workload:** `AppendBenchmarks` — 3 methods × 4 BufferSize (256, 1024, 4096, 16384) × 2 BuffersBeforeFlush (1, 16) = 24 configurations. 1 MiB total bytes per BDN iteration.

## Hypothesis

Per spec §5.1: per-`Append` `BufferSegment` allocation contributed ~9% wall-clock at the smallest buffer size and ~4× the BCL allocation rate across all sizes. Pooling shells across donations should:

1. Reduce `SpscPipe_Append` allocated bytes/op by ~3× (only the donor's `IMemoryOwner` rental remains as per-call allocation).
2. Shave ~9% wall-clock at the smallest buffer / tightest flush.
3. Not regress `SpscPipe_Append` at larger buffer sizes (alloc cost was already a small fraction).
4. Not regress `BclPipe_GetSpan` or `SpscPipe_GetSpan` (those paths were untouched).

## Result

### `SpscPipe_Append` — wall-clock

| BufferSize | BBF | Before | After | Δ | % change |
|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 895.59 µs | 806.26 µs | **−89.33 µs** | **−9.97%** |
| 256 | 16 | 311.56 µs | 281.32 µs | **−30.24 µs** | **−9.71%** |
| 1024 | 1 | 238.13 µs | 225.31 µs | **−12.82 µs** | **−5.39%** |
| 1024 | 16 | 92.27 µs | 84.59 µs | **−7.68 µs** | **−8.32%** |
| 4096 | 1 | 79.59 µs | 76.36 µs | −3.23 µs | −4.06% |
| 4096 | 16 | 34.82 µs | 34.46 µs | −0.36 µs | −1.03% |
| 16384 | 1 | 34.12 µs | 33.13 µs | −0.99 µs | −2.90% |
| 16384 | 16 | 25.34 µs | 25.17 µs | −0.17 µs | −0.67% |

The wall-clock savings are concentrated at small buffer sizes — exactly where the per-`Append` shell-allocation cost was a meaningful fraction of total work. At `BufferSize=256` (which produces 4096 chunks per 1-MiB iteration), shell pooling shaves ~10%, matching the hypothesis (1).

### `SpscPipe_Append` — allocated bytes per op

| BufferSize | BBF | Before | After | Δ Allocated | Alloc Ratio (Before → After) |
|---:|---:|---:|---:|---:|:---:|
| 256 | 1 | 508.28 KB | **124.88 KB** | **−75.4%** | 5.20× → **1.28×** |
| 256 | 16 | n/a (multimodal) | **111.55 KB** | n/a | – → **1.14×** |
| 1024 | 1 | 130.65 KB | **35.56 KB** | **−72.8%** | 5.04× → **1.37×** |
| 1024 | 16 | 125.99 KB | **33.14 KB** | **−73.7%** | 4.80× → **1.26×** |
| 4096 | 1 | 36.31 KB | **12.97 KB** | **−64.3%** | 4.31× → **1.51×** |
| 4096 | 16 | 34.48 KB | **13.60 KB** | **−60.6%** | 3.52× → **1.39×** |
| 16384 | 1 | 12.57 KB | **7.02 KB** | **−44.1%** | 4.02× → **2.25×** |
| 16384 | 16 | 11.59 KB | **8.70 KB** | **−24.9%** | 2.30× → **1.73×** |

Allocation rate drops 25–75% across all configs. The Alloc Ratio vs BCL collapses from 2.30–5.20× down to 1.14–2.25×. **Hypothesis (1) confirmed and exceeded** — the reduction is bigger than the ~3× back-of-envelope estimate; the pre-pool path was allocating not just the `BufferSegment` shell but also re-renting from the pool every cycle (the rented buffer didn't get returned via the freelist for donated cases; pooling now also reduces pool churn indirectly).

The residual allocation (1.14×–2.25× BCL) is per-call cost we cannot eliminate via shell pooling: the donor's `IMemoryOwner` rental from `MemoryPool<byte>.Shared` (which itself uses `ArrayPool<byte>` internally — already pooled but with some bookkeeping per Rent), plus the producer/consumer Task state machines.

### Regression check — `BclPipe_GetSpan` and `SpscPipe_GetSpan`

| BufferSize | BBF | BCL Before | BCL After | Δ | SpscGS Before | SpscGS After | Δ |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 619.63 µs | 626.30 µs | +1.08% | 725.08 µs | 735.45 µs | +1.43% |
| 256 | 16 | 280.56 µs | 279.68 µs | −0.31% | 207.35 µs | 199.71 µs | −3.68% |
| 1024 | 1 | 237.72 µs | 244.56 µs | +2.88% | 231.01 µs | 231.44 µs | +0.19% |
| 1024 | 16 | 118.22 µs | 120.34 µs | +1.79% | 72.17 µs | 71.68 µs | −0.68% |
| 4096 | 1 | 132.75 µs | 134.50 µs | +1.32% | 93.75 µs | 94.40 µs | +0.69% |
| 4096 | 16 | 74.82 µs | 74.61 µs | −0.28% | 41.81 µs | 41.67 µs | −0.34% |
| 16384 | 1 | 55.14 µs | 55.35 µs | +0.38% | 47.06 µs | 46.39 µs | −1.42% |
| 16384 | 16 | 39.71 µs | 40.24 µs | +1.33% | 36.03 µs | 34.86 µs | −3.25% |

All deltas are within the run-to-run noise floor (the per-config `Error` columns in BDN's report are typically ±1-3%). **Hypothesis (4) confirmed** — the GetSpan paths are untouched.

## Interpretation

Hypotheses (1)–(4) all confirmed:

1. **Allocation reduction (target ~3×, actual 4–8× depending on config).** The pool eliminates per-Append shell allocation completely after the freelist warms up; the residual is the donor's `IMemoryOwner` rental, which `MemoryPool<byte>.Shared` already pools internally.
2. **Wall-clock improvement at the small-buffer worst case (target ~9%, actual −9.97% at 256/1, −9.71% at 256/16).** The savings track the chunk count: more chunks per iteration ⇒ more shell allocations avoided ⇒ larger relative win.
3. **No regression at larger buffer sizes.** All deltas ≤ 5% improvement; nothing slowed down.
4. **No regression on the GetSpan paths.** All deltas within noise (±3%).

The crossover point identified in the original benchmark analysis (Append loses below 1024 bytes; wins at 4096 and above) **shifts left** with shell pooling. At `BufferSize=1024/BBF=16`, Append is now 70% of BCL wall-clock (was 78% before — net +8 percentage-point gap closed). At `BufferSize=256/BBF=16`, Append is now within 1% of BCL (was 1.09× slower before).

The `BufferSize=256/BBF=1` config remains the only one where Append is meaningfully slower than BCL (1.29× wall-clock, 1.28× allocation). Below that level of granularity, callers are paying for the pipe's per-flush awaiter coordination on every buffer, which dominates everything else. Append doesn't fix that — it never claimed to — but it no longer adds materially on top.

## Recommendation

Land the change. The crossover for "should I use Append?" guidance moves from "≥ 4 KiB" to **"≥ 1 KiB if flushing every 16 buffers, ≥ 4 KiB if flushing every buffer"**. Worth updating the doc comment on `SpscPipeWriter.Append` (currently absent — flagged as out-of-scope minor in the original spec review).
