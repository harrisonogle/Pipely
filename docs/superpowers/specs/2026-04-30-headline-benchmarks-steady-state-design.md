# Flip headline benchmarks to steady-state shape

**Date:** 2026-04-30
**Status:** approved (brainstorming) — pending implementation plan

## Motivation

The README's headline benchmark table currently reports per-iteration
allocations with a fresh `Pipe` constructed each iteration. Because Pipely
allocates two `TripleBuffer<T>` and two `PipelyAwaiter<T>` as classes (where
the BCL embeds the equivalent state directly in `Pipe`), per-`Pipe`
construction is ~1.7 KB heavier than BCL's. That makes Pipely's headline
allocation ratio look 1.20–1.30× heavier than BCL even though the
*steady-state* per-op cost ties BCL byte-for-byte at every scheduler.

For the production use case — long-lived pipes — the steady-state number
is the more representative measurement. The reused-`Pipe` companion classes
already exist (`SteadyStateSchedulerBenchmarks`, `SteadyStateThroughputBenchmarks`)
and produce that number; this change makes them the headline and
demotes the fresh-`Pipe` shape to a companion that exposes per-`Pipe`
construction cost via the per-iter / steady-state delta.

## Scope

In scope:
- Code: flip class roles in two BDN file pairs in `tests/Pipely.Benchmarks/`.
- Docs: update README headline table and explanatory paragraph; reorder
  `RESULTS.md` so headline sections report steady-state numbers; update
  `--filter` examples to match new class names.
- Re-measure both BDN classes on the current commit so the headline
  numbers come from a single process invocation each.

Out of scope:
- No `Pipely` source changes.
- No public API changes.
- Latency harness untouched.
- No new benchmark variants, scheduler tunables, or chunk-size sweeps.

## Code changes

Two file pairs swap roles. The reused-`Pipe` shape becomes canonical; the
fresh-`Pipe` shape becomes a "FreshPipe" companion.

| Before (fresh-Pipe, headline)                           | Before (reused, companion)                                                | After (reused, headline)                | After (fresh-Pipe, companion)               |
|---------------------------------------------------------|---------------------------------------------------------------------------|-----------------------------------------|---------------------------------------------|
| `SchedulerBenchmarks.cs` (class `SchedulerBenchmarks`)  | `SteadyStateSchedulerBenchmarks.cs` (class `SteadyStateSchedulerBenchmarks`) | `SchedulerBenchmarks.cs` (reused-Pipe)  | `FreshPipeSchedulerBenchmarks.cs`           |
| `ThroughputBenchmarks.cs` (class `ThroughputBenchmarks`) | `SteadyStateThroughputBenchmarks.cs` (class `SteadyStateThroughputBenchmarks`) | `ThroughputBenchmarks.cs` (reused-Pipe) | `FreshPipeThroughputBenchmarks.cs`          |

Mechanically per pair:
1. Take the body of the current `SteadyState*Benchmarks.cs` and move it into
   `*Benchmarks.cs` (rename the class to drop the `SteadyState` prefix).
2. Take the body of the current `*Benchmarks.cs` and move it into
   `FreshPipe*Benchmarks.cs` (rename the class with the `FreshPipe` prefix).
3. Rewrite the file-header comment in each so the reused-`Pipe` version
   describes itself as the canonical/headline measurement and the fresh-`Pipe`
   version describes itself as the companion that surfaces per-`Pipe`
   construction cost via the delta to the headline.

No `using` changes are expected; the Pipely / BCL `Pipe` disambiguation
already in those files (`Pipely.Pipe` for the qualified name) is unaffected.

## Documentation changes

### README headline table

Replace the current per-iteration table with steady-state numbers from a
fresh BDN run on this commit. Rework the trailing paragraph that begins
"Allocations above are per-iteration with a fresh `Pipe` constructed each
time" — the new framing should be:

- The headline table reports per-1 MiB-transfer cost with the `Pipe` reused
  across iterations (production shape: long-lived pipes).
- For per-`Pipe` construction cost, see `RESULTS.md` — the
  `FreshPipeSchedulerBenchmarks` companion measures fresh-`Pipe` per
  iteration and the delta is the per-`Pipe` ctor cost.

### RESULTS.md

Reorder so headline = steady-state and fresh-`Pipe` = companion:

1. **Throughput** section reports `ThroughputBenchmarks` (reused-`Pipe`)
   numbers. Update the prose so the per-op allocation discussion is the
   headline statement, not a "see steady-state below" footnote.
2. **Five-way scheduler matrix** reports `SchedulerBenchmarks` (reused-`Pipe`)
   numbers as the headline. Same prose flip.
3. **Per-`Pipe` construction cost** section keeps its current shape: it
   reports the delta between the headline (now steady-state) and the
   `FreshPipe*` companion. The numbers stay equivalent — only the framing
   changes (delta instead of "extra cost in the fresh-Pipe row").
4. The fresh-`Pipe` numbers themselves move into the construction-cost
   section as the secondary data point used to compute the delta.

### Bench command examples

Update `--filter` invocations in README and RESULTS.md to use anchored
patterns, because `*SchedulerBenchmarks*` and `*ThroughputBenchmarks*` will
now match both the headline class and the `FreshPipe*` companion (the
suffix is shared). Anchor against the full namespace-qualified name:

- Headline scheduler matrix:
  `--filter 'PipelyBenchmarks.SchedulerBenchmarks.*'`
- FreshPipe companion (scheduler):
  `--filter 'PipelyBenchmarks.FreshPipeSchedulerBenchmarks.*'`
- Headline throughput:
  `--filter 'PipelyBenchmarks.ThroughputBenchmarks.*'`
- FreshPipe companion (throughput):
  `--filter 'PipelyBenchmarks.FreshPipeThroughputBenchmarks.*'`

Existing invocations that cite `SteadyStateSchedulerBenchmarks` /
`SteadyStateThroughputBenchmarks` get rewritten to the headline class
name (since the steady-state shape is now the headline) or to the
`FreshPipe*` name when the per-`Pipe` ctor cost is the target.

## Re-measurement

Numbers in the headline table must come from a single BDN process
invocation so the five rows are directly comparable (current RESULTS.md
methodology rule). Concretely:

- `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*SchedulerBenchmarks*'`
  on the post-rename HEAD. Capture the five-row table, paste into both the
  README headline and the matching RESULTS.md section.
- `dotnet run -c Release --project tests/Pipely.Benchmarks -- --filter '*ThroughputBenchmarks*'`
  on the post-rename HEAD. Capture the two-row table, paste into the
  RESULTS.md "Throughput" section.
- Optionally: re-run the `FreshPipe*` companions to refresh the
  per-`Pipe` ctor delta in RESULTS.md. The existing per-iter numbers in
  RESULTS.md are recent (commit `af5f617`) and can stand in if the
  re-measurement budget is tight.

Sanity check: the new headline numbers should match the existing
"Steady-state, five-way scheduler matrix" / "Steady-state throughput" rows
in RESULTS.md within run noise. Any large divergence is a signal to
investigate before publishing.

## Risks / non-issues

- **Reader confusion** ("why does Pipely list per-1 MiB allocations under
  a kilobyte while BCL community benchmarks routinely cite >5 KB?"): the
  README's reworked paragraph addresses this directly by naming the
  measurement shape and pointing at the construction-cost breakdown.
- **Cherry-picking concern**: the steady-state shape is the production-shape
  measurement, not a more-flattering selection. The fresh-`Pipe` data
  remains in RESULTS.md, so a reader can compute any framing they want.
- **Filter-pattern collisions**: `*ThroughputBenchmarks*` will now match
  both `ThroughputBenchmarks` and `FreshPipeThroughputBenchmarks` (because
  the suffix is shared). For headline runs, use the more specific
  `--filter 'PipelyBenchmarks.ThroughputBenchmarks*'` (anchored to the
  full name) to exclude the FreshPipe class. Same for the scheduler pair.
  This is documented inline in the bench command examples.

## Done criteria

- Both BDN file pairs renamed/swapped per the table above.
- README headline table reports steady-state numbers from a single fresh
  BDN run; explanatory paragraph reworked.
- RESULTS.md sections reordered; per-`Pipe` ctor section updated to use
  delta framing.
- `dotnet build -c Release` and `dotnet test` pass.
- Re-measurement run captured and the numbers reflected in both files.
