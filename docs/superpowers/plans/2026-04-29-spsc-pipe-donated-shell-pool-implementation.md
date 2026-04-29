# Donated Shell Pool — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pool `BufferSegment` shells across donated `Append` cycles to eliminate per-call shell allocation. Per the §5.1 amendment to `2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md`. AppendBenchmarks results showed per-Append shell allocation contributing ~9% wall-clock at small buffers and ~4× the BCL allocation rate across all sizes.

**Architecture:** Add a writer-private LIFO freelist (`_donatedShellFreelistHead`/`_donatedShellFreelistCount`) physically separate from the existing rented-segment freelist. After `RecycleDrainedSegments` calls `DisposeOwned` on a donated segment, push the now-blank shell to the donated-shell freelist (capped at `_options.MaxFreelistSegments`; over-cap drops to GC). `Append` pops a shell from this freelist before falling back to `new BufferSegment()`. `AdoptFrom` overwrites every mutable field, so a recycled shell is functionally indistinguishable from a fresh one.

**Tech Stack:** C# / .NET 10, xUnit 2.9.3.

**Reference docs (engineer should re-read before starting):**
- `docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md` — §5 Recycle path (updated), §5.1 Donated-shell freelist rationale (NEW), §10.3 test additions, §11 (out-of-scope item brought in).
- `docs/superpowers/measurements/append-benchmark-baseline.txt` — pre-change benchmark baseline; compare against post-change run.
- `src/Pipely/Pipe.cs` — modified in Task 2 (fields, helpers, recycle branch, Dispose clearing).
- `src/Pipely/Pipe.Writer.cs` — modified in Task 2 (Append's allocation step in both bootstrap and steady-state branches).
- `tests/Pipe.Tests/PipeWriterAppendTests.cs` — modified in Task 1 (new shell-pooling tests; one existing test renamed/retargeted).

**Working directory for all commands:** `/home/harrison/src/worktrees/Pipe/donated-shell-pool/` (already a dedicated worktree on branch `donated-shell-pool` off master `616a2d3`).

**Pre-work invariants this plan preserves:**
- All existing tests continue to pass; the only test change is replacing `Recycle_DonatedSegmentNotPushedToFreelist` (whose name is now ambiguous) with a tighter, name-corrected version, plus added shell-freelist coverage.
- Rented-segment recycle behavior is untouched.
- `RecycleDrainedSegments`'s walk predicate is unchanged.
- `BufferSegment.AdoptFrom` is unchanged.
- The cap on the new freelist is `_options.MaxFreelistSegments` (default 256), reusing the existing option to keep the surface minimal.

---

## File structure

```
src/Pipely/
├── Pipe.cs                        (modified — Task 2: + 2 fields, + 2 helpers,
│                                                    branch RecycleDrainedSegments, clear in Dispose)
├── Pipe.Writer.cs                 (modified — Task 2: pop shell before new BufferSegment()
│                                                    in both Append branches)

tests/Pipe.Tests/
└── PipeWriterAppendTests.cs       (modified — Task 1: rename + retarget one existing test;
                                                   add 4 new shell-freelist tests)

docs/superpowers/measurements/
└── append-benchmark-baseline.txt      (NEW — captured before this plan's first commit;
                                              compared against post-change run in Task 3)
```

---

## Task 0: Pre-flight — confirm clean baseline + benchmark capture

**Files:** none modified.

- [ ] **Step 1: Confirm working directory and branch**

```bash
pwd && git branch --show-current && git status --short
```

Expected:
```
/home/harrison/src/worktrees/Pipe/donated-shell-pool
donated-shell-pool

```

- [ ] **Step 2: Confirm spec amendment is in place**

```bash
grep -nE "Section 5.1|donated-shell freelist|PushDonatedShellFreelist" \
  docs/superpowers/specs/2026-04-28-spsc-pipe-buffer-ownership-transfer-design.md
```

Expected: matches in §5.1, §10.3, and §11 cross-references.

- [ ] **Step 3: Confirm baseline benchmark file exists**

```bash
ls -la docs/superpowers/measurements/append-benchmark-baseline.txt
```

Expected: file exists with the pre-change BDN output. If missing, the controller must run the baseline benchmark before proceeding.

- [ ] **Step 4: Build the solution**

```bash
dotnet build Pipe.slnx -c Release --nologo
```

Expected: 6 projects build, 0 errors, 1 pre-existing xUnit1030 warning.

- [ ] **Step 5: Run the full test suite**

```bash
dotnet test Pipe.slnx -c Release --nologo
```

Expected: 134 tests passing (119 Pipe.Tests + 15 Pipely.HotHandoff.Tests). This is the baseline for Task 2's "no regressions" check.

---

## Task 1: Write/replace failing tests for shell pooling (TDD red bar)

**Goal:** Capture the new behavior in tests before any source change. The TDD red bar: tests reference `_donatedShellFreelistCount`, which doesn't exist yet, so compilation fails. Implementation in Task 2 makes them green.

**Files:**
- Modify: `tests/Pipe.Tests/PipeWriterAppendTests.cs`

- [ ] **Step 1: Replace the existing `Recycle_DonatedSegmentNotPushedToFreelist`-style test (now renamed `Recycle_DonatedSegmentDisposesOwner_FreelistDoesNotAbsorbIt` after the previous code-review pass)**

Find the existing test in `tests/Pipe.Tests/PipeWriterAppendTests.cs`:

```csharp
    [Fact]
    public async Task ReaderDrainsPastDonated_DisposesOwner_FreelistDoesNotAbsorbIt()
    {
        // ...
    }
```

Rename and retarget it to be unambiguous about which freelist is asserted, and add explicit shell-freelist assertions:

```csharp
    [Fact]
    public async Task ReaderDrainsPastDonated_DisposesOwner_RentedFreelistUntouchedAndShellPooled()
    {
        // Use a donated-only chain (donated1 + donated2) so the freelist count assertion
        // is exact: zero donated segments should land in the rented freelist regardless of
        // the recycle path's behavior on rented segments. Donated shells go to the
        // separate _donatedShellFreelist instead.
        using var pipe = new Pipely.Pipe();
        var donated1 = new TrackingMemoryOwner(30);
        var donated2 = new TrackingMemoryOwner(20);
        pipe.Writer.Append(donated1);
        pipe.Writer.Append(donated2);

        int rentedFreelistBefore = pipe._freelistCount;
        int shellFreelistBefore  = pipe._donatedShellFreelistCount;

        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        // Drain past donated1 (consume the first 30 bytes; donated2 stays as _writingHead).
        pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(30));

        // Next FlushAsync runs RecycleDrainedSegments and recycles donated1.
        await pipe.Writer.FlushAsync();

        // donated1 is foreign-owner: must be Disposed, must NOT enter the rented freelist,
        // must enter the donated-shell freelist.
        Assert.Equal(1, donated1.DisposeCount);
        Assert.Equal(rentedFreelistBefore, pipe._freelistCount);
        Assert.Equal(shellFreelistBefore + 1, pipe._donatedShellFreelistCount);
        // donated2 is still the active tail; not yet recycled.
        Assert.Equal(0, donated2.DisposeCount);
    }
```

- [ ] **Step 2: Add four new tests**

Append to the same file (inside the existing class), in the `// ---------- Recycle path ...` section:

```csharp
    [Fact]
    public async Task Append_AfterRecycle_ReusesShellFromFreelist()
    {
        // After a donated segment recycles into the shell freelist, the next Append
        // pops that shell instead of allocating a new BufferSegment. The popped shell
        // gets fully reinitialized via AdoptFrom — caller cannot distinguish from fresh.
        using var pipe = new Pipely.Pipe();
        var donated1 = new TrackingMemoryOwner(30);
        var donated2 = new TrackingMemoryOwner(20);
        pipe.Writer.Append(donated1);
        pipe.Writer.Append(donated2);

        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(30));
        await pipe.Writer.FlushAsync();

        // Now donated1's shell is on the shell freelist.
        Assert.Equal(1, pipe._donatedShellFreelistCount);

        // Append a third donation. The shell freelist should drain.
        var donated3 = new TrackingMemoryOwner(15);
        pipe.Writer.Append(donated3);

        Assert.Equal(0, pipe._donatedShellFreelistCount);
        // The new tail is donated and correctly initialized via AdoptFrom.
        Assert.True(pipe._writingHead!.IsDonated);
        Assert.Same(pipe, pipe._writingHead.OwnerToken);
        Assert.Equal(15, pipe._writingHead.End);
    }

    [Fact]
    public async Task ShellFreelist_RespectsCap()
    {
        // With MaxFreelistSegments = 2, only 2 shells should pool; the rest drop to GC.
        // We don't have a public way to observe GC drops directly, but we can assert
        // the freelist count never exceeds the cap.
        var options = new PipeOptions(maxFreelistSegments: 2);
        using var pipe = new Pipely.Pipe(options);

        // Cycle: append + flush + drain + flush, repeated, with a final donated tail
        // each cycle that doesn't get recycled (so the chain has > 2 recyclable donateds).
        var owners = new List<TrackingMemoryOwner>();
        for (int i = 0; i < 5; i++)
        {
            var o = new TrackingMemoryOwner(8);
            owners.Add(o);
            pipe.Writer.Append(o);
        }
        // Active tail (last Append) prevents recycle of the 5th; first 4 are recyclable.

        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(8 * 4));   // drain past first 4
        await pipe.Writer.FlushAsync();

        // 4 donated segments were recycled; only 2 fit in the shell freelist.
        Assert.Equal(2, pipe._donatedShellFreelistCount);
    }

    [Fact]
    public void Dispose_ClearsShellFreelist()
    {
        // After Dispose, the shell freelist is reset. No IMemoryOwners to dispose
        // (those were released in RecycleDrainedSegments before pooling); just clear
        // the head + count.
        var pipe = new Pipely.Pipe();
        var donated1 = new TrackingMemoryOwner(8);
        var donated2 = new TrackingMemoryOwner(8);
        pipe.Writer.Append(donated1);
        pipe.Writer.Append(donated2);
        // Force at least one recycle so the shell freelist has an entry.
        pipe.Writer.FlushAsync().GetAwaiter().GetResult();
        var rr = pipe.Reader.ReadAsync().GetAwaiter().GetResult();
        pipe.Reader.AdvanceTo(rr.Buffer.GetPosition(8));
        pipe.Writer.FlushAsync().GetAwaiter().GetResult();
        Assert.Equal(1, pipe._donatedShellFreelistCount);

        pipe.Dispose();

        Assert.Null(pipe._donatedShellFreelistHead);
        Assert.Equal(0, pipe._donatedShellFreelistCount);
    }

    [Fact]
    public async Task ShellFreelist_PoppedShellHasIsDonatedTrue_AndNoStaleOwnerToken()
    {
        // Defensive regression guard: a popped shell goes through AdoptFrom which sets
        // OwnerToken = pipe and IsDonated = true unconditionally. Even though the
        // pre-pop shell already had IsDonated=true and OwnerToken=pipe (set by the
        // previous AdoptFrom), this test pins the property so a future change to the
        // pop logic (e.g., reset-on-pop) doesn't accidentally regress.
        using var pipe = new Pipely.Pipe();
        var donated1 = new TrackingMemoryOwner(8);
        pipe.Writer.Append(donated1);
        await pipe.Writer.FlushAsync();
        var rr = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(rr.Buffer.End);
        await pipe.Writer.FlushAsync();
        Assert.Equal(1, pipe._donatedShellFreelistCount);

        var donated2 = new TrackingMemoryOwner(8);
        pipe.Writer.Append(donated2);

        var seg = pipe._writingHead!;
        Assert.True(seg.IsDonated);
        Assert.Same(pipe, seg.OwnerToken);
    }
```

- [ ] **Step 3: Run tests to verify red bar**

```bash
dotnet test tests/Pipe.Tests/Pipe.Tests.csproj --filter "FullyQualifiedName~PipeWriterAppendTests" --nologo 2>&1 | tail -10
```

Expected: compilation error referencing `_donatedShellFreelistCount` and/or `_donatedShellFreelistHead` — these symbols don't exist yet.

If the build succeeds at this step, that means the new tests didn't actually reference the new fields. That's a TDD failure (test isn't pinning the new behavior). Re-read Step 2's tests and confirm `pipe._donatedShellFreelistCount` and `pipe._donatedShellFreelistHead` are referenced.

- [ ] **Step 4: Commit the failing tests**

```bash
git add tests/Pipe.Tests/PipeWriterAppendTests.cs
git commit -m "$(cat <<'EOF'
Tests: shell-pooling expectations for donated-segment recycle (TDD red bar)

Pins the spec §5.1 contract before implementing it:
  - ReaderDrainsPastDonated_*RentedFreelistUntouchedAndShellPooled
    (rename + tighten of the pre-existing donated-recycle test):
    asserts _freelistCount unchanged AND _donatedShellFreelistCount
    incremented exactly by 1 per donated drain.
  - Append_AfterRecycle_ReusesShellFromFreelist: shell freelist
    drains by 1 when Append fires after a recycle; the popped shell
    is correctly reinitialized via AdoptFrom.
  - ShellFreelist_RespectsCap: with maxFreelistSegments=2 and 4
    recyclable donated segments, the shell freelist count caps at 2
    (over-cap shells drop to GC).
  - Dispose_ClearsShellFreelist: Pipe.Dispose nulls the shell
    freelist head and resets count to 0.
  - ShellFreelist_PoppedShellHasIsDonatedTrue_AndNoStaleOwnerToken:
    regression guard for AdoptFrom's overwrite-everything contract.

Compilation fails at this commit because _donatedShellFreelistCount
and _donatedShellFreelistHead don't exist yet — Task 2 makes them
exist and turns the bar green.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

The commit is intentionally a red-bar commit — the test project will not build at this SHA. Task 2's commit makes it green again.

---

## Task 2: Implement the donated shell freelist

**Goal:** Add the fields, push/pop helpers, recycle branch, Append pop, and Dispose clearing. Bring the red bar from Task 1 to green.

**Files:**
- Modify: `src/Pipely/Pipe.cs`
- Modify: `src/Pipely/Pipe.Writer.cs`

- [ ] **Step 1: Add fields**

Edit `src/Pipely/Pipe.cs`. Find the existing freelist field declarations (around line 20):

```csharp
    internal BufferSegment? _freelistHead;
    internal int  _freelistCount;
```

Replace with:

```csharp
    internal BufferSegment? _freelistHead;
    internal int  _freelistCount;
    internal BufferSegment? _donatedShellFreelistHead;
    internal int  _donatedShellFreelistCount;
```

- [ ] **Step 2: Add Push/Pop helpers**

Edit `src/Pipely/Pipe.cs`. Find the existing `PushFreelist` method (around line 125). After its closing `}`, add:

```csharp

    internal void PushDonatedShellFreelist(BufferSegment shell)
    {
        if (_donatedShellFreelistCount >= _options.MaxFreelistSegments)
            return;     // cap exceeded; drop the shell to GC
        shell.SetFreelistNext(_donatedShellFreelistHead);
        _donatedShellFreelistHead = shell;
        _donatedShellFreelistCount++;
    }

    internal BufferSegment? PopDonatedShellFreelist()
    {
        var head = _donatedShellFreelistHead;
        if (head == null) return null;
        _donatedShellFreelistHead = head.Next;
        head.SetFreelistNext(null);   // detach from the freelist link
        _donatedShellFreelistCount--;
        return head;
    }
```

- [ ] **Step 3: Wire `RecycleDrainedSegments` to push donated shells**

Edit `src/Pipely/Pipe.cs`. Find `RecycleDrainedSegments` (around line 315). Current loop body:

```csharp
            if (recycled.IsDonated)
                recycled.DisposeOwned();      // foreign owner: release; drop the BufferSegment shell
            else
                PushFreelist(recycled);       // pool-rented: existing freelist path (with cap-overflow handling)
```

Replace with:

```csharp
            if (recycled.IsDonated)
            {
                recycled.DisposeOwned();              // foreign owner: release the IMemoryOwner
                PushDonatedShellFreelist(recycled);   // shell pooled for re-use; over-cap drops to GC
            }
            else
            {
                PushFreelist(recycled);               // pool-rented: existing freelist path (with cap-overflow handling)
            }
```

- [ ] **Step 4: Clear shell freelist in `Dispose`**

Edit `src/Pipely/Pipe.cs`. Find the `Dispose` method (around line 61). After the existing freelist-walk block (the existing code that walks `_freelistHead` and sets `_freelistHead = null; _freelistCount = 0;`), add:

```csharp
        // Donated-shell freelist: shells have no IMemoryOwner (released in RecycleDrainedSegments
        // before pooling). Just clear the head and count; nothing to dispose.
        _donatedShellFreelistHead = null;
        _donatedShellFreelistCount = 0;
```

The exact existing block to find is the one that currently ends with these two lines:

```csharp
        _freelistHead = null;
        _freelistCount = 0;
    }
```

Insert the new block right before the closing `}`:

```csharp
        _freelistHead = null;
        _freelistCount = 0;

        // Donated-shell freelist: shells have no IMemoryOwner (released in RecycleDrainedSegments
        // before pooling). Just clear the head and count; nothing to dispose.
        _donatedShellFreelistHead = null;
        _donatedShellFreelistCount = 0;
    }
```

- [ ] **Step 5: Wire `Append` to pop shells before allocating**

Edit `src/Pipely/Pipe.Writer.cs`. The `Append(IMemoryOwner<byte> buffer, int start, int length)` method has two branches that allocate a `BufferSegment`:

**Bootstrap branch** — currently:

```csharp
        // Bootstrap: pipe has no writing head yet.
        if (_pipe._writingHead == null)
        {
            var donated = new BufferSegment();
            donated.AdoptFrom(buffer, slice, runningIndex: 0, pipeOwner: _pipe);
```

Replace with:

```csharp
        // Bootstrap: pipe has no writing head yet.
        if (_pipe._writingHead == null)
        {
            var donated = _pipe.PopDonatedShellFreelist() ?? new BufferSegment();
            donated.AdoptFrom(buffer, slice, runningIndex: 0, pipeOwner: _pipe);
```

**Steady-state branch** — currently:

```csharp
        var newDonated = new BufferSegment();
        newDonated.AdoptFrom(buffer, slice, newRI, pipeOwner: _pipe);
```

Replace with:

```csharp
        var newDonated = _pipe.PopDonatedShellFreelist() ?? new BufferSegment();
        newDonated.AdoptFrom(buffer, slice, newRI, pipeOwner: _pipe);
```

- [ ] **Step 6: Run the new tests to verify green bar**

```bash
dotnet test tests/Pipe.Tests/Pipe.Tests.csproj --filter "FullyQualifiedName~PipeWriterAppendTests" --nologo 2>&1 | tail -8
```

Expected: all PipeWriterAppendTests pass (the existing 28 plus the 4 new = 32 cases). The previously-renamed test is now in its corrected form.

- [ ] **Step 7: Run the full test suite to verify no regressions**

```bash
dotnet test Pipe.slnx -c Release --nologo 2>&1 | tail -4
```

Expected: total count = 134 + 4 (new) = 138. No failures.

- [ ] **Step 8: Commit**

```bash
git add src/Pipely/Pipe.cs src/Pipely/Pipe.Writer.cs
git commit -m "$(cat <<'EOF'
Pipe: pool BufferSegment shells across donated Append cycles

Implements the spec §5.1 donated-shell freelist: a writer-private
LIFO stack physically separate from the existing rented-segment
freelist. After RecycleDrainedSegments calls DisposeOwned on a
donated segment, the now-blank shell pushes onto the donated-shell
freelist (capped at _options.MaxFreelistSegments; over-cap drops to
GC). Append pops a shell before falling back to new BufferSegment().
AdoptFrom overwrites every mutable field, so a recycled shell is
functionally indistinguishable from a fresh one.

Two freelists rather than one: a donated shell post-DisposeOwned
has AvailableMemory.Length == 0, so it would always fail
PopFreelist's size-check predicate and immediately get disposed-and-
discarded — defeating the pooling. Keeping the freelists physically
separate avoids the interaction.

The cap reuses MaxFreelistSegments; usage data may motivate a
separate option later, but a single cap is simpler and matches the
rented freelist's cap.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: After-benchmark and write up the delta

**Goal:** Re-run AppendBenchmarks against the post-implementation tip; capture into `docs/superpowers/measurements/append-benchmark-after-shell-pool.txt`. Compare to baseline; write a markdown summary of the delta.

**Files:**
- Create: `docs/superpowers/measurements/append-benchmark-after-shell-pool.txt`
- Create: `docs/superpowers/measurements/append-benchmark-shell-pool-comparison.md`

- [ ] **Step 1: Run the benchmark**

```bash
cd /home/harrison/src/worktrees/Pipe/donated-shell-pool && \
  dotnet run --project tests/Pipe.Benchmarks -c Release -- --filter '*AppendBenchmarks*' \
  2>&1 | tee docs/superpowers/measurements/append-benchmark-after-shell-pool.txt
```

Expected: ~12 minutes wall-clock, no errors, summary table at the end.

- [ ] **Step 2: Compare to baseline**

Read both files. The post-change run should show:

- **Allocated bytes per op for `Pipe_Append`** drops substantially across all `BufferSize`/`BuffersBeforeFlush` combinations (target: ~4× → ~1.5–2× the BCL baseline; the residual is the pool-rent for the donor's `IMemoryOwner` itself).
- **Wall-clock for `Pipe_Append` at small buffer sizes** (256, 1024) improves measurably (target: ~9% at 256/1; the alloc cost we identified).
- **Wall-clock for `Pipe_Append` at large buffer sizes** (4096, 16384) stays roughly flat or slightly improves (alloc cost was already a small fraction at large sizes).
- **No regression for `BclPipe_GetSpan` or `Pipe_GetSpan`** — those paths aren't touched.

If any column regresses unexpectedly, STOP and investigate before proceeding. Do not proceed to merge with an unexplained regression.

- [ ] **Step 3: Write the comparison summary**

Create `docs/superpowers/measurements/append-benchmark-shell-pool-comparison.md` with this structure:

```markdown
# Donated Shell Pool — Benchmark Delta

**Date:** 2026-04-29
**Branch:** `donated-shell-pool` (off master `616a2d3`)

## Hypothesis

Per the spec §5.1 motivation: per-Append `BufferSegment` allocation contributes ~9% wall-clock at small buffer sizes and ~4× the BCL allocation rate. Pooling shells across donations should:
- Reduce `Pipe_Append` allocated bytes/op by ~3× (only the donor's `IMemoryOwner` rental remains).
- Shave ~9% wall-clock at the smallest buffer size + tightest flush.
- Not regress `Pipe_Append` at larger buffer sizes.
- Not regress `BclPipe_GetSpan` or `Pipe_GetSpan` (those paths untouched).

## Result

| BufferSize | BBF | Method            | Mean (before) | Mean (after) | Δ Mean | Allocated (before) | Allocated (after) | Δ Allocated |
|---|---|---|---|---|---|---|---|---|
| 256 | 1 | BclPipe_GetSpan | … | … | … | … | … | … |
| 256 | 1 | Pipe_GetSpan | … | … | … | … | … | … |
| 256 | 1 | Pipe_Append | … | … | … | … | … | … |
| 256 | 16 | (same triple) | … | … | … | … | … | … |
| (… all 8 BufferSize × BBF combos …) |

(Fill in from the benchmark output files.)

## Interpretation

(2-3 sentences interpreting the delta against the hypothesis.)
```

Fill in the table with actual numbers from the two benchmark files.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/measurements/append-benchmark-after-shell-pool.txt \
        docs/superpowers/measurements/append-benchmark-shell-pool-comparison.md
git commit -m "$(cat <<'EOF'
Benchmark: capture post-shell-pool AppendBenchmarks results + delta summary

Re-ran AppendBenchmarks against the donated-shell-pool branch tip
to measure the impact of pooling BufferSegment shells across
donated Append cycles. Comparison to the pre-change baseline lives
in shell-pool-comparison.md alongside both raw output files.

(See the comparison markdown for the actual delta.)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Final verification — clean build, full tests, brief stress run

**Goal:** Last sanity check before merging.

**Files:** none modified.

- [ ] **Step 1: Clean build**

```bash
dotnet clean Pipe.slnx -c Release && \
  dotnet build Pipe.slnx -c Release --nologo
```

Expected: 0 errors.

- [ ] **Step 2: Full test suite**

```bash
dotnet test Pipe.slnx -c Release --nologo
```

Expected: 138 passing (134 baseline + 4 new shell-pool tests).

- [ ] **Step 3: Short stress run**

```bash
dotnet run --project tests/Pipe.Stress -c Release
```

Expected: exit 0; the existing zero-leak owner-accounting check (StressHarness.cs lines ~127-132) confirms `DisposeCount == 1` per donated owner across all seeds. This invariant is unchanged by shell pooling — shells are pooled but the donor's `IMemoryOwner.Dispose` still fires exactly once in `RecycleDrainedSegments` before the shell goes to the freelist.

- [ ] **Step 4: Inspect git log**

```bash
git log --oneline master..HEAD
```

Expected: a sequence of focused commits — spec amendment, plan, baseline benchmark, red-bar tests, implementation, after-benchmark + comparison.

---

## Spec coverage check (post-plan self-review)

| Spec section | Task |
|---|---|
| §5 Recycle path (updated pseudocode) | Task 2 (Step 3) |
| §5.1 Donated-shell freelist (NEW) | Task 2 (Steps 1, 2, 4, 5) |
| §10.3 New tests (5 entries) | Task 1 (Steps 1, 2) |
| §11 Strikethrough on shell-pooling out-of-scope | (already done in spec amendment commit) |
| §8 Edge-case catalog rows updated | (already done in spec amendment commit) |

No gaps.
