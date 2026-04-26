# Review: SPSC Pipe — Design (TripleBuffer-based)

**Reviewer:** Claude (Opus 4.7, 1M context, max effort)
**Date:** 2026-04-25
**Reviewed file:** `docs/superpowers/specs/2026-04-25-spsc-pipe-tripleBuffer-design.md`
**Underlying primitive examined:** `src/SpscPipelines/TripleBuffer.cs`
**Scope:** Correctness and completeness as a gate before implementation planning. Nits and non-functional improvements omitted unless they materially impair clarity.

---

## Verdict

The architectural core is sound. The two-TripleBuffer + two-awaiter architecture is well-thought-through, the head==tail head-of-segment freeze discipline (I3/I4/I5) is the right load-bearing invariant for safety, and the Pattern 2 (stash-and-construct) handoff cleanly removes the thread-affinity constraint on continuations.

I traced the awaiter state machine, the bootstrap sequence, the recycle predicate, the freeze/torn-`Memory<byte>` argument, and all 11 internal paths in the cancellation matrix end-to-end against the pseudocode. They all resolve correctly.

I found:

- **§1.1**: One real (minor) bug — `Dispose()` doesn't dispose the two awaiters' `_ctr` fields, which can leak the `SpscPipe` instance via callback closure rooting in long-lived `CancellationTokenSource` scenarios.
- **§1.2**: One pseudocode/text inconsistency — the spec text promises `ObjectDisposedException` from public methods after `Dispose`, but the pseudocode entry guards in §4 don't show this check.
- **§2**: Three small completeness/clarity gaps — a missing edge case for the lost-wakeup-integrate-without-unpark interaction, a missing exception path on `RentSegment` failure, and a minor wording inconsistency on "round-2" vs. "round-3".

After §1.1 and §1.2 are addressed, the spec is ready to feed an implementation plan.

---

## §1. Bugs / errors

### 1.1 BUG (minor leak): `Dispose()` does not dispose the awaiter `_ctr` fields

`SpscPipe.Dispose()` (Section 3, lines 322–330) walks the chain and freelist but does not dispose `_readAwaiter._ctr` or `_flushAwaiter._ctr`:

```csharp
public void Dispose()
{
    if (_disposed) return;
    _disposed = true;
    DisposeChain(_chainHead);
    DisposeFreelist(_freelistHead);
}
```

In the **token-callback-wins case** (path 11 in the cancellation matrix), the callback runs and CASes `Pending → Inactive`, but does *not* dispose `_ctr` (because it's executing inside `_ctr` itself — line 855: "Don't dispose _ctr here (running inside it); next ParkReadAwaiter cleans up"). The cleanup is deferred to **R5b**: the next `Park*Awaiter` call disposes the leftover `_ctr` at its top.

But if the user never calls `ReadAsync` (or `FlushAsync`) again after the OCE, R5b never fires. The CTR remains registered.

A live `CancellationTokenRegistration` keeps the registration node alive on the CTS's internal list, which keeps the callback delegate alive, which keeps `SpscPipe` alive (the closure captures `this` via `state = this` in `UnsafeRegister(static p => ((SpscPipe)p!).OnReadAwaiterTokenCancel(), this)`). For a long-lived CTS (e.g., an application-wide cancellation token), every `SpscPipe` ever cancelled-via-token without a follow-up read is leaked indefinitely.

The user satisfies the I15 Dispose precondition (no in-flight ops) — the OCE-throwing await *has* resolved — and yet `Dispose()` does not clean up the lingering registration.

**Fix.** `Dispose()` should call `_readAwaiter._ctr.Dispose(); _flushAwaiter._ctr.Dispose();` after the chain/freelist walk. `CancellationTokenRegistration.Dispose()` is idempotent and safe on default value, so this is unconditionally correct (covers both the "ctr was leftover" and "ctr was never registered" cases).

This is genuinely small in absolute terms — many users won't hit it, and even those who do may not care if their pipe lifetimes are short or their CTSes are not application-global. But it's a clean fix and the asymmetry with the very-careful R5/R5b handling elsewhere is conspicuous.

### 1.2 INCONSISTENCY: §3 promises `ObjectDisposedException` from post-Dispose calls; §4 pseudocode doesn't show the check

§3 line 332 states:

> "After `Dispose`, the pipe is unusable (further public method calls throw `ObjectDisposedException`)."

But §4's pseudocode for `GetMemory`, `Advance`, `FlushAsync`, `ReadAsync`, `TryRead`, `AdvanceTo`, etc. only shows the `_writerCompleted` / `_readerCompleted` entry guard (S8). There is no `if (_disposed) throw new ObjectDisposedException(...)` at the entry of any public method's pseudocode.

This is one of two things:

- **Either** the pseudocode is incomplete and every public method needs an explicit `_disposed` check before its `_*Completed` check.
- **Or** the §3 claim is incorrect — Dispose has the I15 precondition that no operation is in flight, and the spec is implicitly relying on the user not calling methods post-Dispose (with undefined behavior if they do, similar to use-after-free on `ReadResult.Buffer`).

Either is defensible, but the spec needs to pick one. If the first, the entry guards in §4 should be updated. If the second, §3's "throw `ObjectDisposedException`" claim should be softened to "is undefined behavior; the pipe is unusable" or equivalent.

I'd recommend the first option (explicit `_disposed` check in entry guards) — it's defensive, cheap, and matches the BCL pattern for disposable resources. The user's most likely "I forgot to await" footgun would otherwise present as a NullReferenceException on a disposed segment's `Memory`, not as a clear `ObjectDisposedException`.

---

## §2. Self-consistency / completeness

### 2.1 `ParkReadAwaiter` lost-wakeup integrate can bootstrap `_readHead` without un-parking

In `ParkReadAwaiter` step 2 (lines 760–793), the lost-wakeup re-check calls `IntegrateAcquiredWriterState()` (line 764). If `_readHead` was null pre-park (first read), this *bootstraps* `_readHead = w.HeadSegment` even when the subsequent un-park condition (writer-completion-exception OR `HasReadableProgress` OR `IsCompleted`) does not fire.

The result: the reader's local `_readHead` becomes non-null while the reader stays parked. The published `_lastPublishedReaderState.HeadSegment` is still null (no `PublishReaderState` happened).

Why this is safe: the writer's recycle predicate (I6) gates on `_lastAcquiredReaderState.HeadSegment`, which is null with `IsCompleted=false`, so the pre-bootstrap guard fires and recycling is blocked. So the segment now referenced by the reader's `_readHead` is guaranteed to remain alive.

Why I'm flagging it: the spec covers a *similar* scenario in the "Bootstrap-via-signaler delays full bootstrap until first AdvanceTo" note (line 865), which addresses the case where `_readHead` is *still* null at park return but the user receives a buffer via Pattern 2 (and uses `AdvanceTo`'s `consumed.GetObject()` to extract the head segment). The lost-wakeup-integrate-without-unpark case is not the same — `_readHead` becomes non-null *during* the park, not at the user's `AdvanceTo`. The same future-maintainer warning applies ("don't 'fix' the recycle guard to allow null `HeadSegment` without `IsCompleted`"), but the trigger is different.

A one-paragraph note alongside the "Bootstrap-via-signaler" note would close the gap. Not a correctness bug — just an under-documented invariant.

### 2.2 Stash isn't refreshed after lost-wakeup integrate; canceler delivers stash-at-park-time, not stash-at-integrate-time

Stash is captured before the CAS to `Pending` (lines 745–748). Lost-wakeup integrate (line 764) updates `_readTail`/`_readTailIdx` from a freshly-acquired `WriterState`. The stash is *not* updated.

If a `CancelPendingRead`-from-third-thread arrives after the integrate but before signaler-driven delivery, the canceler-wins-CAS path constructs the buffer from the **stale stash**, not the freshly-integrated `_readTail`. The user receives a buffer with the older tail boundary.

This is the M5 trade-off (canceler can't safely touch reader-private cursors), and it is acknowledged in spirit at line 1049–1050 ("isCompleted: false is conservative"). But that comment only talks about `IsCompleted` — it doesn't call out that the **buffer contents** can also be slightly older than a perfect-BCL-match would deliver in this specific interleaving.

This isn't a bug — it's a deliberate consequence of using the stash for cross-thread cancel, made worse only by the ParkReadAwaiter integrate happening between stash capture and cancel arrival. The fix (refresh stash inside the integrate) would introduce a torn-stash window for a concurrent canceler, which is worse. Leaving as-is is correct.

What I'd suggest: add one sentence to the stash discipline note at line 722, to the effect of "Note: the stash is not refreshed if `ParkReadAwaiter`'s lost-wakeup re-check runs `IntegrateAcquiredWriterState` and stays parked. A subsequent canceler-via-stash will deliver the buffer as it was at park time, not at integrate time. This is acceptable per M5; cancel buffers from any thread are 'best-effort BCL parity'."

### 2.3 `RentSegment` failure modes not specified

`RentSegment` is called from `GetMemory` (lines 360, 370) and is referenced as part of the allocation path in §3 (lines 231–240). Its semantics are well-described, but its **failure** semantics are not.

`MemoryPool<byte>.Rent(int)` can throw `OutOfMemoryException` (or, for a `MemoryPool` with a max-size cap, `ArgumentOutOfRangeException` if `sizeHint > MaxBufferSize`). What does `GetMemory` do in those cases?

Likely: `GetMemory` lets the exception propagate to the user. But there's a state question — has anything been mutated mid-`GetMemory`?

Looking at the pseudocode in lines 358–376:

```csharp
if (_writingHead == null)
{
    _writingHead = RentSegment(sizeHint, runningIndex: 0);   // may throw
    _chainHead   = _writingHead;
}
else
{
    int remaining = _writingHead.AvailableMemory.Length - _writingHeadBytesBuffered;
    if (remaining < sizeHint)
    {
        int filled  = _writingHeadBytesBuffered;
        long newRI  = _writingHead.RunningIndex + filled;
        var newTail = RentSegment(...);   // may throw
        _writingHead.Freeze(filled, newTail);
        ...
    }
}
```

In the first branch, if `RentSegment` throws, `_writingHead` is unchanged (still null), `_chainHead` is unchanged. Safe to retry.

In the second branch, if `RentSegment` throws *before* `Freeze`, `_writingHead` is unchanged, `_writingHeadBytesBuffered` unchanged. Safe to retry. ✓

So the pseudocode is implicitly exception-safe (RentSegment failures leave state intact). But this should be stated explicitly — either in §3's RentSegment description, or as a note next to GetMemory. Implementers who don't realize the exception-safety is order-dependent could refactor in a way that breaks it (e.g., setting `_writingHead = newTail` before calling Freeze).

Also: should `GetMemory` documented as nothrow-on-OOM, or document that it can propagate exceptions? BCL `PipeWriter.GetMemory` propagates allocation failures. Spec should say "matches BCL" or specify.

### 2.4 Spec status header says "round-2 revision"; latest commit is "round-3 revision"

Line 4: `**Status:** Design (pre-implementation) — round-2 revision`. The latest commit on `master` is `1678030 Spec: round-3 revision per multi-agent review synthesis`. Trivial wording inconsistency, fix when convenient.

---

## §3. What I checked and found correct

For the next reviewer's time-budgeting, here are the scenarios I traced and found resolve correctly:

### Pseudocode-level

- **`BuildReadResult` constructs from local cursors regardless of `isCanceled`** (lines 654–665): consistent with the prose at line 1027 ("constructs the buffer from the reader's local cursors"). The R3-1 comment explicitly cites BCL parity. Both cancel paths (sync-entry sticky-cancel consume *and* cross-thread `CancelPendingRead`-wins-CAS via stash) now deliver the same kind of buffer (current-pipe-contents), just from different sources.
- **`SignalFlushIfBackpressureRelieved` no longer has the dead `|| _readerCompleted` clause** (line 962); the explanatory comment at lines 960–961 is correct (reader completion goes through the unconditional variant).
- **`_ctr` field publication discipline** (line 721): the spec correctly notes that `_state`'s CAS does *not* release-publish `_ctr`, and that safety rests on idempotent Dispose + R5 re-check. Important for future maintainers; the warning against optimizing-away R5 is well-placed.
- **`RunContinuationsAsynchronously = true` set once at construction**, not per-Reset (line 726). Easy to miss; well-documented.
- **`Interlocked.Or` portability note** (line 730): targets net10.0; available since .NET 7. Confirmed against `src/SpscPipelines/SpscPipelines.csproj` (`<TargetFramework>net10.0</TargetFramework>`).
- **Cross-thread visibility of `_lastPublishedWriterState` under Pattern 2** (line 728): correct — writer-thread to writer-thread is plain memory, then `_core.SetResult`'s release/acquire publishes to the continuation thread.

### Concurrency-correctness

- **Three-actor race on awaiter `_state` CAS** (signaler / canceler / token callback): all interleavings produce exactly one delivery; losers back off silently (Example D at line 1127 demonstrates).
- **Lost-wakeup defense for both data and writer-completion-exception**: ParkReadAwaiter steps 2 and 3 cover all the sub-cases in `R4` order (throw-first → data → cancel).
- **Pre-bootstrap park + Pattern 2 delivery**: stash null → fallback to `w.HeadSegment` (line 832); if also null, `Empty` buffer. ✓
- **Reader.Complete with `HeadSegment = null` terminal publish**: writer's recycle predicate (line 256) correctly distinguishes pre-bootstrap (`null AND !IsCompleted`) from terminal (`null AND IsCompleted`), enabling chain sweep.
- **Writer.Complete(ex) delivery via three paths** (sync entry, lost-wakeup re-check, signaler) without double-throw: each path is gated by an awaiter CAS; only one fires per park cycle (I11).
- **AdvanceTo's TryAcquire (R2-7) for upper-bound validation under Pattern 2 cursor lag**: correct — the writer's publish sets the dirty bit, the signaler reads `_lastPublishedWriterState` (writer-private, doesn't consume the dirty bit), so AdvanceTo's TryAcquire reliably picks up the publish. ✓
- **Freeze step happens-before publish**; torn `Memory<byte>` slice `[0.._readTailIdx]` safety relies on `_object` and `_index` being preserved by `Slice(0, n)`. Hedged by N5 startup-time assertion. ✓
- **Stash holds segment refs across one cycle**: benign per line 722; recycle predicate gates on published `HeadSegment`, not on the stash, so stash never references a recycled segment.
- **TripleBuffer initial state**: verified `src/SpscPipelines/TripleBuffer.cs` ctor sets `_state.Value = 1 << 1` (dirty=0, slot 1 published). I10's bootstrap argument and the explicit "TripleBuffer contract" at lines 65–74 are aligned.
- **Backpressure thresholds `>=` / `<`**: pause `>=` at line 460; resume `<` at line 909 and 962. Matches BCL semantics.
- **`MemoryPool<byte>.Rent(sizeHint)` returns `>=` not `==`**: called out at line 244; freelist size-mismatch handling (N5) correctly disposes too-small segments rather than holding them.
- **Empty-buffer `AdvanceTo(default, default)` path** (line 589): the `consumedSeg == null && examinedSeg == null` short-circuit publishes a no-op `ReaderState` (current `_readHead`/byte counters unchanged). Doesn't bootstrap _readHead because it returns before the TryAcquire. ✓
- **`SpscPipeOptions` validation**: `PauseWriterThreshold = 0` ⇒ unbounded; `ResumeWriterThreshold ≤ PauseWriterThreshold` constraint enforced (line 1191). `PauseWriterThreshold > 0 && unconsumed >= ...` guard (line 459) correctly handles the `=0` case.
- **`Reader.Complete` from a never-bootstrapped reader**: publishes `ReaderState{HeadSegment=null, IsCompleted=true}`; writer's recycle correctly distinguishes from pre-bootstrap. ✓
- **`Writer.Complete(null)` with unflushed buffered data**: publishes WriterState with `TailWritten = _writingHeadBytesBuffered` and `IsCompleted = true`. Reader sees data + `IsCompleted=true`. Drain works. ✓
- **`Writer.Complete(ex)` semantics under Option A**: every subsequent `ReadAsync`/`TryRead` throws via the entry guard (lines 491–492 in `ReadAsync`, lines 545–546 in `TryRead`); no `_exceptionAlreadySurfaced` flag needed. ✓

### Surface area

`PipeReader` / `PipeWriter` abstract members all covered:

- Reader: `ReadAsync`, `TryRead`, `AdvanceTo` (both overloads), `CancelPendingRead`, `Complete`. ✓
- Writer: `GetMemory`, `GetSpan`, `Advance`, `FlushAsync`, `CancelPendingFlush`, `Complete`. ✓
- Virtual methods (`CompleteAsync`, `CopyToAsync`, `WriteAsync`, `ReadAtLeastAsync`, `AsStream`) inherit BCL defaults — explicitly noted at line 63.

Optional virtual properties (`PipeWriter.CanGetUnflushedBytes` / `PipeWriter.UnflushedBytes`) are not mentioned. Their BCL defaults are `false` / throws. Spec is silent — fine to leave them as defaults; could be worth a one-line "BCL defaults retained for these; we could trivially override `UnflushedBytes` to expose `_writingHeadBytesBuffered` if a future user requests it." But not blocking.

---

## §4. Action items before plan-writing

**Blocking-ish (real but small):**

- [ ] **§1.1** Have `Dispose()` call `_readAwaiter._ctr.Dispose()` and `_flushAwaiter._ctr.Dispose()` to plug the long-lived-CTS leak path.

**Should-fix (clarity):**

- [ ] **§1.2** Decide whether public methods check `_disposed` and throw `ObjectDisposedException`, or whether post-Dispose calls are undefined behavior (matching the I15 "no in-flight ops" precondition style). Update §3 wording or §4 entry guards to match.

**Nice-to-have (clarifications, none blocking):**

- [ ] **§2.1** Add a one-paragraph note acknowledging that `ParkReadAwaiter`'s lost-wakeup integrate can bootstrap `_readHead` without un-parking, and that the recycle pre-bootstrap guard (gated on the *published* `HeadSegment`) keeps this safe.
- [ ] **§2.2** Add one sentence to the stash discipline note acknowledging that the stash is not refreshed by lost-wakeup integrate; canceler-via-stash buffers reflect park-time, not integrate-time, contents (acceptable per M5).
- [ ] **§2.3** Document `GetMemory`'s exception-safety on `RentSegment` failure (state untouched if RentSegment throws; safe to retry; matches BCL behavior of propagating allocation failures).
- [ ] **§2.4** Update the "round-2 revision" status header to "round-3 revision" to match the commit history.

Once §1.1 is fixed (small change) and §1.2 is decided (text or pseudocode update), the spec is ready for an implementation plan.

---

## Appendix — methodology

I traced these scenarios end-to-end against the pseudocode (rather than the prose) to catch divergences:

1. Awaiter state machine — all 11 paths in the cancellation matrix, plus the `_ctr` lifecycle interleavings (callback-wins, signaler-wins, canceler-wins, plus the no-Pending-canceler case).
2. Pattern 2 stash + signaler delivery — pre-bootstrap, post-bootstrap, with concurrent Writer.Complete, with concurrent CancelPendingRead.
3. Reader.Complete and Writer.Complete in both orders, both with and without exceptions.
4. Lost-wakeup defense for both data delivery and writer-completion-exception.
5. AdvanceTo upper-bound validation under Pattern 2 cursor lag.
6. Freeze step happens-before publish, including the torn-`Memory<byte>` slice safety argument.
7. Recycle predicate at chain boundaries (single-segment chain, 1+1 chain, multi-segment, with `Reader.Complete` terminal publish).
8. `_ctr` field publication race — owner step 4/5 vs. signaler/canceler/token-callback in all interleavings.
9. SPSC violation Debug.Asserts at park entry (both reader and writer).
10. `RentSegment` failure mid-`GetMemory` — state remains consistent for retry.
11. `Dispose()` lifecycle and the CTR-rooting GC chain through long-lived CTS callbacks (the §1.1 finding).
12. TripleBuffer initial-state contract ⇔ I10 bootstrap argument; verified `src/SpscPipelines/TripleBuffer.cs` matches.

The pseudocode is pleasingly executable-looking; very little had to be re-derived from the prose. The 11-paths-to-4-outcomes matrix and the trace examples were the most useful cross-reference for the awaiter races.
