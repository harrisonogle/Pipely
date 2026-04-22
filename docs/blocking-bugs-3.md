---

(Reviewer 1)

# Blocking bug in SpscPipe v5

**One blocking bug: §7.1 step 3 and §8.3 step C load order causes silent data loss.**

## The bug

Both sites load `state.Tail` before `state.WriterCompletionState`:

```
tail = Volatile.Read(ref state.Tail)
writerDone = Volatile.Read(ref state.WriterCompletionState) == 2
```

The two acquire-loads are independent. The reader's acquire of `WriterCompletionState == 2` establishes a happens-before edge covering loads program-order-*after* it; the earlier `Tail` load is not covered. The reader can observe a stale `Tail` paired with fresh completion.

## The failure scenario

1. Writer publishes segment A. Reader reads A, calls `AdvanceTo` with `examined` at end of A. `_examinedPosition = end of A`.
2. Writer publishes segment B, then calls `Complete`. After this: `state.Tail = B`, `state.WriterCompletionState = 2`.
3. Reader calls `ReadAsync`:
   - Step 3: observes `tail = A` (stale) and `writerDone = true`.
   - Step 6: `availableEndPosition = end of A = _examinedPosition`, so `hasNewData = false`.
   - Step 7: skipped.
   - Step 8: `writerDone == true`, returns empty buffer with `IsCompleted = true`.
4. Consumer's standard `while (!result.IsCompleted)` loop exits. Segment B's bytes never delivered.

§8.3 step C has the identical load order and the same failure mode: a reader that arms, fences, and re-checks can make the same stale-tail-with-fresh-completion observation, unarm synchronously, and return the empty-completed result with B lost.

This is data loss, not deferred delivery. The BCL's `Pipe` avoids this by observing tail and completion under its `SyncObject` lock; `SpscPipe` must achieve the same atomicity via load ordering.

## The fix

Swap the load order at both sites:

```
writerDone = Volatile.Read(ref state.WriterCompletionState) == 2   // acquire first
tail = Volatile.Read(ref state.Tail)                                // then acquire tail
```

When `writerDone == true`, the reader's acquire of completion synchronizes with the writer's release of `WriterCompletionState = 2`, which was program-order-after the writer's final publication release of `Tail`. The subsequent `Tail` load is program-order-after the completion acquire and is guaranteed to see the final tail. `Complete` is terminal, so no publications follow `WriterCompletionState = 2` and the reader cannot observe a newer tail slipping in.

Zero performance cost.

## Everything else

All four previously-identified blocking bugs (first-read segment loss, `ComputeReadResult` on writer thread, reader-complete buffer leak, tail-segment retirement) are correctly fixed in v5. The polish items from prior rounds are all applied.

Apply the load-order fix at §7.1 step 3 and §8.3 step C and v5 is ready.

---

(Reviewer 2)

## Blocking bugs in SpscPipe v4

**One remaining blocking bug.**

### §7.1 step 3 / §8.3 Step C load order

The reader acquire-loads `state.Tail` before `state.WriterCompletionState`:

```
tail = Volatile.Read(ref state.Tail)
writerDone = Volatile.Read(ref state.WriterCompletionState) == 2
```

The two acquire-loads are independent. The reader can observe a stale `Tail` paired with fresh `WriterCompletionState == 2`. Scenario:

1. Writer publishes segment A. Reader consumes and examines all of A via `AdvanceTo`. `_examinedPosition` is at A's end.
2. Writer publishes segment B (release-store `state.Tail = B`), then calls `Complete` (release-store `state.WriterCompletionState = 2`).
3. Reader enters `ReadAsync`. Step 3 observes `tail = A` (stale) and `writerDone = true` (fresh).
4. Step 6: `availableEndPosition = A.End == _examinedPosition`, so `hasNewData = false`. Step 7 is skipped.
5. Step 8 returns empty buffer with `IsCompleted = true`. Segment B's bytes are never delivered.

Under the standard `PipeReader` consumer pattern documented by Microsoft (`if (result.IsCompleted) break;`), the loop terminates and B's bytes are silently dropped. This is distinct from the BCL-permitted case of `IsCompleted = true` alongside a *partial* buffer — here the buffer is empty while data exists.

The BCL `Pipe` avoids this by observing `Tail` and `WriterCompletionState` under a shared `SyncObject` lock, making the observation atomic. `SpscPipe` has no lock, so the atomicity must come from load ordering.

**Fix:** swap the two acquire-loads so `WriterCompletionState` is loaded before `Tail`:

```
writerDone = Volatile.Read(ref state.WriterCompletionState) == 2  // acquire
tail = Volatile.Read(ref state.Tail)                               // acquire
```

The writer's `Complete` (§6.6) release-stores `WriterCompletionState = 2` *after* its final `Tail` release-store. When the reader observes `writerDone = true` via acquire, the release-acquire edge establishes happens-before on the earlier `Tail` store, so the subsequent `Tail` acquire-load sees the final value. `Complete` is terminal — no publications occur after it — so there is no "writer publishes between the two loads" counterexample.

Apply the same swap in §8.3 Step C (the re-check after arming the read awaiter). Zero runtime cost; two line swaps per site.

The tautological `availableEndPosition == _bytesRead + buffer.Length` check in step 7 was already removed in v4 as part of the intended editorial cleanup around this fix, but the load-order swap itself — the actual correctness change — was not made.

### Other items

Everything else from prior review rounds is resolved. Remaining items are spec polish only:

- `_cachedBytesRead` in §4.3 is declared but unused — either wire in or remove.

---

(Reviewer 3)

One blocking bug remains.

**`ReadAsync` step 3 load order causes data loss.** The reader loads `Tail` before `WriterCompletionState`. If the writer publishes a final segment and then completes, the reader can observe a stale `Tail` (missing the final segment) while seeing `WriterCompletionState == 2`. When the reader has already examined everything up to the stale tail, `hasNewData` is false, step 7 is skipped, and step 8 returns an empty buffer with `IsCompleted = true`. The standard `PipeReader` consumption loop breaks on `IsCompleted`, so the final segment's bytes are silently dropped. The BCL `Pipe` doesn't exhibit this because it reads both fields under a lock. Fix: swap the two loads in step 3 and in §8.3 Step C so `WriterCompletionState` is acquired first — the happens-before edge from the completion acquire then guarantees the subsequent `Tail` load sees the writer's final publication. Zero cost.

---

(Reviewer 4)

Sure. Here's a clean writeup for the other reviewers.

---

## The `ReadAsync` step 3 load order is a blocking bug

### The scenario

1. Writer publishes segment A. Reader reads it, examines all of it, calls `AdvanceTo`. Now `_examinedPosition = A.RunningIndex + A.WrittenLength`.
2. Writer publishes segment B (setting `state.Tail = B` via release-store), then calls `Complete` (setting `state.WriterCompletionState = 2` via release-store).
3. Reader enters `ReadAsync`. Step 3 executes two independent acquire-loads:
   - `tail = Volatile.Read(ref state.Tail)` — observes stale value A
   - `writerDone = Volatile.Read(ref state.WriterCompletionState) == 2` — observes `true`
4. Step 6: `availableEndPosition = A.RunningIndex + A.WrittenLength`, which equals `_examinedPosition`. So `hasNewData = false`. Step 7 is skipped.
5. Step 8: `writerDone` is true, so the reader returns an empty buffer with `IsCompleted = true`.
6. Segment B's bytes are never delivered.

### Why the stale `Tail` is possible

The two acquire-loads in step 3 are independent. The reader's acquire of `WriterCompletionState == 2` establishes a happens-before edge that covers loads *program-order-after* it — but the `Tail` load is program-order-*before* it. The `Tail` acquire pairs with the writer's release-store of `Tail`, but nothing forces the reader to observe the *latest* release-store of `Tail` just because a later acquire on a different variable succeeded. The reader can see the old `Tail` and the new `WriterCompletionState`.

### Why this is data loss, not just a deferred delivery

The counterargument is that the reader loops and picks up the remaining bytes on the next `ReadAsync`. But there is no next `ReadAsync`. The standard `PipeReader` consumption pattern from Microsoft's own documentation is:

```csharp
while (true)
{
    ReadResult result = await reader.ReadAsync();
    // ... process result.Buffer ...
    reader.AdvanceTo(consumed, examined);
    if (result.IsCompleted)
        break;
}
```

This terminates on `IsCompleted` regardless of buffer contents. An empty buffer with `IsCompleted = true` exits the loop. The bytes in segment B are silently dropped.

Some consumers use `result.IsCompleted && result.Buffer.IsEmpty` as the termination condition, which would loop back and recover. But the spec cannot rely on a specific caller pattern that goes beyond the documented `PipeReader` contract.

### Why "the BCL does the same thing" is incorrect

The BCL `Pipe` observes `Tail` and `WriterCompletionState` under a shared `SyncObject` lock. The reader never sees a stale `Tail` combined with a fresh completion flag — the lock makes the observation atomic. `SpscPipe` has no lock, so the atomicity must come from load ordering.

### Why swapping the loads fixes it

The writer's `Complete` (§6.6) executes stores in this order:

1. `Volatile.Write(ref state.Tail, seg)` — in `PublishActiveSegment`, called from `Complete` step 1
2. `Volatile.Write(ref state.WriterCompletionState, 2)` — in `Complete` step 4

If the reader swaps to:

```
writerDone = Volatile.Read(ref state.WriterCompletionState) == 2   // acquire
tail = Volatile.Read(ref state.Tail)                                // acquire
```

...then when `writerDone` is observed as `true`, the happens-before edge from acquiring `WriterCompletionState == 2` guarantees that the subsequent `Tail` load sees everything the writer stored before releasing `WriterCompletionState`. That includes the final `Tail` update. The window is closed.

The counterargument that "the writer could publish additional segments between the two loads" does not apply: `Complete` is the terminal writer operation. No further segments are published after `WriterCompletionState = 2`.

### What to change

Swap the two loads in `ReadAsync` step 3 and in §8.3 Step C (the re-check after arming). Zero performance cost.