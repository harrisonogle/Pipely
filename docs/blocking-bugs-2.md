## SpscPipe v4 — Finalized review summary

### Blocking bug

**The reader retires segments still referenced by shared state pointers.** The segment pool is shared; `seg.Reset()` followed by `SegmentPool.Return(seg)` makes the object eligible for re-rental. After such retirement, any shared-state pointer still referencing that segment is dangling.

This manifests in three places:

1. **§7.3 `AdvanceTo` (hot path).** If the reader fully consumes a `ReadResult` whose end segment was `state.Tail` at read time, retirement walks up to and including `state.Tail`. Until the writer next publishes, `state.Tail` points at a recycled object. The writer's next `PublishActiveSegment` does `prevTail = state.Tail` followed by `Volatile.Write(ref prevTail.Next, seg)`, writing into whatever consumer now holds that pool slot. Cross-consumer memory corruption.

2. **§7.4 reader `Complete`.** Retires all segments from `_head` to `state.Tail`. A writer that hasn't yet observed `ReaderCompletionState == 2` and performs another publication hits the same dangling-pointer write.

3. **§10.5 `Reset` / §10.6 `Dispose` cleanup walk.** Starts from `state.Head`, which is dangling once the first segment has been retired. The walk terminates early (missing later segments) or follows `Next` pointers into unrelated live data.

All three are the same root cause: the spec has no invariant preventing the reader from returning to the pool a segment still referenced by `state.Tail` or `state.Head`.

### Fix

Introduce the invariant: **the reader does not retire any segment currently referenced by `state.Tail`.** Three concrete spec changes, all self-contained:

1. **§7.3 `AdvanceTo`.** Acquire-load `state.Tail` at the start of retirement. The retirement loop stops before reaching the segment currently at `state.Tail`. If `consumedSeg == currentTail`, set `_head = consumedSeg` and `_headConsumedOffset` but do not call `RetireSegment` on it. The previous tail-segment is retired on the next `AdvanceTo` after the writer has published again and `state.Tail` has advanced.

2. **§7.4 reader `Complete`.** Remove segment retirement entirely. Set `state.ReaderException` (if any), release-store `state.ReaderCompletionState = 2`, signal the writer awaiter. All segment cleanup is delegated to `Dispose` / `Reset`.

3. **§10.5 `Reset` / §10.6 `Dispose` walk root.** Walk from `_head` if non-null; fall back to `state.Head` only if the reader never performed a `ReadAsync`. Both routines already require no in-flight operations, so reader-local state is safe to access. The chain from `_head` through `state.Tail` is always intact under the invariant, so the walk picks up all live segments including any post-completion orphans the writer may have published before observing `ReaderCompletionState`.

Add to §11 (invariants): *the segment referenced by `state.Tail` is never returned to the segment pool while `state.Tail` references it.*

Under this fix, `state.Head` returns to serving only its first-read bootstrap role. No new fields, no anchor-swap bookkeeping, no changes to the publication protocol or awaiter coordination.

### Cost

One segment and its buffer holder are retained across the `AdvanceTo` call that would otherwise have retired them — retirement is delayed by one publication cycle. Under steady streaming this is a constant overhead. Under bursty workloads with idle periods, one segment plus one `BufferHolder` is retained across the idle gap, which is not a leak.

### Status of prior blocking bugs

| # | Bug | Status |
|---|---|---|
| 1 | First-read segment loss | Fixed in v3 (§4.2, §6.4, §7.1 step 4) |
| 2 | `ComputeReadResult` on writer thread | Fixed in v3 (§4.4 `ReadSignal`, §8.2, §8.7) |
| 3 | Reader `Complete` buffer leak | Attempted in v3 but incorrectly; subsumed by the blocking bug above |
| 4 | Reader retires segment at `state.Tail` | New consensus blocking bug; fix described above |

### Spec polish (non-blocking)

Carry-overs from prior review rounds, still present in v3:

- **§7.1 step 7.** The `availableEndPosition == _bytesRead + buffer.Length` term in the `isCompleted` computation is identically true by construction. The condition collapses to `writerDone`. Remove as dead code.

- **§11 `state.Head` row.** Labeled "W (once) / R (once)" but `Reset()` re-initializes it. Clarify to "W (once per pipe lifecycle)" or reconcile with `Reset()` semantics.

- **§11 `seg.Holder` row.** Says visibility is "covered by release-store of `seg.Next` or `state.Tail`". Should read `prevSeg.Next` — the reader reaches `seg` via the predecessor's `Next` pointer, not `seg`'s own.

- **§6.4 Head/Tail ordering.** First-read correctness relies on the writer writing `state.Head` before `state.Tail` and the reader reading `state.Tail` before `state.Head`. Correct as specified but not defended in prose; a future reorder would silently break it. Add a comment.

- **§8.1 four-state machine.** Defines Idle/Arming/Armed/Signaled then says "we simplify to three in practice." Editorial noise; drop or clearly label conceptual.

- **§8.3 Step B compiler-elision argument.** The justification for keeping an explicit `Interlocked.MemoryBarrier()` after the Step A CAS includes a reason about compiler elision across conditional returns. Misleading — the CAS fence is a runtime property of the executed instruction, not something a compiler would elide. Keep the barrier for the other two stated reasons; drop the elision reason.

- **`_cachedBytesRead` / `_cachedBytesWritten` / `_cachedTail`.** Declared in §4.3 / §4.4 but never used in the algorithms. Either wire in or remove.

- **`WriterCompletionState = 1` ("completing").** Defined but never referenced. Every check is `== 2`. Clarify or remove.

- **§10.6 finalizer synchronization.** Reads writer-local `_activeBufferHolder` and reader-local `_head` without explicit synchronization. GC typically induces sufficient fencing in practice, but the spec should either acknowledge this reliance or have the finalizer use `Volatile.Read` for traversal loads.

- **§4.2 cache-line alignment caveat.** `[StructLayout(LayoutKind.Explicit, Size = 384)]` prevents false sharing between writer-published and reader-published fields *within* the struct, but does not guarantee 64-byte alignment of the struct relative to physical cache lines. The CLR aligns objects to pointer boundaries, which on 64-bit is 8-byte. The "trailing pad protects against adjacent allocations" claim is weaker than stated.

- **§3 64-bit runtime assumption.** `Volatile.Write(ref long)` is only atomic on 64-bit runtimes. The spec says ".NET 8+" but doesn't pin process bitness. For the AF_XDP target this is fine; make the assumption explicit.

### Recommendation

Adopt the "don't retire tail" fix with the three spec changes in §7.3, §7.4, and §10.5 / §10.6, and add the corresponding invariant to §11. Merge the spec-polish list into the v4 cleanup. No further correctness issues have surfaced across three review rounds; the review appears to have converged.