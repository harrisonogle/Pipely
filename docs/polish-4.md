---

(Reviewer 1)

One polish item introduced by the v6 fix:

**§6.4 comment on reader-side load order.** In the first-segment publication case, the inline comment justifying the Head-before-Tail writer-side ordering says:

> Head MUST be written before Tail. The reader reads Tail first (§7.1 step 3), then reads Head (§7.1 step 4).

With v6's load-order fix, the reader now reads `WriterCompletionState` first, then `Tail`, then `Head`. The "reads Tail first" phrasing is out of date. The Head-before-Tail writer-side requirement is still correct, and the underlying argument (reader's acquire of `Tail` carries the preceding release of `Head`) still holds.

Suggested rewording: "Head MUST be written before Tail. The reader reads `Tail` before `Head` (§7.1 steps 3–4); the acquire-load of `Tail` seeing this segment establishes a happens-before edge that makes the preceding release-store of `Head` visible. Reversing the writer-side order would allow the reader to observe `Tail = seg` while `Head` is still null."

Editorial only — no correctness impact.

All other polish items from prior rounds are already resolved in v5/v6.

---

(Reviewer 2)

## Spec polish items for SpscPipe v5

**§4.3 — unused `_cachedBytesRead` field.**

The `SpscPipeWriter` struct declares:

```csharp
private long _cachedBytesRead;        // cached snapshot of BytesReadPublished
```

This field is never referenced in any algorithm in §6 or §8. The writer's `FlushAsync` (§6.3 step 5) and §8.4 both read `state.BytesReadPublished` directly via `Volatile.Read` without consulting or updating this cache.

Either:

- **Remove it.** If no acquire-load elision is intended, drop the field from §4.3.
- **Wire it in.** If the intent was to let the writer skip the acquire-load of `BytesReadPublished` when the cached value already proves no backpressure (i.e., `_bytesWritten - _cachedBytesRead < PauseWriterThreshold`), specify when the cache is populated (presumably on every acquire-load of `BytesReadPublished`) and when the fresh load is required (presumably only when the cached check indicates backpressure). This is a real optimization — it removes the acquire from the `FlushAsync` fast path entirely when the writer is nowhere near the pause threshold — but it needs to be spelled out explicitly rather than left as a dangling field.

Recommend removing unless the optimization is deliberately planned, in which case §6.3 step 5 should be updated to use the two-tier check.

---

That is the only remaining polish item. All prior review items — tautological `availableEndPosition` check, `state.Head` lifecycle labeling, `seg.Holder` visibility row, Head/Tail ordering comment, four-state awaiter noise, compiler-elision rationale, `WriterCompletionState = 1`, finalizer synchronization caveat, cache-line alignment caveat, 64-bit runtime pinning — have been addressed in the v4/v5 iterations.

---

(Reviewer 3)

One remaining polish item: `TailIndicatesNewDataPast` is referenced in §8.3 Step C but never defined. The meaning is clear from context — it checks whether `tail` has data past `_examinedPosition` — but an implementer shouldn't have to infer the logic of a function called in the critical double-check path. Either inline the check or add a one-line definition.

---

(Reviewer 4)

1. **Define `TailIndicatesNewDataPast`** — called in §8.3 Step C but never specified. Presumably `tail != null && tail.RunningIndex + tail.WrittenLength > _examinedPosition`.

2. **Clarify whether `ReadSignal.IsCompleted` is used or redundant** — §8.7 says the reader re-executes steps 3–7 to build the real `ReadResult`, which would re-acquire `WriterCompletionState` itself. If so, `ReadSignal` can be simplified to just `{ IsCanceled }`. If the reader uses the signal's `IsCompleted` as a fast-path hint, document that.