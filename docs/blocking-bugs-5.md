**Blocking bug: `BufferStart`-contaminated indices overflow `ReadOnlySequence` bounds and corrupt `AdvanceTo` arithmetic when multiple segments share a backing buffer**

---

**Root cause.** The spec conflates two coordinate systems: offsets relative to the backing buffer (`Holder.Owner.Memory`) and offsets relative to a segment's `Memory` property. `ReadOnlySequence<byte>` operates entirely in the latter coordinate system — its constructor arguments, its `SequencePosition.GetInteger()` return values, and its internal traversal all use offsets into each segment's `Memory`. The spec's segment initialization correctly produces a zero-based `Memory` slice:

```
seg.Memory = _activeBufferHolder.Owner.Memory.Slice(BufferStart, WrittenLength)
```

This means `seg.Memory` has length `WrittenLength` and valid offsets 0 through `WrittenLength`, regardless of where the segment sits within the backing buffer. `BufferStart` is the offset into the *backing buffer*, not into `Memory`.

**Where it breaks.** Three locations use `BufferStart` in contexts that require `Memory`-relative offsets:

1. **`ReadAsync` step 5 (§7.1)** computes `endIdx = tail.BufferStart + tail.WrittenLength` and passes it to the `ReadOnlySequence<byte>` constructor as the end offset into `tail.Memory`. Since `tail.Memory.Length == WrittenLength`, this value exceeds `Memory.Length` by `BufferStart`. When `BufferStart == 0` (the first segment from a buffer), the arithmetic happens to be correct. When `BufferStart > 0` (any subsequent segment carved from the same buffer), the constructor receives an out-of-range index.

2. **`AdvanceTo` step 2 (§7.3)** extracts `consumedIdx = consumed.GetInteger()`, which is `Memory`-relative (returned by `ReadOnlySequence`), then computes `offsetInCurrent = consumedIdx - current.BufferStart`. Subtracting a buffer-absolute offset from a `Memory`-relative offset produces a negative or otherwise incorrect value whenever `BufferStart > 0`.

3. **`AdvanceTo` step 4 (§7.3)** computes `_examinedPosition = examinedSeg.RunningIndex + (examinedIdx - examinedSeg.BufferStart)`. Same issue — `examinedIdx` from `GetInteger()` is `Memory`-relative, so subtracting `BufferStart` corrupts the absolute position calculation.

**`_headConsumedOffset` is also ambiguous.** The spec describes it as "bytes consumed within `_head` beyond `BufferStart`", implying buffer-absolute semantics. But it is used as `startIndex` in the `ReadOnlySequence` constructor (§7.1 step 7), which expects a `Memory`-relative offset. If `_headConsumedOffset` is truly buffer-absolute, the constructor call is wrong. If it is actually `Memory`-relative, the description is misleading and the `AdvanceTo` accounting that computes it via `consumedIdx - current.BufferStart` is wrong.

**When it triggers.** The bug is latent when every backing buffer produces exactly one segment (`BufferStart` is always 0 and both coordinate systems coincide). It activates when a buffer produces multiple segments — the second segment has `BufferStart > 0`. This is precisely the scenario §6.5 is designed to optimize: a writer doing small flushes (e.g., 100 bytes with a 4 KiB `MinimumSegmentSize`) carves multiple segments from one buffer to avoid pool churn. The bug therefore affects the workload the shared-buffer design exists to serve.

**Fix.** Adopt `Memory`-relative (zero-based) offsets uniformly in all reader-facing index arithmetic:

- §7.1 step 5: `endIdx = tail.WrittenLength` (not `BufferStart + WrittenLength`).
- §7.3 step 2: use `consumedIdx` directly from `GetInteger()` without subtracting `BufferStart`. The retirement byte-accounting delta within a segment is `consumedIdx - _headConsumedOffset`, where both values are `Memory`-relative.
- §7.3 step 4: `_examinedPosition = examinedSeg.RunningIndex + examinedIdx` (not `examinedIdx - BufferStart`). This works because `RunningIndex` already represents the absolute byte position of the segment's `Memory[0]`.
- Redefine `_headConsumedOffset` as `Memory`-relative: "offset of the first unconsumed byte within `_head.Memory`", ranging from 0 to `_head.WrittenLength`.
- `BufferStart` remains as an internal field used solely during segment initialization for slicing `Holder.Owner.Memory`. It never participates in `SequencePosition` arithmetic or `ReadOnlySequence` construction.