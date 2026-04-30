# Pipely Rebrand Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebrand the library from `SpscPipelines` to `Pipely` — namespace, assembly, project, type, file, and folder names — while preserving public behavior. Lift the Reader to top-level public to mirror Writer (so future Pipely-specific reader extensions don't require a breaking change). Adopt the documented consumer convention in tests and the FastScheduler companion project.

**Architecture:** Rename refactor plus one structural change (Reader visibility). No behavior change. The brand `Pipely` is intended to act as a *qualifier*, not an imported namespace, so the BCL types `System.IO.Pipelines.PipeWriter`/`PipeReader` and the Pipely subclasses `Pipely.PipeWriter`/`PipeReader` coexist in consumer code under a single `using System.IO.Pipelines;` import. After this change, both `Pipely.PipeWriter` and `Pipely.PipeReader` are top-level public types in the `Pipely` namespace, and `Pipe.Reader` returns the concrete `Pipely.PipeReader` (parallel to `Pipe.Writer` returning the concrete `Pipely.PipeWriter`).

**Tech Stack:** C# / .NET 10, xUnit, BenchmarkDotNet, the `slnx` solution format.

---

## Identifier rename table (canonical reference)

Apply in this order. Order matters: more-specific tokens first so that later substitutions don't over-match.

| # | From | To | Notes |
|---|---|---|---|
| 1 | `SpscPipelines.FastScheduler` | `Pipely` | Catches `.FastScheduler.Tests`, `.FastScheduler.Benchmarks` namespaces and folder/file paths |
| 2 | `SpscPipelines.Tests` | `Pipely.Tests` | |
| 3 | `SpscPipelines.Stress` | `Pipely.Stress` | |
| 4 | `SpscPipelines.Benchmarks` | `Pipely.Benchmarks` | |
| 5 | `SpscPipelines` | `Pipely` | Catches plain namespace, `using`, assembly name, paths |
| 6 | `SpscPipeWriter` | `PipeWriter` | Type rename — must run before `SpscPipe` |
| 7 | `SpscPipeReader` | `PipeReader` | Type rename — must run before `SpscPipe` |
| 8 | `SpscPipeOptions` | `PipeOptions` | Type rename — must run before `SpscPipe` |
| 9 | `SpscPipe` | `Pipe` | Type rename. Also catches `SpscPipeXxxTests` class names → `PipeXxxTests` and `SpscPipeAdapter` → `PipeAdapter` |
| 10 | `SpscAwaiter` | `PipelyAwaiter` | Type rename. Also catches `SpscAwaiterTests` → `PipelyAwaiterTests` |

After step 5, the only remaining `Spsc*` tokens are type identifiers; steps 6–10 cover them.

## File rename table (canonical reference)

### Source files

| From | To |
|---|---|
| `src/SpscPipelines/SpscPipe.cs` | `src/Pipely/Pipe.cs` |
| `src/SpscPipelines/SpscPipe.Reader.cs` | `src/Pipely/Pipe.Reader.cs` |
| `src/SpscPipelines/SpscPipe.Writer.cs` | `src/Pipely/Pipe.Writer.cs` |
| `src/SpscPipelines/SpscPipeOptions.cs` | `src/Pipely/PipeOptions.cs` |
| `src/SpscPipelines/SpscAwaiter.cs` | `src/Pipely/PipelyAwaiter.cs` |
| `src/SpscPipelines/IContinuationDispatcher.cs` | `src/Pipely/IContinuationDispatcher.cs` (move only) |
| `src/SpscPipelines/BufferSegment.cs` | `src/Pipely/BufferSegment.cs` (move only) |
| `src/SpscPipelines/ReaderState.cs` | `src/Pipely/ReaderState.cs` (move only) |
| `src/SpscPipelines/WriterState.cs` | `src/Pipely/WriterState.cs` (move only) |
| `src/SpscPipelines/TripleBuffer.cs` | `src/Pipely/TripleBuffer.cs` (move only) |
| `src/SpscPipelines/SpscPipelines.csproj` | `src/Pipely/Pipely.csproj` |
| `src/SpscPipelines.FastScheduler/FastScheduler.cs` | `src/Pipely/FastScheduler.cs` (move only) |
| `src/SpscPipelines.FastScheduler/SpscPipelines.FastScheduler.csproj` | `src/Pipely/Pipely.csproj` |

### Test files

| From | To |
|---|---|
| `tests/SpscPipelines.Tests/SpscAwaiterTests.cs` | `tests/Pipely.Tests/PipelyAwaiterTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeAdvanceToTests.cs` | `tests/Pipely.Tests/PipeAdvanceToTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeCancellationTests.cs` | `tests/Pipely.Tests/PipeCancellationTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeContinuationDispatcherTests.cs` | `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeDisposeTests.cs` | `tests/Pipely.Tests/PipeDisposeTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeLifecycleTests.cs` | `tests/Pipely.Tests/PipeLifecycleTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeReadInProgressTests.cs` | `tests/Pipely.Tests/PipeReadInProgressTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeReaderTests.cs` | `tests/Pipely.Tests/PipeReaderTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeWriterSpliceTests.cs` | `tests/Pipely.Tests/PipeWriterSpliceTests.cs` |
| `tests/SpscPipelines.Tests/SpscPipeWriterTests.cs` | `tests/Pipely.Tests/PipeWriterTests.cs` |
| `tests/SpscPipelines.Tests/BclParityTests.cs` | `tests/Pipely.Tests/BclParityTests.cs` (move only) |
| `tests/SpscPipelines.Tests/BufferSegmentTests.cs` | `tests/Pipely.Tests/BufferSegmentTests.cs` (move only) |
| `tests/SpscPipelines.Tests/TrackingMemoryOwner.cs` | `tests/Pipely.Tests/TrackingMemoryOwner.cs` (move only) |
| `tests/SpscPipelines.Tests/SpscPipelines.Tests.csproj` | `tests/Pipely.Tests/Pipely.Tests.csproj` |
| `tests/SpscPipelines.Stress/*.cs` | `tests/Pipely.Stress/*.cs` (move only) |
| `tests/SpscPipelines.Stress/SpscPipelines.Stress.csproj` | `tests/Pipely.Stress/Pipely.Stress.csproj` |
| `tests/SpscPipelines.Benchmarks/SpscPipeAdapter.cs` | `tests/Pipely.Benchmarks/PipeAdapter.cs` |
| `tests/SpscPipelines.Benchmarks/*.cs` (others) | `tests/Pipely.Benchmarks/*.cs` (move only) |
| `tests/SpscPipelines.Benchmarks/SpscPipelines.Benchmarks.csproj` | `tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj` |
| `tests/SpscPipelines.FastScheduler.Tests/*.cs` | `tests/Pipely.Tests/*.cs` (move only) |
| `tests/SpscPipelines.FastScheduler.Tests/SpscPipelines.FastScheduler.Tests.csproj` | `tests/Pipely.Tests/Pipely.Tests.csproj` |
| `tests/SpscPipelines.FastScheduler.Benchmarks/*.cs` | `tests/Pipely.Benchmarks/*.cs` (move only) |
| `tests/SpscPipelines.FastScheduler.Benchmarks/SpscPipelines.FastScheduler.Benchmarks.csproj` | `tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj` |

### Solution

| From | To |
|---|---|
| `SpscPipelines.slnx` | `Pipely.slnx` |

---

## Consumer convention rules (for Task 8)

Apply to every file outside the main `Pipely` library: `src/Pipely/**`, `tests/Pipely.Tests/**`, `tests/Pipely.Stress/**`, `tests/Pipely.Benchmarks/**`, `tests/Pipely.Tests/**`, `tests/Pipely.Benchmarks/**`.

1. **No `using Pipely;` or `using Pipely;`.** Drop these. Keep `using System.IO.Pipelines;` where present (and add it if needed).
2. **Construction sites use `Pipely.X` qualified inline.** Always `new Pipely.Pipe(...)`, `new Pipely.PipeOptions { ... }`, `new Pipely.FastScheduler()`.
3. **Variable / parameter / return types prefer the BCL abstract type when the extended surface isn't used.** `PipeWriter writer = pipe.Writer;` is preferred over `Pipely.PipeWriter writer = pipe.Writer;` *unless* the test exercises a Pipely-only method (e.g. `Splice`).
4. **Always qualify Pipely-only types.** `Pipely.IContinuationDispatcher`, `Pipely.PipelyAwaiter<T>`, `Pipely.PipeOptions` (the BCL type with the same name is a different class — qualifying disambiguates).
5. **Inside the main `Pipely` library** (`src/Pipely/**`): code is in `namespace Pipely;` already — no qualification needed for own types. Base classes that share a name with the derived class must be fully qualified (see Task 6).

---

## Task 1: Baseline verification

**Files:** none (verification only)

- [ ] **Step 1: Confirm clean working tree**

```bash
git status
```

Expected: `nothing to commit, working tree clean` (or only the new plan file untracked).

- [ ] **Step 2: Verify build is green**

```bash
dotnet build
```

Expected: `0 Error(s)`. Warnings are acceptable (the existing `xUnit1030` warning is pre-existing).

- [ ] **Step 3: Run all tests; capture pass count**

```bash
dotnet test --nologo --verbosity quiet 2>&1 | tail -20
```

Expected: All tests pass. Record the total passing test count somewhere (paste into the next agent's context or a scratch note) — Task 9 will compare against it.

---

## Task 2: Substitute identifiers across `.cs`, `.csproj`, and `.slnx`

**Files:** all `.cs` under `src/` and `tests/`; all `.csproj`; `SpscPipelines.slnx`. Markdown is handled in Task 11.

After this task the source tree will contain `namespace Pipely;`, `Pipe`/`PipeWriter`/`PipeReader`/`PipeOptions`/`PipelyAwaiter` identifiers, and `Pipely`-pathed `<ProjectReference>`/`<InternalsVisibleTo>` entries — but the actual files and folders still have their old names. The build will not compile until Tasks 3–8 land. Do **not** attempt `dotnet build` until Task 9.

- [ ] **Step 1: Apply ordered identifier substitutions**

Run the following from the repo root:

```bash
files=$(find src tests -type f \( -name '*.cs' -o -name '*.csproj' \); find . -maxdepth 1 -name '*.slnx')

sed -i \
  -e 's/SpscPipelines\.FastScheduler/Pipely/g' \
  -e 's/SpscPipelines\.Tests/Pipely.Tests/g' \
  -e 's/SpscPipelines\.Stress/Pipely.Stress/g' \
  -e 's/SpscPipelines\.Benchmarks/Pipely.Benchmarks/g' \
  -e 's/SpscPipelines/Pipely/g' \
  -e 's/SpscPipeWriter/PipeWriter/g' \
  -e 's/SpscPipeReader/PipeReader/g' \
  -e 's/SpscPipeOptions/PipeOptions/g' \
  -e 's/SpscPipe/Pipe/g' \
  -e 's/SpscAwaiter/PipelyAwaiter/g' \
  $files
```

- [ ] **Step 2: Verify no `Spsc` tokens remain in code/config**

```bash
grep -rn "Spsc" src tests *.slnx --include='*.cs' --include='*.csproj' --include='*.slnx' 2>/dev/null
```

Expected: empty output. If anything remains, fix manually before continuing.

- [ ] **Step 3: Spot-check one file for sanity**

```bash
grep -n 'namespace ' src/SpscPipelines/SpscPipe.Writer.cs
```

(File still has its old name; only contents have been rewritten.) Expected:
```
src/SpscPipelines/SpscPipe.Writer.cs:6:namespace Pipely;
```

---

## Task 3: Rename source `.cs` files

**Files:** see "File rename table → Source files" and "Test files" tables above. This is purely `git mv` of file names within their existing folders.

- [ ] **Step 1: Rename main library source files**

```bash
git mv src/SpscPipelines/SpscPipe.cs            src/SpscPipelines/Pipe.cs
git mv src/SpscPipelines/SpscPipe.Reader.cs     src/SpscPipelines/Pipe.Reader.cs
git mv src/SpscPipelines/SpscPipe.Writer.cs     src/SpscPipelines/Pipe.Writer.cs
git mv src/SpscPipelines/SpscPipeOptions.cs     src/SpscPipelines/PipeOptions.cs
git mv src/SpscPipelines/SpscAwaiter.cs         src/SpscPipelines/PipelyAwaiter.cs
```

- [ ] **Step 2: Rename test source files**

```bash
git mv tests/SpscPipelines.Tests/SpscAwaiterTests.cs                       tests/SpscPipelines.Tests/PipelyAwaiterTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeAdvanceToTests.cs                 tests/SpscPipelines.Tests/PipeAdvanceToTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeCancellationTests.cs              tests/SpscPipelines.Tests/PipeCancellationTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeContinuationDispatcherTests.cs    tests/SpscPipelines.Tests/PipeContinuationDispatcherTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeDisposeTests.cs                   tests/SpscPipelines.Tests/PipeDisposeTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeLifecycleTests.cs                 tests/SpscPipelines.Tests/PipeLifecycleTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeReadInProgressTests.cs            tests/SpscPipelines.Tests/PipeReadInProgressTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeReaderTests.cs                    tests/SpscPipelines.Tests/PipeReaderTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeWriterSpliceTests.cs              tests/SpscPipelines.Tests/PipeWriterSpliceTests.cs
git mv tests/SpscPipelines.Tests/SpscPipeWriterTests.cs                    tests/SpscPipelines.Tests/PipeWriterTests.cs
git mv tests/SpscPipelines.Benchmarks/SpscPipeAdapter.cs                   tests/SpscPipelines.Benchmarks/PipeAdapter.cs
```

- [ ] **Step 3: Verify no `Spsc` filenames remain**

```bash
find src tests -type f -name '*Spsc*'
```

Expected: empty output. (Folders still have `Spsc*` names — Task 4 fixes that.)

---

## Task 4: Rename folders, csproj files, and the solution file

**Files:** all project folders, csproj files, and the solution.

- [ ] **Step 1: Rename csproj files in place** (before folder rename so `git mv` is sane)

```bash
git mv src/SpscPipelines/SpscPipelines.csproj                          src/SpscPipelines/Pipely.csproj
git mv src/SpscPipelines.FastScheduler/SpscPipelines.FastScheduler.csproj    src/SpscPipelines.FastScheduler/Pipely.csproj
git mv tests/SpscPipelines.Tests/SpscPipelines.Tests.csproj                   tests/SpscPipelines.Tests/Pipely.Tests.csproj
git mv tests/SpscPipelines.Stress/SpscPipelines.Stress.csproj                 tests/SpscPipelines.Stress/Pipely.Stress.csproj
git mv tests/SpscPipelines.Benchmarks/SpscPipelines.Benchmarks.csproj         tests/SpscPipelines.Benchmarks/Pipely.Benchmarks.csproj
git mv tests/SpscPipelines.FastScheduler.Tests/SpscPipelines.FastScheduler.Tests.csproj             tests/SpscPipelines.FastScheduler.Tests/Pipely.Tests.csproj
git mv tests/SpscPipelines.FastScheduler.Benchmarks/SpscPipelines.FastScheduler.Benchmarks.csproj   tests/SpscPipelines.FastScheduler.Benchmarks/Pipely.Benchmarks.csproj
```

- [ ] **Step 2: Rename folders**

```bash
git mv src/SpscPipelines                          src/Pipely
git mv src/SpscPipelines.FastScheduler               src/Pipely
git mv tests/SpscPipelines.Tests                  tests/Pipely.Tests
git mv tests/SpscPipelines.Stress                 tests/Pipely.Stress
git mv tests/SpscPipelines.Benchmarks             tests/Pipely.Benchmarks
git mv tests/SpscPipelines.FastScheduler.Tests       tests/Pipely.Tests
git mv tests/SpscPipelines.FastScheduler.Benchmarks  tests/Pipely.Benchmarks
```

- [ ] **Step 3: Rename the solution file**

```bash
git mv SpscPipelines.slnx Pipely.slnx
```

- [ ] **Step 4: Verify directory layout**

```bash
ls src tests
find . -maxdepth 2 -name '*.slnx'
```

Expected: only `Pipely`, `Pipely`, `Pipely.Tests`, `Pipely.Stress`, `Pipely.Benchmarks`, `Pipely.Tests`, `Pipely.Benchmarks` directories; only `Pipely.slnx` at root. No `Spsc*` paths anywhere.

```bash
find . -path './docs' -prune -o -type f -name '*Spsc*' -print
```

Expected: empty output (the `-prune` skips `docs/`, which Task 11 handles).

---

## Task 5: Verify csproj/slnx internal references are correct

**Files:**
- Read-check: `Pipely.slnx`, all 7 csprojs

After Tasks 2 and 4, the `<ProjectReference>` paths and the `.slnx` `Project Path="..."` entries should already point to `src/Pipely/Pipely.csproj`, `src/Pipely/Pipely.csproj`, etc. — Task 2's substitution updated the path strings; Task 4 made the paths real. Verify.

- [ ] **Step 1: Verify slnx contents**

```bash
cat Pipely.slnx
```

Expected:

```xml
<Solution>
  <Folder Name="/src/">
    <Project Path="src/Pipely/Pipely.csproj" />
    <Project Path="src/Pipely/Pipely.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj" />
    <Project Path="tests/Pipely.Stress/Pipely.Stress.csproj" />
    <Project Path="tests/Pipely.Tests/Pipely.Tests.csproj" />
    <Project Path="tests/Pipely.Tests/Pipely.Tests.csproj" />
    <Project Path="tests/Pipely.Benchmarks/Pipely.Benchmarks.csproj" />
  </Folder>
</Solution>
```

- [ ] **Step 2: Verify all `<ProjectReference>` and `<InternalsVisibleTo>` in csprojs**

```bash
grep -rnE 'ProjectReference|InternalsVisibleTo' src tests --include='*.csproj'
```

Expected: every value should contain `Pipely`, no `Spsc` substrings.

If any reference still says `SpscPipelines`, edit it to `Pipely`.

---

## Task 6: Disambiguate base class declarations

**Files:**
- Modify: `src/Pipely/Pipe.Writer.cs:8`
- Modify: `src/Pipely/Pipe.Reader.cs:10`

Inside `namespace Pipely;`, the declaration `public sealed class PipeWriter : PipeWriter` self-references. Same for `PipeReader`. The base class must be fully qualified.

- [ ] **Step 1: Fix `PipeWriter` base class**

In `src/Pipely/Pipe.Writer.cs`, change:

```csharp
public sealed class PipeWriter : PipeWriter
```

to:

```csharp
public sealed class PipeWriter : System.IO.Pipelines.PipeWriter
```

- [ ] **Step 2: Fix `PipeReader` base class**

In `src/Pipely/Pipe.Reader.cs`, change:

```csharp
internal sealed class PipeReader : PipeReader
```

to:

```csharp
internal sealed class PipeReader : System.IO.Pipelines.PipeReader
```

(Note: visibility stays `internal` here; Task 7 promotes it to `public` and lifts it out of the partial-class wrapper.)

- [ ] **Step 3: Verify both fixes**

```bash
grep -nE 'class PipeWriter|class PipeReader' src/Pipely/Pipe.Writer.cs src/Pipely/Pipe.Reader.cs
```

Expected:
```
src/Pipely/Pipe.Writer.cs:8:public sealed class PipeWriter : System.IO.Pipelines.PipeWriter
src/Pipely/Pipe.Reader.cs:10:    internal sealed class PipeReader : System.IO.Pipelines.PipeReader
```

---

## Task 7: Lift `PipeReader` to top-level and make it public

**Files:**
- Modify: `src/Pipely/Pipe.Reader.cs`

Today `PipeReader` is nested inside `partial class Pipe` and declared `internal sealed`. Goal of this task: lift it out of the partial-class wrapper and make it `public sealed` so consumers can name the type. The constructor stays callable only by `Pipe`, mirroring the existing `Pipely.PipeWriter` pattern (top-level public, internal constructor).

All `_pipe.*` member accesses in the Reader target fields and methods that are already declared `internal` on `Pipe` — verified at plan-writing time. No additional visibility changes are needed on `Pipe`.

- [ ] **Step 1: Lift `PipeReader` out of the `Pipe` partial-class wrapper**

The current structure of `src/Pipely/Pipe.Reader.cs` (post-Tasks 2 and 6):

```csharp
namespace Pipely;

public sealed partial class Pipe
{
    internal sealed class PipeReader : System.IO.Pipelines.PipeReader
    {
        private readonly Pipe _pipe;
        public PipeReader(Pipe pipe) => _pipe = pipe;
        // ... overrides
    }
}
```

Change it to:

```csharp
namespace Pipely;

public sealed class PipeReader : System.IO.Pipelines.PipeReader
{
    private readonly Pipe _pipe;
    internal PipeReader(Pipe pipe) => _pipe = pipe;
    // ... overrides (unchanged)
}
```

Concrete edits:

1. Delete the `public sealed partial class Pipe` opening line and its matching closing brace at the bottom of the file.
2. Change `internal sealed class PipeReader` → `public sealed class PipeReader`.
3. Change the constructor `public PipeReader(Pipe pipe)` → `internal PipeReader(Pipe pipe)`.
4. Reduce one level of indentation throughout the class body so the now-top-level class is properly indented.

- [ ] **Step 2: Verify the structural change**

```bash
grep -nE 'class PipeReader|partial class Pipe|^\s*internal PipeReader\(' src/Pipely/Pipe.Reader.cs
```

Expected:
```
src/Pipely/Pipe.Reader.cs:<N>:public sealed class PipeReader : System.IO.Pipelines.PipeReader
src/Pipely/Pipe.Reader.cs:<M>:    internal PipeReader(Pipe pipe) => _pipe = pipe;
```

The `partial class Pipe` line should be gone.

- [ ] **Step 3: Confirm `Pipe.Reader` property still resolves correctly**

In `src/Pipely/Pipe.cs` the line is:

```csharp
public PipeReader Reader => _readerInstance;
```

Inside `namespace Pipely;`, with `using System.IO.Pipelines;` also present, the bare `PipeReader` token now resolves to the local `Pipely.PipeReader` (own-namespace types are preferred over imported types). This makes the property return type the concrete Pipely class — parallel to `Pipe.Writer` returning `Pipely.PipeWriter`. No edit needed; just verify the line is unchanged:

```bash
grep -n 'PipeReader Reader' src/Pipely/Pipe.cs
```

Expected:
```
src/Pipely/Pipe.cs:61:    public PipeReader Reader => _readerInstance;
```

---

## Task 8: Adopt consumer convention in FastScheduler and tests

**Files:**
- Modify: `src/Pipely/FastScheduler.cs` (1 file)
- Modify: every `.cs` file under `tests/Pipely.*/` (~22 files across 5 projects)

The substitution in Task 2 has already turned `using SpscPipelines;` into `using Pipely;`. The convention says consumers must not have that line — drop it and inline-qualify Pipely-namespace references instead.

- [ ] **Step 1: Remove `using Pipely;` and `using Pipely;` lines**

```bash
files=$(grep -rl '^using Pipely\b' src/Pipely tests --include='*.cs' 2>/dev/null)
sed -i -E '/^using Pipely(\.FastScheduler)?;$/d' $files
```

Verify:

```bash
grep -rn '^using Pipely' src tests --include='*.cs'
```

Expected: empty output.

- [ ] **Step 2: Drop the `using SpPipe = ...;` alias in `tests/Pipely.Benchmarks/PipeAdapter.cs`**

Task 2's substitution should have turned `using SpPipe = SpscPipelines.SpscPipe;` into `using SpPipe = Pipely.Pipe;`. Drop the alias entirely and inline every `SpPipe` reference as `Pipely.Pipe`:

```bash
sed -i -e '/^using SpPipe = /d' -e 's/\bSpPipe\b/Pipely.Pipe/g' tests/Pipely.Benchmarks/PipeAdapter.cs
```

- [ ] **Step 3: Inline-qualify Pipely-only types mechanically**

For each consumer file, add `Pipely.` (or `Pipely.`) qualification to symbols that exist only in Pipely's namespace:

```bash
files=$(grep -rl 'IContinuationDispatcher\|PipelyAwaiter\|FastScheduler' \
  src/Pipely tests --include='*.cs')

for f in $files; do
  sed -i \
    -e 's/\bIContinuationDispatcher\b/Pipely.IContinuationDispatcher/g' \
    -e 's/\bPipelyAwaiter\b/Pipely.PipelyAwaiter/g' \
    -e 's/\bFastScheduler\b/Pipely.FastScheduler/g' \
    "$f"
done

# Second pass: collapse any accidental Pipely.Pipely.X over-qualification
for f in $files; do
  sed -i \
    -e 's/Pipely\.Pipely\.FastScheduler\./Pipely./g' \
    -e 's/Pipely\.Pipely\./Pipely./g' \
    "$f"
done
```

- [ ] **Step 4: Manually qualify `Pipe`, `PipeOptions`, and (where extended) `PipeWriter`**

This step is per-file because `Pipe`/`PipeOptions`/`PipeWriter` exist in *both* Pipely and BCL namespaces and the right qualification depends on which type the call site means.

Files to review and edit (use the file's own context to decide each call site):

- `src/Pipely/FastScheduler.cs` — references Pipely-only types (e.g., `IContinuationDispatcher`); should already be qualified by Step 3.
- `tests/Pipely.Tests/BclParityTests.cs` — has both BCL and Pipely references; the existing `new SpscPipelines.SpscPipe()` (now `new Pipely.Pipe()` after Task 2) is already correctly qualified. The `new Pipe(...)` reference at the BCL branch should remain `new Pipe(...)` (resolves via `using System.IO.Pipelines;`).
- `tests/Pipely.Tests/BufferSegmentTests.cs` — Pipely-internal type. Reference as `Pipely.BufferSegment` if any direct usage; check.
- `tests/Pipely.Tests/TrackingMemoryOwner.cs` — pure helper, may not need qualification.
- `tests/Pipely.Tests/PipeAdvanceToTests.cs` — `new Pipe(...)` constructions → `new Pipely.Pipe(...)`. Variable types `PipeWriter`/`PipeReader` left as BCL.
- `tests/Pipely.Tests/PipeCancellationTests.cs` — same pattern.
- `tests/Pipely.Tests/PipeContinuationDispatcherTests.cs` — Pipely-specific dispatcher tests; many `Pipe`/`PipeOptions` references → all `Pipely.Pipe`/`Pipely.PipeOptions`.
- `tests/Pipely.Tests/PipeDisposeTests.cs` — Pipely-specific lifecycle.
- `tests/Pipely.Tests/PipeLifecycleTests.cs` — same.
- `tests/Pipely.Tests/PipeReadInProgressTests.cs` — same.
- `tests/Pipely.Tests/PipeReaderTests.cs` — same. Now that `Pipely.PipeReader` is public (Task 7), tests *could* type Reader variables as `Pipely.PipeReader`, but per the convention prefer BCL `PipeReader` unless a Pipely-only API is exercised (none currently exists on the Reader).
- `tests/Pipely.Tests/PipeWriterSpliceTests.cs` — exercises the Pipely-only `Splice` method, so any local writer variable used to call `Splice` MUST be typed as `Pipely.PipeWriter`. Other writer references can stay BCL.
- `tests/Pipely.Tests/PipeWriterTests.cs` — depends on whether any test calls `Splice`. Type each writer locally; BCL where possible.
- `tests/Pipely.Tests/PipelyAwaiterTests.cs` — direct test of Pipely's awaiter; all references qualify as `Pipely.PipelyAwaiter`.
- `tests/Pipely.Stress/Program.cs`, `StressHarness.cs`, `ByteSequence.cs` — Pipely construction qualifies; variable types BCL where possible.
- `tests/Pipely.Benchmarks/PipeAdapter.cs` — qualified per Step 2.
- `tests/Pipely.Benchmarks/SpliceBenchmarks.cs` — uses `Splice` extended API; writer variables typed as `Pipely.PipeWriter`.
- `tests/Pipely.Benchmarks/Program.cs`, `LatencyHarness.cs`, `ThroughputBenchmarks.cs`, `BclPipeAdapter.cs`, `IPipeAdapter.cs` — review each.
- `tests/Pipely.Tests/FastSchedulerTests.cs` — every `new FastScheduler()` already qualified by Step 3; remaining `Pipe`/`PipeOptions` references qualify as Pipely.
- `tests/Pipely.Benchmarks/DispatcherLatencyHarness.cs`, `DispatcherThroughputBench.cs`, `Program.cs` — same.

Mechanical helper for spotting call sites that need attention in a file:

```bash
grep -nE '\bnew (Pipe|PipeOptions)\b|^[^/]*\b(Pipe|PipeOptions)\s+\w+\s*=' <file>
```

Then per match, edit the call site to use `Pipely.Pipe` / `Pipely.PipeOptions` if the call refers to Pipely's type. Leave BCL call sites alone.

The `BclParityTests.cs` and `BclPipeAdapter.cs` files are the main places where BCL `Pipe`/`PipeOptions` are referenced — handle those carefully so you don't over-qualify.

- [ ] **Step 5: Spot-check that no `using Pipely;` snuck back in**

```bash
grep -rn '^using Pipely' src tests --include='*.cs'
```

Expected: empty.

---

## Task 9: Verify clean build and all tests pass

**Files:** none (verification only)

- [ ] **Step 1: Build the solution**

```bash
dotnet build
```

Expected: `0 Error(s)`. Same warning count as Task 1 baseline (the pre-existing `xUnit1030` warning will follow the renamed test file).

If errors:
- "Cannot resolve type `Pipe`" → consumer file is missing `Pipely.` qualification at that site; edit per Task 8 rules.
- "Type `PipeOptions` has no constructor matching..." → likely qualified to BCL when Pipely's was meant (or vice versa); pick the right one.
- "Cannot find file `...SpscPipelines...`" → a path substitution was missed in Task 2; locate and fix.
- "Cannot access nested class `PipeReader`" → Task 7's lift wasn't applied; revisit it.

- [ ] **Step 2: Run all tests**

```bash
dotnet test --nologo --verbosity quiet 2>&1 | tail -20
```

Expected: All tests pass. Total passing test count must match the baseline recorded in Task 1 Step 3. If counts diverge, investigate which tests vanished or were duplicated.

- [ ] **Step 3: Smoke-build stress and benchmarks projects** (optional but recommended)

```bash
dotnet build tests/Pipely.Stress -c Release
dotnet build tests/Pipely.Benchmarks -c Release
dotnet build tests/Pipely.Benchmarks -c Release
```

Expected: all build clean. Don't run them — the existing tests cover correctness.

---

## Task 10: Commit the code rename

**Files:** all the staged changes from Tasks 2–8.

- [ ] **Step 1: Inspect the change set**

```bash
git status
```

Verify: ~30+ renamed files (`R` status) and ~30+ modifications (`M` status).

- [ ] **Step 2: Stage and commit**

```bash
git add -u
git status
```

Expect: clean working tree once nothing remains untracked. (The plan file in `docs/superpowers/plans/` will appear untracked — leave it; it's added in Task 12.)

```bash
git commit -m "$(cat <<'EOF'
Rebrand: SpscPipelines -> Pipely

Rename the assembly, namespace, types, files, and folders. Public
behavior is unchanged. Lift Pipely.PipeReader to top-level public
(was internal nested) so future Pipely-specific reader methods can
be added without a breaking change. Adopt the consumer convention
in tests and the FastScheduler companion project: no `using Pipely;`,
Pipely types are qualified inline, BCL abstract types are preferred
for variables and parameters when the extended surface is not used.

Type renames:
  SpscPipelines.SpscPipe        -> Pipely.Pipe
  SpscPipelines.SpscPipeWriter  -> Pipely.PipeWriter
  SpscPipelines.SpscPipeReader  -> Pipely.PipeReader  (now public)
  SpscPipelines.SpscPipeOptions -> Pipely.PipeOptions
  SpscPipelines.SpscAwaiter     -> Pipely.PipelyAwaiter
EOF
)"
```

- [ ] **Step 3: Verify commit**

```bash
git log -1 --stat | head -50
git status
```

Expected: clean tree (modulo the still-untracked plan file).

---

## Task 11: Update markdown documentation

**Files:**
- Modify: `docs/IContinuationDispatcher.md`
- Modify: every file under `docs/superpowers/specs/` and `docs/superpowers/plans/` that mentions `Spsc*` identifiers
- Modify: `docs/superpowers/measurements/append-benchmark-shell-pool-comparison.md`
- Skip: `docs/superpowers/measurements/*.txt` (raw benchmark output — preserve as historical artifact)
- Skip: `docs/superpowers/plans/2026-04-29-pipely-rebrand-implementation.md` (this plan itself)

User direction: historical specs/plans should be updated since everything will be squashed before going public, so post-squash readers should see consistent naming throughout.

- [ ] **Step 1: Apply identifier substitutions to markdown files**

Same substitution sequence as Task 2, scoped to markdown:

```bash
files=$(find docs -type f -name '*.md' \
  ! -path 'docs/superpowers/plans/2026-04-29-pipely-rebrand-implementation.md')

sed -i \
  -e 's/SpscPipelines\.FastScheduler/Pipely/g' \
  -e 's/SpscPipelines\.Tests/Pipely.Tests/g' \
  -e 's/SpscPipelines\.Stress/Pipely.Stress/g' \
  -e 's/SpscPipelines\.Benchmarks/Pipely.Benchmarks/g' \
  -e 's/SpscPipelines/Pipely/g' \
  -e 's/SpscPipeWriter/PipeWriter/g' \
  -e 's/SpscPipeReader/PipeReader/g' \
  -e 's/SpscPipeOptions/PipeOptions/g' \
  -e 's/SpscPipe/Pipe/g' \
  -e 's/SpscAwaiter/PipelyAwaiter/g' \
  $files
```

- [ ] **Step 2: Verify no `Spsc` tokens remain in markdown**

```bash
grep -rn 'Spsc' docs --include='*.md' | grep -v '2026-04-29-pipely-rebrand-implementation.md'
```

Expected: empty output (the rebrand plan itself contains `Spsc` tokens by necessity).

- [ ] **Step 3: Spot-check `docs/IContinuationDispatcher.md`**

Read the API section and ensure code samples follow the consumer convention from the spec — Pipely types qualified as `Pipely.X`, no `using Pipely;`, BCL types referenced via `using System.IO.Pipelines;`.

```bash
grep -nE '^using |Pipely\.|Pipe[A-Z]' docs/IContinuationDispatcher.md | head -30
```

If any code sample shows `using Pipely;` or bare `Pipe`/`PipeOptions` referring to Pipely's types, edit to the convention. (The current source has the API block as `public interface IContinuationDispatcher` and `public sealed class PipeOptions`; these need to be `Pipely.IContinuationDispatcher` and `Pipely.PipeOptions` if shown in user-facing example code, or kept as bare type declarations if shown as the library's own API surface — read the surrounding prose to decide.)

---

## Task 12: Final verification and commit docs

**Files:** doc edits from Task 11 + the plan file itself.

- [ ] **Step 1: Confirm code build is still clean**

```bash
dotnet build
```

Expected: same result as Task 9. Doc edits should not have broken anything.

- [ ] **Step 2: Stage and commit doc updates**

```bash
git add docs
git status
```

Expect: modified markdown under `docs/`, plus the new plan file `docs/superpowers/plans/2026-04-29-pipely-rebrand-implementation.md`.

```bash
git commit -m "$(cat <<'EOF'
Docs: update SpscPipelines references to Pipely

Apply the rebrand to all design specs, plans, measurements, and the
IContinuationDispatcher reference doc. Code samples in user-facing
docs follow the consumer convention.
EOF
)"
```

- [ ] **Step 3: Final whole-tree check for stragglers**

```bash
grep -rn 'SpscPipelines\|SpscPipe\|SpscAwaiter' \
  --include='*.cs' --include='*.csproj' --include='*.slnx' --include='*.md' \
  src tests docs *.slnx 2>/dev/null \
  | grep -v '2026-04-29-pipely-rebrand-implementation.md\|append-benchmark-baseline.txt\|append-benchmark-after-shell-pool.txt'
```

Expected: empty. Anything that shows up is a leftover; either fix it (small follow-up commit) or document why it's intentional.

```bash
git log --oneline -3
```

Expected: two new commits — the rebrand and the doc update — on top of the prior `674319c Project: rename namespace and test projects to SpscPipelines` commit.

---

## Out of scope (per the user spec)

- README creation — confirmed not needed for this change.
- NuGet package metadata (`<PackageId>`, `<Authors>`, `<Description>`, `<PackageTags>`, `<RepositoryUrl>`) — confirmed as a separate change.
- Public method signatures, internal implementation, pooling strategy, tree structures — untouched.
- Behavior changes of any kind.

## In scope additions beyond the original spec

- `SpscPipeOptions` → `Pipely.PipeOptions` (renamed; lives in `Pipely` namespace, shadowing BCL when qualified). Confirmed by user.
- `SpscAwaiter` → `Pipely.PipelyAwaiter`. Confirmed by user.
- `SpscPipeReader` lifted from nested `internal sealed` to top-level `public sealed`. Confirmed by user — enables future Pipely-specific reader API additions without a breaking change.
- `Pipely` companion project renamed alongside the main library. Confirmed by user.
- All five test/aux projects renamed. Confirmed by user.
- Historical design docs in `docs/superpowers/` updated. Confirmed by user (will squash before public).
- Tests follow the consumer convention so they double as usage examples. Confirmed by user.
