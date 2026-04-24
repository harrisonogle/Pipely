using System.Text;

namespace SpscPipe;

// Scoped diagnostic log for the audit in Phase 3.  Not thread-safe in the
// strictest sense — we use a lock to serialize appends — but the order of
// appends reflects the actual chronological order on a given observer's
// view (the lock's memory barrier ensures it).  Removed after the audit.
internal sealed class DiagLog
{
    internal struct Entry
    {
        internal long Seq;
        internal int ThreadId;
        internal string Kind;
        internal long D1, D2, D3, D4;
        internal string Note;
    }

    private readonly List<Entry> _entries = new(1024);
    private long _seq;
    private readonly object _gate = new();

    internal void Log(string kind, long d1 = 0, long d2 = 0, long d3 = 0, long d4 = 0, string note = "")
    {
        lock (_gate)
        {
            // Assign seq inside the lock so seq order == list-add order.
            _entries.Add(new Entry
            {
                Seq = ++_seq,
                ThreadId = Environment.CurrentManagedThreadId,
                Kind = kind,
                D1 = d1, D2 = d2, D3 = d3, D4 = d4,
                Note = note,
            });
        }
    }

    internal string Dump()
    {
        lock (_gate)
        {
            var sb = new StringBuilder();
            foreach (var e in _entries)
            {
                sb.Append('#').Append(e.Seq).Append(' ')
                  .Append("T").Append(e.ThreadId).Append(' ')
                  .Append(e.Kind).Append(' ')
                  .Append(e.D1).Append(' ')
                  .Append(e.D2).Append(' ')
                  .Append(e.D3).Append(' ')
                  .Append(e.D4);
                if (!string.IsNullOrEmpty(e.Note))
                {
                    sb.Append(' ').Append(e.Note);
                }
                sb.AppendLine();
            }
            return sb.ToString();
        }
    }
}
