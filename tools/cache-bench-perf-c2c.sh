#!/usr/bin/env bash
# tools/cache-bench-perf-c2c.sh
#
# Captures `perf c2c` HITM cache-line attribution for the Pipely cache-bench
# harness, both BCL and Pipely arms back-to-back in the same session. Writes
# the perf reports + harness stdout into a dated directory under artifacts/
# (gitignored, alongside BDN's own output).
#
# Intended invocation:
#
#     sudo ./tools/cache-bench-perf-c2c.sh
#
# Why sudo: `perf c2c record` requires CAP_PERFMON / CAP_SYS_ADMIN to enable
# the IBS / PEBS PMU events that drive HITM attribution. The script keeps
# perf as root but drops back to $SUDO_USER for the dotnet build and the
# inner cache-bench process — so NuGet cache, obj/bin, and report files
# stay owned by your real user.
#
# Tunables (env vars):
#     PRODUCER_CORE     CPU index for the producer thread       (default 2)
#     CONSUMER_CORE     CPU index for the consumer thread       (default 4)
#     DURATION          Run duration per arm, seconds           (default 20)
#     OUTPUT_DIR        Output directory                        (default
#                       artifacts/cache-bench-YYYY-MM-DD)
#
# Pick producer/consumer cores that share L3 on your CPU. On Ryzen 7 8700F
# (Zen 4, 8 cores in 2 CCDs of 4) cores 0/2 share an L3; the historical
# RESULTS.md numbers used 2/4 (same CCD, different SMT siblings).
#
# Exit codes:
#     0  success
#     1  not running as root
#     2  SUDO_USER not set (script invoked from a root login)
#     3  perf not installed
#     4  build failed
#     5  record or report failed for one arm

set -euo pipefail

# ---- preflight ----

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: this script must be run with sudo (perf c2c record needs root)." >&2
    exit 1
fi

if [[ -z "${SUDO_USER:-}" ]]; then
    echo "ERROR: SUDO_USER is empty. Invoke via 'sudo ./tools/cache-bench-perf-c2c.sh'" >&2
    echo "       from a normal user account, not from a root login." >&2
    exit 2
fi

if ! command -v perf >/dev/null 2>&1; then
    echo "ERROR: perf not found. On Ubuntu/Debian:" >&2
    echo "       sudo apt install linux-tools-\$(uname -r) linux-tools-generic" >&2
    exit 3
fi

USER_HOME=$(getent passwd "$SUDO_USER" | cut -d: -f6)
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

PRODUCER_CORE=${PRODUCER_CORE:-2}
CONSUMER_CORE=${CONSUMER_CORE:-4}
DURATION=${DURATION:-20}
DATE_STAMP=$(date -u +%Y-%m-%d)
OUTPUT_DIR=${OUTPUT_DIR:-artifacts/cache-bench-${DATE_STAMP}}

mkdir -p "$OUTPUT_DIR"

PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid)

cat <<EOF
==> repo root:    $REPO_ROOT
==> output dir:   $OUTPUT_DIR
==> cores:        producer=$PRODUCER_CORE  consumer=$CONSUMER_CORE
==> duration:     ${DURATION}s per arm
==> calling user: $SUDO_USER  ($USER_HOME)
==> perf paranoid level: $PARANOID
EOF

if [[ $PARANOID -gt 1 ]]; then
    echo "    (note: c2c works fine while running as root regardless; this is just FYI)"
fi
echo

# ---- build (as the real user) ----

echo "==> Building Pipely.slnx (Release) as $SUDO_USER ..."
if ! sudo -u "$SUDO_USER" -E env HOME="$USER_HOME" \
        dotnet build -c Release Pipely.slnx --nologo --verbosity quiet; then
    echo "ERROR: dotnet build failed." >&2
    exit 4
fi
echo

# ---- run ----

run_arm() {
    local arm=$1
    local data_file=/tmp/c2c-${arm}.data
    local report_file=$OUTPUT_DIR/c2c-${arm}.txt
    local stdout_file=$OUTPUT_DIR/c2c-${arm}-stdout.txt

    echo "==> [$arm] perf c2c record (${DURATION}s) ..."
    rm -f "$data_file"

    # perf runs as root (we are root); drop to $SUDO_USER for the dotnet child.
    # `-E env` preserves environment vars across the inner sudo's privilege drop.
    if ! perf c2c record \
            -o "$data_file" \
            -- sudo -u "$SUDO_USER" -E env \
                HOME="$USER_HOME" \
                DOTNET_EnableWriteXorExecute=0 \
                dotnet run -c Release --project tests/Pipely.Benchmarks --no-build -- \
                cache-bench --pipe "$arm" \
                    --producer-core "$PRODUCER_CORE" \
                    --consumer-core "$CONSUMER_CORE" \
                    --duration "$DURATION" \
            > "$stdout_file" 2>&1; then
        echo "ERROR: perf c2c record failed for $arm. See $stdout_file" >&2
        return 1
    fi

    echo "    record → $data_file"
    echo "    stdout → $stdout_file"

    echo "==> [$arm] perf c2c report ..."
    if ! perf c2c report -i "$data_file" --stdio > "$report_file" 2>&1; then
        echo "ERROR: perf c2c report failed for $arm. See $report_file" >&2
        return 1
    fi
    echo "    report → $report_file"

    # Hand ownership back to the real user.
    chown "$SUDO_USER:$SUDO_USER" "$report_file" "$stdout_file"

    # Drop the .data files (large, regenerable). Comment out if you want
    # them for offline `perf c2c report` exploration.
    rm -f "$data_file"

    echo
}

if ! run_arm pipely; then exit 5; fi
if ! run_arm bcl;    then exit 5; fi

# ---- summary ----

chown "$SUDO_USER:$SUDO_USER" "$OUTPUT_DIR"

echo "==> Done. Files in $OUTPUT_DIR:"
ls -la "$OUTPUT_DIR"
echo

echo "==> Throughput from harness stdout:"
for arm in pipely bcl; do
    f=$OUTPUT_DIR/c2c-${arm}-stdout.txt
    line=$(grep -E "Throughput:" "$f" 2>/dev/null || true)
    if [[ -n "$line" ]]; then
        echo "    $arm:  $line"
    else
        echo "    $arm:  (no throughput line found in $f — check for errors)"
    fi
done
echo

echo "==> HITM summary (from perf c2c report header):"
for arm in pipely bcl; do
    f=$OUTPUT_DIR/c2c-${arm}.txt
    block=$(grep -E "^ +(Load LLC hit|Load Local HITM|Load Remote HITM|LLC hits on shared lines) +:" "$f" 2>/dev/null || true)
    if [[ -n "$block" ]]; then
        echo "    $arm:"
        echo "$block" | sed 's/^/      /'
    fi
done

echo
echo "==> HITM per GiB transferred (lower = less cross-thread cache-line bouncing):"
for arm in pipely bcl; do
    so=$OUTPUT_DIR/c2c-${arm}-stdout.txt
    rep=$OUTPUT_DIR/c2c-${arm}.txt
    mibps=$(grep -oE "Throughput: [0-9.]+ MiB/s" "$so" 2>/dev/null | grep -oE "[0-9.]+" | head -1 || true)
    secs=$(grep -oE "Elapsed: [0-9.]+s" "$so" 2>/dev/null | grep -oE "[0-9.]+" | head -1 || true)
    hitm=$(grep -E "^ +Load Local HITM +:" "$rep" 2>/dev/null | grep -oE "[0-9]+" | tail -1 || true)
    if [[ -n "$mibps" && -n "$secs" && -n "$hitm" ]]; then
        # GiB transferred = MiB/s × seconds / 1024
        per_gib=$(awk -v h="$hitm" -v m="$mibps" -v s="$secs" 'BEGIN{ printf "%.2f", h * 1024 / (m * s) }')
        echo "    $arm:  $hitm HITM / ($mibps MiB/s × ${secs}s ÷ 1024) GiB  ≈  $per_gib HITM/GiB"
    fi
done

echo
echo "Tip: open the per-arm reports for the cache-line / field-level breakdown."
