"""
Utility helpers that wrap the **sacct** call and common transformations –
*stream‑friendly* version.

Why this rewrite?
=================
Reading an entire month of accounting output into one giant string can blow up
RSS.  Instead we **stream** stdout line‑by‑line and aggregate on the fly.  We
also expose an option to *reuse* (or immediately clear) the aggregation
`dict`, so callers can free memory as soon as each month is done.
"""
from __future__ import annotations

import datetime as dt
import subprocess
from collections import defaultdict
from typing import Dict, Iterable, Iterator, Tuple

__all__ = [
    "month_bounds",
    "run_sacct_iter",
    "aggregate_iter",
    # legacy wrappers
    "run_sacct",
    "aggregate_lines",
]

# --------------------------------------------------------------------------- #
# Time helpers
# --------------------------------------------------------------------------- #

def month_bounds(start: dt.date, end: dt.date) -> Iterable[Tuple[dt.date, dt.date]]:
    """Yield *(first_day, last_day)* tuples for every month in *[start, end]*."""
    first = dt.date(start.year, start.month, 1)
    while first <= end:
        nxt = dt.date(first.year + (first.month == 12), (first.month % 12) + 1, 1)
        yield first, min(nxt - dt.timedelta(days=1), end)
        first = nxt


# --------------------------------------------------------------------------- #
# sacct wrapper – streaming
# --------------------------------------------------------------------------- #

def _build_sacct_cmd(
    start: dt.date,
    end: dt.date,
    *,
    partition: str | None,
    fields: str,
) -> list[str]:
    cmd = [
        "sacct",
        "--allusers",
        "--noconvert",          # avoid conversions we don’t need
        "-n", "-P",             # no header, pipe‑sep
        "-o", fields,
        "-S", start.isoformat(),
        "-E", end.isoformat(),
    ]
    # Pre‑filter only when the name looks fully qualified (≥3 dashes).
    if partition and partition.count("-") >= 3:
        cmd.extend(["--partition", partition])
    return cmd


def run_sacct_iter(
    start: dt.date,
    end: dt.date,
    *,
    partition: str | None = None,
    fields: str = "User,Partition,CPUTimeRAW",
) -> Iterator[str]:
    """Yield decoded lines from **sacct** one by one (no header)."""
    proc = subprocess.Popen(
        _build_sacct_cmd(start, end, partition=partition, fields=fields),
        stdout=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        bufsize=1,
    )
    try:
        assert proc.stdout is not None  # for type checkers
        for line in proc.stdout:
            yield line.rstrip("\n")
    finally:
        if proc.stdout is not None:
            proc.stdout.close()
        proc.wait()


# --------------------------------------------------------------------------- #
# Aggregation helpers
# --------------------------------------------------------------------------- #

def aggregate_iter(
    lines: Iterable[str],
    partition_prefix: str,
    *,
    usage: Dict[str, int] | None = None,
) -> Dict[str, int]:
    """Sum CPUTimeRAW seconds per user for partitions starting with *prefix*."""
    if usage is None:
        usage = defaultdict(int)
    for ln in lines:
        # guard against empty / malformed rows
        if "|" not in ln:
            continue
        user, part, secs = ln.split("|", 2)
        if part.startswith(partition_prefix) and user:
            try:
                usage[user] += int(secs)
            except ValueError:
                # skip rows with non‑integer CPUTimeRAW
                continue
    return usage


# --------------------------------------------------------------------------- #
# Legacy, string‑based wrappers (kept for backward compatibility)
# --------------------------------------------------------------------------- #

def run_sacct(
    start: dt.date,
    end: dt.date,
    *,
    partition: str | None = None,
    fields: str = "User,Partition,CPUTimeRAW",
) -> str:
    """Deprecated string variant.  Joins the streamed output for old callers."""
    return "\n".join(run_sacct_iter(start, end, partition=partition, fields=fields))


def aggregate_lines(lines: str, partition_prefix: str) -> Dict[str, int]:
    """Deprecated helper that keeps the old signature but uses streaming core."""
    return aggregate_iter(lines.splitlines(), partition_prefix)

