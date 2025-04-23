# Topusers

CLI helpers for querying **SLURM accounting** data on LRZ HPC clusters and producing per-user CPU‑time statistics.

---

## Installation

```bash
# clone the repo or copy the project directory
pip install -e .
```

`pip install -e .` installs the console entry‑point **`topusers`** into your `$PATH`.

---

## Usage

The workflow is split into three independent sub‑commands so that the heavy data collection happens in memory‑safe monthly chunks, while post‑processing is fast and file‑based.

> **Tip:** `topusers --help` and `topusers <subcommand> --help` show full CLI reference docs.

### 1 · Collect month‑wise data

```bash
topusers monthly \
  --start 2024-01-01 \   # first day (inclusive)
  --end   2024-12-31 \   # last  day (inclusive)
  --partition lrz-hgx-h100-94x4 \   # SLURM partition to analyse
  --outdir stats          # save YYYY-MM.txt files here
```

* For every month in the interval the command
  * calls `sacct` only for that month, avoiding out‑of‑memory errors,
  * filters rows whose **Partition** equals `lrz-hgx-h100-94x4` (or any prefix you provide),
  * aggregates `CPUTimeRAW` seconds per user, and
  * writes `stats/2024-01.txt`, `stats/2024-02.txt`, …

Each of these text files contains two whitespace‑separated columns:

```
<userid> <cpu_seconds>
```

### 2 · Aggregate across months

```bash
topusers aggregate \
  --datadir stats \        # directory with the monthly *.txt files
  --ofile   all_users.txt  # combined totals per user
```

The resulting `all_users.txt` has the same two‑column format but the CPU‑time values are summed across **all** input months.

### 3 · Remove MCML‑affiliated users

```bash
topusers nomcml \
  --ifile all_users.txt \     # aggregated input
  --mcmlprojects abc123,def456 \  # comma‑separated UNIX group names
  --ofile all_users_nomcml.txt
```

Users whose UNIX group list intersects with any of the supplied project IDs (`abc123` or `def456` in the example) are excluded from the output file.

The final `all_users_nomcml.txt` thus contains **only non‑MCML users** with their total CPU‑time over the chosen period.

---

## Finding historical partition names

If a month‑file is empty you may be querying the wrong partition prefix. Ask the accounting database which partitions actually had jobs in that period:

```bash
sacct --allusers -n -P -X \
      -o Partition \
      -S 2024-05-01 -E 2024-05-31 | \
  sort -u
```

Replace the dates to suit your timeframe. The list includes partitions that existed *then*, even if they have since been renamed or removed.

---

## Common pitfalls

| Issue | Fix |
|-------|-----|
| `error: argument --end: invalid value '2025-04-31'` | Make sure the date really exists (April has only 30 days). |
| No data in a month‑file | Confirm that the partition prefix is correct **and** that jobs actually ran in that period. |

---

## License

Released under the MIT License.

