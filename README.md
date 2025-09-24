# Topusers

CLI helpers for querying **SLURM accounting** data on LRZ HPC clusters and producing per-user CPU‑time statistics.

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
  --start 2024-01 \      # month or full date (inclusive)
  --end   2024-12-31 \   # optional end (inclusive)
  --partition lrz-hgx-h100-94x4 \   # SLURM partition to analyse
  --outdir stats          # save YYYY-MM.txt files here
```

* For every month in the interval the command
  * calls `sacct` only for that month, avoiding out‑of‑memory errors,
  * filters rows whose **Partition** equals `lrz-hgx-h100-94x4` (or any prefix you provide),
  * aggregates `CPUTimeRAW` seconds per user, and
  * writes `stats/2024-01.txt`, `stats/2024-02.txt`, …

If you only pass a month (e.g. `--start 2024-08`) the command automatically
processes that entire month. When the chosen month is still in progress, the
end date is capped at “today” so that partial monthly data can be collected
without specifying `--end`.

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

### 4 · Enrich with user data

```bash
topusers enrich \
  --ifile all_users_nomcml.txt \   # two-column input (user and measure)
  --ofile enriched_list_of_topusers.csv  # output CSV with user details
```

This command reads the input two-column file, fetches each user’s JSON record from the SIM API (`https://simapi.sim.lrz.de/user/<user>`) using `curl --netrc-file ~/.netrc`, and writes a CSV with columns:
  - `user` (ID)
  - `measure` (original second column)
  - Additional user fields extracted from the JSON response (e.g. name, email, department)
  - `projekt` (project ID from the SIM API response)

### 5 · Aggregate by project

```bash
topusers aggregate_groups \
  --ifile enriched_list_of_topusers.csv \
  --ofile enriched_list_of_topgroups.csv
```

This command reads the enriched CSV (must include `projekt` and `measure` columns), sums the integer `measure` values for each unique `projekt`, and writes a CSV with columns:
  - `projekt` (project ID)
  - `measure` (total sum of measures per project)

### 6 · Extract email addresses

```bash
topusers emails \
  --ifile enriched_list_of_topusers.csv \   # CSV with an 'Email address' column
  --ofile emails.txt \                     # output file for semicolon-separated list
  -n 50                                    # number of top email addresses to extract
```

This command reads the enriched CSV (must include an 'Email address' column), filters out any email addresses whose domain contains "lrz", and writes the top N email addresses as a semicolon-separated list to the output file.

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

