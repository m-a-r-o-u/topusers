#!/usr/bin/env python3
"""
Entry point for the *topusers* command with four sub-commands:

    topusers monthly   …
    topusers aggregate …
    topusers nomcml    … (legacy: filter out MCML-affiliated users)
    topusers mcml       … filter users based on MCML affiliation (keep or drop)
"""
from __future__ import annotations
import argparse
import calendar
import csv
import datetime as dt
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable

from .sacct_tools import month_bounds, run_sacct, aggregate_lines


@dataclass(frozen=True)
class DateSpec:
    value: dt.date
    is_month: bool


def parse_date_or_month(value: str) -> DateSpec:
    """Parse *value* as YYYY-MM-DD or YYYY-MM and mark month-only inputs."""
    try:
        dt_value = dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        try:
            dt_value = dt.datetime.strptime(value, "%Y-%m").date()
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                "expected YYYY-MM-DD or YYYY-MM"
            ) from exc
        return DateSpec(dt.date(dt_value.year, dt_value.month, 1), True)
    return DateSpec(dt_value, False)


def _end_of_month(day: dt.date) -> dt.date:
    """Return the last day of the month that *day* falls into."""
    last_day = calendar.monthrange(day.year, day.month)[1]
    return day.replace(day=last_day)


def write_kv_file(path: Path, usage: Dict[str, int]) -> None:
    """Write “user seconds” lines sorted by descending seconds."""
    with path.open("w", encoding="utf-8") as fh:
        for user, secs in sorted(usage.items(), key=lambda x: x[1], reverse=True):
            fh.write(f"{user} {secs}\n")


def read_mcml_file(path: str) -> list[str]:
    """
    Read one MCML project ID per line, skip blank lines.
    """
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def user_groups(user: str) -> set[str]:
    """Return all group names a user belongs to (primary + supplementary)."""
    try:
        out = subprocess.check_output(["id", "-Gn", user], text=True)
        return set(out.strip().split())
    except subprocess.CalledProcessError:
        # user not found or error querying groups; treat as no groups
        return set()


def mcml_initiative(user: str) -> str:
    """Return "mcml" if *user* has a secondary group ending with ai-h-mcml."""
    try:
        out = subprocess.check_output(["id", user], text=True)
    except subprocess.CalledProcessError:
        return ""
    if "groups=" not in out:
        return ""
    groups_part = out.split("groups=", 1)[1]
    names = re.findall(r"\(([^)]+)\)", groups_part)
    for name in names[1:]:
        if name.endswith("ai-h-mcml"):
            return "mcml"
    return ""


def parse_partition_filters(raw: str | Iterable[str] | None) -> list[str]:
    """Split comma-separated partition filters and drop blanks."""
    if raw is None:
        return []
    if isinstance(raw, str):
        items = raw.split(",")
    else:
        items = list(raw)
    return [item.strip() for item in items if item.strip()]


def cmd_monthly(args: argparse.Namespace) -> None:
    outdir = Path(args.outdir).expanduser()
    outdir.mkdir(parents=True, exist_ok=True)

    start_spec: DateSpec = args.start
    end_spec: DateSpec | None = args.end

    partition_filters = parse_partition_filters(args.partition)

    start = start_spec.value
    if end_spec is None:
        if start_spec.is_month:
            end = _end_of_month(start)
            today = dt.date.today()
            if start.year == today.year and start.month == today.month:
                end = min(end, today)
        else:
            raise SystemExit("[monthly] --end is required when --start includes a day")
    else:
        end = end_spec.value
        if end_spec.is_month:
            end = _end_of_month(end)

    if end < start:
        raise SystemExit("[monthly] --end must not be before --start")

    for first, last in month_bounds(start, end):
        sys.stderr.write(f"[monthly] {first:%Y-%m} … ")
        raw = run_sacct(first, last, partition=None)
        usage = aggregate_lines(raw, partition_filters)
        write_kv_file(outdir / f"{first:%Y-%m}.txt", usage)
        sys.stderr.write("done\n")


def cmd_aggregate(args: argparse.Namespace) -> None:
    total: Dict[str, int] = {}
    datadir = Path(args.datadir).expanduser()

    for txt in sorted(datadir.glob("*.txt")):
        for line in txt.read_text().splitlines():
            user, secs = line.split()
            total[user] = total.get(user, 0) + int(secs)

    write_kv_file(Path(args.ofile).expanduser(), total)
    sys.stderr.write(f"[aggregate] wrote {args.ofile}\n")


def cmd_nomcml(args: argparse.Namespace) -> None:
    # unify mcml project IDs from either a comma-list or a file
    if getattr(args, 'mcmlfile', None):
        mcml = set(read_mcml_file(args.mcmlfile))
    else:
        mcml = set(args.mcmlprojects.split(","))
    keep: Dict[str, int] = {}

    for line in Path(args.ifile).read_text().splitlines():
        user, secs = line.split()
        if user_groups(user).isdisjoint(mcml):
            keep[user] = int(secs)

    write_kv_file(Path(args.ofile).expanduser(), keep)
    sys.stderr.write(f"[nomcml] wrote {args.ofile}\n")
   
def cmd_mcml(args: argparse.Namespace) -> None:
    # unify mcml project IDs from either a comma-list or a file
    if getattr(args, 'mcmlfile', None):
        mcml = set(read_mcml_file(args.mcmlfile))
    else:
        mcml = set(args.mcmlprojects.split(','))
    filtered: Dict[str, int] = {}

    for line in Path(args.ifile).read_text().splitlines():
        user, secs = line.split()
        groups = user_groups(user)
        if args.no and groups.isdisjoint(mcml):
            filtered[user] = int(secs)
        elif args.yes and not groups.isdisjoint(mcml):
            filtered[user] = int(secs)

    write_kv_file(Path(args.ofile).expanduser(), filtered)
    mode = 'yes' if args.yes else 'no'
    sys.stderr.write(f"[mcml {mode}] wrote {args.ofile}\n")

def cmd_enrich(args: argparse.Namespace) -> None:
    """Fetch user details via SIM API and write CSV with input measure."""
    infile = Path(args.ifile).expanduser()
    outfile = Path(args.ofile).expanduser()
    rows: list[dict] = []
    # Define fixed CSV columns, include project ID per user
    fieldnames = ["user", "measure", "Email address", "Vorname", "Nachname", "geschlecht", "status", "projekt", "initiative"]
    netrc = os.path.expanduser("~/.netrc")
    for line in infile.read_text().splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        user, measure = parts[0], parts[1]
        sys.stderr.write(f"[enrich] fetching {user} ... ")
        try:
            out = subprocess.check_output([
                "curl", "-sS", "--netrc-file", netrc,
                "-H", "Accept: application/json",
                f"https://simapi.sim.lrz.de/user/{user}"
            ], text=True)
            data = json.loads(out)
            if not isinstance(data, dict):
                data = {"data": data}
        except subprocess.CalledProcessError as e:
            sys.stderr.write("error\n")
            sys.stderr.write(f"[enrich] curl failed for {user}: {e}\n")
            data = {}
        except json.JSONDecodeError as e:
            sys.stderr.write("error\n")
            sys.stderr.write(f"[enrich] JSON decode failed for {user}: {e}\n")
            data = {}
        else:
            sys.stderr.write("ok\n")
        # Extract nested data under 'daten' if present, otherwise use top-level data
        if isinstance(data.get("daten"), dict):
            details = data["daten"]
        else:
            details = data
        # Condense emailadressen to a comma-separated list of addresses
        raw_emails = details.get("emailadressen", [])
        email_list: list[str] = []
        if isinstance(raw_emails, list):
            for rec in raw_emails:
                if isinstance(rec, dict) and "adresse" in rec:
                    email_list.append(rec["adresse"])
                else:
                    email_list.append(str(rec))
        else:
            email_list.append(str(raw_emails))
        # remove duplicates while preserving order
        email_list = list(dict.fromkeys(email_list))
        # Select a single email per user
        first_name = details.get("vorname", "").lower()
        last_name = details.get("nachname", "").lower()
        selected_email = ""
        if email_list:
            if first_name and last_name:
                for email_addr in email_list:
                    email_lower = email_addr.lower()
                    if first_name in email_lower and last_name in email_lower:
                        selected_email = email_addr
                        break
                else:
                    selected_email = email_list[0]
            else:
                selected_email = email_list[0]
        # Build row with fixed columns
        row = {
            "user": user,
            "measure": measure,
            "Email address": selected_email,
            "Vorname": details.get("vorname", ""),
            "Nachname": details.get("nachname", ""),
            "geschlecht": details.get("geschlecht", ""),
            "status": data.get("status", ""),
        }
        # include project ID from SIM API response
        row["projekt"] = data.get("projekt", "")
        row["initiative"] = mcml_initiative(user)
        rows.append(row)
    # write CSV with fixed columns
    with outfile.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out_row = {}
            for k in fieldnames:
                v = row.get(k, "")
                if not isinstance(v, (str, int, float, bool)) and v is not None:
                    v = json.dumps(v, ensure_ascii=False)
                out_row[k] = v
            writer.writerow(out_row)
    sys.stderr.write(f"[enrich] wrote {outfile}\n")
    
def cmd_emails(args: argparse.Namespace) -> None:
    """Extract top N email addresses from enriched CSV, skipping LRZ addresses."""
    infile = Path(args.ifile).expanduser()
    outfile = Path(args.ofile).expanduser()
    emails: list[str] = []
    # Read input CSV and collect emails
    with infile.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        if 'Email address' not in (reader.fieldnames or []):
            sys.stderr.write("[emails] error: input file missing 'Email address' column\n")
            sys.exit(1)
        for row in reader:
            email = (row.get('Email address') or '').strip()
            if not email:
                continue
            # skip LRZ addresses (domain contains 'lrz')
            parts = email.split('@', 1)
            if len(parts) == 2 and 'lrz' in parts[1].lower():
                continue
            emails.append(email)
            if len(emails) >= args.n:
                break
    # Write output as semicolon-separated list
    with outfile.open('w', encoding='utf-8') as fh:
        fh.write(';'.join(emails))
        fh.write('\n')
    sys.stderr.write(f"[emails] wrote {outfile}\n")
   
def cmd_aggregate_groups(args: argparse.Namespace) -> None:
    """Sum measures per project from enriched CSV and write CSV."""
    infile = Path(args.ifile).expanduser()
    outfile = Path(args.ofile).expanduser()
    totals: dict[str, int] = {}
    # Read enriched CSV and accumulate measures per project
    with infile.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        # Ensure required columns
        fn = reader.fieldnames or []
        if 'projekt' not in fn or 'measure' not in fn:
            sys.stderr.write("[aggregate_groups] error: input file missing 'projekt' or 'measure' column\n")
            sys.exit(1)
        for row in reader:
            proj = (row.get('projekt') or '').strip()
            val = row.get('measure', '').strip()
            try:
                m = int(val)
            except ValueError:
                m = 0
            totals[proj] = totals.get(proj, 0) + m
    # Write aggregated CSV
    with outfile.open('w', encoding='utf-8', newline='') as fh:
        writer = csv.writer(fh)
        writer.writerow(['projekt', 'measure'])
        for proj, m in sorted(totals.items()):
            writer.writerow([proj, m])
    sys.stderr.write(f"[aggregate_groups] wrote {outfile}\n")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="topusers", description="LRZ SLURM usage helpers")
    sub = p.add_subparsers(dest="command", required=True)

    # monthly
    pm = sub.add_parser("monthly", help="collect monthly sacct stats")
    pm.add_argument(
        "--start",
        required=True,
        type=parse_date_or_month,
        help="start date (YYYY-MM-DD) or month (YYYY-MM)",
    )
    pm.add_argument(
        "--end",
        type=parse_date_or_month,
        help="optional end date; accepts YYYY-MM-DD or YYYY-MM",
    )
    pm.add_argument(
        "--partition",
        default="lrz-hgx-h100-94x4",
        help=(
            "comma-separated partition filters (supports wildcards like 'lrz*'); "
            "prefix matches without wildcards"
        ),
    )
    pm.add_argument("--outdir", default=".", help="output directory for YYYY-MM.txt files")
    pm.set_defaults(func=cmd_monthly)

    # aggregate
    pa = sub.add_parser("aggregate", help="merge all monthly txt files")
    pa.add_argument("--datadir", required=True, help="directory with monthly *.txt files")
    pa.add_argument("--ofile",   required=True, help="output file for totals")
    pa.set_defaults(func=cmd_aggregate)

    # nomcml
    pn = sub.add_parser("nomcml", help="filter out MCML-affiliated users")
    pn.add_argument("--ifile", required=True, help="aggregated per-user stats to filter")
    grp = pn.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--mcmlprojects",
        help="comma-separated list of MCML group names (e.g. abc123,def456)"
    )
    grp.add_argument(
        "--mcmlfile",
        help="path to file with one MCML group name per line"
    )
    pn.add_argument("--ofile", required=True, help="output file after filtering")
    pn.set_defaults(func=cmd_nomcml)

    # mcml: filter users based on MCML affiliation (keep or drop)
    pmc = sub.add_parser(
        "mcml",
        help="filter users based on MCML affiliation (keep or drop)"
    )
    pmc.add_argument(
        "--ifile",
        required=True,
        help="aggregated per-user stats to filter"
    )
    grp_proj2 = pmc.add_mutually_exclusive_group(required=True)
    grp_proj2.add_argument(
        "--mcmlprojects",
        help="comma-separated list of MCML group names (e.g. abc123,def456)"
    )
    grp_proj2.add_argument(
        "--mcmlfile",
        help="path to file with one MCML group name per line"
    )
    grp_mode = pmc.add_mutually_exclusive_group(required=True)
    grp_mode.add_argument(
        "--yes",
        action="store_true",
        help="keep only MCML-affiliated users"
    )
    grp_mode.add_argument(
        "--no",
        action="store_true",
        help="filter out MCML-affiliated users (like nomcml)"
    )
    pmc.add_argument(
        "--ofile",
        required=True,
        help="output file after filtering"
    )
    pmc.set_defaults(func=cmd_mcml)
    
    # enrich: fetch user details from SIM API and output CSV
    pe = sub.add_parser(
        "enrich",
        help="enrich per-user stats via SIM API and write CSV"
    )
    pe.add_argument(
        "--ifile",
        required=True,
        help="input two-column file (user and measure)"
    )
    pe.add_argument(
        "--ofile",
        required=True,
        help="output CSV file for enriched user data"
    )
    pe.set_defaults(func=cmd_enrich)
    # emails: extract top-N email addresses from enriched CSV
    pe2 = sub.add_parser(
        "emails",
        help="extract top N email addresses from enriched CSV, skipping LRZ addresses"
    )
    pe2.add_argument(
        "--ifile",
        required=True,
        help="input CSV file with email addresses (must include 'Email address' column)"
    )
    pe2.add_argument(
        "--ofile",
        required=True,
        help="output file for semicolon-separated email list"
    )
    pe2.add_argument(
        "-n",
        dest="n",
        type=int,
        required=True,
        help="number of top email addresses to extract"
    )
    pe2.set_defaults(func=cmd_emails)
    # aggregate_groups: sum measures per project from enriched CSV
    pag = sub.add_parser(
        "aggregate_groups",
        help="sum measures per project from enriched CSV"
    )
    pag.add_argument(
        "--ifile",
        required=True,
        help="input enriched CSV file with 'projekt' and 'measure' columns"
    )
    pag.add_argument(
        "--ofile",
        required=True,
        help="output CSV file for aggregated project measures"
    )
    pag.set_defaults(func=cmd_aggregate_groups)

    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

