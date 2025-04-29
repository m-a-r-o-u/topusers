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
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict

from .sacct_tools import month_bounds, run_sacct, aggregate_lines
import json
import csv
import os


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


def cmd_monthly(args: argparse.Namespace) -> None:
    outdir = Path(args.outdir).expanduser()
    outdir.mkdir(parents=True, exist_ok=True)

    for first, last in month_bounds(args.start, args.end):
        sys.stderr.write(f"[monthly] {first:%Y-%m} … ")
        raw = run_sacct(first, last, partition=args.partition)
        usage = aggregate_lines(raw, args.partition)
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
    # Define fixed CSV columns
    fieldnames = ["user", "measure", "emailadressen", "Vorname", "Nachname", "geschlecht", "status"]
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
        email_str = ",".join(email_list)
        # Build row with fixed columns
        row = {
            "user": user,
            "measure": measure,
            "emailadressen": email_str,
            "Vorname": details.get("vorname", ""),
            "Nachname": details.get("nachname", ""),
            "geschlecht": details.get("geschlecht", ""),
            "status": data.get("status", ""),
        }
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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="topusers", description="LRZ SLURM usage helpers")
    sub = p.add_subparsers(dest="command", required=True)

    # monthly
    pm = sub.add_parser("monthly", help="collect monthly sacct stats")
    pm.add_argument("--start", required=True, type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date())
    pm.add_argument("--end",   required=True, type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date())
    pm.add_argument("--partition", default="lrz-hgx-h100-94x4", help="SLURM partition")
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

    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

