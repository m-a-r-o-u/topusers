#!/usr/bin/env python3
"""
Entry point for the *topusers* command with three sub-commands:

    topusers monthly   …
    topusers aggregate …
    topusers nomcml    …
"""
from __future__ import annotations
import argparse
import datetime as dt
import grp
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict

from .sacct_tools import month_bounds, run_sacct, aggregate_lines


def write_kv_file(path: Path, usage: Dict[str, int]) -> None:
    """Write “user seconds” lines sorted by descending seconds."""
    with path.open("w", encoding="utf-8") as fh:
        for user, secs in sorted(usage.items(), key=lambda x: x[1], reverse=True):
            fh.write(f"{user} {secs}\n")


# --------------------------------------------------------------------------- #
#  sub-command: monthly
# --------------------------------------------------------------------------- #
def cmd_monthly(args: argparse.Namespace) -> None:
    outdir = Path(args.outdir).expanduser()
    outdir.mkdir(parents=True, exist_ok=True)

    for first, last in month_bounds(args.start, args.end):
        sys.stderr.write(f"[monthly] {first:%Y-%m} … ")
        raw = run_sacct(first, last, partition=args.partition)
        usage = aggregate_lines(raw, args.partition)
        write_kv_file(outdir / f"{first:%Y-%m}.txt", usage)
        sys.stderr.write("done\n")


# --------------------------------------------------------------------------- #
#  sub-command: aggregate
# --------------------------------------------------------------------------- #
def cmd_aggregate(args: argparse.Namespace) -> None:
    total: Dict[str, int] = {}
    datadir = Path(args.datadir).expanduser()

    for txt in sorted(datadir.glob("*.txt")):
        for line in txt.read_text().splitlines():
            user, secs = line.split()
            total[user] = total.get(user, 0) + int(secs)

    write_kv_file(Path(args.ofile).expanduser(), total)
    sys.stderr.write(f"[aggregate] wrote {args.ofile}\n")


# --------------------------------------------------------------------------- #
#  sub-command: nomcml
# --------------------------------------------------------------------------- #
def user_groups(user: str) -> set[str]:
    """Return all group names a user belongs to (primary + supplementary)."""
    # id -Gn works reliably in LDAP-backed environments
    out = subprocess.check_output(["id", "-Gn", user], text=True)
    return set(out.strip().split())


def cmd_nomcml(args: argparse.Namespace) -> None:
    mcml = set(args.mcmlprojects.split(","))
    keep: Dict[str, int] = {}

    for line in Path(args.ifile).read_text().splitlines():
        user, secs = line.split()
        if user_groups(user).isdisjoint(mcml):
            keep[user] = int(secs)

    write_kv_file(Path(args.ofile).expanduser(), keep)
    sys.stderr.write(f"[nomcml] wrote {args.ofile}\n")


# --------------------------------------------------------------------------- #
#  dispatcher
# --------------------------------------------------------------------------- #
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
    pn.add_argument("--ifile",        required=True, help="aggregated per-user stats to filter")
    pn.add_argument("--mcmlprojects", required=True, help="comma-separated list of MCML group names")
    pn.add_argument("--ofile",        required=True, help="output file after filtering")
    pn.set_defaults(func=cmd_nomcml)

    return p


def main(argv: list[str] | None = None) -> None:  # noqa: D401
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

