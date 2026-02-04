#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter weather station records by temperature.")
    p.add_argument("-i", "--input", required=True, type=Path, help="Input file path")
    p.add_argument("-o", "--output", required=True, type=Path, help="Output file path")
    p.add_argument("--temp-min", required=True, type=float, help="Minimum temperature threshold")
    p.add_argument("--log", default=Path("weather_filter.log"), type=Path, help="Log file path")
    return p.parse_args()


def load_csv(path: Path) -> Tuple[List[Dict[str, Any]], List[str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    return rows, fieldnames


def write_csv(path: Path, records: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            writer.writerow(r)


def main() -> int:
    args = parse_args()

    if not args.input.exists():
        print(f"Error: input file does not exist: {args.input}", file=sys.stderr)
        return 2

    records, fieldnames = load_csv(args.input)
    write_csv(args.output, records, fieldnames)

    print(f"Read {len(records)} records; wrote {len(records)} to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
