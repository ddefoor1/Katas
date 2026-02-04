#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


TEMP_FIELD_CANDIDATES = ("temperature", "temp", "tavg", "tmax", "tmin")


@dataclass
class Stats:
    read: int = 0
    written: int = 0
    skipped_missing_temp: int = 0
    skipped_bad_temp: int = 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter weather station records by temperature.")
    p.add_argument("-i", "--input", required=True, type=Path, help="Input file path")
    p.add_argument("-o", "--output", required=True, type=Path, help="Output file path")
    p.add_argument("--temp-min", required=True, type=float, help="Minimum temperature threshold")
    p.add_argument("--temp-field", default=None, help="Temperature column name (optional)")
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


def choose_temp_field(records: List[Dict[str, Any]], preferred: Optional[str]) -> Optional[str]:
    if preferred:
        return preferred
    if not records:
        return None

    for r in records[:25]:
        lower_map = {k.lower(): k for k in r.keys()}
        for cand in TEMP_FIELD_CANDIDATES:
            if cand in lower_map:
                return lower_map[cand]
    return None


def to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def filter_records(records: List[Dict[str, Any]], temp_field: str, temp_min: float) -> Tuple[List[Dict[str, Any]], Stats]:
    stats = Stats(read=len(records))
    kept: List[Dict[str, Any]] = []

    for r in records:
        if temp_field not in r:
            stats.skipped_missing_temp += 1
            continue

        t = to_float(r.get(temp_field))
        if t is None:
            stats.skipped_bad_temp += 1
            continue

        if t >= temp_min:
            kept.append(r)

    stats.written = len(kept)
    return kept, stats


def append_log(log_path: Path, input_path: Path, output_path: Path, temp_field: str, temp_min: float, stats: Stats) -> None:
    ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    line = (
        f"{ts} | input={input_path} | output={output_path} | "
        f"temp_field={temp_field} | temp_min={temp_min} | "
        f"read={stats.read} | written={stats.written} | "
        f"skip_missing_temp={stats.skipped_missing_temp} | skip_bad_temp={stats.skipped_bad_temp}\n"
    )

    if log_path.parent and not log_path.parent.exists():
        log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)


def main() -> int:
    args = parse_args()

    if not args.input.exists():
        print(f"Error: input file does not exist: {args.input}", file=sys.stderr)
        return 2

    records, fieldnames = load_csv(args.input)

    temp_field = choose_temp_field(records, args.temp_field)
    if not temp_field:
        print("Error: could not determine temperature field. Use --temp-field.", file=sys.stderr)
        return 2

    filtered, stats = filter_records(records, temp_field=temp_field, temp_min=args.temp_min)
    write_csv(args.output, filtered, fieldnames)

    try:
        append_log(args.log, args.input, args.output, temp_field, args.temp_min, stats)
    except Exception as e:
        print(f"Warning: failed to write log: {e}", file=sys.stderr)

    print(f"Read {stats.read} records; wrote {stats.written} to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
