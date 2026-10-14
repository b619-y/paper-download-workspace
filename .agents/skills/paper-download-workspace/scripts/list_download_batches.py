#!/usr/bin/env python3
"""List paper-download batches, optionally filtered by status."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
PRIORITY_ORDER = {"urgent": 0, "high": 1, "normal": 2, "low": 3}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--status", default="READY")
    args = parser.parse_args()
    path = Path(args.root).expanduser().resolve() / "00_control/BATCH_INDEX.csv"
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if args.status != "ALL":
        rows = [row for row in rows if row.get("status") == args.status]
    rows.sort(key=lambda row: (PRIORITY_ORDER.get(row.get("priority", "normal"), 9), row.get("created_at", "")))
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
