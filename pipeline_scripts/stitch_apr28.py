"""Stitch DCM fact-table CSV.gz files for 28-Apr-2026 UTC into per-table outputs.

Inputs : E:/Projects/dcm/22 and 28/dcm_account848755_<table>_<datepart>_*.csv.gz
Outputs: E:/Projects/dcm/stitched_20260428/dcm_<table>_20260428.csv.gz

Fact tables: activity (daily), click / impression / rich_media (hourly 00..23).
Header from the first file is written once; subsequent files have their first
row dropped. Hourly files are sorted by the hour token in the filename.
"""

from __future__ import annotations

import gzip
import re
import shutil
import sys
from pathlib import Path

SRC_DIR = Path(r"E:/Projects/dcm/22 and 28")
OUT_DIR = Path(r"E:/Projects/dcm/stitched_20260428")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ACCT = "dcm_account848755"
DATE = "20260428"
NEXT = "20260429"

# (logical name, glob, sort-key regex, expected count)
JOBS: list[tuple[str, str, str | None, int]] = [
    ("activity",   f"{ACCT}_activity_{DATE}_{NEXT}_*.csv.gz",   None,                  1),
    ("click",      f"{ACCT}_click_{DATE}??_*.csv.gz",            r"_click_(\d{10})_",   24),
    ("impression", f"{ACCT}_impression_{DATE}??_*.csv.gz",       r"_impression_(\d{10})_", 24),
    ("rich_media", f"{ACCT}_rich_media_{DATE}??_*.csv.gz",       r"_rich_media_(\d{10})_", 24),
]


def stitch(table: str, pattern: str, key_re: str | None, expected: int) -> None:
    files = sorted(SRC_DIR.glob(pattern))
    if key_re:
        rx = re.compile(key_re)
        files.sort(key=lambda p: rx.search(p.name).group(1) if rx.search(p.name) else p.name)

    if not files:
        print(f"[{table}] no files matched {pattern}", file=sys.stderr)
        return
    if len(files) != expected:
        print(f"[{table}] WARNING: matched {len(files)} files, expected {expected}", file=sys.stderr)

    out_path = OUT_DIR / f"dcm_{table}_{DATE}.csv.gz"
    rows_total = 0
    header_written = False

    with gzip.open(out_path, "wb") as out_fh:
        for i, src in enumerate(files):
            with gzip.open(src, "rb") as in_fh:
                header = in_fh.readline()  # always consume first line
                if not header_written:
                    out_fh.write(header)
                    header_written = True
                shutil.copyfileobj(in_fh, out_fh, length=8 * 1024 * 1024)
            print(f"  [{table}] [{i+1:>2}/{len(files)}] stitched {src.name}")

    # post-count rows (decompressed) for sanity
    with gzip.open(out_path, "rb") as fh:
        for _ in fh:
            rows_total += 1
    data_rows = rows_total - 1
    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"[{table}] -> {out_path.name}  files={len(files)}  data_rows={data_rows:,}  size={size_mb:.1f} MB")


def main() -> int:
    print(f"Source: {SRC_DIR}")
    print(f"Output: {OUT_DIR}\n")
    for table, pattern, key_re, expected in JOBS:
        print(f"=== {table} ===")
        stitch(table, pattern, key_re, expected)
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
