"""Stitch one week of DCM Data Transfer files (per table) into per-table CSVs.

For each of the 26 DCM tables, find all hourly/daily .csv.gz files in the GCS
download directory whose filename date stamp falls inside the requested UTC
week, concatenate them in chronological order, keep the header from the first
file only, and append a synthetic ``batch_id`` column (YYYYMMDD).

Usage:
    python stitch_weekly.py \
        --src   /path/to/dcm/raw_downloads \
        --out   /path/to/weekly_<start>_<end>/_stitched \
        --start 2026-04-22 --end 2026-04-28

Filename patterns expected in --src:
    dcm_account848755_<table>_<YYYYMMDDHH>_<dl_yyyymmdd>_<dl_hhmmss>_<file_id>.csv.gz   (hourly, fact)
    dcm_account848755_<table>_<start_yyyymmdd>_<end_yyyymmdd>_*.csv.gz                  (daily, dim)

When Google re-delivers (multiple files for the same (table, hour)), the most
recent <dl_yyyymmdd>_<dl_hhmmss> wins.
"""
from __future__ import annotations

import argparse
import gzip
import re
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

ACCT_PREFIX = "dcm_account848755"

# Tables in scope (26 = 4 fact + 22 dim).
FACT_TABLES = {
    "activity", "click", "impression", "rich_media", "custom_rich_media",
}
DIM_TABLES = {
    "activity_categories", "activity_types", "ad_placement_assignments",
    "ads", "advertisers", "assets", "browsers", "campaigns", "cities",
    "creative_ad_assignments", "creatives", "custom_creative_fields",
    "custom_floodlight_variables", "designated_market_areas",
    "keyword_value", "landing_page_url", "operating_systems",
    "placement_cost", "placements", "sites", "states",
}
ALL_TABLES = FACT_TABLES | DIM_TABLES

# Filenames in the GCS bucket use these short table tokens.
HOURLY_PATTERN = re.compile(
    rf"^{ACCT_PREFIX}_(?P<table>[a-z_]+?)_"
    r"(?P<dt>\d{10})_(?P<dl_date>\d{8})_(?P<dl_time>\d{6})_(?P<file_id>\d+)\.csv\.gz$"
)
DAILY_PATTERN = re.compile(
    rf"^{ACCT_PREFIX}_(?P<table>[a-z_]+?)_"
    r"(?P<start>\d{8})_(?P<end>\d{8})_(?P<rest>.+)\.csv\.gz$"
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, required=True,
                    help="Local directory holding the downloaded .csv.gz files from GCS")
    ap.add_argument("--out", type=Path, required=True,
                    help="Output dir; writes one stitched <table>.csv per table")
    ap.add_argument("--start", required=True,
                    help="UTC week start (inclusive), YYYY-MM-DD, e.g. 2026-04-22")
    ap.add_argument("--end", required=True,
                    help="UTC week end (inclusive), YYYY-MM-DD, e.g. 2026-04-28")
    return ap.parse_args()


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def select_files(src: Path, table: str, start: date, end: date) -> list[Path]:
    """Return the files for one table inside the UTC window, with re-delivery
    de-duped (most recent <dl_date>_<dl_time> wins per (table, hour))."""
    in_window = lambda yyyymmdd: start <= date(
        int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8])
    ) <= end

    chosen: dict[str, tuple[str, Path]] = {}  # key = hour bucket, value = (delivery key, path)
    for p in src.glob(f"{ACCT_PREFIX}_{table}_*.csv.gz"):
        m = HOURLY_PATTERN.match(p.name)
        if m:
            hour_bucket = m.group("dt")          # YYYYMMDDHH
            delivery_key = m.group("dl_date") + m.group("dl_time")
            if not in_window(hour_bucket[:8]):
                continue
            cur = chosen.get(hour_bucket)
            if cur is None or delivery_key > cur[0]:
                chosen[hour_bucket] = (delivery_key, p)
            continue
        m = DAILY_PATTERN.match(p.name)
        if m:
            file_start = m.group("start")
            if not in_window(file_start):
                continue
            chosen[file_start] = ("", p)  # daily files don't multi-deliver here
    return [chosen[k][1] for k in sorted(chosen)]


def hour_to_batch_id(filename: str) -> str:
    """Extract YYYYMMDD batch_id from the filename's hour or day stamp."""
    m = HOURLY_PATTERN.match(filename)
    if m:
        return m.group("dt")[:8]
    m = DAILY_PATTERN.match(filename)
    if m:
        return m.group("start")
    return ""


def stitch_table(table: str, files: list[Path], out_path: Path) -> int:
    """Stitch hourly/daily files into one CSV with header + batch_id column.
    Returns total data row count."""
    rows = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as out_fh:
        header_written = False
        for i, src in enumerate(files):
            batch_id = hour_to_batch_id(src.name)
            with gzip.open(src, "rb") as in_fh:
                header = in_fh.readline().rstrip(b"\r\n")
                if not header_written:
                    out_fh.write(header + b",batch_id\n")
                    header_written = True
                for line in in_fh:
                    line = line.rstrip(b"\r\n")
                    if not line:
                        continue
                    out_fh.write(line + b"," + batch_id.encode() + b"\n")
                    rows += 1
            print(f"  [{table}] [{i+1:>3}/{len(files)}] stitched {src.name}")
    return rows


def main() -> int:
    args = parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    print(f"Window : {start} .. {end} (UTC, inclusive)")
    print(f"Source : {args.src}")
    print(f"Output : {args.out}")

    for table in sorted(ALL_TABLES):
        print(f"\n=== {table} ===")
        files = select_files(args.src, table, start, end)
        if not files:
            print(f"  no files matched for {table}")
            continue
        out_path = args.out / f"dcm_{table}_daily_l.csv"
        rows = stitch_table(table, files, out_path)
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  -> {out_path.name}  files={len(files)}  rows={rows:,}  size={size_mb:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
