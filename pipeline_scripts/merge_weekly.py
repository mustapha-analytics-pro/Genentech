"""Merge Apr 28 stitched fact files into weekly _stitched, restrict to Apr 22-28 UTC.

Strategy per fact table:
  1. Stream existing weekly *.csv:
       - keep header verbatim
       - keep rows where event_time_us in [APR22_UTC, APR29_UTC)
       - if table has Apr 28 batch already, drop rows where batch_id == 20260428
         (the new stitched is treated as authoritative — clean dedup by construction)
  2. Stream Apr 28 stitched *.csv.gz:
       - skip header
       - keep rows where event_time_us in [APR22_UTC, APR29_UTC)
       - append `,20260428\\n` so schema matches existing weekly's trailing batch_id

Disk-tight strategy:
  - Small tables (activity, clicks): write directly to <name>.csv.new on E:.
  - Large tables (impressions, rich_media): write gzip-compressed temp on E:
    (~25x smaller), delete original to free space, then decompress in place.

Verify: post-write row count and Event Time bounds reported per table.
"""

from __future__ import annotations

import datetime as dt
import gzip
import os
import shutil
import sys
import time
from pathlib import Path

WEEKLY_DIR = Path(r"E:/Projects/dcm/weekly_20260422_to_20260428/_stitched")
APR28_DIR  = Path(r"E:/Projects/dcm/stitched_20260428")

# Apr 22 00:00:00 UTC and Apr 29 00:00:00 UTC, microseconds since epoch
LO_US = 1776816000_000_000
HI_US = 1777420800_000_000

BATCH_ID = b"20260428"

# (table, weekly_csv, apr28_gz, drop_existing_apr28_batch, big)
JOBS = [
    ("activity",    WEEKLY_DIR / "dcm_activity_daily_l.csv",
                    APR28_DIR  / "dcm_activity_20260428.csv.gz",   False, False),
    ("clicks",      WEEKLY_DIR / "dcm_clicks_daily_l.csv",
                    APR28_DIR  / "dcm_click_20260428.csv.gz",       True,  False),
    ("impressions", WEEKLY_DIR / "dcm_impressions_daily_l.csv",
                    APR28_DIR  / "dcm_impression_20260428.csv.gz",  True,  True),
    ("rich_media",  WEEKLY_DIR / "dcm_rich_media_daily_l.csv",
                    APR28_DIR  / "dcm_rich_media_20260428.csv.gz",  True,  True),
]


def first_token(line: bytes) -> bytes:
    i = line.find(b",")
    return line if i < 0 else line[:i]


def last_token(line: bytes) -> bytes:
    s = line.rstrip(b"\r\n")
    i = s.rfind(b",")
    return s if i < 0 else s[i + 1 :]


def fmt_us(us: int) -> str:
    return dt.datetime.fromtimestamp(us / 1_000_000, dt.timezone.utc).isoformat()


def merge_one(table: str, weekly: Path, apr28: Path, drop_apr28_batch: bool, big: bool) -> None:
    print(f"\n=== {table} ===", flush=True)
    if not weekly.exists():
        print(f"  weekly missing: {weekly}")
        return
    if not apr28.exists():
        print(f"  apr28 missing: {apr28}")
        return

    t0 = time.time()
    if big:
        out_path_tmp = weekly.with_suffix(".csv.merged.tmp.gz")
        out_open = lambda: gzip.open(out_path_tmp, "wb", compresslevel=4)
    else:
        out_path_tmp = weekly.with_suffix(".csv.merged.tmp")
        out_open = lambda: open(out_path_tmp, "wb")

    rows_kept_existing = 0
    rows_dropped_date  = 0
    rows_dropped_batch = 0
    rows_kept_apr28    = 0
    rows_dropped_apr28_date = 0
    et_min = None
    et_max = None
    progress_every = 5_000_000 if big else 250_000

    with out_open() as out:
        # --- existing weekly ---
        with open(weekly, "rb") as fh:
            header = fh.readline()
            out.write(header)
            scanned = 0
            for line in fh:
                if not line or line == b"\n":
                    continue
                scanned += 1
                et_raw = first_token(line)
                try:
                    et = int(et_raw)
                except ValueError:
                    # malformed line, skip safely
                    rows_dropped_date += 1
                    continue
                if et < LO_US or et >= HI_US:
                    rows_dropped_date += 1
                    continue
                if drop_apr28_batch and last_token(line) == BATCH_ID:
                    rows_dropped_batch += 1
                    continue
                out.write(line)
                rows_kept_existing += 1
                if et_min is None or et < et_min: et_min = et
                if et_max is None or et > et_max: et_max = et
                if scanned % progress_every == 0:
                    elapsed = time.time() - t0
                    print(f"  [existing] scanned {scanned:,}  kept {rows_kept_existing:,}  "
                          f"date_drop {rows_dropped_date:,}  batch_drop {rows_dropped_batch:,}  "
                          f"({elapsed:.0f}s)", flush=True)

        # --- apr28 stitched ---
        with gzip.open(apr28, "rb") as fh:
            fh.readline()  # discard apr28 header
            scanned = 0
            for line in fh:
                if not line or line == b"\n":
                    continue
                scanned += 1
                et_raw = first_token(line)
                try:
                    et = int(et_raw)
                except ValueError:
                    rows_dropped_apr28_date += 1
                    continue
                if et < LO_US or et >= HI_US:
                    rows_dropped_apr28_date += 1
                    continue
                row = line.rstrip(b"\r\n") + b"," + BATCH_ID + b"\n"
                out.write(row)
                rows_kept_apr28 += 1
                if et_min is None or et < et_min: et_min = et
                if et_max is None or et > et_max: et_max = et
                if scanned % progress_every == 0:
                    elapsed = time.time() - t0
                    print(f"  [apr28]    scanned {scanned:,}  kept {rows_kept_apr28:,}  "
                          f"date_drop {rows_dropped_apr28_date:,}  ({elapsed:.0f}s)", flush=True)

    elapsed_write = time.time() - t0
    total_out = rows_kept_existing + rows_kept_apr28
    print(f"  WRITE DONE  total_rows={total_out:,}  "
          f"existing_kept={rows_kept_existing:,}  apr28_kept={rows_kept_apr28:,}  "
          f"date_drop_existing={rows_dropped_date:,}  batch_drop_existing={rows_dropped_batch:,}  "
          f"date_drop_apr28={rows_dropped_apr28_date:,}  in {elapsed_write:.0f}s", flush=True)
    if et_min is not None:
        print(f"  Event Time min: {et_min}  ({fmt_us(et_min)})")
        print(f"  Event Time max: {et_max}  ({fmt_us(et_max)})")

    # --- swap into place ---
    backup = weekly.with_suffix(".csv.preMerge.bak")
    if big:
        # rename original -> .preMerge.bak (instant), but we still need to free space first.
        # Order: delete original (frees ~58 GB) -> decompress tmp.gz to weekly path.
        # Risk mitigation: keep tmp.gz until decompression succeeds.
        size_orig = weekly.stat().st_size
        size_tmp_gz = out_path_tmp.stat().st_size
        free_before = shutil.disk_usage(weekly.parent).free
        print(f"  pre-swap:  original={size_orig/1e9:.2f} GB  tmp.gz={size_tmp_gz/1e9:.2f} GB  free={free_before/1e9:.2f} GB", flush=True)

        # Move original to backup name and immediately delete (no rename safety net needed; tmp.gz is the safety net)
        weekly.unlink()
        free_mid = shutil.disk_usage(weekly.parent).free
        print(f"  original deleted, free now {free_mid/1e9:.2f} GB; decompressing tmp.gz -> {weekly.name}", flush=True)
        with gzip.open(out_path_tmp, "rb") as src, open(weekly, "wb") as dst:
            shutil.copyfileobj(src, dst, length=64 * 1024 * 1024)
        out_path_tmp.unlink()
        print(f"  decompressed; final size = {weekly.stat().st_size/1e9:.2f} GB", flush=True)
    else:
        # small file: original is small, fine to keep around briefly
        if backup.exists():
            backup.unlink()
        weekly.rename(backup)
        out_path_tmp.rename(weekly)
        backup.unlink()  # success -> drop backup
        print(f"  swapped: {weekly.name} now {weekly.stat().st_size/1e6:.1f} MB", flush=True)


def main() -> int:
    print(f"UTC bounds: [{fmt_us(LO_US)}, {fmt_us(HI_US)})  ({LO_US}..{HI_US})")
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for table, weekly, apr28, drop_b, big in JOBS:
        if only and only != table:
            continue
        merge_one(table, weekly, apr28, drop_b, big)
    return 0


if __name__ == "__main__":
    sys.exit(main())
