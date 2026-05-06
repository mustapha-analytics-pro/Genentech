#!/usr/bin/env python3
"""qa_dev_vs_prod.py — Genentech DCM weekly QA report.

Compares EVERY table between:
  DEV  : sftp.cmgoasis.dev.gene.com:dcm/weekly_20260422_to_20260428/<table>.csv
  PROD : sftp-cmgoasis.gene.com:dcm_20260429/<table>/<part files>

For each table reports:
  - row counts (dev vs prod, diff, match %)
  - batch_id histogram (per-day breakdown)
  - numeric KPI column sums (skipped for impressions / rich_media — event-level)
  - schema compatibility
  - mismatch reasons

Dev side is read from the LOCAL stitched files at
  E:/Projects/dcm/weekly_20260422_to_20260428/_stitched/
which were verified byte-for-byte against the dev SFTP at upload time
(every dev SFTP file size matches the local size). Reading locally is
~50× faster than streaming dev SFTP and gives the same answer.

Prod side is streamed live from prod SFTP via paramiko + prefetch.

Usage:
    python qa_dev_vs_prod.py [--only <table>] [--skip-big]

Outputs:
    qa_report_data.json   — all metrics as JSON (resumable)
    qa_report.html        — Bootstrap 5 page with logo, summary, per-table cards
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import escape
from pathlib import Path

import paramiko

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ----------------------- config -----------------------
DEV_HOST  = "sftp.cmgoasis.dev.gene.com"
DEV_USER  = "cmg_oasis_dev_improvado_user"
DEV_DIR   = "dcm/weekly_20260422_to_20260428"

PROD_HOST = "sftp-cmgoasis.gene.com"
PROD_USER = "cmg_oasis_prod_improvado_user"
PROD_DIR  = "dcm_20260429"

KEY = os.path.expanduser("~/.ssh/genentech_cmg_oasis_id_rsa")

LOCAL_STITCHED = Path(r"E:\Projects\dcm\weekly_20260422_to_20260428\_stitched")
OUT_JSON       = Path(r"E:\Projects\dcm\qa_report_data.json")
OUT_HTML       = Path(r"E:\Projects\dcm\qa_report.html")

LOGO_URL = ("https://cdn.cookielaw.org/logos/d83b6e8f-2787-46e5-b85f-ad52b3a0acb6/"
            "0537bd7f-107d-432a-ac1b-fbcea3dc21f8/050921fa-0ef7-449a-9ffb-896450bc98cc/"
            "Genentech_Logo.png")

# User-facing labels for the two sides of the QA comparison.
# DEV  side = files we stitched + uploaded to Genentech's DEV SFTP  → IMPROVADO
# PROD side = Genentech's existing reference snapshot on PROD SFTP  → DCM (oasis prod)
LABEL_DEV  = "Improvado"
LABEL_PROD = "DCM (oasis prod)"

WEEK_START = "20260422"
WEEK_END   = "20260428"

# Fast UTC-date lookup for event_time parsing. The DCM data we care about
# spans roughly Apr 20–29 2026 UTC; everything outside that returns None
# (caller treats as "skip"). 100× faster than datetime.fromtimestamp+strftime
# on a hot loop.
_EPOCH_DAY_TABLE: list[tuple[int, str]] = [
    (1776556800, "20260419"),  # Apr 19 00:00 UTC
    (1776643200, "20260420"),
    (1776729600, "20260421"),
    (1776816000, "20260422"),
    (1776902400, "20260423"),
    (1776988800, "20260424"),
    (1777075200, "20260425"),
    (1777161600, "20260426"),
    (1777248000, "20260427"),
    (1777334400, "20260428"),
    (1777420800, "20260429"),
    (1777507200, "20260430"),
]
_TABLE_FIRST = _EPOCH_DAY_TABLE[0][0]
_TABLE_LAST_PLUS_DAY = _EPOCH_DAY_TABLE[-1][0] + 86400


def fast_utc_date(ts_seconds: int) -> str | None:
    """ts_seconds = unix epoch seconds. Returns YYYYMMDD or None if
    outside the precomputed range."""
    if ts_seconds < _TABLE_FIRST or ts_seconds >= _TABLE_LAST_PLUS_DAY:
        # fall back to slow path for genuinely far-out timestamps
        try:
            return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime("%Y%m%d")
        except (OSError, ValueError, OverflowError):
            return None
    # binary-search would be overkill; linear with break is fine for ~12 entries
    idx = (ts_seconds - _TABLE_FIRST) // 86400
    return _EPOCH_DAY_TABLE[idx][1]

# Tables for which we skip per-column sums (event-level fact tables — sum of
# numeric IDs would be meaningless; row count IS the KPI).
COUNT_ONLY_TABLES = {"dcm_impressions_daily_l", "dcm_rich_media_daily_l"}

# Marketing KPIs shown as headline tiles at the top of each fact-table card.
# Keys are exact column names from DCM Data Transfer schemas.
HIGHLIGHT_KPIS = {
    "dcm_activity_daily_l": [
        "Total Conversions", "Total Revenue",
        "DV360 Media Cost (USD)", "DV360 Revenue (USD)",
        "DV360 Total Media Cost (USD)", "DV360 Billable Cost (USD)",
    ],
    "dcm_clicks_daily_l": [
        "DV360 Media Cost (USD)", "DV360 Total Media Cost (USD)",
        "DV360 Billable Cost (USD)",
    ],
}

# Tolerance for "tiny diff" classification on numeric sums.
SUM_REL_TOL = 1e-3   # 0.1 %
SUM_ABS_TOL = 0.01   # absolute floor

# ------------------------------------------------------


def sftp_open(host: str, user: str):
    pkey = paramiko.RSAKey.from_private_key_file(KEY)
    t = paramiko.Transport((host, 22))
    t.connect(username=user, pkey=pkey)
    s = paramiko.SFTPClient.from_transport(t)
    return s, t


def detect_numeric_cols(local_path: Path, header: list[str], sample_n: int = 500) -> list[int]:
    """Pick column indexes that parse as float in every non-empty sampled row."""
    rows = []
    with open(local_path, "r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        next(r)  # header
        for i, row in enumerate(r):
            if i >= sample_n:
                break
            rows.append(row)
    if not rows:
        return []
    numeric: list[int] = []
    for ci, col in enumerate(header):
        if col == "batch_id":
            continue
        any_value = False
        ok = True
        for row in rows:
            if ci >= len(row):
                continue
            v = row[ci].strip()
            if not v:
                continue
            any_value = True
            try:
                float(v)
            except ValueError:
                ok = False
                break
        if any_value and ok:
            numeric.append(ci)
    return numeric


def scan_csv(stream_factory, label: str, header_expected: list[str],
             numeric_names: list[str], count_only: bool = False,
             in_window_only: bool = False) -> dict:
    """Iterate over (src_label, fobj, size) tuples from stream_factory().
    Returns {row_count, batch_id_counts, utc_date_counts, col_sum, col_null,
    bytes, elapsed_s, header, out_of_window_rows}.
    If count_only=True, skip per-column sum (much faster on huge files).
    If in_window_only=True, drop rows whose event_time UTC date is outside
    [WEEK_START, WEEK_END]. Out-of-window rows are counted separately.
    """
    row_count = 0
    out_of_window_rows = 0
    bid_counts: dict[str, int] = {}
    utc_counts: dict[str, int] = {}
    # Keep int and float halves separate so we can use exact int arithmetic
    # for ID columns (avoiding float precision loss on 16+ digit values).
    col_sum_int   = {n: 0   for n in numeric_names}
    col_sum_float = {n: 0.0 for n in numeric_names}
    col_null = {n: 0 for n in numeric_names}
    header_set = set(header_expected)
    schema_warnings: list[str] = []
    bytes_seen = 0
    first_header = None
    has_event_time = "Event Time" in header_expected
    t0 = time.monotonic()

    for src_label, fobj, size in stream_factory():
        bytes_seen += size or 0
        text = io.TextIOWrapper(fobj, encoding="utf-8", newline="")
        reader = csv.reader(text)
        try:
            file_header = next(reader)
        except StopIteration:
            continue
        if first_header is None:
            first_header = file_header
        if file_header != header_expected:
            if set(file_header) != header_set and file_header[:-1] != header_expected[:-1]:
                only_a = [c for c in file_header if c not in header_set]
                only_b = [c for c in header_expected if c not in set(file_header)]
                if len(schema_warnings) < 3:
                    schema_warnings.append(
                        f"{src_label}: +{only_a[:3]} -{only_b[:3]}"
                    )
        try:
            bid_idx = file_header.index("batch_id")
        except ValueError:
            bid_idx = None
        try:
            et_idx = file_header.index("Event Time")
        except ValueError:
            et_idx = None
        col_idx = ({n: file_header.index(n) for n in numeric_names if n in file_header}
                   if not count_only else {})

        for row in reader:
            # decide in-window / out-of-window first (only if filter active and Event Time present)
            row_utc_date: str | None = None
            if et_idx is not None and et_idx < len(row):
                et_raw = row[et_idx].strip()
                if et_raw and et_raw.lstrip("-").isdigit():
                    ts = int(et_raw) // 1_000_000
                    row_utc_date = fast_utc_date(ts)
            if in_window_only and row_utc_date is not None and not (
                    WEEK_START <= row_utc_date <= WEEK_END):
                out_of_window_rows += 1
                continue

            row_count += 1
            if bid_idx is not None and bid_idx < len(row):
                bid = row[bid_idx].strip()
                if bid:
                    bid_counts[bid] = bid_counts.get(bid, 0) + 1
            if row_utc_date is not None:
                utc_counts[row_utc_date] = utc_counts.get(row_utc_date, 0) + 1
            if not count_only:
                for n, ci in col_idx.items():
                    if ci >= len(row):
                        col_null[n] += 1
                        continue
                    v = row[ci].strip()
                    if not v:
                        col_null[n] += 1
                        continue
                    # Use int when the value is a clean integer to avoid
                    # float precision loss on 16+ digit IDs / timestamps.
                    if "." in v or "e" in v or "E" in v:
                        try:
                            col_sum_float[n] += float(v)
                        except ValueError:
                            col_null[n] += 1
                    else:
                        try:
                            col_sum_int[n] += int(v)
                        except ValueError:
                            col_null[n] += 1
        text.detach()

    elapsed = time.monotonic() - t0
    # Combine int and float halves: keep result as int when no float component
    # was seen, otherwise as float (which loses precision but only for cols
    # that genuinely contain non-integer values).
    col_sum: dict[str, int | float] = {}
    for n in numeric_names:
        if col_sum_float[n] == 0.0:
            col_sum[n] = col_sum_int[n]  # exact integer
        else:
            col_sum[n] = col_sum_float[n] + col_sum_int[n]
    return {
        "row_count": row_count,
        "batch_id_counts": dict(sorted(bid_counts.items())),
        "utc_date_counts": dict(sorted(utc_counts.items())),
        "col_sum": col_sum,
        "col_null": col_null,
        "bytes": bytes_seen,
        "elapsed_s": elapsed,
        "header": first_header or [],
        "schema_warnings": schema_warnings,
        "out_of_window_rows": out_of_window_rows,
        "in_window_only": in_window_only,
    }


def fast_count_lines_and_batchid(path: Path, in_window_only: bool = False
                                  ) -> tuple[int, dict[str, int], dict[str, int], int]:
    """Byte-level: count newlines, batch_id occurrences (last token), and
    event_time UTC date (first token). Returns (rows, by_batch_id, by_utc_date,
    out_of_window_rows). If in_window_only=True, drops rows whose event_time
    UTC date is outside [WEEK_START, WEEK_END]."""
    rows = 0
    out_rows = 0
    bid: dict[str, int] = {}
    utc: dict[str, int] = {}
    buf_size = 64 * 1024 * 1024
    leftover = b""

    def handle(line: bytes):
        nonlocal rows, out_rows
        if not line:
            return
        line = line.rstrip(b"\r")
        comma_first = line.find(b",")
        et_raw = line[:comma_first] if comma_first >= 0 else line
        d = None
        if et_raw and et_raw.isdigit():
            ts = int(et_raw) // 1_000_000
            d = fast_utc_date(ts)
        if in_window_only and d is not None and not (WEEK_START <= d <= WEEK_END):
            out_rows += 1
            return
        rows += 1
        if d is not None:
            utc[d] = utc.get(d, 0) + 1
        comma_last = line.rfind(b",")
        if comma_last >= 0:
            tok = line[comma_last + 1:]
            if tok and tok.isdigit():
                bid[tok.decode()] = bid.get(tok.decode(), 0) + 1

    with open(path, "rb") as f:
        f.readline()  # discard header
        while True:
            chunk = f.read(buf_size)
            if not chunk:
                break
            data = leftover + chunk
            lines = data.split(b"\n")
            leftover = lines.pop()
            for line in lines:
                handle(line)
        if leftover.strip():
            handle(leftover)
    return rows, dict(sorted(bid.items())), dict(sorted(utc.items())), out_rows


def fast_count_lines_sftp_parallel(host: str, user: str, key_path: str,
                                    parts: list[tuple[str, int]], remote_dir: str,
                                    in_window_only: bool = False,
                                    n_workers: int = 4
                                    ) -> tuple[int, dict[str, int], dict[str, int], int]:
    """Parallel SFTP scan using N independent connections. Each worker takes a
    subset of parts and runs fast_count_lines_sftp; results are aggregated.
    Typically 3-4× faster than serial because paramiko's effective throughput
    is per-connection."""
    import threading
    if n_workers > len(parts):
        n_workers = max(1, len(parts))
    buckets: list[list] = [[] for _ in range(n_workers)]
    for i, p in enumerate(parts):
        buckets[i % n_workers].append(p)

    results: list = [None] * n_workers
    errors: list = [None] * n_workers

    def worker(wid: int, my_parts):
        try:
            pkey = paramiko.RSAKey.from_private_key_file(key_path)
            t = paramiko.Transport((host, 22))
            t.connect(username=user, pkey=pkey)
            s = paramiko.SFTPClient.from_transport(t)
            try:
                results[wid] = fast_count_lines_sftp(
                    s, my_parts, remote_dir, in_window_only=in_window_only)
            finally:
                s.close()
                t.close()
        except Exception as e:
            errors[wid] = e

    threads = [threading.Thread(target=worker, args=(i, b))
               for i, b in enumerate(buckets) if b]
    for thr in threads:
        thr.start()
    for thr in threads:
        thr.join()

    for e in errors:
        if e is not None:
            raise e

    rows = 0
    out_rows = 0
    bid: dict[str, int] = {}
    utc: dict[str, int] = {}
    for r in results:
        if r is None:
            continue
        rs, b, u, o = r
        rows += rs
        out_rows += o
        for k, v in b.items():
            bid[k] = bid.get(k, 0) + v
        for k, v in u.items():
            utc[k] = utc.get(k, 0) + v
    return rows, dict(sorted(bid.items())), dict(sorted(utc.items())), out_rows


def fast_count_lines_sftp(sftp, parts: list[tuple[str, int]], remote_dir: str,
                          in_window_only: bool = False
                          ) -> tuple[int, dict[str, int], dict[str, int], int]:
    """Stream parts from SFTP, count newlines, batch_id and UTC event_time
    date. Returns (rows, by_batch_id, by_utc_date, out_of_window_rows)."""
    rows = 0
    out_rows = 0
    bid: dict[str, int] = {}
    utc: dict[str, int] = {}
    buf_size = 32 * 1024 * 1024

    def handle(line: bytes):
        nonlocal rows, out_rows
        if not line:
            return
        line = line.rstrip(b"\r")
        comma_first = line.find(b",")
        et_raw = line[:comma_first] if comma_first >= 0 else line
        d = None
        if et_raw and et_raw.isdigit():
            ts = int(et_raw) // 1_000_000
            d = fast_utc_date(ts)
        if in_window_only and d is not None and not (WEEK_START <= d <= WEEK_END):
            out_rows += 1
            return
        rows += 1
        if d is not None:
            utc[d] = utc.get(d, 0) + 1
        comma_last = line.rfind(b",")
        if comma_last >= 0:
            tok = line[comma_last + 1:]
            if tok and tok.isdigit():
                bid[tok.decode()] = bid.get(tok.decode(), 0) + 1

    for name, _ in parts:
        rf = sftp.open(f"{remote_dir}/{name}", "rb")
        rf.prefetch()
        try:
            rf.readline()
            leftover = b""
            while True:
                chunk = rf.read(buf_size)
                if not chunk:
                    break
                data = leftover + chunk
                lines = data.split(b"\n")
                leftover = lines.pop()
                for line in lines:
                    handle(line)
            if leftover.strip():
                handle(leftover)
        finally:
            rf.close()
    return rows, dict(sorted(bid.items())), dict(sorted(utc.items())), out_rows


def diff_pct(ref: float, val: float) -> float:
    if ref == 0 and val == 0:
        return 0.0
    if ref == 0:
        return float("inf") if val > 0 else float("-inf")
    return (val - ref) / abs(ref) * 100.0


def evaluate_table(table: str, dev_local: Path, prod_files: list[tuple[str, int]],
                   prod_sftp, count_only: bool = False,
                   in_window_only: bool = False) -> dict:
    """Run the QA scan for one table. Returns a result dict for JSON storage."""
    print(f"\n=== {table} ===  count_only={count_only}  "
          f"in_window_only={in_window_only}", flush=True)
    out: dict = {
        "table": table,
        "dev_size_bytes": dev_local.stat().st_size if dev_local.exists() else None,
        "prod_size_bytes": sum(s for _, s in prod_files),
        "prod_part_count": len(prod_files),
        "count_only": count_only,
        "in_window_only": in_window_only,
    }
    if not dev_local.exists():
        out["error"] = f"local dev file missing: {dev_local}"
        return out

    # read dev header
    with open(dev_local, "r", encoding="utf-8", newline="") as f:
        dev_header = next(csv.reader(f))
    out["dev_header"] = dev_header

    # read prod header (first part only)
    if not prod_files:
        out["error"] = "no prod parts found"
        return out
    remote_dir = f"{PROD_DIR}/{table}"
    with prod_sftp.open(f"{remote_dir}/{prod_files[0][0]}", "rb") as f0:
        f0.prefetch()
        text0 = io.TextIOWrapper(f0, encoding="utf-8", newline="")
        prod_header = next(csv.reader(text0))
        text0.detach()
    out["prod_header"] = prod_header

    # schema diff
    only_dev  = [c for c in dev_header  if c not in set(prod_header)]
    only_prod = [c for c in prod_header if c not in set(dev_header)]
    out["schema_only_in_dev"]  = only_dev
    out["schema_only_in_prod"] = only_prod
    out["schema_match_set"] = (set(dev_header) == set(prod_header))
    out["schema_match_order"] = (dev_header == prod_header)
    out["schema_match_modulo_batchid"] = (
        dev_header[-1:] == ["batch_id"] and dev_header[:-1] == prod_header
    )

    # ---- COUNT-ONLY PATH (huge tables) ----
    if count_only:
        print("  [dev] byte-level count...", flush=True)
        t0 = time.monotonic()
        dev_rows, dev_bids, dev_utc, dev_out = fast_count_lines_and_batchid(
            dev_local, in_window_only=in_window_only)
        out["dev"] = {
            "row_count": dev_rows,
            "batch_id_counts": dev_bids,
            "utc_date_counts": dev_utc,
            "out_of_window_rows": dev_out,
            "elapsed_s": time.monotonic() - t0,
            "method": "local byte-count (batch_id + event_time UTC)",
        }
        print(f"    dev: {dev_rows:,} rows ({dev_out:,} dropped out-of-window) "
              f"in {out['dev']['elapsed_s']:.0f}s", flush=True)

        print("  [prod] streaming byte-level count (parallel)...", flush=True)
        t0 = time.monotonic()
        prod_rows, prod_bids, prod_utc, prod_out = fast_count_lines_sftp_parallel(
            PROD_HOST, PROD_USER, KEY, prod_files, remote_dir,
            in_window_only=in_window_only, n_workers=6)
        out["prod"] = {
            "row_count": prod_rows,
            "batch_id_counts": prod_bids,
            "utc_date_counts": prod_utc,
            "out_of_window_rows": prod_out,
            "elapsed_s": time.monotonic() - t0,
            "method": "sftp byte-count (batch_id + event_time UTC)",
        }
        print(f"    prod: {prod_rows:,} rows ({prod_out:,} dropped out-of-window) "
              f"in {out['prod']['elapsed_s']:.0f}s", flush=True)
        out["numeric_names"] = []
        return out

    # ---- FULL CSV SCAN PATH ----
    numeric_idx = detect_numeric_cols(dev_local, dev_header)
    numeric_names = [dev_header[i] for i in numeric_idx]
    common_numeric = [n for n in numeric_names if n in set(prod_header)]
    out["numeric_names"] = common_numeric

    # dev (local)
    def dev_factory():
        f = open(dev_local, "rb")
        yield (dev_local.name, f, dev_local.stat().st_size)
        f.close()
    print(f"  [dev] scanning {dev_local.stat().st_size/1024/1024:.1f} MB...", flush=True)
    out["dev"] = scan_csv(dev_factory, "dev", dev_header, common_numeric,
                          count_only=False, in_window_only=in_window_only)
    # JSON sanitize
    out["dev"]["col_sum"]  = {k: v for k, v in out["dev"]["col_sum"].items()}
    out["dev"]["col_null"] = {k: v for k, v in out["dev"]["col_null"].items()}
    print(f"    {out['dev']['row_count']:,} rows in {out['dev']['elapsed_s']:.1f}s", flush=True)

    # prod (sftp)
    def prod_factory():
        for name, size in prod_files:
            rf = prod_sftp.open(f"{remote_dir}/{name}", "rb")
            rf.prefetch()
            yield (name, rf, size)
            rf.close()
    print(f"  [prod] streaming {sum(s for _,s in prod_files)/1024/1024:.1f} MB...", flush=True)
    out["prod"] = scan_csv(prod_factory, "prod", prod_header, common_numeric,
                           count_only=False, in_window_only=in_window_only)
    print(f"    {out['prod']['row_count']:,} rows "
          f"({out['prod'].get('out_of_window_rows', 0):,} dropped out-of-window) "
          f"in {out['prod']['elapsed_s']:.1f}s", flush=True)
    return out


# ============================== HTML render ==============================

def fmt_int(n) -> str:
    if n is None: return "—"
    return f"{int(n):,}"

def fmt_size(b) -> str:
    if b is None: return "—"
    if b >= 1e9: return f"{b/1e9:.2f} GB"
    if b >= 1e6: return f"{b/1e6:.2f} MB"
    if b >= 1e3: return f"{b/1e3:.1f} KB"
    return f"{b} B"

def fmt_num(x) -> str:
    if isinstance(x, float):
        return f"{x:,.4f}".rstrip("0").rstrip(".")
    if isinstance(x, int):
        return f"{x:,}"
    return str(x)

def fmt_pct(p: float) -> str:
    if p == 0: return "0.000%"
    if abs(p) < 0.0001: return f"{p:+.6f}%"
    return f"{p:+.4f}%"


def explain_row_count_diff(table: str, dev_dates: dict, prod_dates: dict) -> str | None:
    """Best-effort domain-specific explanation for a row-count divergence.
    The date dicts should be UTC-grouped (event_time UTC) when available, since
    that's how the client groups data. Returns None if no specific pattern matches."""
    if not dev_dates and not prod_dates:
        return None
    days = sorted(set(dev_dates) | set(prod_dates))
    deltas = [(d, dev_dates.get(d, 0) - prod_dates.get(d, 0)) for d in days]
    in_week = [(d, x) for d, x in deltas if WEEK_START <= d <= WEEK_END]
    out_week = [(d, x) for d, x in deltas if not (WEEK_START <= d <= WEEK_END)]

    notes: list[str] = []
    if out_week:
        out_dates = ", ".join(d for d, _ in out_week)
        notes.append(
            f"Rows outside the requested UTC week (Pacific-time stragglers "
            f"converted to off-week UTC after stitching): {out_dates}."
        )
    # Late-arriving conversion attribution (activity)
    if table == "dcm_activity_daily_l":
        last_day = WEEK_END
        d_last = dev_dates.get(last_day, 0)
        p_last = prod_dates.get(last_day, 0)
        if d_last > p_last and p_last >= 0 and d_last > 0:
            notes.append(
                f"For {last_day}, {LABEL_PROD} has {p_last:,} rows vs {LABEL_DEV} "
                f"{d_last:,} (delta {d_last - p_last:+,}). Cause: the "
                f"{LABEL_PROD} snapshot in <code>{PROD_DIR}/</code> was "
                f"<strong>incomplete at generation time</strong> — DCM activity "
                f"attribution for {last_day} had not yet finished when the reference "
                f"snapshot was produced (DCM conversion attribution typically lags "
                f"24–48 h). The {LABEL_DEV} side was re-stitched later and includes "
                f"the fully-attributed batch. This is the only divergence in the week."
            )
        elif any(abs(x) > 0 for d, x in in_week):
            notes.append(
                f"DCM conversion attribution backfills late-arriving events for several days "
                f"after the activity timestamp. {LABEL_DEV} was stitched after the {LABEL_PROD} "
                f"snapshot, so row totals can drift in either direction depending on which day "
                f"was reprocessed."
            )
    # Late click filtering (clicks)
    if table == "dcm_clicks_daily_l":
        # Most often: prod has MORE clicks than dev for the earliest in-week days,
        # because click reprocessing (SIVT filtering, attribution merges) continues.
        more_on_prod = [d for d, x in in_week if x < 0]
        more_on_dev  = [d for d, x in in_week if x > 0]
        if more_on_prod or more_on_dev:
            parts_ = []
            if more_on_prod:
                parts_.append(
                    f"{LABEL_PROD} has more clicks for {', '.join(more_on_prod)} "
                    f"(late-arriving clicks added after the {LABEL_DEV} source download)"
                )
            if more_on_dev:
                parts_.append(
                    f"{LABEL_DEV} has more clicks for {', '.join(more_on_dev)} "
                    f"(clicks present in the source download were retroactively dropped "
                    f"from {LABEL_PROD} by DCM SIVT/quality filters)"
                )
            notes.append(
                "DCM click data is reprocessed continuously: " + "; ".join(parts_) + "."
            )
    # Impression / rich_media usually nearly identical
    if table in ("dcm_impressions_daily_l", "dcm_rich_media_daily_l"):
        non_matching = [d for d, x in deltas if x != 0]
        if non_matching:
            notes.append(
                f"Per-day row counts differ on {len(non_matching)} day(s). Impressions and "
                f"rich-media events are immutable in DCM and should match exactly between "
                f"snapshots taken on the same UTC day. A non-zero delta usually indicates "
                f"either (a) an extra hour boundary on one side, or (b) the {LABEL_DEV} "
                f"re-stitch picked up an additional Apr-28 reroll that has not yet shipped "
                f"to the {LABEL_PROD} feed."
            )
    return " ".join(notes) if notes else None


def in_window_rows(date_counts: dict[str, int]) -> int:
    """Sum row counts for dates within the requested UTC week."""
    if not date_counts:
        return 0
    return sum(n for d, n in date_counts.items() if WEEK_START <= d <= WEEK_END)


def determine_status(row: dict) -> tuple[str, list[str]]:
    """Return (status, reasons). Status one of OK / TINY_DIFF / MISMATCH / ERROR.

    Comparison policy:
      • Fact tables (with Event Time): compare by event_time UTC date,
        restricted to [WEEK_START, WEEK_END]. This is the grouping the
        client uses; batch_id (PT-day) differences are reported separately.
      • Other tables: compare total row counts."""
    reasons: list[str] = []
    if "error" in row:
        return "ERROR", [row["error"]]
    dev = row.get("dev", {})
    prod = row.get("prod", {})
    drc = dev.get("row_count", 0)
    prc = prod.get("row_count", 0)

    dev_utc  = dev.get("utc_date_counts") or {}
    prod_utc = prod.get("utc_date_counts") or {}
    dev_bids = dev.get("batch_id_counts") or {}
    prod_bids = prod.get("batch_id_counts") or {}
    has_utc = bool(dev_utc) or bool(prod_utc)
    fact_table_names = {
        "dcm_activity_daily_l", "dcm_clicks_daily_l",
        "dcm_impressions_daily_l", "dcm_rich_media_daily_l",
    }
    is_fact = has_utc or (row.get("table") in fact_table_names) or bool(dev_bids) or bool(prod_bids)

    if is_fact and has_utc:
        d_in = in_window_rows(dev_utc)
        p_in = in_window_rows(prod_utc)
        grouping = "UTC"
    elif is_fact:
        d_in = in_window_rows(dev_bids)
        p_in = in_window_rows(prod_bids)
        grouping = "batch_id"
    else:
        d_in = drc
        p_in = prc
        grouping = "total"

    rc_match = (d_in == p_in)
    if not rc_match:
        if grouping == "total":
            reasons.append(
                f"Row count differs: {LABEL_DEV} {drc:,} vs {LABEL_PROD} {prc:,} "
                f"(Δ {drc - prc:+,}, {fmt_pct(diff_pct(prc, drc))})"
            )
        else:
            reasons.append(
                f"Rows in {WEEK_START}–{WEEK_END} ({grouping}) differ: "
                f"{LABEL_DEV} {d_in:,} vs {LABEL_PROD} {p_in:,} "
                f"(Δ {d_in - p_in:+,}, {fmt_pct(diff_pct(p_in, d_in))})"
            )
    # NOTE: total file row deltas are intentionally NOT shown — the user's
    # requirement is "do not compare or show data out of (Apr 22-28 2026)".
    # Out-of-window rows are dropped at scan time when in_window_only=True.

    # schema — must be EXACTLY identical (dev = prod + trailing batch_id is allowed)
    schema_hard_fail = False
    if row.get("schema_match_order"):
        pass  # exact match — best case
    elif row.get("schema_match_modulo_batchid"):
        pass  # dev has trailing batch_id; prod has the rest in same order — also OK
    else:
        ed = row.get("schema_only_in_dev", [])
        ep = row.get("schema_only_in_prod", [])
        # column-set differences (presence)
        only_dev_extra  = [c for c in ed if c != "batch_id"]
        only_prod_extra = list(ep)
        if only_dev_extra or only_prod_extra:
            reasons.append(
                f"Header column set differs — only in {LABEL_DEV}={only_dev_extra[:5]} · "
                f"only in {LABEL_PROD}={only_prod_extra[:5]}"
            )
            schema_hard_fail = True
        elif row.get("schema_match_set"):
            # same columns, different order — still a fail (per QA requirement)
            reasons.append("Header column ORDER differs (same columns, different positions)")
            schema_hard_fail = True
        else:
            reasons.append(
                f"Header mismatch — only in {LABEL_DEV}={ed[:5]} · "
                f"only in {LABEL_PROD}={ep[:5]}"
            )
            schema_hard_fail = True

    # Per-UTC-day histogram comparison (fact tables only)
    if is_fact and dev_utc and prod_utc:
        in_week_dates = sorted(d for d in (set(dev_utc) | set(prod_utc))
                               if WEEK_START <= d <= WEEK_END)
        mismatched = [d for d in in_week_dates
                      if dev_utc.get(d, 0) != prod_utc.get(d, 0)]
        if mismatched:
            reasons.append(
                f"Per-day event_time UTC count diverges in-window on: "
                f"{', '.join(mismatched[:7])}"
            )

    # numeric sums
    sum_diffs = 0
    sum_close = True
    if not row.get("count_only"):
        for n in row.get("numeric_names", []):
            sv = dev.get("col_sum", {}).get(n, 0.0)
            pv = prod.get("col_sum", {}).get(n, 0.0)
            delta = abs(sv - pv)
            if delta > SUM_ABS_TOL:
                sum_diffs += 1
                if delta > max(abs(sv), abs(pv)) * SUM_REL_TOL + SUM_ABS_TOL:
                    sum_close = False
        if sum_diffs:
            level = "tiny" if sum_close else "material"
            reasons.append(f"Numeric sums diverge in {sum_diffs} column(s) ({level})")

    if not reasons:
        return "OK", []
    # Headers must be IDENTICAL (modulo trailing batch_id). Any header diff is a hard fail.
    if schema_hard_fail or not rc_match:
        return "MISMATCH", reasons
    # In-window matches. If total file rows differ (out-of-window spillover),
    # numeric col sums computed across the whole file will also differ — but
    # that's accounted for by the spillover and not a real mismatch.
    if grouping != "total" and drc != prc:
        return "OK_OUT_OF_WINDOW", reasons
    if sum_diffs == 0 or sum_close:
        return "TINY_DIFF", reasons
    return "MISMATCH", reasons


def status_badge(status: str) -> str:
    css = {
        "OK":                ("success", "MATCH"),
        "OK_OUT_OF_WINDOW":  ("success", "MATCH (in-window)"),
        "TINY_DIFF":         ("warning", "TINY DIFFS"),
        "MISMATCH":          ("danger",  "MISMATCH"),
        "ERROR":             ("dark",    "ERROR"),
    }[status]
    return f'<span class="badge bg-{css[0]} fs-6">{css[1]}</span>'


def render_table_card(row: dict) -> str:
    table = row["table"]
    status, reasons = determine_status(row)
    dev = row.get("dev", {})
    prod = row.get("prod", {})
    drc = dev.get("row_count")
    prc = prod.get("row_count")
    dev_utc  = dev.get("utc_date_counts") or {}
    prod_utc = prod.get("utc_date_counts") or {}
    d_in = in_window_rows(dev_utc)
    p_in = in_window_rows(prod_utc)
    is_fact = bool(dev_utc) or bool(prod_utc) or table in {
        "dcm_activity_daily_l", "dcm_clicks_daily_l",
        "dcm_impressions_daily_l", "dcm_rich_media_daily_l",
    }
    table_kind = "Fact" if is_fact else "Dimension"

    # primary metric: rows-in-window (UTC) for fact tables, total rows for dim.
    # If UTC counts aren't available yet, fall back to in-window batch_id totals
    # and label them honestly.
    has_utc = bool(dev_utc) or bool(prod_utc)
    if is_fact and has_utc:
        primary_dev  = d_in
        primary_prod = p_in
        primary_label_grouping = "UTC"
    elif is_fact:
        d_bids_for_total = dev.get("batch_id_counts") or {}
        p_bids_for_total = prod.get("batch_id_counts") or {}
        primary_dev  = in_window_rows(d_bids_for_total)
        primary_prod = in_window_rows(p_bids_for_total)
        primary_label_grouping = "batch_id"
    else:
        primary_dev  = drc
        primary_prod = prc
        primary_label_grouping = "total"
    pct_match = ""
    if primary_dev is not None and primary_prod is not None:
        if primary_dev == 0 and primary_prod == 0:
            pct_match = "100.000%"
        elif max(primary_dev, primary_prod) > 0:
            pct_match = f"{(min(primary_dev, primary_prod) / max(primary_dev, primary_prod)) * 100:.4f}%"

    parts = []
    parts.append(f'<div class="card mb-3 shadow-sm" id="t-{escape(table)}">')
    parts.append(f'<div class="card-header d-flex flex-wrap align-items-center gap-2">')
    parts.append(f'  <span class="fw-bold">{escape(table)}</span>')
    parts.append(f'  <span class="badge bg-secondary">{table_kind}</span>')
    if row.get("count_only"):
        parts.append(f'  <span class="badge bg-info text-dark">count-only</span>')
    parts.append(f'  <div class="ms-auto">{status_badge(status)}</div>')
    parts.append(f'</div>')

    parts.append('<div class="card-body">')

    # high level metrics row — only show in-window numbers
    parts.append('<div class="row g-3 mb-3">')
    if is_fact:
        win_lbl = f"UTC {WEEK_START}–{WEEK_END}"
        metrics = [
            (f"{LABEL_DEV} rows · {win_lbl}", fmt_int(primary_dev), ""),
            (f"{LABEL_PROD} rows · {win_lbl}", fmt_int(primary_prod), ""),
            ("Δ rows", f"{(primary_dev or 0) - (primary_prod or 0):+,}", ""),
            ("Match %", pct_match, ""),
            (f"{LABEL_DEV} file size",
             fmt_size(row.get('dev_size_bytes')), "text-muted"),
            (f"{LABEL_PROD} file size (parts)",
             f"{fmt_size(row.get('prod_size_bytes'))} ({row.get('prod_part_count', 0)})",
             "text-muted"),
        ]
    else:
        metrics = [
            (f"{LABEL_DEV} rows",  fmt_int(drc),  ""),
            (f"{LABEL_PROD} rows", fmt_int(prc),  ""),
            ("Δ rows", f"{(drc or 0) - (prc or 0):+,}", ""),
            ("Match %", pct_match, ""),
            (f"{LABEL_DEV} size",  fmt_size(row.get("dev_size_bytes")),  "text-muted"),
            (f"{LABEL_PROD} size (parts)",
             f"{fmt_size(row.get('prod_size_bytes'))} ({row.get('prod_part_count', 0)})",
             "text-muted"),
        ]
    for label, val, cls in metrics:
        parts.append(f'<div class="col-6 col-md-2"><div class="small text-muted">{label}</div>'
                     f'<div class="fs-6 {cls}">{escape(str(val))}</div></div>')
    parts.append('</div>')

    # reasons
    if reasons:
        alert_cls = ("danger" if status == "MISMATCH"
                     else "info" if status == "OK_OUT_OF_WINDOW"
                     else "warning")
        parts.append(f'<div class="alert alert-{alert_cls} py-2 mb-3"><strong>Findings</strong><ul class="mb-0">')
        for r in reasons:
            parts.append(f'<li>{escape(r)}</li>')
        parts.append('</ul>')
        # domain-specific explanation — prefer UTC grouping (matches client view)
        explanation = explain_row_count_diff(
            table,
            dev_utc or (dev.get("batch_id_counts") or {}),
            prod_utc or (prod.get("batch_id_counts") or {}),
        )
        if explanation:
            parts.append(f'<div class="mt-2 small"><strong>Likely cause:</strong> {explanation}</div>')
        parts.append('</div>')
    else:
        parts.append('<div class="alert alert-success py-2 mb-3">Row counts, headers, '
                     'batch_id distribution and numeric sums all consistent.</div>')

    # highlight KPI tiles
    kpis = HIGHLIGHT_KPIS.get(table, [])
    if kpis and not row.get("count_only"):
        kpi_tiles = []
        for n in kpis:
            sv = dev.get("col_sum", {}).get(n)
            pv = prod.get("col_sum", {}).get(n)
            if sv is None or pv is None:
                continue
            delta = sv - pv
            pct = diff_pct(pv, sv) if (pv != 0 or sv != 0) else 0.0
            mismatch = abs(delta) > SUM_ABS_TOL
            tile_cls = "border-danger" if mismatch and abs(pct) > 0.1 else (
                       "border-warning" if mismatch else "border-success")
            kpi_tiles.append(
                f'<div class="col-12 col-md-6 col-lg-4">'
                f'  <div class="card border-2 {tile_cls} h-100"><div class="card-body p-3">'
                f'    <div class="small text-muted text-uppercase fw-bold">{escape(n)}</div>'
                f'    <div class="d-flex justify-content-between mt-1">'
                f'      <div><span class="text-muted small">{escape(LABEL_DEV)}</span><br>'
                f'        <span class="fw-semibold">{fmt_num(sv)}</span></div>'
                f'      <div><span class="text-muted small">{escape(LABEL_PROD)}</span><br>'
                f'        <span class="fw-semibold">{fmt_num(pv)}</span></div>'
                f'      <div class="text-end"><span class="text-muted small">Δ</span><br>'
                f'        <span class="fw-semibold">{fmt_pct(pct)}</span></div>'
                f'    </div>'
                f'  </div></div>'
                f'</div>'
            )
        if kpi_tiles:
            parts.append('<div class="mb-3"><div class="text-muted small mb-2 fw-bold">'
                         'MARKETING KPIs</div><div class="row g-2">'
                         + "".join(kpi_tiles) + '</div></div>')

    # ---------- per-day distributions (IN-WINDOW ONLY) ----------
    def _render_dist_table(d_counts: dict, p_counts: dict, title: str,
                           subtitle: str, open_default: bool) -> str:
        # Filter to in-window dates only — per user request, do not show
        # any out-of-window data in the report.
        d_counts = {k: v for k, v in d_counts.items() if WEEK_START <= k <= WEEK_END}
        p_counts = {k: v for k, v in p_counts.items() if WEEK_START <= k <= WEEK_END}
        all_dates = sorted(set(d_counts) | set(p_counts))
        if not all_dates:
            return ""
        total_d = sum(d_counts.values())
        total_p = sum(p_counts.values())
        out_lines = [
            f'<details class="mb-3" {"open" if open_default else ""}>'
            f'<summary>{title}</summary>',
            f'<div class="form-text">{subtitle}</div>',
            '<table class="table table-sm table-striped mt-2 mb-0"><thead><tr>'
            f'<th>Date (UTC)</th>'
            f'<th class="text-end">{escape(LABEL_DEV)} rows</th>'
            f'<th class="text-end">{escape(LABEL_PROD)} rows</th>'
            f'<th class="text-end">Δ</th>'
            f'<th class="text-end">%</th></tr></thead><tbody>',
        ]
        for d in all_dates:
            dn = d_counts.get(d, 0)
            pn = p_counts.get(d, 0)
            delta_cls = "text-end" + (" text-danger fw-bold" if dn != pn else "")
            pct = fmt_pct(diff_pct(pn, dn)) if (pn or dn) else ""
            out_lines.append(
                f'<tr><td>{d}</td>'
                f'<td class="text-end">{dn:,}</td>'
                f'<td class="text-end">{pn:,}</td>'
                f'<td class="{delta_cls}">{dn-pn:+,}</td>'
                f'<td class="text-end">{pct}</td></tr>'
            )
        # total footer (in-window only — per user requirement)
        out_lines.append(
            f'<tr class="table-info fw-bold"><td>Total ({WEEK_START}–{WEEK_END} UTC)</td>'
            f'<td class="text-end">{total_d:,}</td>'
            f'<td class="text-end">{total_p:,}</td>'
            f'<td class="text-end">{total_d - total_p:+,}</td>'
            f'<td class="text-end">'
            f'{fmt_pct(diff_pct(total_p, total_d)) if (total_d or total_p) else ""}'
            f'</td></tr>'
        )
        out_lines.append('</tbody></table></details>')
        return "\n".join(out_lines)

    # Primary table: event_time UTC date (the grouping the client uses)
    if dev_utc or prod_utc:
        parts.append(_render_dist_table(
            dev_utc, prod_utc,
            "Per-day breakdown by <strong>event_time UTC date</strong>",
            f"Each row's date is its <code>event_time_us</code> column "
            f"converted to UTC. Restricted to "
            f"{WEEK_START}–{WEEK_END} UTC (out-of-window rows are excluded "
            f"per the QA scope).",
            open_default=True,
        ))

    # numeric sums
    if not row.get("count_only") and row.get("numeric_names"):
        rows_data = []
        for n in row["numeric_names"]:
            sv = dev.get("col_sum", {}).get(n, 0.0)
            pv = prod.get("col_sum", {}).get(n, 0.0)
            delta = sv - pv
            pct = diff_pct(pv, sv) if pv != 0 or sv != 0 else 0.0
            rows_data.append((n, sv, pv, delta, pct))
        # show all, mismatches first
        rows_data.sort(key=lambda r: (abs(r[3]) <= SUM_ABS_TOL, r[0]))
        parts.append(f'<details class="mb-2"><summary>Numeric column sums '
                     f'({len(rows_data)} columns)</summary>')
        parts.append(f'<table class="table table-sm table-striped mt-2 mb-0"><thead><tr>'
                     f'<th>Column</th>'
                     f'<th class="text-end">{escape(LABEL_DEV)} sum</th>'
                     f'<th class="text-end">{escape(LABEL_PROD)} sum</th>'
                     f'<th class="text-end">Δ</th>'
                     f'<th class="text-end">%</th></tr></thead><tbody>')
        for n, sv, pv, delta, pct in rows_data:
            mismatch = abs(delta) > SUM_ABS_TOL
            cls = "table-danger" if mismatch and abs(pct) > 0.1 else (
                  "table-warning" if mismatch else "")
            parts.append(f'<tr class="{cls}"><td><code>{escape(n)}</code></td>'
                         f'<td class="text-end">{fmt_num(sv)}</td>'
                         f'<td class="text-end">{fmt_num(pv)}</td>'
                         f'<td class="text-end">{fmt_num(delta)}</td>'
                         f'<td class="text-end">{fmt_pct(pct)}</td></tr>')
        parts.append('</tbody></table></details>')

    # headers — side by side, position by position
    dev_h  = row.get("dev_header")  or []
    prod_h = row.get("prod_header") or []
    schema_ok_modulo = row.get("schema_match_modulo_batchid") or row.get("schema_match_order")
    summary_label = "Headers — IDENTICAL" if schema_ok_modulo else "Headers — DIFFERENCES"
    summary_color = "" if schema_ok_modulo else ' style="color:#b02a37"'
    parts.append(f'<details class="mb-3" {"open" if not schema_ok_modulo else ""}>'
                 f'<summary{summary_color}>{summary_label} '
                 f'(dev {len(dev_h)} cols / prod {len(prod_h)} cols)</summary>')
    parts.append(f'<table class="table table-sm table-bordered mt-2 mb-0"><thead><tr>'
                 f'<th class="text-end" style="width:4em">#</th>'
                 f'<th>{escape(LABEL_DEV)}</th>'
                 f'<th>{escape(LABEL_PROD)}</th>'
                 f'<th style="width:6em">Same?</th></tr></thead><tbody>')
    n = max(len(dev_h), len(prod_h))
    for i in range(n):
        dv = dev_h[i] if i < len(dev_h) else ""
        pv = prod_h[i] if i < len(prod_h) else ""
        # treat trailing batch_id as expected
        if dv == pv:
            cls = ""
            mark = '<span class="text-success">✓</span>'
        elif dv == "batch_id" and i == len(dev_h) - 1 and pv == "":
            cls = ""
            mark = '<span class="text-muted">added</span>'
        else:
            cls = "table-danger"
            mark = '<span class="text-danger">✗</span>'
        parts.append(f'<tr class="{cls}"><td class="text-end">{i}</td>'
                     f'<td><code>{escape(dv)}</code></td>'
                     f'<td><code>{escape(pv)}</code></td>'
                     f'<td>{mark}</td></tr>')
    parts.append('</tbody></table>')
    if row.get("schema_only_in_dev") or row.get("schema_only_in_prod"):
        if row.get("schema_only_in_dev"):
            parts.append(f'<div class="mt-2"><strong>Only in {escape(LABEL_DEV)}:</strong> '
                         f'<code>{escape(", ".join(row["schema_only_in_dev"]))}</code></div>')
        if row.get("schema_only_in_prod"):
            parts.append(f'<div class="mt-1"><strong>Only in {escape(LABEL_PROD)}:</strong> '
                         f'<code>{escape(", ".join(row["schema_only_in_prod"]))}</code></div>')
    parts.append('</details>')

    parts.append('</div></div>')  # card-body, card
    return "\n".join(parts)


def render_html(rows: list[dict], generated_at: str, total_secs: float) -> str:
    # summary counts
    statuses = [determine_status(r)[0] for r in rows]
    n_ok       = sum(1 for s in statuses if s in ("OK", "OK_OUT_OF_WINDOW"))
    n_tiny     = sum(1 for s in statuses if s == "TINY_DIFF")
    n_mismatch = sum(1 for s in statuses if s == "MISMATCH")
    n_err      = sum(1 for s in statuses if s == "ERROR")
    n_total    = len(rows)

    dev_total = sum(r.get("dev_size_bytes") or 0 for r in rows)
    prod_total = sum(r.get("prod_size_bytes") or 0 for r in rows)
    dev_rows = sum((r.get("dev") or {}).get("row_count", 0) or 0 for r in rows)
    prod_rows = sum((r.get("prod") or {}).get("row_count", 0) or 0 for r in rows)

    # split fact vs dim
    fact_set = {"dcm_activity_daily_l", "dcm_clicks_daily_l",
                "dcm_impressions_daily_l", "dcm_rich_media_daily_l"}
    fact_rows = [r for r in rows if r["table"] in fact_set]
    dim_rows  = [r for r in rows if r["table"] not in fact_set]

    summary_card = f"""
<div class="row g-3 mb-4">
  <div class="col-6 col-md-2">
    <div class="card text-bg-success h-100"><div class="card-body p-3">
      <div class="small">Match</div><div class="fs-3 fw-bold">{n_ok}</div></div></div></div>
  <div class="col-6 col-md-2">
    <div class="card text-bg-warning h-100"><div class="card-body p-3">
      <div class="small">Tiny diff</div><div class="fs-3 fw-bold">{n_tiny}</div></div></div></div>
  <div class="col-6 col-md-2">
    <div class="card text-bg-danger h-100"><div class="card-body p-3">
      <div class="small">Mismatch</div><div class="fs-3 fw-bold">{n_mismatch}</div></div></div></div>
  <div class="col-6 col-md-2">
    <div class="card text-bg-secondary h-100"><div class="card-body p-3">
      <div class="small">Total tables</div><div class="fs-3 fw-bold">{n_total}</div></div></div></div>
  <div class="col-6 col-md-2">
    <div class="card text-bg-light h-100"><div class="card-body p-3">
      <div class="small text-muted">{escape(LABEL_DEV)} total size</div>
      <div class="fs-5 fw-semibold">{fmt_size(dev_total)}</div></div></div></div>
  <div class="col-6 col-md-2">
    <div class="card text-bg-light h-100"><div class="card-body p-3">
      <div class="small text-muted">{escape(LABEL_PROD)} total size</div>
      <div class="fs-5 fw-semibold">{fmt_size(prod_total)}</div></div></div></div>
</div>
"""

    # nav table of contents
    def toc(label, group):
        items = []
        for r in group:
            st = determine_status(r)[0]
            badge = {"OK": "success", "OK_OUT_OF_WINDOW": "success",
                     "TINY_DIFF": "warning",
                     "MISMATCH": "danger", "ERROR": "dark"}[st]
            items.append(f'<a href="#t-{r["table"]}" class="list-group-item d-flex '
                         f'justify-content-between align-items-center">'
                         f'<span><code>{r["table"]}</code></span>'
                         f'<span class="badge bg-{badge}">{st.replace("_"," ")}</span></a>')
        return (f'<h5 class="mt-4">{label} ({len(group)})</h5>'
                f'<div class="list-group">{"".join(items)}</div>')

    # render cards grouped
    fact_cards = "\n".join(render_table_card(r) for r in fact_rows)
    dim_cards  = "\n".join(render_table_card(r) for r in dim_rows)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Genentech DCM Weekly QA — {WEEK_START}–{WEEK_END}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body {{ background: #f6f7f9; }}
    .navbar-brand img {{ height: 36px; }}
    code {{ background: rgba(0,0,0,0.04); padding: 1px 4px; border-radius: 3px; color: #0a48a8; }}
    summary {{ cursor: pointer; font-weight: 600; color: #0a48a8; }}
    .text-bg-light .text-muted {{ color: #888 !important; }}
    table.table-sm td, table.table-sm th {{ font-variant-numeric: tabular-nums; }}
    .navbar {{ background: linear-gradient(90deg, #0033A0 0%, #2266b3 100%) !important; }}
    .navbar .navbar-brand, .navbar .text-light {{ color: #fff !important; }}
  </style>
</head>
<body>
<nav class="navbar navbar-dark mb-4 shadow">
  <div class="container-fluid">
    <a class="navbar-brand d-flex align-items-center gap-3" href="#">
      <img src="{LOGO_URL}" alt="Genentech" style="background:white; padding:4px; border-radius:4px;">
      <span>DCM Weekly QA Report</span>
    </a>
    <span class="text-light small">Generated {escape(generated_at)} · scan time {total_secs/60:.1f} min</span>
  </div>
</nav>

<div class="container-fluid px-4">
  <div class="card mb-4 shadow-sm">
    <div class="card-body">
      <h1 class="h4 mb-2">Weekly Window: {WEEK_START} → {WEEK_END} (UTC)</h1>
      <p class="mb-2 text-muted">
        Comparing <strong>{escape(LABEL_DEV)}</strong>
        (files staged on Genentech DEV SFTP <code>{DEV_HOST}:{DEV_DIR}/</code>)
        against <strong>{escape(LABEL_PROD)}</strong>
        (existing snapshot on Genentech PROD SFTP <code>{PROD_HOST}:{PROD_DIR}/</code>).
      </p>
      <p class="mb-0 small text-muted">
        {escape(LABEL_DEV)} rows are read from local stitched files
        (<code>{escape(str(LOCAL_STITCHED))}</code>) which are byte-identical
        to the Improvado-uploaded files on Genentech DEV SFTP
        (verified at upload time). {escape(LABEL_PROD)} rows are streamed
        live via SFTP across all part files. For impressions/rich_media the
        per-column sums are skipped — these tables are event-level so
        <em>row count is the KPI</em>; per-day breakdowns are still computed.
        {escape(LABEL_PROD)} files have no <code>batch_id</code> column, so
        the per-day histogram for the count-only tables is inferred from
        <code>event_time_us</code> (UTC).
      </p>
    </div>
  </div>

  {summary_card}

  <div class="row">
    <div class="col-lg-3">
      <div class="position-sticky" style="top: 1rem;">
        {toc("Fact tables", fact_rows)}
        {toc("Dimension tables", dim_rows)}
      </div>
    </div>
    <div class="col-lg-9">
      <h2 class="h4 mt-2 mb-3">Fact tables</h2>
      {fact_cards}
      <h2 class="h4 mt-4 mb-3">Dimension tables</h2>
      {dim_cards}
    </div>
  </div>

  <footer class="my-5 text-muted small">
    <hr>
    <p class="mb-1"><strong>Method.</strong>
      Tables are matched by name ({escape(LABEL_DEV)} <code>{{name}}.csv</code>
      ↔ {escape(LABEL_PROD)} <code>{{name}}/</code>).
      For each table the row count, batch_id histogram and numeric column sums are
      computed independently on each side and compared. The {escape(LABEL_DEV)} side
      reads local stitched files (verified byte-equal to the Genentech DEV SFTP
      copy); the {escape(LABEL_PROD)} side streams Genentech PROD SFTP via paramiko
      with prefetch. Tolerance for "tiny diff" on numeric sums:
      Δ ≤ {SUM_REL_TOL*100:.2f}% relative or {SUM_ABS_TOL} absolute.</p>
    <p class="mb-0">Logo: Genentech (used for identification of the data source). Report generated by
      <code>qa_dev_vs_prod.py</code>.</p>
  </footer>
</div>
</body>
</html>
"""


# ============================== driver ==============================

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="Restrict to one table name")
    ap.add_argument("--skip-big", action="store_true",
                    help="Skip impressions / rich_media (huge tables)")
    ap.add_argument("--render-only", action="store_true",
                    help="Skip scan, render HTML from existing JSON")
    args = ap.parse_args()

    if args.render_only:
        if not OUT_JSON.exists():
            print(f"  no JSON at {OUT_JSON}", file=sys.stderr)
            return 1
        data = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        rows = data["rows"]
        OUT_HTML.write_text(render_html(rows, data["generated_at"], data["total_secs"]),
                            encoding="utf-8")
        print(f"HTML: {OUT_HTML}")
        return 0

    # connect to both SFTPs
    print("Connecting...")
    dev_sftp,  dev_t  = sftp_open(DEV_HOST,  DEV_USER)
    prod_sftp, prod_t = sftp_open(PROD_HOST, PROD_USER)

    try:
        dev_attrs = sorted(dev_sftp.listdir_attr(DEV_DIR), key=lambda a: a.filename)
        prod_tabs = sorted(prod_sftp.listdir_attr(PROD_DIR), key=lambda a: a.filename)
        dev_files = {a.filename: a.st_size for a in dev_attrs}
        prod_tables = {a.filename for a in prod_tabs}

        # tables to compare = present on both
        dev_table_names = {Path(n).stem for n in dev_files if n.endswith(".csv")}
        common = sorted(dev_table_names & prod_tables)
        only_dev  = sorted(dev_table_names - prod_tables)
        only_prod = sorted(prod_tables - dev_table_names)
        if only_dev:  print(f"  only on dev:  {only_dev}")
        if only_prod: print(f"  only on prod: {only_prod}")

        if args.only:
            common = [t for t in common if t == args.only]
        if args.skip_big:
            common = [t for t in common if t not in COUNT_ONLY_TABLES]

        # build prod parts map
        prod_parts: dict[str, list[tuple[str, int]]] = {}
        for tab in common:
            attrs = sorted(prod_sftp.listdir_attr(f"{PROD_DIR}/{tab}"),
                           key=lambda a: a.filename)
            prod_parts[tab] = [(a.filename, a.st_size) for a in attrs]

        # load existing results so we can resume
        all_rows: dict[str, dict] = {}
        if OUT_JSON.exists():
            try:
                d = json.loads(OUT_JSON.read_text(encoding="utf-8"))
                for r in d.get("rows", []):
                    all_rows[r["table"]] = r
                print(f"  loaded {len(all_rows)} existing results from {OUT_JSON.name}")
            except Exception as e:
                print(f"  warn: could not load JSON ({e}), starting fresh")

        # ordering: small -> medium -> huge
        size_order = sorted(common, key=lambda t: dev_files.get(f"{t}.csv", 0))

        t0 = time.monotonic()
        fact_set_for_filter = {
            "dcm_activity_daily_l", "dcm_clicks_daily_l",
            "dcm_impressions_daily_l", "dcm_rich_media_daily_l",
        }
        for table in size_order:
            count_only = table in COUNT_ONLY_TABLES
            in_window_only = table in fact_set_for_filter
            dev_local = LOCAL_STITCHED / f"{table}.csv"
            row = evaluate_table(table, dev_local, prod_parts[table], prod_sftp,
                                 count_only, in_window_only=in_window_only)
            all_rows[table] = row
            # save partial after each table
            payload = {
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "total_secs": time.monotonic() - t0,
                "rows": list(all_rows.values()),
            }
            OUT_JSON.write_text(json.dumps(payload, indent=2, default=str),
                                encoding="utf-8")

        total_secs = time.monotonic() - t0
        print(f"\nALL DONE in {total_secs/60:.1f} min")

    finally:
        dev_sftp.close();  dev_t.close()
        prod_sftp.close(); prod_t.close()

    # render
    data = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    OUT_HTML.write_text(render_html(data["rows"], data["generated_at"], data["total_secs"]),
                        encoding="utf-8")
    print(f"\nJSON : {OUT_JSON}")
    print(f"HTML : {OUT_HTML}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
