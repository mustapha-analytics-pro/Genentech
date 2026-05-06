# pipeline_scripts/

Reference Python scripts that power the manual weekly Genentech DCM (CM360)
delivery. Fork these into the production pipeline.

See the top-level [`README.md`](../README.md) for the end-to-end flow.

| File | Purpose |
|------|---------|
| `stitch_weekly.py` | Stitches DCM Data Transfer hourly/daily `.csv.gz` files into one CSV per table. Filters by filename pattern. Appends synthetic `batch_id` column. Re-delivery aware (latest delivery per `(table, hour)` wins). |
| `merge_weekly.py` | Trims a stitched fact-table CSV to a strict UTC window `[Mon 00:00, next Mon 00:00)` based on `event_time_us`. Disk-tight strategy for huge files. |
| `qa_weekly.py` | Validates stitched files vs Improvado's Discovery API (`im_300072_116.creative_advanced_300072_google_cm`). Renders Bootstrap HTML QA report. Returns non-zero on failure → pipeline aborts the prod-SFTP upload. |
| `sftp_upload_weekly.py` | Mirrors a local stitched directory to Genentech SFTP. `--env dev` or `--env prod`. Idempotent: skips files whose remote size already matches local. |

## DCM Data Transfer file naming conventions

```
dcm_account848755_<table>_<YYYYMMDDHH>_<dl_yyyymmdd>_<dl_hhmmss>_<file_id>.csv.gz   ← hourly (fact tables)
dcm_account848755_<table>_<start_yyyymmdd>_<end_yyyymmdd>_*.csv.gz                  ← daily (dim tables)
```

When the same `(table, hour)` is present multiple times, **pick the most recent
delivery** (`<dl_yyyymmdd>_<dl_hhmmss>` lexicographically max). Google
re-delivers when late events arrive.
