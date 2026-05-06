# Genentech DCM Pipeline — Reference Scripts

Reference Python scripts used for the manual weekly Genentech DCM (CM360) data delivery to Genentech's prod SFTP. Used as the basis for the automated pipeline.

## Scripts

| File | Purpose |
|------|---------|
| `stitch_apr28.py` | Stitches DCM Data Transfer hourly `.csv.gz` files into one CSV per table. Filters by filename pattern. Appends synthetic `batch_id` column. |
| `merge_weekly.py` | Trims a stitched weekly file to a strict UTC window `[Mon 00:00 UTC, next Mon 00:00 UTC)` based on `event_time_us`. Disk-tight strategy for huge files (gzip temp → swap). |
| `sftp_upload_weekly.py` | Mirrors a local stitched directory to Genentech SFTP. Supports `--env dev|prod`. Idempotent: skips files whose remote size already matches local. |
| `qa_dev_vs_prod.py` | QA report builder. Compares local stitched (Improvado side) vs prod SFTP reference. Bootstrap HTML output with per-day UTC breakdown, batch_id histogram, numeric column sums, header check. |
| `build_3way_full.py` | 3-way QA report: Improvado / DCM (oasis prod) / Discovery API. Adds Discovery API column from `im_300072_116.creative_advanced_300072_google_cm`. |

## 26 tables in scope

```
dcm_activity_categories_daily_l       (dim)
dcm_activity_daily_l                  (fact)
dcm_activity_types_daily_l            (dim)
dcm_ad_placement_assignments_daily_l  (dim)
dcm_ads_daily_l                       (dim)
dcm_advertisers_daily_l               (dim)
dcm_assets_daily_l                    (dim)
dcm_browsers_daily_l                  (dim)
dcm_campaigns_daily_l                 (dim)
dcm_cities_daily_l                    (dim)
dcm_clicks_daily_l                    (fact)
dcm_creative_ad_assignments_daily_l   (dim)
dcm_creatives_daily_l                 (dim)
dcm_custom_creative_fields_daily_l    (dim)
dcm_custom_floodlight_variables_daily_l (dim)
dcm_custom_rich_media_daily_l         (dim)
dcm_designated_market_areas_daily_l   (dim)
dcm_impressions_daily_l               (fact)
dcm_keyword_value_daily_l             (dim)
dcm_landing_page_url_daily_l          (dim)
dcm_operating_systems_daily_l         (dim)
dcm_placement_cost_daily_l            (dim)
dcm_placements_daily_l                (dim)
dcm_rich_media_daily_l                (fact)
dcm_sites_daily_l                     (dim)
dcm_states_daily_l                    (dim)
```

## DCM Data Transfer file naming pattern

```
dcm_account848755_<table>_<YYYYMMDDHH>_<dl_yyyymmdd>_<dl_hhmmss>_<file_id>.csv.gz   ← hourly (fact)
dcm_account848755_<table>_<start_yyyymmdd>_<end_yyyymmdd>_*.csv.gz                  ← daily (dim)
```

When picking files for a UTC week, pick the **most recent** delivery (`<dl_yyyymmdd>_<dl_hhmmss>` lexicographically max) per `(table, hour)` because Google re-delivers late events.

## SFTP endpoints

- **Dev**: `sftp.cmgoasis.dev.gene.com` user `cmg_oasis_dev_improvado_user`
- **Prod**: `sftp-cmgoasis.gene.com` user `cmg_oasis_prod_improvado_user`
- SSH key: `~/.ssh/genentech_cmg_oasis_id_rsa` (manage in vault)

## Discovery API (cross-check source)

- Improvado connector: `google_dcmbp` (id 13989), profile `8578553`
- ClickHouse table for QA cross-check: `im_300072_116.creative_advanced_300072_google_cm`
