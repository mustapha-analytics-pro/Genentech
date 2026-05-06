# Genentech DCM Weekly Pipeline

Weekly delivery of Google Campaign Manager 360 (DCM / CM360) Data Transfer files
to Genentech's SFTP server.

This repo holds the **reference scripts** and the **QA reports** for the manual
weekly delivery (PS-5344). The intent is for the dev team to fork these scripts
into a production pipeline that runs automatically every week.

---

## What the pipeline does

```
       GCS (Google delivers)                   Stitch + trim         Genentech SFTP
   ┌──────────────────────────┐    ┌──────────────────────────┐   ┌──────────────┐
   │ dcm_account848755_*.gz   │ -> │ <table>.csv  (UTC week)  │ ->│ SFTP dev   │
   │ hourly + daily files     │    │ + batch_id column        │   │              │
   └──────────────────────────┘    └──────────────────────────┘   └──────────────┘
                                              │
                                              v
                                       ┌─────────────────────┐
                                       │ QA report           │
                                       │ (vs Discovery API)  │
                                       └─────────────────────┘
```

**One run per week**, every Monday for the prior UTC week (Mon 00:00 → Sun 23:59).

---

## Repo layout

```
Genentech/
├── README.md                   ← this file
├── qa_report.html              ← latest manual QA report (preview-able)
└── pipeline_scripts/
    ├── README.md               ← script-level docs and DCM file conventions
    ├── stitch_weekly.py        ← step 2: stitch GCS files → CSV per table
    ├── merge_weekly.py         ← step 3: trim fact tables to UTC week
    ├── qa_weekly.py            ← step 4: QA vs Discovery API
    └── sftp_upload_weekly.py   ← step 5: upload to dev / prod SFTP
```

---

## Pipeline steps

| # | Step | Script | Notes |
|---|------|--------|-------|
| 1 | **Pull** raw `.csv.gz` files from GCS for the target UTC week | (dev to wire up the GCS reader, see `stitch_weekly.py` for filename conventions) | Filename pattern: `dcm_account848755_<table>_<YYYYMMDDHH>_<dl>_<file_id>.csv.gz` |
| 2 | **Stitch** hourly/daily files into one CSV per table, append `batch_id` column | `stitch_weekly.py` | One CSV per table, header from first file only |
| 3 | **Trim fact tables** to strict UTC week using `event_time_us` | `merge_weekly.py` | Disk-tight strategy for multi-GB tables |
| 4 | **QA** — row counts, schema check, cross-check fact totals vs Discovery API | `qa_weekly.py` | Aborts the pipeline if any check exceeds 1% tolerance |
| 5 | **Upload** to Genentech DEV SFTP first | `sftp_upload_weekly.py --env dev` | Idempotent; resumes partial uploads |
| 6 | (Manual gate) **Upload** to Genentech PROD SFTP after dev validation | `sftp_upload_weekly.py --env prod` | Remove gate after a few clean weeks |
| 7 | **Publish QA HTML** to `tekliner/ai-dashboards/clients/im_300072_116___Genentech/dashboards/` | (CI step) | Live URL: `report.improvado.io/ai-dashboards/api/file?path=...` |
| 8 | **Notify** PS team via Jira comment (REST API, `jsdPublic: false`) | (CI step) | Include QA verdict + report URL |

---

## 26 tables in scope

Reference (empty templates with locked schemas):
https://drive.google.com/drive/folders/1IXi7ZCv7rTffzfdDyCx8tMdcztMKf_s3

| Fact (4) | Dim (22) |
|----------|----------|
| `dcm_activity_daily_l` | `dcm_activity_categories_daily_l`, `dcm_activity_types_daily_l`, `dcm_ad_placement_assignments_daily_l`, `dcm_ads_daily_l`, `dcm_advertisers_daily_l`, `dcm_assets_daily_l`, `dcm_browsers_daily_l`, `dcm_campaigns_daily_l`, `dcm_cities_daily_l`, `dcm_creative_ad_assignments_daily_l`, `dcm_creatives_daily_l`, `dcm_custom_creative_fields_daily_l`, `dcm_custom_floodlight_variables_daily_l`, `dcm_custom_rich_media_daily_l`, `dcm_designated_market_areas_daily_l`, `dcm_keyword_value_daily_l`, `dcm_landing_page_url_daily_l`, `dcm_operating_systems_daily_l`, `dcm_placement_cost_daily_l`, `dcm_placements_daily_l`, `dcm_sites_daily_l`, `dcm_states_daily_l` |
| `dcm_clicks_daily_l` |  |
| `dcm_impressions_daily_l` (multi-GB) |  |
| `dcm_rich_media_daily_l` (multi-GB) |  |

---

## Column discipline (must match exactly)

The stitcher rejects any drift. Each table's columns must match the contract sheet exactly:

- **Names** — identical to the contract Google Sheet header
- **Order** — identical
- **Case** — identical (case-sensitive: `Campaign ID` ≠ `campaign_id`)
- **Final column** — `batch_id` (YYYYMMDD as integer)

Use `_schemas/<tab>.csv` (one-line dump of each sheet header) as the contract.
Run `compare_schemas.py` (TBD in dev pipeline) before shipping to flag drift.

---

## SFTP endpoints

| | Host | User |
|---|------|------|
| **Dev** | `sftp.cmgoasis.dev.gene.com` | `cmg_oasis_dev_improvado_user` |
| **Prod** | `sftp-cmgoasis.gene.com` | `cmg_oasis_prod_improvado_user` |

- SSH key: `~/.ssh/genentech_cmg_oasis_id_rsa` (manage in vault)
- Target dir: `dcm/weekly_<start>_<end>/`

---

## Discovery API (used for the QA cross-check)

- Improvado connector: `google_dcmbp` (id `13989`), profile `8578553`
- ClickHouse table: `im_300072_116.creative_advanced_300072_google_cm` (customer cluster)
- Metrics used in the QA: `impressions`, `clicks`, `total_conversions`, `total_conversions_revenue`, `mediaCost` (DCM Reports API)

The QA step queries this table for the same UTC week and compares totals
against the stitched files. Per Google's design, raw DT files and Reports API
counts diverge by ~0.05–0.5% (SIVT filter); the QA applies the standard
billable normalization before declaring a match.

---

## Live QA report (last manual run)


- **QA Report Weekly DCM (22- 28 April 2026) **: `https://report.improvado.io/ai-dashboards/api/file?path=clients%2Fim_300072_116___Genentech%2Fdashboards%2FGenentech_DCM_Weekly_QA_3way_Improvado_vs_SFTP_vs_DiscoveryAPI_20260422_to_20260428.html`

---

## Acceptance criteria for the automated pipeline

- [ ] End-to-end weekly run with no human intervention (after manual prod-gate is removed)
- [ ] Idempotent — safe to re-run any week without duplicating or corrupting data
- [ ] Failure pages PS Team
- [ ] QA report auto-published to `report.improvado.io/ai-dashboards/...`
- [ ] Runbook in repo README (re-run, partial-week, debug, gating, secret rotation)

---

## Reference

- Last manual delivery: PS-5344 (Apr 22 – Apr 28 2026)
- Existing Glue job (daily extraction → S3) — preserves `truncate_load`-based behavior:
  - 5 fact tables (`dcm_impressions_daily_l`, `dcm_clicks_daily_l`, `dcm_activity_daily_l`, `dcm_rich_media_daily_l`, `dcm_custom_rich_media_daily_l`) → all 7 days
  - All other 21 dim tables → latest day only (`truncate_load=true`)
