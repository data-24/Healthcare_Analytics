# 🏥 Healthcare Analytics Platform

An end-to-end data engineering pipeline that turns raw healthcare files into
business-ready analytics — fully automated, validated, monitored, and
queryable through dashboards and AI chat.

---

## 📌 What this project does (at a glance)

Three healthcare datasets — **patient admissions, treatment records, and
insurance claims** — land as CSV files in cloud storage. The pipeline then:

1. **Validates** every file before loading (12 quality checks — bad files are
   quarantined and an alert email is sent).
2. **Loads** good data into a layered "medallion" warehouse (Bronze → Silver → Gold).
3. **Transforms** it into a clean **star schema** ready for analytics.
4. **Tests** the data automatically (40+ data-quality tests).
5. **Serves** it to **Power BI dashboards** and a **Cortex AI** chat assistant.
6. **Runs on a schedule** and **emails alerts** if anything breaks.

The result: drop a file in cloud storage, and a few minutes later it is
validated, loaded, transformed, tested, and available for analysis — with full
logging and alerting, no manual steps.

---

## 🎯 Who it's for / why it exists

- **Analysts & business users** — clean KPIs (admissions, length of stay,
  readmission rates, claim approvals, treatment costs) via dashboards or plain-English AI questions.
- **Data team** — a production-grade, monitored, testable pipeline with full audit trails.
- **Compliance** — every file, every quality check, and every failure is logged;
  historical doctor attributes are preserved point-in-time (SCD2).

---

## 🧱 Architecture

```
                          ┌─────────────────────────────────────┐
                          │      Airflow  (orchestration)        │
                          │  gatekeeper_dag  +  healthcare_pipe  │
                          └───────────────┬─────────────────────┘
                                          │ runs on schedule
                                          ▼
   ┌──────────┐     ┌──────────────┐     ┌─────────┐     ┌──────────┐     ┌──────────┐
   │  AWS S3  │ ──▶ │  Gatekeeper  │ ──▶ │   RAW   │ ──▶ │ STAGING  │ ──▶ │  MARTS   │
   │ incoming │     │  (Snowpark)  │     │ Bronze  │     │  Silver  │     │   Gold   │
   │ processed│     │  12 checks   │     │ (landed)│     │ (cleaned)│     │  (star)  │
   │quarantine│     │ validate→load│     └─────────┘     └──────────┘     └────┬─────┘
   └──────────┘     └──────┬───────┘                                           │
                          │ bad file                                           ▼
                          ▼ → quarantine + email alert            ┌────────────────────────┐
                  ┌───────────────┐                               │  Power BI  +  Cortex AI │
                  │  AUDIT (logs) │                               │  dashboards  +  chat    │
                  └───────────────┘                               └────────────────────────┘
```

**The medallion layers**

| Layer | Schema | Built by | Holds |
|---|---|---|---|
| Bronze | `RAW` | Gatekeeper | Exact copies of source data |
| Silver | `STAGING` | dbt | Cleaned, typed, decoded data |
| Gold | `MARTS` | dbt | Star schema: dimensions + facts |
| History | `SNAPSHOTS` | dbt | Slowly-changing history (SCD2) |
| Monitoring | `AUDIT` | hand-built SQL | Logs of every file, check, error |

**The Gold star schema** — 3 fact tables, 5 dimensions, 8 declared relationships:

```
        DIM_PATIENT   DIM_DOCTOR   DIM_HOSPITAL
              \           |            /
               \          |           /
                →   FCT_ADMISSIONS   ←
                          |
                       DIM_DATE   ← (shared by all three facts)
                          |
        FCT_TREATMENTS ←      → FCT_CLAIMS
              |                      |
          DIM_DOCTOR             DIM_INSURANCE
```

---

## 🛠️ Tools used

| Tool | Role in the project |
|---|---|
| **AWS S3** | Cloud storage — where source files land (incoming / processed / quarantine) |
| **Snowflake** | Cloud data warehouse — stores all layers (Bronze/Silver/Gold/Audit) |
| **Snowpark (Python)** | Runs the "gatekeeper" validation before any data is loaded |
| **dbt Core** | Builds & tests the Silver and Gold layers (SQL transformations + data tests) |
| **Apache Airflow** | Orchestrates the whole pipeline on a schedule, with failure alerts |
| **Snowflake Cortex AI** | Natural-language chat over the Gold star schema |
| **Power BI** | Dashboards built on the Gold star schema |
| **Git / GitHub** | Version control for all code |

---

## 📂 Repository structure

```
Healthcare_Analytics/
├── snowflake_setup/        ← numbered SQL to build Snowflake from scratch
│   ├── 01_account_setup.sql
│   ├── 02_storage_integration.sql
│   ├── 03_file_formats_and_stages.sql
│   ├── 04_raw_tables.sql
│   ├── 05_audit_tables.sql
│   ├── 06_email_integration.sql
│   ├── 07_dbt_handoff_note.sql
│   └── 08_add_star_schema_keys.sql
├── models/                 ← dbt transformations
│   ├── silver/             ← Silver (staging) models  → STAGING schema
│   └── gold/               ← Gold (dims + facts)      → MARTS schema
├── seeds/                  ← reference CSVs (doctors, hospitals, insurers)
├── snapshots/              ← SCD2 history (doctor changes over time)
├── macros/                 ← reusable dbt SQL functions
├── tests/                  ← custom data-quality tests
├── snowpark/               ← gatekeeper.py + quality_check.py (validation workers)
├── airflow/dags/           ← gatekeeper_dag.py + healthcare_pipeline.py
└── dbt_project.yml         ← dbt configuration
```

---

## ❄️ Snowflake setup — run these in order

This folder (`snowflake_setup/`) builds the entire Snowflake side from scratch.
A new person can run these top to bottom and end up with a working warehouse,
database, schemas, role, S3 link, tables, audit logs, and email alerts — ready
for dbt to build the Silver/Gold layers.

### Before you start, replace these placeholders

Search-and-replace across the files:

| Placeholder | Replace with |
|---|---|
| `<YOUR_AWS_ACCOUNT_ID>` | your 12-digit AWS account number |
| `<YOUR_BUCKET>` | your S3 bucket name |
| `<YOUR_ALERT_EMAIL>` | the inbox that receives alerts |
| `ADMIN` (in script 01) | your Snowflake login username, if different |

### Run order (in a Snowsight worksheet, whole file each time)

1. **01_account_setup.sql** — warehouse, database, 5 schemas, role + grants
2. **02_storage_integration.sql** — secure key-less link to S3 (do the AWS steps in its comments)
3. **03_file_formats_and_stages.sql** — CSV recipes + 3 stages (incoming/processed/quarantine)
4. **04_raw_tables.sql** — the 3 Bronze landing tables
5. **05_audit_tables.sql** — 4 monitoring/log tables (+ seeds your alert email)
6. **06_email_integration.sql** — email alerts + a test send
7. **07_dbt_handoff_note.sql** — (reading only) what dbt builds vs what you built

Then, from the dbt project folder in a terminal:

```bash
dbt deps     # install packages
dbt seed     # load doctors / hospitals / insurers into RAW
dbt run      # build Silver → Gold → Snapshots
dbt test     # run all data-quality tests
```

8. **08_add_star_schema_keys.sql** — run in Snowsight AFTER the first `dbt run`
   to declare the Gold primary/foreign keys (for Power BI / Cortex auto-joins).
   Re-run it after any `dbt run --full-refresh`.

---

## 📖 What each thing is, in one line

- **Warehouse** = the compute engine (you pay only while it runs)
- **Database > Schema > Table** = folder > sub-folder > data
- **Role** = a job badge that owns objects and gets only the permissions it needs
- **Storage integration** = secure handshake to S3 (no AWS keys in SQL)
- **Stage** = a pointer to an S3 folder
- **File format** = how to read the CSV
- **RAW tables** = exact copies of source data (Bronze)
- **Audit tables** = the pipeline's flight recorder
- **Email integration** = lets Snowflake send alert emails
- **Gatekeeper** = validates every file (12 checks) before loading; quarantines bad ones
- **Seeds** = small reference CSVs loaded as tables (doctors, hospitals, insurers)
- **Snapshots** = track dimension changes over time (SCD2 — e.g. a doctor's seniority history)
- **Star schema** = facts in the middle, dimensions around them — the BI-friendly model

---

## 🔁 Daily operation

1. A file is dropped into **S3 `incoming/`**.
2. The **gatekeeper** (run by Airflow every 10 min) validates it:
   - ✅ passes → loaded into RAW, file moved to `processed/`
   - ❌ fails → moved to `quarantine/`, **alert email sent**, logged in AUDIT
3. The **pipeline DAG** runs dbt: `run → snapshot → test → reconciliation`.
4. Fresh data appears in the **Gold star schema** for Power BI and Cortex.
5. Any failure → **alert email** + a row in the AUDIT logs.

---

## ⚠️ Things never to do by hand

- Don't **truncate seeds** (doctors/hospitals/insurers) — reload with `dbt seed`.
- Don't **manually create/truncate** Silver, Gold, or Snapshot tables — dbt owns them (use `dbt run`, or `--full-refresh` to rebuild).
- Don't **truncate `EMAIL_RECIPIENT_LOG`** — it controls who gets alerts.
- After any `dbt run --full-refresh`, **re-run `08_add_star_schema_keys.sql`** (a full refresh clears the declared keys).