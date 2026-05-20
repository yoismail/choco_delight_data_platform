# ChocoDelight Data Platform
**A production-grade ETL + three-layer data warehouse built in Python and PostgreSQL.**

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-blue)
![ETL Pipeline](https://img.shields.io/badge/ETL-Production--Grade-green)
![Advanced Logging](https://img.shields.io/badge/Logging-Advanced-orange)
![Chocolate Sales Dataset](https://img.shields.io/badge/Data-Chocolate%20Sales-yellow)

Takes a flat Kaggle chocolate sales dataset through five clearly-separated layers (extract → explore → clean → transform → load) and lands it in a star schema warehouse with real foreign-key constraints, CHECK constraints, indexes, and a 7-mode wipe utility for safe developer iteration.

Built to be **reliable, reproducible, maintainable, analytics-ready, and observable**. Not just to transform CSVs.

---

## What this project is

A three-schema data platform that turns a messy Kaggle chocolate sales dataset into a queryable warehouse. The pipeline runs end to end with a single command, and every stage is independently runnable for debugging or partial reloads.

```
Kaggle source CSVs
       ↓
   data/raw/          ──→  PostgreSQL.raw_data         (mirror of source)
       ↓
   data/clean/        ──→  PostgreSQL.operationals     (cleaned + features engineered)
       ↓
   data/transform/    ──→  PostgreSQL.analytics        (star schema warehouse)
```

Three schemas, three loaders, one orchestrator. Analysts can query any layer to trace discrepancies back to source.

---

## The pipeline at a glance

| Stage | Module | What it does | Output |
|---|---|---|---|
| 1. Wipe | `etl/wipe_all.py` | Clears local data folders and Postgres schemas | clean state |
| 2. Extract | `etl/extract.py` | Downloads the Kaggle dataset via the Kaggle CLI, unzips it, validates | `data/raw/*.csv` |
| 3. Explore | `etl/explore.py` | Standalone profiling pass: shape, nulls, dtypes, duplicates per dataset | log output |
| 4. Clean | `etl/clean.py` | Six dedicated cleaners with schema validation and feature engineering | `data/clean/*.csv` |
| 5. Transform | `etl/transform.py` | Builds the star schema, validates five foreign-key relationships before fact assembly | `data/transform/*.csv` |
| 6. Load raw | `etl/load_raw_schema.py` | Mirrors the source CSVs into `raw_data` schema | Postgres tables |
| 7. Load operational | `etl/load_operationals_schema.py` | Loads cleaned data into `operationals` schema | Postgres tables |
| 8. Load analytics | `etl/load_analytics_schema.py` | Executes the DDL, then loads the star schema in FK-aware order | Postgres tables |

Run the whole thing with one command:

```bash
python -m etl.run_all
```

---

## The warehouse

Star schema with five dimensions and one fact table, with a small intentional snowflake on region for analyst convenience.

```
                          dim_calendar
                                │
                                │ calendar_key
                                ▼
dim_customers ──────────►  fact_sales  ◄────────── dim_products
                                ▲
                                │ store_key
                                │
                          dim_stores ────► dim_regions
                                │              ▲
                                └──────────────┘
                                  region_key
                                  (direct shortcut on fact)
```

**Why region appears twice:** `fact_sales` has a direct `region_key` foreign key to `dim_regions` *and* `dim_stores` also has `region_key`. "Revenue by region" is the most common query against this warehouse, and the direct fact-to-region link saves an extra join. The full normalised path through `dim_stores` is still there for joins that need other store attributes.

**Real constraints, not just types:**

- **Five `FOREIGN KEY ... ON DELETE RESTRICT`** constraints on `fact_sales`
- **One `FOREIGN KEY` on `dim_stores`** linking back to `dim_regions`
- **Seven `CHECK` constraints**: age ≥ 0, order_date ≤ CURRENT_DATE, quantity ≥ 0, unit_price ≥ 0, discount ≥ 0, revenue ≥ 0, cost ≥ 0
- **UNIQUE constraints** on every natural ID (customer_id, product_id, store_id, calendar_date)
- **13 indexes**: 5 dim natural-key indexes, 5 fact FK indexes, plus 3 analytical indexes on `revenue_bucket`, `margin_bucket`, `time_of_day`

If you try to delete a dimension row that's still referenced by a fact, Postgres refuses. If you try to load a future-dated order, the CHECK constraint refuses. The warehouse defends itself.

---

## Engineering principles applied

This project demonstrates seven engineering decisions made deliberately:

### 1. Schema is the source of truth, not Python (analytics layer)

`sql/analytics_schema.sql` is a hand-authored DDL file with primary keys, foreign keys, CHECK constraints, and indexes. The analytics loader reads that file and executes it via `text(schema_sql)` before any data lands. Tables get their structure from the DDL, not from `pandas.to_sql` inference.

The raw and operational layers take a different trade-off: they use `pandas.to_sql(if_exists="replace")` and let pandas infer dtypes from the source CSVs. Their DDL files (`sql/raw_schema.sql`, `sql/operationals_schema.sql`) are committed to the repo as documentation of intent, but the actual table creation is driven by the data, not the file. This is deliberate: the layer analysts query gets DDL discipline; the upstream layers are transient snapshots whose contracts are dictated by their CSV outputs.

### 2. Idempotency through deterministic rebuild

Every full pipeline run starts with `wipe_all.py` clearing local folders and dropping the three Postgres schemas. The pipeline then rebuilds everything from source. Running it twice in a row produces the same warehouse, every time. The idempotency contract is "deterministic rebuild from same input," not "skip rows that already exist."

The `wipe_all.py` utility supports seven modes (`raw`, `clean`, `transform`, `analytics`, `operationals`, `all`, plus per-folder targeting). During development, you can wipe just one layer and re-run from there, without nuking the whole project.

### 3. Logs that hold up at 2am

`utils/logging.py` is a bespoke logging framework, not a default Python logger. It does five things most pipeline loggers don't:

- **Cross-platform UTF-8 detection** — six fallback checks (VS Code, Windows Terminal, PowerShell 7+, stdout encoding, Windows code page 65001, then OS) to decide whether emojis should render or be stripped
- **Dual-handler routing** — colorized output to the console, clean UTF-8 to a rotating file (no ANSI codes in logs)
- **Emoji-safe console output** — emojis stripped only when the terminal can't render them; the file always gets the full Unicode
- **Section banners** via a `section()` helper, so log files clearly mark stage transitions
- **`@timed` decorator** wraps each pipeline stage and logs elapsed time in `Xm Ys` format
- **SQLAlchemy noise suppression** — `sqlalchemy.engine` is set to WARNING so it doesn't drown out the pipeline messages

Logs rotate at 2MB with 7 backups kept. When something fails, the traceback lands in `logs/pipeline.log` ready to be searched later.

### 4. Validate before you load, not after

`transform.py` calls `validate_foreign_keys()` five times before the fact table is assembled. Each check confirms that every `customer_key`, `product_key`, `store_key`, `region_key`, and `calendar_key` in the sales data resolves to a real row in the dimension. If any row fails, the pipeline halts with `sys.exit(1)` and a structured error. Bad referential integrity is caught at the transform stage, before any database load.

`clean.py` does the equivalent at the schema level: `validate_schema()` exits with code 1 if a cleaned table is missing any required column. Schema drift in the source CSV is caught at the start of cleaning, not three stages later when something tries to load a NULL into a NOT NULL column.

### 5. Configuration via environment variables

Every database-touching module loads `DB_URL` from `.env` via `python-dotenv`. No credentials in code, no hardcoded paths to the warehouse. The `.env` file is gitignored. Changing where the warehouse lives is a one-line change in environment, not a code change.

### 6. Modular ETL, not monolithic scripts

Each pipeline stage is a self-contained module with its own `main()` (or `run_pipeline()`) entry point. You can run any stage in isolation:

```bash
python -m etl.extract        # just download + unzip
python -m etl.clean          # just run the cleaning
python -m etl.transform      # just build the star schema
python -m etl.load_analytics_schema  # just push to the analytics warehouse
```

Or run the whole thing via `python -m etl.run_all`. The orchestrator uses subprocess calls with `sys.exit(returncode)` checks so a failure in any stage halts the whole pipeline immediately.

### 7. Engineer features in the warehouse, not in dashboards

The cleaning stage produces ready-to-query analytical features so dashboards don't have to re-derive them every load:

- **Customers:** `tenure_days`, `tenure_months`, `tenure_years`, `customer_segment` (New / Active / Loyal / VIP based on months of tenure)
- **Products:** `brand_tier` (Premium for Lindt/Godiva/Green & Black; Standard otherwise)
- **Stores:** `region` (six-region mapping from country), `region_tier` (High-Value / Medium-Value / Low-Value)
- **Sales:** `profit_margin`, `revenue_bucket` (Low/Medium/High/VIP revenue), `margin_bucket` (Low/Medium/High/Very High margin), `outlier_flag` (top 1% by revenue), `time_of_day` (Morning/Afternoon/Evening/Night from order hour)
- **Calendar:** `quarter`, `season`, `day_type` (Weekend / Weekday), `calendar_date_formatted`

These features land in the warehouse as columns. Analytical queries don't need to recompute them.

---

## Real engineering decisions

A small selection of the harder choices in this project, documented honestly:

### Why surrogate keys are positional, not content-based

Every dimension gets its surrogate key assigned by `df.sort_values(natural_id).reset_index(drop=True).index + 1`. This is **deterministic for the same input data**, but if new customers or products are added, all subsequent keys shift. That's fine for this project because the warehouse is rebuilt from scratch on every run — keys never need to be stable across runs.

This is a deliberate trade-off vs the content-hash approach (e.g. SHA-256 of natural keys), which is what you'd want if the warehouse merged data over time instead of rebuilding. ChocoDelight rebuilds; positional keys are simpler and sufficient.

### Why the schema is slightly snowflaked on region

`fact_sales` has a `region_key` foreign key directly to `dim_regions`. `dim_stores` also has a `region_key` foreign key to `dim_regions`. This is **intentional denormalisation for query performance**. The most common analytical question against this warehouse is "revenue by region", and the direct fact-to-region path eliminates an extra join.

### Why the analytics schema is recreated from DDL every load

`load_analytics_schema.py` drops `analytics.fact_sales` first, then executes `analytics_schema.sql` to recreate the entire schema, then loads data in FK-aware order (regions first because stores depend on it; fact last because it depends on all five dimensions). This is the same idea as `if_exists="replace"` but with explicit DDL ownership, so primary keys, foreign keys, CHECK constraints, and indexes survive the rebuild.

### Why product_id had a phantom matching problem

During development, some sales records referenced product_ids that didn't exist in the products dimension. The forensic tool (`etl/debug_product.py`) was written to enumerate them. The fix landed in `clean.py`: two `Unknown Product` placeholder rows (P0000 and P0201) so the fact-to-dimension foreign key constraint doesn't reject the affected sales records. The placeholder approach preserves the data and makes the integrity issue visible in queries, rather than silently dropping rows.

---

## Project structure

```
choco_delight_data_platform/
├── data/                              # generated at runtime
│   ├── raw/                           # extract.py output
│   ├── clean/                         # clean.py output
│   └── transform/                     # transform.py output
├── etl/
│   ├── extract.py                     # Kaggle CLI download + unzip
│   ├── explore.py                     # raw-data profiling
│   ├── clean.py                       # 6 cleaners, feature engineering
│   ├── transform.py                   # star schema build + FK validation
│   ├── load_raw_schema.py             # raw_data schema loader
│   ├── load_operationals_schema.py    # operationals schema loader
│   ├── load_analytics_schema.py       # analytics DDL + FK-ordered loader
│   ├── wipe_all.py                    # 7-mode wipe utility
│   ├── run_all.py                     # orchestrator (full pipeline)
│   └── debug_product.py               # forensic FK debugging tool
├── logs/
│   └── pipeline.log                   # rotating file logs (2MB, 7 backups)
├── sql/
│   ├── setup_database.sql             # one-time DB + schema bootstrap
│   ├── raw_schema.sql                 # raw_data DDL (documentation)
│   ├── operationals_schema.sql        # operationals DDL (documentation)
│   └── analytics_schema.sql           # analytics DDL (executed at load)
├── utils/
│   ├── __init__.py
│   └── logging.py                     # cross-platform logging framework
├── .env                               # DB_URL (gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Quick start

### Prerequisites

1. **Python 3.10+** with pip
2. **PostgreSQL 14+** running locally
3. **Kaggle CLI** installed and authenticated. The pipeline uses Kaggle CLI to download the source dataset:
```bash
   pip install kaggle
```
   Then place your `kaggle.json` API token in `~/.kaggle/` (macOS/Linux) or `%USERPROFILE%\.kaggle\` (Windows).

### Setup

```bash
# 1. Clone and enter the project
git clone https://github.com/yoismail/choco_delight_data_platform.git
cd choco_delight_data_platform

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Bootstrap the database (one-time, manual)
psql -U postgres -f sql/setup_database.sql

# 4. Create .env file with the database URL
echo 'DB_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/choco_delight_db' > .env

# 5. Run the full pipeline
python -m etl.run_all
```

### What the run looks like

```
[Wipe] cleared data/raw, data/clean, data/transform + 3 Postgres schemas
[Extract] downloaded chocolate-sales-dataset-2023-2024.zip, unzipped 5 CSVs
[Clean] processed 6 datasets, applied feature engineering
[Transform] validated 5 foreign-key relationships, built fact_sales
[Load raw] pushed 5 tables to raw_data schema
[Load operational] pushed 6 tables to operationals schema
[Load analytics] executed DDL, loaded 5 dims + 1 fact (FK-ordered)
FULL PIPELINE COMPLETED SUCCESSFULLY
```

A full run takes a few minutes on a laptop. Re-runs are the same duration because the whole warehouse rebuilds from scratch.

### Partial re-runs

```bash
# Just re-extract the source data (e.g. after a Kaggle dataset update)
python -m etl.extract

# Just rebuild the analytics layer
python -m etl.wipe_all analytics
python -m etl.load_analytics_schema

# Just re-run the transform step (no DB activity)
python -m etl.transform
```

---

## Querying the warehouse

The analytics schema is what you'll query most. A few examples of what the engineered features make easy:

```sql
-- Top 10 customers by revenue, by segment
SELECT c.customer_segment, c.customer_id, SUM(f.revenue) AS total_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment, c.customer_id
ORDER BY total_revenue DESC
LIMIT 10;

-- Revenue by region (uses the direct fact-to-region shortcut)
SELECT r.region, SUM(f.revenue) AS total_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_regions r ON f.region_key = r.region_key
GROUP BY r.region
ORDER BY total_revenue DESC;

-- Time-of-day patterns by season
SELECT cal.season, f.time_of_day, COUNT(*) AS orders, SUM(f.revenue) AS revenue
FROM analytics.fact_sales f
JOIN analytics.dim_calendar cal ON f.calendar_key = cal.calendar_key
GROUP BY cal.season, f.time_of_day
ORDER BY cal.season, revenue DESC;

-- Outliers in the high-margin segment
SELECT *
FROM analytics.fact_sales
WHERE outlier_flag = 'Outlier (Top 1%)'
AND margin_bucket = 'Very High Margin';
```

---

## Tech stack

- **Python 3.10+** for ETL orchestration and transforms
- **pandas** for cleaning, type casting, feature engineering
- **PostgreSQL 14+** for the three-schema warehouse
- **SQLAlchemy** for DB engine management and bulk loads
- **psycopg2-binary** as the Postgres driver
- **python-dotenv** for environment-based config
- **Kaggle CLI** for source data ingestion
- **Custom logging framework** built in-house (see `utils/logging.py`)

---

## What I'd do next

A few honest "tightening" notes for future iterations:

- **Type mismatch** between `fact_sales.order_date` (DATE) and `dim_calendar.calendar_date` (VARCHAR(10)). The current pipeline joins via `calendar_key` so this doesn't matter in practice, but the inconsistency could bite a future developer.
- **Raw and operational schema DDLs are not executed.** They're documentation. To make them the source of truth, the loaders would need to execute the DDL files and switch from `if_exists="replace"` to `if_exists="append"`. Doable, but adds maintenance burden.
- **Airflow or Prefect orchestration** for scheduled runs with retry, alerting, and proper run history. The current `run_all.py` is fine for development but isn't a production scheduler.
- **dbt** on top of the analytics layer for analyst-authored transformations with tested SQL models.

---

## Author

**Yomi Ismail**
Data Engineer · Suffolk, UK
[Portfolio](https://yoismail.github.io/portfolio/) · [GitHub](https://github.com/yoismail) · [LinkedIn](https://www.linkedin.com/in/yomi-ismail)
