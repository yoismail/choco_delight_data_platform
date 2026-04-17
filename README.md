---

## 📦 **ChocoDelight Data Platform** 
An end-to-end data engineering project that transforms a flat chocolate sales dataset into a structured, analytics-ready PostgreSQL data platform.

This project demonstrates how to design a modular ETL pipeline that:

- downloads raw source files from Kaggle
- cleans and validates operational data with Python and Pandas
- engineers business-friendly features
- transforms records into a dimensional model
- loads raw, operational, and analytics layers into PostgreSQL

---

## 🚀 Project Overview

The ChocoDelight Data Platform is built around a layered data engineering workflow:

- **Raw layer** stores the original extracted source tables
- **Operational layer** stores cleaned and standardized tables
- **Analytics layer** stores dimensional tables and a fact table for reporting and analysis

The pipeline is designed to be reproducible, modular, and easier to maintain than a single notebook or monolithic script.

---

## What This Project Demonstrates

### 1. End-to-End ETL Engineering
- dataset download using the Kaggle CLI
- ZIP extraction and source exploration
- modular cleaning and transformation logic
- PostgreSQL loading across multiple schemas
- one-command orchestration with a pipeline runner

### 2. Data Cleaning and Standardization
- column normalization
- schema validation
- duplicate removal
- missing value handling
- date parsing and numeric coercion

### 3. Feature Engineering
- customer tenure metrics
- customer segmentation
- calendar quarter / season / day type
- product brand tier classification
- store region and region tier
- sales revenue buckets
- profit margin buckets
- outlier detection
- time-of-day categorization

### 4. Dimensional Modeling
- dimension tables for calendar, customers, products, and stores
- fact table for sales
- surrogate key assignment
- foreign key validation before fact construction
- indexes on dimensions and fact keys for analytics performance

---

## Architecture

```text
Kaggle Dataset
    ↓
data/raw/
    ↓
data/clean/
    ↓
data/transform/
    ↓
PostgreSQL
├── raw_data
├── operationals
└── analytics
````

---

## Tech Stack

* **Python**
* **Pandas**
* **PostgreSQL**
* **SQLAlchemy**
* **psycopg2-binary**
* **python-dotenv**
* **Kaggle API / Kaggle CLI**
* **Structured logging**

---

## Repository Structure

```bash
choco_delight_data_platform/
│
├── etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── clean.py
│   ├── transform.py
│   ├── load_raw_schema.py
│   ├── load_operationals_schema.py
│   ├── load_analytics_schema.py
│   ├── wipe_all.py
│   ├── run_all.py
│   └── debug_product.py
│
├── sql/
│   ├── raw_schema.sql
│   ├── operationals_schema.sql
│   ├── analytics_schema.sql
│   └── setup_database.sql
│
├── utils/
│   └── logging.py
│
├── requirements.txt
├── README.md
└── __init__.py
```

### Generated Data Folders

These folders are created during pipeline execution:

```text
data/
├── raw/
│   ├── chocolate-sales-dataset-2023-2024.zip
│   ├── calendar.csv
│   ├── customers.csv
│   ├── products.csv
│   ├── sales.csv
│   └── stores.csv
│
├── clean/
│   ├── cleaned_calendar.csv
│   ├── cleaned_customers.csv
│   ├── cleaned_products.csv
│   ├── cleaned_sales.csv
│   └── cleaned_stores.csv
│
└── transform/
    ├── dim_calendar.csv
    ├── dim_customers.csv
    ├── dim_products.csv
    ├── dim_stores.csv
    └── fact_sales.csv
```

---

## Source Dataset

The pipeline downloads the dataset from Kaggle:

**Dataset:** `ssssws/chocolate-sales-dataset-2023-2024`

Source files handled by the pipeline:

* `calendar.csv`
* `customers.csv`
* `products.csv`
* `sales.csv`
* `stores.csv`

---

## Data Layers

## 1. Raw Layer (`raw_data` schema)

The raw layer stores the extracted source data with minimal structural changes.

### Tables

* `raw_data.calendar`
* `raw_data.customers`
* `raw_data.products`
* `raw_data.sales`
* `raw_data.stores`

This layer is useful for:

* preserving source fidelity
* auditing source data
* debugging extraction issues

---

## 2. Operational Layer (`operationals` schema)

The operational layer stores cleaned and standardized datasets produced by `clean.py`.

### Tables

* `operationals.cleaned_calendar`
* `operationals.cleaned_customers`
* `operationals.cleaned_products`
* `operationals.cleaned_sales`
* `operationals.cleaned_stores`

### Cleaning Highlights

#### Calendar

* renames `date` to `calendar_date`
* parses dates properly
* creates:

  * `calendar_key`
  * `quarter`
  * `season`
  * `day_type`
  * `calendar_date_formatted`

#### Customers

* standardizes gender values
* parses `join_date`
* creates:

  * `customer_key`
  * `tenure_days`
  * `tenure_months`
  * `tenure_years`
  * `customer_segment`

#### Products

* standardizes text fields
* adds missing debug products:

  * `P0000`
  * `P0201`
* creates:

  * `product_key`
  * `brand_tier`

#### Stores

* standardizes location fields
* maps countries to regions
* creates:

  * `store_key`
  * `region`
  * `region_tier`

#### Sales

* coerces numeric metrics
* parses `order_date`
* creates:

  * `revenue_bucket`
  * `profit_margin`
  * `margin_bucket`
  * `outlier_flag`
  * `time_of_day`

---

## 3. Analytics Layer (`analytics` schema)

The analytics layer contains transformed dimension tables and the central fact table.

### Dimension Tables

* `analytics.dim_calendar`
* `analytics.dim_customers`
* `analytics.dim_products`
* `analytics.dim_stores`

### Fact Table

* `analytics.fact_sales`

### Dimensional Model

```text
dim_calendar   ─┐
dim_customers  ─┼──> fact_sales
dim_products   ─┤
dim_stores     ─┘
```

### Analytics Design Features

* surrogate keys for dimensions
* natural key uniqueness
* foreign key constraints
* fact table validation against dimensions
* indexes for joins and analytical filters

---

## ETL Pipeline Modules

## `etl/extract.py`

Responsible for:

* creating `data/raw`
* downloading the Kaggle dataset ZIP
* extracting raw CSV files
* performing initial exploration and logging

## `etl/clean.py`

Responsible for:

* validating source schemas
* normalizing columns
* cleaning each source table
* applying feature engineering
* writing cleaned CSVs to `data/clean`

## `etl/transform.py`

Responsible for:

* loading cleaned CSVs
* cleaning date columns again for safe merging
* merging surrogate keys into sales
* validating foreign keys
* creating dimension and fact CSVs in `data/transform`

## `etl/load_raw_schema.py`

Responsible for:

* loading extracted CSVs into the `raw_data` schema

## `etl/load_operationals_schema.py`

Responsible for:

* loading cleaned CSVs into the `operationals` schema

## `etl/load_analytics_schema.py`

Responsible for:

* recreating the analytics schema from SQL
* loading dimension tables first
* loading `fact_sales` last

## `etl/wipe_all.py`

Responsible for:

* deleting pipeline output folders
* resetting PostgreSQL schemas:

  * `raw_data`
  * `operationals`
  * `analytics`

## `etl/run_all.py`

Pipeline orchestrator that runs:

1. wipe
2. extract
3. clean
4. transform
5. raw schema load
6. operationals schema load
7. analytics schema load

---

## SQL Schema Files

### `sql/raw_schema.sql`

Creates raw ingestion tables:

* `sales`
* `calendar`
* `customers`
* `products`
* `stores`

### `sql/operationals_schema.sql`

Creates cleaned operational tables:

* `cleaned_calendar`
* `cleaned_customers`
* `cleaned_products`
* `cleaned_sales`
* `cleaned_stores`

### `sql/analytics_schema.sql`

Creates:

* `dim_calendar`
* `dim_customers`
* `dim_products`
* `dim_stores`
* `fact_sales`

Also adds indexes for:

* dimension natural keys
* fact foreign keys
* analytical filter columns such as:

  * `revenue_bucket`
  * `margin_bucket`
  * `time_of_day`

### `sql/setup_database.sql`

Provides initial database setup commands for:

* creating the database
* connecting to it
* creating the required schemas

---

## Requirements

Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Current requirements include:

* `pandas`
* `numpy`
* `SQLAlchemy`
* `psycopg2-binary`
* `python-dotenv`
* `greenlet`
* `python-dateutil`
* `typing_extensions`
* `tzdata`
* `six`

---

## Environment Setup

Create a `.env` file in the project root:

```env
DB_URL=postgresql://username:password@localhost:5432/choco_delight_db
```

Make sure `.env` is included in `.gitignore`.

---

## PostgreSQL Setup

You can initialize the database using the SQL scripts in the `sql/` folder.

Example:

```sql
CREATE DATABASE choco_delight_db;
```

Then create the schemas:

```sql
CREATE SCHEMA raw_data;
CREATE SCHEMA operationals;
CREATE SCHEMA analytics;
```

You can also use the provided setup script:

```bash
psql -U postgres -d postgres -f sql/setup_database.sql
```

---

## Kaggle Setup

Because `extract.py` uses the Kaggle CLI, you need Kaggle configured locally.

### Install Kaggle

```bash
pip install kaggle
```

### Configure Kaggle API

Place your `kaggle.json` in the correct directory:

* **Windows:** `C:\Users\<your_username>\.kaggle\kaggle.json`
* **Linux / macOS:** `~/.kaggle/kaggle.json`

Then make sure the Kaggle CLI works:

```bash
kaggle datasets list
```

---

## How to Run the Pipeline

## Option 1: Run the Full Pipeline

From the project root:

```bash
python -m etl.run_all
```

This runs the complete workflow from wipe to final analytics load.

---

## Option 2: Run Individual Steps

### Wipe folders and schemas

```bash
python -m etl.wipe_all all
```

You can also wipe individual targets:

```bash
python -m etl.wipe_all raw
python -m etl.wipe_all clean
python -m etl.wipe_all transform
python -m etl.wipe_all operationals
python -m etl.wipe_all analytics
```

### Extract

```bash
python -m etl.extract
```

### Clean

```bash
python -m etl.clean
```

### Transform

```bash
python -m etl.transform
```

### Load raw schema

```bash
python -m etl.load_raw_schema
```

### Load operational schema

```bash
python -m etl.load_operationals_schema
```

### Load analytics schema

```bash
python -m etl.load_analytics_schema
```

---

## Final Output

After a successful full run, you will have:

### Files

* extracted raw CSVs in `data/raw`
* cleaned CSVs in `data/clean`
* transformed dimension/fact CSVs in `data/transform`

### Database Schemas

* `raw_data`
* `operationals`
* `analytics`

### Analytics Tables

* `analytics.dim_calendar`
* `analytics.dim_customers`
* `analytics.dim_products`
* `analytics.dim_stores`
* `analytics.fact_sales`

---

## Example Analytical Use Cases

This model supports analysis such as:

* revenue by product
* revenue by store or country
* customer segmentation by tenure
* sales distribution by time of day
* seasonality and quarter-based performance
* margin analysis by product or category
* outlier detection for unusually large transactions

---

## Logging

The project uses a custom colorized logging utility in `utils/logging.py` to make pipeline execution easier to follow in the terminal.

This improves:

* debugging
* visibility into each ETL stage
* error tracing during failures

---

## Debugging Utility

`etl/debug_product.py` helps investigate mismatched `product_id` values between:

* `cleaned_sales.csv`
* `cleaned_products.csv`

It normalizes product IDs for comparison and is useful when diagnosing foreign key issues during transformation.

---

## Why This Project Matters

This project reflects strong data engineering fundamentals:

* layered data architecture
* reproducible ETL design
* schema-aware cleaning and validation
* dimensional modeling for analytics
* PostgreSQL integration
* maintainable modular code organization

It is designed as both a portfolio project and a practical demonstration of how to move from raw files to a scalable analytics foundation.

---

## Author

**Yomi Ismail**
Data Engineer & Product Operations Specialist

---

```
