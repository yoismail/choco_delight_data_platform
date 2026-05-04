
---

# 🍫 ChocoDelight Data Platform — End‑to‑End Data Engineering Case Study  
*A production‑grade ETL + Data Warehouse pipeline I designed and built using Python, Pandas, and PostgreSQL.*

---

## 🏷️ Badges  
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-blue)
![ETL Pipeline](https://img.shields.io/badge/ETL-Production--Grade-green)
![Advanced Logging](https://img.shields.io/badge/Logging-Advanced-orange)
![Chocolate Sales Dataset](https://img.shields.io/badge/Data-Chocolate%20Sales-yellow)

---

# 🌟 Why I Built This Project  

I built the **ChocoDelight Data Platform** to demonstrate how I approach real‑world data engineering problems — not just transforming CSVs, but designing a **scalable, layered, analytics‑ready data system**.

My goals were to build a pipeline that is:

- **Reliable** — deterministic runs, schema‑aware validation  
- **Reproducible** — clean separation of raw, operational, and analytics layers  
- **Maintainable** — modular ETL components instead of monolithic scripts  
- **Analytics‑ready** — a proper dimensional model with surrogate keys  
- **Observable** — a production‑grade logging system with timing and color  

This project reflects how I think as a data engineer:  
**structured, intentional, and focused on long‑term maintainability.**

---

# 🧠 What This Project Demonstrates

### 🔹 End‑to‑End ETL Engineering  
I built a complete pipeline that downloads, cleans, transforms, and loads data into a multi‑schema PostgreSQL warehouse.

### 🔹 Data Cleaning & Feature Engineering  
I engineered business‑friendly features such as customer tenure, brand tiers, region tiers, revenue buckets, margin buckets, and time‑of‑day segmentation.

### 🔹 Dimensional Modeling  
I designed a star schema with four dimensions and a central fact table, including surrogate keys and foreign key validation.

### 🔹 Production‑Grade Logging  
I implemented a cross‑platform, UTF‑8‑aware logging framework with colorized console output, rotating file logs, emoji‑safe formatting, and execution timing.

### 🔹 Modular, Maintainable Architecture  
Each ETL stage is isolated, testable, and orchestrated through a clean pipeline runner.

---

# 🌐 High‑Level Architecture  

```
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
```

---

# 🧩 ERD Diagrams  

## 📘 Operational → Analytics Flow

```
dim_calendar   ─┐
dim_customers  ─┼──> fact_sales
dim_products   ─┤
dim_stores     ─┘
```

---

# ⭐ Features at a Glance  

- Multi‑layer warehouse (raw → operational → analytics)  
- Modular ETL pipeline (extract → clean → transform → load)  
- Dimensional model with surrogate keys  
- Feature engineering for business insights  
- Production‑grade logging system  
- One‑command pipeline orchestration  
- Schema‑aware validation and foreign key checks  

---

# 🗂 Project Structure (Clean + Portfolio‑Friendly)

```
etl/
│── extract.py
│── clean.py
│── transform.py
│── load_raw_schema.py
│── load_operationals_schema.py
│── load_analytics_schema.py
│── wipe_all.py
│── run_all.py
│── debug_product.py

sql/
│── raw_schema.sql
│── operationals_schema.sql
│── analytics_schema.sql
│── setup_database.sql

utils/
│── logging.py

data/ (generated)
│── raw/
│── clean/
│── transform/
```

---

# 🔄 Pipeline Flow 

### 1️⃣ Extract  
I download the Kaggle dataset, extract the CSVs, and log metadata.

### 2️⃣ Clean  
I validate schemas, normalize columns, engineer features, and write cleaned datasets.

### 3️⃣ Transform  
I build dimension tables, validate foreign keys, and construct the fact table.

### 4️⃣ Load  
I load raw, operational, and analytics layers into PostgreSQL.

### 5️⃣ Orchestrate  
`run_all.py` executes the full pipeline with structured logs and execution timings.

---

# 🧩 Advanced Logging System (My Implementation)

I engineered a **production‑grade logging system** designed specifically for ETL pipelines that run across different environments.

### ✔ Cross‑platform UTF‑8 detection  
The logger automatically detects whether the terminal supports emojis and UTF‑8:

- Windows Terminal  
- VS Code  
- PowerShell 7+  
- macOS/Linux  
- Python stdout encoding  
- Windows code page 65001  

### ✔ Emoji‑safe output  
If the terminal cannot display emojis, they are stripped **only from console output**, not from file logs.

### ✔ Dual‑handler logging  
- **Console handler** → colorized, emoji‑aware  
- **Rotating file handler** → clean UTF‑8 logs with no ANSI codes  

### ✔ Message‑only colorization  
I colorize only the message portion, keeping timestamps clean and readable.

### ✔ Section banners  
I added a `section()` helper to visually separate ETL stages.

### ✔ Execution timing  
The `timed()` decorator logs how long each ETL step takes.

### ✔ SQLAlchemy noise suppression  
Engine logs are suppressed for cleaner output.

### Example snippet:

```python
def section(title: str):
    logging.info("\n" + "=" * 50)
    logging.info(f"🔷 {title}")
    logging.info("=" * 31 + "\n")
```

This logging system gives me full observability into the pipeline while keeping logs readable and production‑ready.

---

# ▶️ Running the Pipeline  

### Install dependencies  
```
pip install -r requirements.txt
```

### Configure environment  
```
DB_URL=postgresql://username:password@localhost:5432/choco_delight_db
```

### Run the full pipeline  
```
python -m etl.run_all
```

---

# 📊 Example Analytical Use Cases  

This model supports insights such as:

- Revenue by product, store, or region  
- Customer segmentation by tenure  
- Seasonality and quarter‑based performance  
- Time‑of‑day sales patterns  
- Margin analysis and outlier detection  

---

# 👤 Author  

**Yomi Ismail**  
Data Engineer & Product Operations Specialist  

---
