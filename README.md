# 🚕 NYC Taxi ELT Data Pipeline
### NYC TLC Dataset | Python · PostgreSQL · dbt · Apache Airflow · Power BI

---

## 📌 Project Overview

An end-to-end ELT (Extract, Load, Transform) data pipeline built on the NYC Taxi and Limousine Commission (TLC) dataset. The pipeline ingests monthly trip data, transforms it through a Medallion architecture (Bronze → Silver → Gold), validates data quality with automated tests, and serves business-ready analytics via a Power BI dashboard — all orchestrated automatically with Apache Airflow.

**The business question:** *How do NYC taxi trip patterns, revenue, and demand vary across time of day, day type, and payment method — and how do we keep this analysis always current?*

---

## 🏗️ Architecture

```
NYC TLC Website (public parquet files, monthly)
          ↓
Python Ingestion Script (chunked load, 100K rows/batch)
          ↓
PostgreSQL — Bronze Layer (raw_trips) — 2,964,624 rows
          ↓  dbt models + tests
PostgreSQL — Silver Layer (stg_trips) — 2,867,747 rows
          ↓  dbt models + tests
PostgreSQL — Gold Layer (3 analytical models)
          ↓
Power BI Dashboard (3 pages)
          ↑
Apache Airflow DAG (6 tasks, scheduled monthly)
```

---

## 🗂️ Project Structure

```
nyc-taxi-pipeline/
│
├── ingestion/
│   └── ingest.py                  # Bronze ingestion script
│
├── dbt_project/
│   ├── dbt_project.yml            # dbt project config
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_trips.sql      # Silver cleaning model
│   │   │   └── schema.yml         # Silver data tests
│   │   └── gold/
│   │       ├── fct_daily_summary.sql
│   │       ├── fct_hourly_demand.sql
│   │       ├── fct_payment_analysis.sql
│   │       └── schema.yml         # Gold data tests
│   └── macros/
│
├── airflow/
│   ├── docker-compose.yml         # Airflow Docker setup
│   └── dags/
│       └── nyc_taxi_pipeline.py   # Airflow DAG definition
│
├── data/
│   └── yellow_tripdata_2024-01.parquet
│
├── dashboard/
│   └── nyc_taxi_dashboard.pbix
│
├── .env                           # DB credentials (not committed)
└── README.md
```

---

## 📊 Dataset

**Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

| Month | File | Rows | Size |
|---|---|---|---|
| January 2024 | yellow_tripdata_2024-01.parquet | 2,964,624 | ~50MB |

The dataset contains trip-level records including pickup/dropoff timestamps, locations, distances, fares, tips, and payment types.

---

## 🔧 Medallion Architecture

### Bronze Layer — Raw Ingestion
- Data loaded exactly as received from source — no transformations
- Added metadata columns: `ingested_at`, `source_file`
- Loaded in chunks of 100,000 rows to handle large file sizes
- Schema defined upfront with explicit column types

### Silver Layer — Cleaning & Validation
Transformations applied in `stg_trips.sql`:

| Issue | Fix Applied |
|---|---|
| Trips with dates outside Jan 2024 (e.g. year 2002) | Filtered out — date range enforced |
| Zero or negative fare amounts | Removed |
| Zero trip distances | Removed |
| Trips where dropoff < pickup | Removed |
| Trips over 5 hours or 100 miles | Removed as outliers |
| Missing passenger count | Filled with 1 (default) |
| Missing airport fee | Filled with 0 |
| Payment type as integer | Decoded to readable label |

Result: **96,877 rows removed** (3.3% of raw data) — each removal has a documented business reason.

### Gold Layer — Business Aggregations

| Model | Rows | Description |
|---|---|---|
| `fct_daily_summary` | 31 | One row per day — trip volume, revenue, avg fare, payment split |
| `fct_hourly_demand` | 744 | One row per hour per day — demand patterns, time periods |
| `fct_payment_analysis` | 5 | One row per payment type — fare, tip, revenue breakdown |

---

## ✅ Data Quality Tests

15 automated dbt tests run after every pipeline execution:

**Silver layer (6 tests):**
- `not_null` on vendor_id, pickup datetime, trip distance, total amount, payment type
- `accepted_values` on payment_type_name

**Gold layer (9 tests):**
- `not_null` on key columns across all 3 models
- `unique` on trip_date in fct_daily_summary
- `unique` on payment_type_name in fct_payment_analysis
- `accepted_values` on time_period in fct_hourly_demand

**Result: 15/15 tests passing ✅**

---

## 🔍 Key Findings

### Finding 1 — Evening Rush is the Busiest Period
Trip volume peaks between 4–7 PM with Evening Rush accounting for the highest demand. Late Night (10 PM–midnight) shows the second highest volume, driven by weekend activity.

### Finding 2 — Credit Card Dominates but Cash Generates Zero Tips
80.1% of trips are paid by credit card. Cash users tip $0 — credit card users average 23% tip rate. This creates a significant revenue differential per trip beyond the fare itself.

### Finding 3 — Weekday vs Weekend Patterns Differ Significantly
Weekday trips cluster around morning (7–9 AM) and evening (4–7 PM) rush hours reflecting commuter patterns. Weekend trips are more evenly distributed across midday and night.

### Finding 4 — Average Fare is $26.80
With 2.86M cleaned trips generating substantial revenue, even small improvements in pricing strategy or tip conversion would have large aggregate impact.

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | Ingestion script, data engineering |
| Pandas | Latest | DataFrame manipulation |
| PyArrow | Latest | Parquet file reading |
| SQLAlchemy | Latest | Database connection management |
| PostgreSQL | 17 | Local data warehouse |
| dbt Core | 1.11.2 | Data transformation and testing |
| dbt-postgres | 1.10.0 | PostgreSQL adapter for dbt |
| Apache Airflow | 2.8.1 | Pipeline orchestration |
| Docker | 29.3.1 | Airflow containerisation |
| Power BI Desktop | Latest | Dashboard and visualisation |

---

## 🚀 How to Run

### Prerequisites
- Python 3.10+
- PostgreSQL 17
- Docker Desktop
- Power BI Desktop

### Setup

**1. Clone the repo and install dependencies:**
```bash
git clone https://github.com/rahulmenonn/nyc-taxi-pipeline
cd nyc-taxi-pipeline
pip install pandas pyarrow sqlalchemy psycopg2-binary dbt-postgres python-dotenv
```

**2. Configure credentials in `.env`:**
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nyc_taxi
DB_USER=postgres
DB_PASSWORD=your_password
```

**3. Create the database:**
```bash
psql -U postgres -c "CREATE DATABASE nyc_taxi;"
```

**4. Download data:**
```bash
curl -L -o data/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

**5. Run ingestion:**
```bash
python ingestion/ingest.py
```

**6. Run dbt transformations and tests:**
```bash
cd dbt_project
dbt run
dbt test
```

**7. Start Airflow:**
```bash
cd airflow
docker-compose up airflow-init
docker-compose up airflow-webserver airflow-scheduler -d
```
Access at `http://localhost:8080` (admin / admin123)

**8. Open Power BI dashboard:**
Connect to PostgreSQL → `nyc_taxi` database → `silver_gold` schema → load all 3 Gold tables.

---

## 📈 Power BI Dashboard

**Page 1 — Trip Overview**
KPI cards (total trips, revenue, avg fare, avg duration), daily trip volume line chart, weekday vs weekend comparison, daily revenue trend.

**Page 2 — Demand Analysis**
Peak hour identification, trips by hour of day, time period breakdown (Morning Rush / Midday / Evening Rush / Night / Late Night), weekday vs weekend demand by hour.

**Page 3 — Payment & Revenue**
Payment type distribution donut, revenue by payment type, avg tip % by payment type, distance vs fare scatter plot.

---

## 👤 Author

**Rahul Menon**
Entry-level Data Analyst | Python · SQL · dbt · Power BI
[LinkedIn](https://www.linkedin.com/in/rahul-menon-132a84245/) · [GitHub](https://github.com/rahulmenonn) · menon9236@gmail.com
