# 🏎️ F1 Analytics Pipeline 

A complete local data pipeline built upon the "Modern Data Stack". It extracts live F1 data alongside historical results, transforming them via Medallion architecture (Bronze ➔ Silver ➔ Gold), and establishes a fully optimized dimensional Star Schema for Power BI.

*For the Czech version, please see [README.md](README.md).*

## 🌟 Architecture and Technologies

- **Data Sources:**
  - **[OpenF1 API](https://openf1.org/):** Live telemetry from the 2024 season (Sessions, Laps, Pit Stops, Driver Data).
  - **Kaggle F1 Dataset (Ergast):** Historical dimensions and results (Circuits, Drivers, Races, Results).
- **Processing (Python 3.11+):**
  - Efficient Extract & Load leveraging `requests.Session()` for API optimization and `DuckDB` direct streaming for memory-efficient CSV ingestion.
  - Data lake organized in the highly compressed **Parquet** format.
- **Transformations (dbt + DuckDB):**
  - **DuckDB** acts as the in-process analytics engine reading from Parquet files on the fly.
  - **dbt-core** paired with the `dbt-duckdb` adapter handles transformations (Star Schema dimensional modeling).

```mermaid
graph LR
    subgraph Data Sources
        API[OpenF1 API]
        CSV[Kaggle CSV]
    end

    subgraph Data Lake (Local)
        Bronze[Bronze Layer<br/>Raw Parquet]
        Silver[Silver Layer<br/>Clean Parquet]
    end
    
    subgraph Analytics (DuckDB)
        Gold[(Gold Layer<br/>f1_data.duckdb)]
    end

    API --> |src/ingest_bronze.py| Bronze
    CSV --> |DuckDB Streaming| Bronze
    Bronze --> |src/transform_silver.py| Silver
    Silver --> |dbt run| Gold
    Gold --> PBI[Power BI]
```

## 🚀 How to run locally

### 1. Environment Setup
Ensure you are using Python 3.11+.
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Running the Pipeline
The pipeline executes sequentially in 3 major steps:
```bash
# STEP 1: Bronze Ingestion (Fetch API and stream CSV using memory-efficient DuckDB)
python src/ingest_bronze.py

# STEP 2: Silver Transformation (Clean, deduplicate, and enforce types onto Parquet)
python src/transform_silver.py

# STEP 3: Gold Transformation via dbt (Run models feeding f1_data.duckdb)
cd f1_dbt
dbt deps
dbt run
```

## 📊 Power BI Connection

The pipeline yields a database artifact `f1_data.duckdb`. You will need the ODBC driver for DuckDB to interact with Power BI natively.

1. Download the latest installer from the [DuckDB GitHub Repository (ODBC release)](https://github.com/duckdb/duckdb/releases) and install it.
2. Open **ODBC Data Sources (64-bit)** on Windows.
3. Configure a new User/System DSN using the **DuckDB Driver**. Provide a name (e.g., `F1_DuckDB_Source`) and point it to the absolute path of `f1_data.duckdb`.
4. Open Power BI Desktop, navigate to **Get Data ➔ ODBC ➔ select your configured DSN**, and import all `dim_*` and `fact_*` tables.

### 💡 Core DAX Measures
```dax
Total Points = SUM('fact_race_results'[points])

Avg Lap Time Loss = AVERAGE('fact_lap_times'[time_loss])

Total Wins = CALCULATE(COUNTROWS('fact_race_results'), 'fact_race_results'[is_winner] = TRUE())
```
