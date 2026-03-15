# Data Engineering Portfolio

A collection of production-style ETL pipelines and analytics projects built with Python, PostgreSQL, and Streamlit. Each project demonstrates a different data engineering pattern — from live API extraction to dimensional modeling to interactive dashboards.

---

## Projects

### 1. Fake Store API — End-to-End Analytics Pipeline
**Repo:** [fakestore-api-etl](https://github.com/aziscript/fakestore-api-etl)

A full-stack data project that extracts live e-commerce data from a REST API, loads it into a PostgreSQL warehouse, and serves it through an interactive Streamlit dashboard.

**Stack:** Python · REST API · PostgreSQL · Streamlit · Plotly

**Highlights:**
- Live HTTP extraction from `fakestoreapi.com` with retry logic and exponential backoff
- Nested JSON flattening — multi-level address and geolocation objects
- Star schema warehouse: `dim_user`, `dim_product`, `dim_date`, `fact_orders`
- 4-page Streamlit dashboard: Overview, Products, Customers, Orders
- Geolocation map of customers using latitude/longitude from API
- Discount impact analysis and rating vs revenue scatter plot

---

### 2. E-Commerce Sales Pipeline with RFM Segmentation
**Repo:** [ecommerce-etl-pipeline](https://github.com/aziscript/ecommerce-etl-pipeline)

An ETL pipeline that generates a realistic e-commerce dataset using Faker, transforms it into a star schema, and adds a customer RFM segmentation model on top.

**Stack:** Python · Faker · Pandas · PostgreSQL · SQLAlchemy

**Highlights:**
- Generates 2,000 customers, 500 products, and 20,000 orders with realistic distributions
- 4-table star schema: `dim_customer`, `dim_product`, `dim_date`, `fact_sales`
- Revenue, COGS, gross profit, and discount calculations per order line
- RFM scoring model (Recency, Frequency, Monetary) with 4 segments:
  - Champions, Loyal, At Risk, Lost
- Automated data quality checks before every load
- Idempotent upsert loading — safe to re-run daily

---

### 3. Stock Price History Warehouse
**Repo:** [stock-price-warehouse](https://github.com/aziscript/stock-price-warehouse)

A financial data warehouse that pulls 6 years of OHLCV data for 10 major US stocks from Yahoo Finance, computes technical indicators, and loads into a partitioned PostgreSQL warehouse.

**Stack:** Python · yfinance · Pandas · PostgreSQL · SQLAlchemy

**Tickers:** AAPL · MSFT · GOOGL · AMZN · TSLA · META · NVDA · JPM · JNJ · XOM

**Highlights:**
- Extracts 15,570 rows of real daily market data (2020–present)
- Computes 7-day, 30-day, 90-day moving averages per ticker
- Bollinger Bands (20-day, 2 standard deviations)
- RSI-14 using Wilder's smoothing method
- Golden cross signal detection (7d MA crossing 30d MA)
- Volume anomaly detection (days with 2x average volume)
- STRONG_BUY / STRONG_SELL signals combining RSI + Bollinger Bands
- Data quality report: gap detection, high/low validation, zero-volume checks

---

## Skills Demonstrated

| Skill | Projects |
|-------|----------|
| REST API extraction with retry logic | Fake Store |
| Nested JSON parsing and flattening | Fake Store |
| Synthetic data generation (Faker) | E-Commerce |
| Star schema dimensional modeling | All 3 |
| Surrogate key management | All 3 |
| Upsert / idempotent loading | All 3 |
| Financial technical indicators (RSI, Bollinger Bands) | Stock Price |
| Customer segmentation (RFM) | E-Commerce |
| Automated data quality checks | All 3 |
| Streamlit dashboard development | Fake Store |
| Plotly interactive charts | Fake Store |
| PostgreSQL warehouse design | All 3 |
| Python (Pandas, SQLAlchemy, requests) | All 3 |
| Git version control | All 3 |

---

## Setup

Each project has its own folder, virtual environment, and README. To run any pipeline:

```bash
# 1. Navigate to the project folder
cd C:\projects\<project_folder>

# 2. Activate virtual environment
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure database
copy .env.example .env
# Edit .env with your PostgreSQL credentials

# 5. Run the ETL
python etl.py

# 6. (Fake Store only) Launch the dashboard
streamlit run dashboard.py
```

---

## Contact

**GitHub:** [github.com/aziscript](https://github.com/aziscript)
**Email:** waltneku@gmail.com
