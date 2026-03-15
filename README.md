# Fake Store API ETL Pipeline

A production-style ETL pipeline that extracts live e-commerce data from the [Fake Store API](https://fakestoreapi.com), transforms it into a star schema, and loads it into a PostgreSQL data warehouse.

---

## What makes this different

Unlike pipelines that generate mock data locally, this pipeline **extracts from a live REST API** — demonstrating real-world skills:

- HTTP request handling with retry logic and exponential backoff
- Nested JSON parsing and flattening
- Handling API rate limits gracefully
- Building a warehouse from external data you don't control

---

## Architecture

```
fakestoreapi.com REST API
    │
    ├── GET /products  → 20 products (title, price, category, rating)
    ├── GET /users     → 10 users (name, email, nested address)
    └── GET /carts     → 20 carts (user + product quantities)
         │
         ▼
    [ TRANSFORM ]
         ├── dim_date     — Full date dimension (2023–2026)
         ├── dim_user     — Flattened user profiles + geolocation
         ├── dim_product  — Product catalog with ratings
         └── fact_orders  — Order line items with revenue metrics
         │
         ▼
    [ LOAD ] → PostgreSQL warehouse (star schema)
```

---

## Star Schema

### dim_user
Flattened from nested API response — name, email, address (city, street, zipcode), geolocation (lat, lng).

### dim_product
| Column | Description |
|--------|-------------|
| product_id | Source API ID |
| title | Full product name |
| price | Unit price |
| category | electronics / clothing / jewelery / home |
| description | Product description (truncated to 300 chars) |
| image_url | Product image URL |
| rating | Average customer rating (0–5) |
| rating_count | Number of ratings |

### fact_orders
| Column | Description |
|--------|-------------|
| order_id | Order grouping key |
| date_key, user_key, product_key | Dimension FKs |
| quantity | Units ordered |
| unit_price | Original price from API |
| discount_pct, discount_amt | Simulated discount |
| net_price, revenue | After-discount figures |

---

## Setup

### 1. Create the database
```sql
CREATE DATABASE fakestore_warehouse;
```

### 2. Install dependencies
```bat
cd C:\projects\fakestore_etl
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure .env
```bat
copy .env.example .env
```

### 4. Run the pipeline
```bat
python etl.py
```

Fetches live data from the API, cycles carts 100 times across a 3-year date range, and loads ~4,000 order line items.

---

## Analysis Queries

See `queries.sql` for 8 ready-to-run queries:

1. Monthly revenue trend
2. Revenue by product category
3. Top products by revenue vs rating
4. Customer lifetime value
5. Discount impact analysis
6. Best rated vs best selling products
7. Year over year revenue
8. Warehouse row counts

---

## Skills Demonstrated

- Live REST API extraction with retry logic
- Nested JSON flattening (multi-level address objects)
- Exponential backoff for resilient API calls
- Star schema with geolocation data
- Idempotent upsert loading
- Data quality checks

---

## Extend with Claude Code

Open this folder in Claude Code and try:

- *"Add a product recommendation query showing which products are bought together most often"*
- *"Add geolocation mapping — cluster users by lat/lng coordinates"*
- *"Add a price sensitivity analysis — do cheaper products sell more units?"*
- *"Fetch from a second API endpoint and join the data"*
