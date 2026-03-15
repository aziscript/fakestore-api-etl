"""
Fake Store API ETL Pipeline
============================
Extract:   REST API from fakestoreapi.com
           - GET /products  (20 products)
           - GET /users     (10 users)
           - GET /carts     (20 carts with line items)

Transform: Parse nested JSON, build star schema dimensions,
           simulate order history by cycling carts over 2 years

Load:      PostgreSQL warehouse
           - dim_date
           - dim_user
           - dim_product
           - fact_orders
"""

import os
import logging
import time
import random
from datetime import datetime, timedelta

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres123@localhost:5432/fakestore_warehouse"
)

BASE_URL    = "https://fakestoreapi.com"
START_DATE  = datetime(2023, 1, 1)
END_DATE    = datetime(2026, 3, 14)

# Number of times to cycle through the 20 carts to simulate order history
CART_CYCLES = 100

# ── Extract ───────────────────────────────────────────────────────────────────

def fetch(endpoint: str, retries: int = 3) -> list:
    """Fetch a JSON endpoint with retry logic."""
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            log.info(f"  {endpoint}: {len(data)} records")
            return data
        except requests.RequestException as e:
            log.warning(f"  Attempt {attempt} failed for {endpoint}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)   # exponential backoff
    log.error(f"  All retries failed for {endpoint}")
    return []


def extract() -> tuple[list, list, list]:
    """Extract all three endpoints from the Fake Store API."""
    log.info("=== Extracting from fakestoreapi.com ===")
    products = fetch("/products")
    users    = fetch("/users")
    carts    = fetch("/carts")
    return products, users, carts

# ── Transform ─────────────────────────────────────────────────────────────────

def build_dim_date(start: datetime, end: datetime) -> pd.DataFrame:
    """Generate a complete date dimension table."""
    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"full_date": dates})
    df["date_key"]       = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"]           = df["full_date"].dt.year
    df["quarter"]        = df["full_date"].dt.quarter
    df["month"]          = df["full_date"].dt.month
    df["month_name"]     = df["full_date"].dt.strftime("%B")
    df["week_of_year"]   = df["full_date"].dt.isocalendar().week.astype(int)
    df["day_of_week"]    = df["full_date"].dt.dayofweek
    df["day_name"]       = df["full_date"].dt.strftime("%A")
    df["is_weekend"]     = df["day_of_week"] >= 5
    df["is_month_start"] = df["full_date"].dt.is_month_start
    df["is_month_end"]   = df["full_date"].dt.is_month_end
    df["is_quarter_end"] = df["full_date"].dt.is_quarter_end
    return df


def build_dim_user(users: list) -> pd.DataFrame:
    """
    Flatten nested user JSON into a clean dimension table.
    API returns: id, email, username, name{firstname, lastname},
                 address{city, street, number, zipcode, geolocation{lat, long}}, phone
    """
    rows = []
    for u in users:
        rows.append({
            "user_id":    u["id"],
            "username":   u.get("username", ""),
            "email":      u.get("email", "").lower().strip(),
            "first_name": u.get("name", {}).get("firstname", ""),
            "last_name":  u.get("name", {}).get("lastname", ""),
            "phone":      u.get("phone", ""),
            "city":       u.get("address", {}).get("city", ""),
            "street":     u.get("address", {}).get("street", ""),
            "zipcode":    u.get("address", {}).get("zipcode", ""),
            "lat":        float(u.get("address", {}).get("geolocation", {}).get("lat", 0) or 0),
            "lng":        float(u.get("address", {}).get("geolocation", {}).get("long", 0) or 0),
        })
    df = pd.DataFrame(rows)
    df.insert(0, "user_key", df.index + 1)
    log.info(f"dim_user: {len(df)} rows")
    return df


def build_dim_product(products: list) -> pd.DataFrame:
    """
    Flatten nested product JSON into a clean dimension table.
    API returns: id, title, price, description, category,
                 image, rating{rate, count}
    """
    rows = []
    for p in products:
        rows.append({
            "product_id":   p["id"],
            "title":        p.get("title", ""),
            "price":        round(float(p.get("price", 0)), 2),
            "category":     p.get("category", "").title(),
            "description":  p.get("description", "")[:300],   # truncate long text
            "image_url":    p.get("image", ""),
            "rating":       p.get("rating", {}).get("rate", None),
            "rating_count": p.get("rating", {}).get("count", None),
        })
    df = pd.DataFrame(rows)
    df.insert(0, "product_key", df.index + 1)
    log.info(f"dim_product: {len(df)} rows")
    return df


def build_fact_orders(
    carts: list,
    dim_user: pd.DataFrame,
    dim_product: pd.DataFrame,
    cycles: int,
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """
    Build fact_orders by expanding carts into line items.
    Cycles through the 20 carts multiple times to simulate
    order history across the date range.
    """
    total_days  = (end - start).days
    rows        = []
    order_id    = 1

    # Build lookup maps for surrogate keys
    user_map    = dict(zip(dim_user["user_id"],    dim_user["user_key"]))
    product_map = dict(zip(dim_product["product_id"], dim_product["product_key"]))
    price_map   = dict(zip(dim_product["product_id"], dim_product["price"]))

    log.info(f"Building fact_orders: {cycles} cycles × {len(carts)} carts")

    for cycle in range(cycles):
        for cart in carts:
            # Spread orders evenly across the date range
            offset     = int((cycle * len(carts) + carts.index(cart)) /
                              (cycles * len(carts)) * total_days)
            order_date = start + timedelta(days=offset)
            date_key   = int(order_date.strftime("%Y%m%d"))

            user_id    = cart.get("userId")
            user_key   = user_map.get(user_id)
            if not user_key:
                continue

            # Apply a random discount per order (0%, 5%, 10%, 15%, 20%)
            discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])

            for item in cart.get("products", []):
                product_id  = item.get("productId")
                quantity    = item.get("quantity", 1)
                product_key = product_map.get(product_id)
                unit_price  = price_map.get(product_id)

                if not product_key or unit_price is None:
                    continue

                discount_amt = round(unit_price * discount_pct / 100, 2)
                net_price    = round(unit_price - discount_amt, 2)
                revenue      = round(net_price * quantity, 2)

                rows.append({
                    "order_id":      order_id,
                    "date_key":      date_key,
                    "user_key":      int(user_key),
                    "product_key":   int(product_key),
                    "order_date":    order_date.date(),
                    "quantity":      quantity,
                    "unit_price":    unit_price,
                    "discount_pct":  discount_pct,
                    "discount_amt":  discount_amt,
                    "net_price":     net_price,
                    "revenue":       revenue,
                })

            order_id += 1

    fact = pd.DataFrame(rows)
    log.info(f"fact_orders: {len(fact)} rows, {fact['order_id'].nunique()} orders")
    return fact


def run_quality_checks(fact: pd.DataFrame) -> None:
    """Run data quality checks before loading."""
    print("\n" + "=" * 60)
    print("DATA QUALITY REPORT")
    print("=" * 60)

    checks = [
        ("Negative revenue rows",    len(fact[fact["revenue"] < 0]) == 0),
        ("Zero quantity rows",       len(fact[fact["quantity"] == 0]) == 0),
        ("Missing user keys",        fact["user_key"].isna().sum() == 0),
        ("Missing product keys",     fact["product_key"].isna().sum() == 0),
        ("Missing date keys",        fact["date_key"].isna().sum() == 0),
    ]

    for label, passed in checks:
        status = "[OK]" if passed else "[WARN]"
        print(f"{status} {label}")

    print(f"\nSummary:")
    print(f"  Total line items:  {len(fact):,}")
    print(f"  Total orders:      {fact['order_id'].nunique():,}")
    print(f"  Unique users:      {fact['user_key'].nunique():,}")
    print(f"  Unique products:   {fact['product_key'].nunique():,}")
    print(f"  Total revenue:     ${fact['revenue'].sum():,.2f}")
    print(f"  Avg order value:   ${fact.groupby('order_id')['revenue'].sum().mean():,.2f}")
    print(f"  Date range:        {fact['order_date'].min()} → {fact['order_date'].max()}")
    print("=" * 60 + "\n")

# ── Schema ────────────────────────────────────────────────────────────────────

def create_schema(engine) -> None:
    """Create warehouse tables if they don't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key        INTEGER PRIMARY KEY,
        full_date       DATE NOT NULL,
        year            SMALLINT,
        quarter         SMALLINT,
        month           SMALLINT,
        month_name      VARCHAR(10),
        week_of_year    SMALLINT,
        day_of_week     SMALLINT,
        day_name        VARCHAR(10),
        is_weekend      BOOLEAN,
        is_month_start  BOOLEAN,
        is_month_end    BOOLEAN,
        is_quarter_end  BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS dim_user (
        user_key    SERIAL PRIMARY KEY,
        user_id     INTEGER UNIQUE NOT NULL,
        username    VARCHAR(50),
        email       VARCHAR(100),
        first_name  VARCHAR(50),
        last_name   VARCHAR(50),
        phone       VARCHAR(30),
        city        VARCHAR(80),
        street      VARCHAR(100),
        zipcode     VARCHAR(20),
        lat         NUMERIC(10,6),
        lng         NUMERIC(10,6)
    );

    CREATE TABLE IF NOT EXISTS dim_product (
        product_key     SERIAL PRIMARY KEY,
        product_id      INTEGER UNIQUE NOT NULL,
        title           VARCHAR(200),
        price           NUMERIC(10,2),
        category        VARCHAR(80),
        description     TEXT,
        image_url       TEXT,
        rating          NUMERIC(3,1),
        rating_count    INTEGER
    );

    CREATE TABLE IF NOT EXISTS fact_orders (
        order_line_id   BIGSERIAL PRIMARY KEY,
        order_id        INTEGER,
        date_key        INTEGER     REFERENCES dim_date(date_key),
        user_key        INTEGER     REFERENCES dim_user(user_key),
        product_key     INTEGER     REFERENCES dim_product(product_key),
        order_date      DATE,
        quantity        SMALLINT,
        unit_price      NUMERIC(10,2),
        discount_pct    NUMERIC(5,2),
        discount_amt    NUMERIC(10,2),
        net_price       NUMERIC(10,2),
        revenue         NUMERIC(12,2)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_orders_date_key    ON fact_orders(date_key);
    CREATE INDEX IF NOT EXISTS idx_fact_orders_user_key    ON fact_orders(user_key);
    CREATE INDEX IF NOT EXISTS idx_fact_orders_product_key ON fact_orders(product_key);
    CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id    ON fact_orders(order_id);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    log.info("Schema created / verified")

# ── Load ──────────────────────────────────────────────────────────────────────

def load_dim_date(engine, df: pd.DataFrame) -> None:
    log.info(f"Loading dim_date: {len(df)} rows")
    df = df.copy()
    df["full_date"] = pd.to_datetime(df["full_date"]).dt.date
    df.to_sql("dim_date_staging", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_date (
                date_key, full_date, year, quarter, month, month_name,
                week_of_year, day_of_week, day_name, is_weekend,
                is_month_start, is_month_end, is_quarter_end
            )
            SELECT
                date_key, full_date::date, year, quarter, month, month_name,
                week_of_year, day_of_week, day_name, is_weekend,
                is_month_start, is_month_end, is_quarter_end
            FROM dim_date_staging
            ON CONFLICT (date_key) DO NOTHING
        """))
        conn.execute(text("DROP TABLE IF EXISTS dim_date_staging"))


def load_dim_user(engine, df: pd.DataFrame) -> None:
    log.info(f"Loading dim_user: {len(df)} rows")
    df.to_sql("dim_user_staging", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_user (
                user_key, user_id, username, email, first_name, last_name,
                phone, city, street, zipcode, lat, lng
            )
            SELECT
                user_key, user_id, username, email, first_name, last_name,
                phone, city, street, zipcode, lat, lng
            FROM dim_user_staging
            ON CONFLICT (user_id) DO UPDATE SET
                email      = EXCLUDED.email,
                city       = EXCLUDED.city,
                phone      = EXCLUDED.phone
        """))
        conn.execute(text("DROP TABLE IF EXISTS dim_user_staging"))


def load_dim_product(engine, df: pd.DataFrame) -> None:
    log.info(f"Loading dim_product: {len(df)} rows")
    df.to_sql("dim_product_staging", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_product (
                product_key, product_id, title, price, category,
                description, image_url, rating, rating_count
            )
            SELECT
                product_key, product_id, title, price, category,
                description, image_url, rating, rating_count
            FROM dim_product_staging
            ON CONFLICT (product_id) DO UPDATE SET
                price        = EXCLUDED.price,
                rating       = EXCLUDED.rating,
                rating_count = EXCLUDED.rating_count
        """))
        conn.execute(text("DROP TABLE IF EXISTS dim_product_staging"))


def load_fact_orders(engine, df: pd.DataFrame) -> None:
    log.info(f"Loading fact_orders: {len(df)} rows")
    df.to_sql("fact_orders_staging", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO fact_orders (
                order_id, date_key, user_key, product_key,
                order_date, quantity, unit_price, discount_pct,
                discount_amt, net_price, revenue
            )
            SELECT
                order_id, date_key, user_key, product_key,
                order_date, quantity, unit_price, discount_pct,
                discount_amt, net_price, revenue
            FROM fact_orders_staging
        """))
        conn.execute(text("DROP TABLE IF EXISTS fact_orders_staging"))
    log.info("fact_orders loaded")

# ── Orchestrate ───────────────────────────────────────────────────────────────

def run():
    log.info("=== Fake Store API ETL Pipeline starting ===")
    engine = create_engine(DB_URL)

    # 1. Schema
    create_schema(engine)

    # 2. Extract
    products, users, carts = extract()

    # 3. Build dimensions
    dim_date    = build_dim_date(START_DATE, END_DATE)
    dim_user    = build_dim_user(users)
    dim_product = build_dim_product(products)

    # 4. Build fact table
    fact = build_fact_orders(
        carts, dim_user, dim_product,
        cycles=CART_CYCLES,
        start=START_DATE,
        end=END_DATE
    )

    # 5. Quality checks
    run_quality_checks(fact)

    # 6. Load
    load_dim_date(engine, dim_date)
    load_dim_user(engine, dim_user)
    load_dim_product(engine, dim_product)
    load_fact_orders(engine, fact)

    log.info("=== Pipeline complete ===")


if __name__ == "__main__":
    run()
