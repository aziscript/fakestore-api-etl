-- ============================================================
-- Fake Store Warehouse — Analysis Queries
-- Run in DBeaver after the ETL loads
-- ============================================================


-- 1. Total revenue by month
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    SUM(f.quantity)                     AS units_sold,
    ROUND(SUM(f.revenue)::numeric, 2)   AS total_revenue,
    ROUND(AVG(f.revenue)::numeric, 2)   AS avg_line_revenue
FROM fact_orders f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;


-- 2. Revenue by product category
SELECT
    p.category,
    COUNT(DISTINCT f.order_id)          AS orders,
    SUM(f.quantity)                     AS units_sold,
    ROUND(SUM(f.revenue)::numeric, 2)   AS total_revenue,
    ROUND(AVG(f.unit_price)::numeric, 2) AS avg_price,
    ROUND(AVG(p.rating)::numeric, 2)    AS avg_rating
FROM fact_orders f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;


-- 3. Top products by revenue
SELECT
    p.product_id,
    p.title,
    p.category,
    p.price,
    p.rating,
    p.rating_count,
    SUM(f.quantity)                     AS units_sold,
    ROUND(SUM(f.revenue)::numeric, 2)   AS total_revenue,
    ROUND(AVG(f.discount_pct)::numeric, 1) AS avg_discount_pct
FROM fact_orders f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.product_id,
    p.title,
    p.category,
    p.price,
    p.rating,
    p.rating_count
ORDER BY total_revenue DESC;


-- 4. Customer order summary
SELECT
    u.user_id,
    u.first_name || ' ' || u.last_name  AS customer_name,
    u.email,
    u.city,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    SUM(f.quantity)                     AS units_bought,
    ROUND(SUM(f.revenue)::numeric, 2)   AS lifetime_value,
    MIN(f.order_date)                   AS first_order,
    MAX(f.order_date)                   AS last_order
FROM fact_orders f
JOIN dim_user u
    ON f.user_key = u.user_key
GROUP BY
    u.user_id,
    customer_name,
    u.email,
    u.city
ORDER BY lifetime_value DESC;


-- 5. Discount impact analysis
SELECT
    f.discount_pct,
    COUNT(DISTINCT f.order_id)              AS orders,
    SUM(f.quantity)                         AS units_sold,
    ROUND(SUM(f.discount_amt)::numeric, 2)  AS total_discount_given,
    ROUND(SUM(f.revenue)::numeric, 2)       AS net_revenue
FROM fact_orders f
GROUP BY f.discount_pct
ORDER BY f.discount_pct;


-- 6. Best rated products vs revenue
-- Do higher-rated products actually sell more?
SELECT
    p.title,
    p.category,
    p.rating,
    p.rating_count,
    SUM(f.quantity)                     AS units_sold,
    ROUND(SUM(f.revenue)::numeric, 2)   AS total_revenue
FROM fact_orders f
JOIN dim_product p
    ON f.product_key = p.product_key
GROUP BY
    p.title,
    p.category,
    p.rating,
    p.rating_count
ORDER BY p.rating DESC;


-- 7. Year over year revenue comparison
SELECT
    d.year,
    ROUND(SUM(f.revenue)::numeric, 2)       AS total_revenue,
    COUNT(DISTINCT f.order_id)              AS total_orders,
    ROUND(AVG(
        f.revenue
    )::numeric, 2)                          AS avg_order_revenue
FROM fact_orders f
JOIN dim_date d
    ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;


-- 8. Warehouse row counts
SELECT 'dim_date'    AS table_name, COUNT(*) AS rows FROM dim_date
UNION ALL
SELECT 'dim_user',                  COUNT(*)          FROM dim_user
UNION ALL
SELECT 'dim_product',               COUNT(*)          FROM dim_product
UNION ALL
SELECT 'fact_orders',               COUNT(*)          FROM fact_orders;
