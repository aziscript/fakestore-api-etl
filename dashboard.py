"""
Fake Store Analytics Dashboard
================================
Streamlit dashboard connected to the fakestore_warehouse PostgreSQL database.
Pages:
    1. Overview      — KPI cards + revenue trend
    2. Products      — rankings, category breakdown, rating vs revenue
    3. Customers     — order history, top customers, city map
    4. Orders        — recent orders, discount analysis
"""

import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Fake Store Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0f1117; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #1a1d2e;
        border-right: 1px solid #2d3149;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background-color: #1a1d2e;
        border: 1px solid #2d3149;
        border-radius: 10px;
        padding: 1rem;
    }

    /* Headers */
    h1, h2, h3 { color: #e2e8f0 !important; }

    /* Divider */
    hr { border-color: #2d3149; }
</style>
""", unsafe_allow_html=True)

# ── Database connection ───────────────────────────────────────────────────────

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres123@localhost:5432/fakestore_warehouse"
)

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame. Cached for 5 minutes."""
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)

# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_kpis():
    return query("""
        SELECT
            COUNT(DISTINCT order_id)            AS total_orders,
            COUNT(*)                            AS total_line_items,
            ROUND(SUM(revenue)::numeric, 2)     AS total_revenue,
            ROUND(AVG(revenue)::numeric, 2)     AS avg_line_revenue,
            ROUND(SUM(discount_amt)::numeric,2) AS total_discounts
        FROM fact_orders
    """)


@st.cache_data(ttl=300)
def load_monthly_revenue():
    return query("""
        SELECT
            d.year,
            d.month,
            d.month_name,
            CONCAT(d.year, '-', LPAD(d.month::text, 2, '0')) AS period,
            ROUND(SUM(f.revenue)::numeric, 2)   AS revenue,
            COUNT(DISTINCT f.order_id)          AS orders
        FROM fact_orders f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
    """)


@st.cache_data(ttl=300)
def load_category_revenue():
    return query("""
        SELECT
            p.category,
            COUNT(DISTINCT f.order_id)              AS orders,
            SUM(f.quantity)                         AS units_sold,
            ROUND(SUM(f.revenue)::numeric, 2)       AS revenue,
            ROUND(AVG(p.rating)::numeric, 2)        AS avg_rating
        FROM fact_orders f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY revenue DESC
    """)


@st.cache_data(ttl=300)
def load_top_products():
    return query("""
        SELECT
            p.title,
            p.category,
            p.price,
            p.rating,
            p.rating_count,
            SUM(f.quantity)                         AS units_sold,
            ROUND(SUM(f.revenue)::numeric, 2)       AS revenue,
            ROUND(AVG(f.discount_pct)::numeric, 1)  AS avg_discount
        FROM fact_orders f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.title, p.category, p.price, p.rating, p.rating_count
        ORDER BY revenue DESC
    """)


@st.cache_data(ttl=300)
def load_customers():
    return query("""
        SELECT
            u.user_id,
            u.first_name || ' ' || u.last_name     AS customer_name,
            u.email,
            u.city,
            u.lat,
            u.lng,
            COUNT(DISTINCT f.order_id)              AS total_orders,
            SUM(f.quantity)                         AS units_bought,
            ROUND(SUM(f.revenue)::numeric, 2)       AS lifetime_value,
            MAX(f.order_date)                       AS last_order
        FROM fact_orders f
        JOIN dim_user u ON f.user_key = u.user_key
        GROUP BY u.user_id, customer_name, u.email, u.city, u.lat, u.lng
        ORDER BY lifetime_value DESC
    """)


@st.cache_data(ttl=300)
def load_recent_orders():
    return query("""
        SELECT
            f.order_id,
            f.order_date,
            u.first_name || ' ' || u.last_name     AS customer,
            p.title                                 AS product,
            p.category,
            f.quantity,
            f.unit_price,
            f.discount_pct,
            f.revenue
        FROM fact_orders f
        JOIN dim_user    u ON f.user_key    = u.user_key
        JOIN dim_product p ON f.product_key = p.product_key
        ORDER BY f.order_date DESC, f.order_id DESC
        LIMIT 200
    """)


@st.cache_data(ttl=300)
def load_discount_analysis():
    return query("""
        SELECT
            f.discount_pct,
            COUNT(DISTINCT f.order_id)              AS orders,
            SUM(f.quantity)                         AS units_sold,
            ROUND(SUM(f.discount_amt)::numeric, 2)  AS total_discounts,
            ROUND(SUM(f.revenue)::numeric, 2)       AS net_revenue
        FROM fact_orders f
        GROUP BY f.discount_pct
        ORDER BY f.discount_pct
    """)


@st.cache_data(ttl=300)
def load_rating_vs_revenue():
    return query("""
        SELECT
            p.title,
            p.category,
            p.rating,
            p.price,
            SUM(f.quantity)                     AS units_sold,
            ROUND(SUM(f.revenue)::numeric, 2)   AS revenue
        FROM fact_orders f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.title, p.category, p.rating, p.price
    """)

# ── Sidebar navigation ────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🛒 Fake Store")
    st.markdown("### Analytics Dashboard")
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["📊 Overview", "📦 Products", "👥 Customers", "🧾 Orders"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    st.caption("Data from fakestoreapi.com")
    st.caption("Warehouse: fakestore_warehouse")

# ── Page: Overview ────────────────────────────────────────────────────────────

if page == "📊 Overview":
    st.title("📊 Store Overview")
    st.markdown("---")

    kpis        = load_kpis().iloc[0]
    monthly     = load_monthly_revenue()
    categories  = load_category_revenue()

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Revenue",   f"${kpis['total_revenue']:,.0f}")
    with col2:
        st.metric("Total Orders",    f"{kpis['total_orders']:,}")
    with col3:
        st.metric("Avg Line Revenue", f"${kpis['avg_line_revenue']:,.2f}")
    with col4:
        st.metric("Total Discounts", f"${kpis['total_discounts']:,.0f}")

    st.markdown("---")

    # Revenue trend
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Monthly Revenue Trend")
        fig = px.line(
            monthly,
            x="period",
            y="revenue",
            markers=True,
            color_discrete_sequence=["#6366f1"]
        )
        fig.update_layout(
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            xaxis=dict(showgrid=False, tickangle=45),
            yaxis=dict(showgrid=True, gridcolor="#2d3149"),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        fig.update_traces(line_width=2.5)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Revenue by Category")
        fig2 = px.pie(
            categories,
            values="revenue",
            names="category",
            color_discrete_sequence=["#6366f1", "#8b5cf6", "#a78bfa", "#c4b5fd"]
        )
        fig2.update_layout(
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", y=-0.2)
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Category breakdown table
    st.subheader("Category Performance")
    st.dataframe(
        categories.rename(columns={
            "category": "Category",
            "orders": "Orders",
            "units_sold": "Units Sold",
            "revenue": "Revenue ($)",
            "avg_rating": "Avg Rating"
        }),
        use_container_width=True,
        hide_index=True
    )

# ── Page: Products ────────────────────────────────────────────────────────────

elif page == "📦 Products":
    st.title("📦 Product Analytics")
    st.markdown("---")

    products = load_top_products()
    rating_rev = load_rating_vs_revenue()

    # Top products bar chart
    st.subheader("Top Products by Revenue")
    top_n = st.slider("Show top N products", 5, 20, 10)
    top = products.head(top_n)

    fig = px.bar(
        top,
        x="revenue",
        y="title",
        orientation="h",
        color="category",
        color_discrete_sequence=px.colors.qualitative.Vivid,
        text="revenue"
    )
    fig.update_layout(
        plot_bgcolor="#1a1d2e",
        paper_bgcolor="#1a1d2e",
        font_color="#e2e8f0",
        yaxis=dict(autorange="reversed", showgrid=False),
        xaxis=dict(showgrid=True, gridcolor="#2d3149"),
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=True,
        height=400
    )
    fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Rating vs Revenue scatter
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Rating vs Revenue")
        fig2 = px.scatter(
            rating_rev,
            x="rating",
            y="revenue",
            size="units_sold",
            color="category",
            hover_name="title",
            color_discrete_sequence=px.colors.qualitative.Vivid
        )
        fig2.update_layout(
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            xaxis=dict(showgrid=True, gridcolor="#2d3149"),
            yaxis=dict(showgrid=True, gridcolor="#2d3149"),
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        st.subheader("Units Sold by Category")
        cat_rev = load_category_revenue()
        fig3 = px.bar(
            cat_rev,
            x="category",
            y="units_sold",
            color="category",
            color_discrete_sequence=px.colors.qualitative.Vivid
        )
        fig3.update_layout(
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#2d3149"),
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Full product table
    st.markdown("---")
    st.subheader("All Products")
    st.dataframe(
        products.rename(columns={
            "title": "Product", "category": "Category",
            "price": "Price ($)", "rating": "Rating",
            "rating_count": "Reviews", "units_sold": "Units Sold",
            "revenue": "Revenue ($)", "avg_discount": "Avg Discount %"
        }),
        use_container_width=True,
        hide_index=True
    )

# ── Page: Customers ───────────────────────────────────────────────────────────

elif page == "👥 Customers":
    st.title("👥 Customer Analytics")
    st.markdown("---")

    customers = load_customers()

    # Top customer KPIs
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Customers", len(customers))
    with col2:
        st.metric("Top Customer LTV", f"${customers['lifetime_value'].max():,.2f}")
    with col3:
        st.metric("Avg Customer LTV", f"${customers['lifetime_value'].mean():,.2f}")

    st.markdown("---")

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("Customer Lifetime Value")
        fig = px.bar(
            customers,
            x="customer_name",
            y="lifetime_value",
            color="lifetime_value",
            color_continuous_scale="Purples",
            text="lifetime_value"
        )
        fig.update_layout(
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#2d3149"),
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False
        )
        fig.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Orders per Customer")
        fig2 = px.bar(
            customers,
            x="customer_name",
            y="total_orders",
            color="total_orders",
            color_continuous_scale="Blues"
        )
        fig2.update_layout(
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#2d3149"),
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Customer map
    st.subheader("Customer Locations")
    map_data = customers[["lat", "lng", "customer_name", "lifetime_value"]].copy()
    map_data = map_data.rename(columns={"lat": "latitude", "lng": "longitude"})
    map_data = map_data.dropna(subset=["latitude", "longitude"])
    map_data = map_data[
        (map_data["latitude"].between(-90, 90)) &
        (map_data["longitude"].between(-180, 180))
    ]
    if not map_data.empty:
        st.map(map_data, zoom=3)
    else:
        st.info("No valid geolocation data available.")

    # Full customer table
    st.markdown("---")
    st.subheader("All Customers")
    st.dataframe(
        customers.rename(columns={
            "customer_name": "Name", "email": "Email", "city": "City",
            "total_orders": "Orders", "units_bought": "Units",
            "lifetime_value": "LTV ($)", "last_order": "Last Order"
        }).drop(columns=["user_id", "lat", "lng"]),
        use_container_width=True,
        hide_index=True
    )

# ── Page: Orders ──────────────────────────────────────────────────────────────

elif page == "🧾 Orders":
    st.title("🧾 Order Analytics")
    st.markdown("---")

    orders   = load_recent_orders()
    discounts = load_discount_analysis()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Discount Impact")
        fig = px.bar(
            discounts,
            x="discount_pct",
            y="net_revenue",
            color="orders",
            color_continuous_scale="Purples",
            labels={"discount_pct": "Discount %", "net_revenue": "Net Revenue ($)"},
            text="orders"
        )
        fig.update_layout(
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="#2d3149"),
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False
        )
        fig.update_traces(texttemplate="%{text} orders", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Revenue vs Discounts Given")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=discounts["discount_pct"],
            y=discounts["net_revenue"],
            name="Net Revenue",
            marker_color="#6366f1"
        ))
        fig2.add_trace(go.Bar(
            x=discounts["discount_pct"],
            y=discounts["total_discounts"],
            name="Discounts Given",
            marker_color="#f43f5e"
        ))
        fig2.update_layout(
            barmode="group",
            plot_bgcolor="#1a1d2e",
            paper_bgcolor="#1a1d2e",
            font_color="#e2e8f0",
            xaxis=dict(showgrid=False, title="Discount %"),
            yaxis=dict(showgrid=True, gridcolor="#2d3149"),
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", y=1.1)
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Filters
    st.markdown("---")
    st.subheader("Recent Orders")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        categories = ["All"] + sorted(orders["category"].unique().tolist())
        selected_cat = st.selectbox("Filter by category", categories)
    with col_f2:
        customers_list = ["All"] + sorted(orders["customer"].unique().tolist())
        selected_cust = st.selectbox("Filter by customer", customers_list)

    filtered = orders.copy()
    if selected_cat != "All":
        filtered = filtered[filtered["category"] == selected_cat]
    if selected_cust != "All":
        filtered = filtered[filtered["customer"] == selected_cust]

    st.dataframe(
        filtered.rename(columns={
            "order_id": "Order ID", "order_date": "Date",
            "customer": "Customer", "product": "Product",
            "category": "Category", "quantity": "Qty",
            "unit_price": "Unit Price", "discount_pct": "Discount %",
            "revenue": "Revenue ($)"
        }),
        use_container_width=True,
        hide_index=True
    )
