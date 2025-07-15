import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ---- CONFIG ----
DB_URI = st.secrets["DB_URI"]
engine = create_engine(DB_URI)

# ---- PAGE SETUP ----
# st.set_page_config(page_title="Clip Analsis", layout="wide")
# st.title("🧠 Trade Log Dashboard - Dynamic Query Builder")

# ---- WIDGETS ----
st.sidebar.header("Controls -%")

query_type = st.sidebar.selectbox("Query Mode", ["Simple Aggregation", "VWAP by Side", "VWAP Buy-Sell Delta"])

# 1. Date range picker
start_date = st.sidebar.date_input("Start Date", datetime.today() - timedelta(days=7))
end_date = st.sidebar.date_input("End Date", datetime.today())

# 2. Instrument filter (optional multi-select) ###
with engine.connect() as conn:  ###
    instruments = pd.read_sql("SELECT DISTINCT instrumentname FROM public.log_big_clips ORDER BY instrumentname", conn)
selected_instruments = st.sidebar.multiselect("Instruments", instruments['instrumentname'].tolist())

# ---- SIMPLE QUERY WIDGETS ----
if query_type == "Simple Aggregate":
    side = st.sidebar.radio("Direction", options=["All", "Buy (B)", "Sell (S)"])
    agg_type = st.sidebar.selectbox("Aggregate Type", ["SUM", "AVG", "COUNT", "VWAP"])
    group_by = st.sidebar.selectbox("Group By", [
        "DATE(local_datetime)",
        "instrumentname",
        "direction",
        "EXTRACT(HOUR FROM local_datetime)"
    ])
    top_n = st.sidebar.slider("Top N Results", 1, 100, 10)
    chart_type = st.sidebar.selectbox("Chart Type", ["Table", "Bar Chart", "Line Chart", "Histogram"])
    show_sql = st.sidebar.checkbox("Show Generated SQL")

    def build_query():
        where_clauses = ["local_datetime BETWEEN :start_date AND :end_date"]
        params = {"start_date": str(start_date), "end_date": str(end_date + timedelta(days=1))}

        if selected_instruments:
            where_clauses.append("instrumentname = ANY(:instruments)")
            params["instruments"] = selected_instruments

        if side == "Buy (B)":
            where_clauses.append("direction = 'B'")
        elif side == "Sell (S)":
            where_clauses.append("direction = 'S'")

        if agg_type == "VWAP":
            select_clause = f"{group_by} AS group_field, SUM(quantity * price)::numeric / NULLIF(SUM(quantity), 0) AS metric"
        elif agg_type == "COUNT":
            select_clause = f"{group_by} AS group_field, COUNT(*) AS metric"
        else:
            target = "price" if agg_type == "AVG" else "quantity"
            select_clause = f"{group_by} AS group_field, {agg_type}({target}) AS metric"

        sql = f"""
            SELECT {select_clause}
            FROM public.log_big_clips
            WHERE {' AND '.join(where_clauses)}
            GROUP BY {group_by}
            ORDER BY metric DESC
            LIMIT {top_n}
        """

        return sql, params

    query, query_params = build_query()

elif query_type == "VWAP by Side":
    show_sql = True  # Always show SQL for advanced queries

    query = f"""
    WITH vwap_by_side AS (
        SELECT 
            local_datetime,
            DATE(local_datetime) AS onlydate,
            instrument_id,
            instrumentname,
            direction,
            quantity,
            price,
            SUM(quantity * price) OVER (
                PARTITION BY DATE(local_datetime), instrument_id, instrumentname, direction
            ) AS total_weighted_price,
            SUM(quantity) OVER (
                PARTITION BY DATE(local_datetime), instrument_id, instrumentname, direction
            ) AS total_quantity,
            SUM(quantity * price) OVER (
                PARTITION BY DATE(local_datetime), instrument_id, instrumentname, direction
            ) / NULLIF(SUM(quantity) OVER (
                PARTITION BY DATE(local_datetime), instrument_id, instrumentname, direction
            ), 0) AS vwap_side
        FROM public.log_big_clips
        WHERE local_datetime BETWEEN :start_date AND :end_date
        {"AND instrumentname = ANY(:instruments)" if selected_instruments else ""}
    )

    SELECT DISTINCT onlydate, instrumentname, direction, total_quantity, 
        ROUND(vwap_side::numeric, 2) AS vwap, MAX(local_datetime) AS latest_datetime
    FROM vwap_by_side
    GROUP BY onlydate, instrumentname, direction, total_quantity, vwap_side
    ORDER BY instrumentname, latest_datetime DESC;
    """
    query_params = {
        "start_date": str(start_date),
        "end_date": str(end_date + timedelta(days=1))
    }
    if selected_instruments:
        query_params["instruments"] = selected_instruments

elif query_type == "VWAP Buy-Sell Delta":
    show_sql = True
    query = f"""
    WITH vwap_calc AS (
        SELECT 
            DATE(local_datetime) AS onlydate,
            instrumentname,
            direction,
            SUM(quantity * price)::numeric / NULLIF(SUM(quantity), 0) AS vwap_side
        FROM public.log_big_clips
        WHERE local_datetime BETWEEN :start_date AND :end_date
        {"AND instrumentname = ANY(:instruments)" if selected_instruments else ""}
        GROUP BY DATE(local_datetime), instrumentname, direction
    ),
    pivoted AS (
        SELECT 
            onlydate,
            instrumentname,
            MAX(CASE WHEN direction = 'B' THEN vwap_side END) AS vwap_buy,
            MAX(CASE WHEN direction = 'S' THEN vwap_side END) AS vwap_sell
        FROM vwap_calc
        GROUP BY onlydate, instrumentname
    )
    SELECT 
        onlydate,
        instrumentname,
        ROUND(vwap_buy, 2) AS vwap_buy,
        ROUND(vwap_sell, 2) AS vwap_sell,
        ROUND(vwap_buy - vwap_sell, 2) AS vwap_delta
    FROM pivoted
    ORDER BY onlydate DESC, instrumentname;
    """

    query_params = {
        "start_date": str(start_date),
        "end_date": str(end_date + timedelta(days=1))
    }
    if selected_instruments:
        query_params["instruments"] = selected_instruments

# ---- RUN QUERY ----
try:
    with engine.connect() as conn: ##############
        df = pd.read_sql(text(query), conn, params=query_params)

    st.subheader(f"Results")
    # st.subheader(f"Results for '{query_type}'")
    
    if show_sql:
        st.code(query, language="sql")

    st.dataframe(df, use_container_width=True)

    if query_type == "Simple Aggregation":
        if chart_type == "Bar Chart":
            st.bar_chart(df.set_index("group_field"))
        elif chart_type == "Line Chart":
            st.line_chart(df.set_index("group_field"))
        elif chart_type == "Histogram":
            st.altair_chart(
                alt.Chart(df).mark_bar().encode(
                    x="metric:Q",
                    y="count()"
                ), use_container_width=True
            )
except Exception as e:
    st.error(f"⚠️ Error running query: {e}")
