import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import altair as alt
from auth import login
import plotly.graph_objects as go
from datetime import date

# ---- CONFIG ----
login()
DB_URI = st.secrets["DB_URI"]
engine = create_engine(DB_URI)

# ---- WIDGETS ----
st.sidebar.header("- Controls -")
query_type = st.sidebar.selectbox("Query Mode", ["Aggregate Queries", "VWAP by Side", "VWAP Buy-Sell Delta", "temp1"])
start_date = st.sidebar.date_input("Start Date", datetime.today() - timedelta(days=7))
end_date = st.sidebar.date_input("End Date", datetime.today())
show_sql = st.sidebar.checkbox("Show SQL Query?", value=False)

## FETCH ALL INSTRUMENTS THAT FALL WITHIN THIS RANGE
with engine.connect() as conn:
    query_text = "SELECT DISTINCT instrumentname FROM public.log_big_clips WHERE local_datetime BETWEEN %s AND %s ORDER BY instrumentname"
    query_params = (str(start_date),str(end_date))
    instruments = pd.read_sql(query_text, conn, params=query_params)

## USER SELECTS SOME INSTRUMENTS (ALL SELECTED BY DEFAULT)
selected_instruments = st.sidebar.multiselect("Instruments", instruments['instrumentname'].tolist())

## DIFFERENT QUERIES
if query_type == "Aggregate Queries":
    side = st.sidebar.radio("Direction", options=["All", "Buy(B)", "Sell(S)"])
    agg_type = st.sidebar.selectbox("Aggregate Type", ["SUM", "AVG", "COUNT", "VWAP"])
    group_by = st.sidebar.selectbox("Group By", [
        "DATE(local_datetime)",
        "instrumentname",
        "direction",
        "EXTRACT(HOUR FROM local_datetime)" ])
    top_n = st.sidebar.slider("Top N Results", 1, 100, 10)
    chart_type = st.sidebar.selectbox("Chart Type", ["Table", "Bar Chart", "Line Chart", "Histogram"])

    def build_query():
        where_clauses = ["local_datetime BETWEEN :start_date AND :end_date"]
        params = {"start_date": str(start_date), "end_date": str(end_date + timedelta(days=1))}

        if selected_instruments:
            where_clauses.append("instrumentname = ANY(:instruments)")
            params["instruments"] = selected_instruments

        if side == "Buy(B)":
            where_clauses.append("direction = 'B'")
        elif side == "Sell(S)":
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

    query = f"""
    WITH raw_data AS (
    SELECT 
        local_datetime,
        DATE(local_datetime) AS Date,
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
        ) AS total_quantity
    FROM public.log_big_clips
    WHERE local_datetime BETWEEN :start_date AND :end_date
    {"AND instrumentname = ANY(:instruments)" if selected_instruments else ""}
    ),
    vwap_by_side AS (
        SELECT *,
            total_weighted_price / NULLIF(total_quantity, 0) AS vwap_side
        FROM raw_data
    )

    SELECT DISTINCT Date, instrumentname, direction, total_quantity, 
        ROUND(vwap_side::numeric, 2) AS vwap, MAX(local_datetime) AS last_trade_time
    FROM vwap_by_side
    GROUP BY Date, instrumentname, direction, total_quantity, vwap_side
    ORDER BY instrumentname, last_trade_time DESC;
    """
    query_params = {
        "start_date": str(start_date),
        "end_date": str(end_date + timedelta(days=1))
    }
    if selected_instruments:
        query_params["instruments"] = selected_instruments

elif query_type == "VWAP Buy-Sell Delta":
    # show_sql = True
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

elif query_type == "temp1":
    query = f"""
    SELECT instrumentname, price, quantity, direction ,local_datetime
    FROM public.log_big_clips
    WHERE local_datetime::date BETWEEN :start_date AND :end_date
    ORDER BY instrumentname, local_datetime;
    """

# ---- RUN QUERY ----
try:
    with engine.connect() as conn: ##############
        df = pd.read_sql( text(query), conn, params=query_params )

    # st.subheader(f"Results")
    # st.subheader(f"Results for '{query_type}'")
    
    if show_sql:
        st.code(query, language="sql")

    st.dataframe(df, use_container_width=True)

    if query_type == "Aggregate Queries":
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
