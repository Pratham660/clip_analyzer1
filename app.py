# app.py
import streamlit as st
import pandas as pd
from psycopg2 import sql

# Example database connection string (replace with your actual database details)
db_connection_str = 'mysql+mysqlconnector://user:password@host/database_name'
db_connection = create_engine(db_connection_str)

# Now use this engine with pandas
df = pd.read_sql("SELECT * FROM your_table", db_connection)

from db_config import get_connection

st.set_page_config(page_title="📊 Clip Log Dashboard", layout="wide")
st.title("📈 PostgreSQL Trade Log Dashboard")

# Controls
aggregation = st.selectbox("Aggregation Type", ["SUM", "AVG", "COUNT", "VWAP"])
group_by = st.selectbox("Group By", ["instrumentname", "direction", "DATE(local_datetime)"])
instrument_filter = st.text_input("Filter by Instrument (optional)")
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

# Query builder
def build_query(agg, group_by, instrument, start, end):
    if agg == "VWAP":
        select_clause = f"""
        {group_by} AS group_field,
        SUM(quantity * price)::numeric / NULLIF(SUM(quantity), 0) AS vwap
        """
    else:
        select_clause = f"""
        {group_by} AS group_field,
        {agg}(quantity) AS metric
        """
    
    query = f"""
    SELECT {select_clause}
    FROM public.log_big_clips
    WHERE local_datetime BETWEEN %s AND %s
    """
    
    params = [start, end]

    if instrument:
        query += " AND instrumentname = %s"
        params.append(instrument)
    
    query += f" GROUP BY {group_by} ORDER BY 2 DESC"
    return query, params

# Get query + run
query, params = build_query(aggregation, group_by, instrument_filter, start_date, end_date)

try:
    with get_connection() as conn:
        # df = pd.read_sql(query, conn, params=params)
        df = pd.read_sql("SELECT * FROM public.log_big_clips", conn, params=params)
    st.dataframe(df, use_container_width=True)
except Exception as e:
    st.error(f"Error: {e}")
