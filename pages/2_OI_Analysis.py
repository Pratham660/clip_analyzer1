import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import warnings
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")
from auth import login
import ast

login()

def safe_eval(val):
    try:
        # Replace 'nan' string with actual np.nan before evaluation
        return ast.literal_eval(str(val).replace('nan', 'np.nan'))
    except Exception:
        return np.nan
    
df = pd.read_csv("C:/Users/kavish.sethi/Downloads/Test/df_last_day_analsyis.csv") 
df = df.loc[:, ~df.columns.str.contains( '^Unnamed|Date', regex=True)]
df = df.reset_index(drop=True)

# df = df[~df["Vol_Chg_%"].str.contains('inf')]
# df = df[~df["OI_Chg_%"].str.contains('inf')]
df.replace([np.inf, -np.inf], np.nan, inplace=True)

# If tuple columns are strings, convert them back to tuples (optional)
tuple_cols = ["OI_Close_Corr", "Close_Pctls", "OI_Pctls", "Vol_Pctls"]
for col in tuple_cols:
    df[col] = df[col].apply(safe_eval)  # Warning: only if input is trusted

st.set_page_config(page_title="OI Dashboard", layout="wide")

# Sidebar filters
with st.sidebar:
    st.header("Filters")

    # Date filter
    # unique_dates = sorted(df["Date"].unique())
    # selected_dates = st.multiselect("Select Date(s)", unique_dates, default=unique_dates[-1:])

    # Product filter
    products = sorted(df["Product"].unique())
    selected_products = st.multiselect("Select Product(s)", products, default=products)

    # Generic_Code filter
    # codes = sorted(df["Generic_Code"].unique())
    # selected_codes = st.multiselect("Select Generic Code(s)", codes, default=codes)

    # Optional: Filter by OI_Change threshold
    min_vol, max_vol = float(df['Vol_Chg_%'].min()), float(df['Vol_Chg_%'].max())
    vol_chg_range = st.sidebar.slider("Select Vol_Chg_% Range", 
                                    min_value=min_vol, 
                                    max_value=max_vol, 
                                    value=(min_vol, max_vol), 
                                    step=10.0)
    
    min_oi, max_oi = float(df['OI_Chg_%'].min()), float(df['OI_Chg_%'].max())
    oi_chg_range = st.sidebar.slider("Select OI_Chg_% Range", 
                                    min_value=min_oi, 
                                    max_value=max_oi, 
                                    value=(min_oi, max_oi), 
                                    step=10.0)
    
    # oi_min = st.number_input("OI_Chg%", value=df["OI_Chg%"])
    # oi_max = st.slider("Vol_Chg%", value=df["Vol_Chg%"])

# Apply filters
filtered_df = df[
    # (df["Date"].isin(selected_dates)) &
    (df["Product"].isin(selected_products)) &
    # (df["Generic_Code"].isin(selected_codes)) &
    (df['Vol_Chg_%'] >= vol_chg_range[0]) & 
    (df['Vol_Chg_%'] <= vol_chg_range[1]) &
    (df['OI_Chg_%'] >= oi_chg_range[0]) & 
    (df['OI_Chg_%'] <= oi_chg_range[1])

]
print(vol_chg_range)
print(oi_chg_range)

# Display filtered DataFrame
st.dataframe(filtered_df, use_container_width=True)
st.text('Note : The cols "OI_Close_Corr", "Close_Pctls", "OI_Pctls", "Vol_Pctls" are based on last 20 Period calculations of 1D, 5D, 10D, 20D')