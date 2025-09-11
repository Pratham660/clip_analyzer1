import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import warnings
import pandas as pd
import numpy as np
import ast
warnings.filterwarnings("ignore")
from auth import login

login()

def safe_eval(val):
    try:
        return ast.literal_eval(str(val).replace('nan', 'np.nan'))
    except Exception:
        return np.nan
    
df = pd.read_csv("I:/SYB_PHM/QH_Data/OI_Data/df_last_day_analsyis.csv")

last_date = df["Date"][0]
formatted_date = pd.to_datetime(last_date ).strftime("%d-%m-%y")

df = df.loc[:, ~df.columns.str.contains( '^Unnamed|Date|Close_Chg', regex=True)]
df = df.reset_index(drop=True)
df.replace([np.inf, -np.inf], np.nan, inplace=True)

# If tuple columns are strings, convert them back to tuples (optional)
tuple_cols = ["OI_Close_Corr", "Close_Pctls", "OI_Pctls", "Vol_Pctls"]
for col in tuple_cols:
    df[col] = df[col].apply(safe_eval)
st.set_page_config(page_title="OI DashB", layout="wide")

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
    
filtered_df = df[
    # (df["Date"].isin(selected_dates)) &
    (df["Product"].isin(selected_products)) &
    # (df["Generic_Code"].isin(selected_codes)) &
    (df['Vol_Chg_%'] >= vol_chg_range[0]) & 
    (df['Vol_Chg_%'] <= vol_chg_range[1]) &
    (df['OI_Chg_%'] >= oi_chg_range[0]) & 
    (df['OI_Chg_%'] <= oi_chg_range[1])
]

st.text(f"Last_Date : {formatted_date}" )
st.dataframe(filtered_df, use_container_width=True)
st.text("Note : The tick size taken is 0.005")
st.text('Note : The cols "OI_Close_Corr", "Close_Pctls", "OI_Pctls", "Vol_Pctls" are based on last 20 Period calculations of 1D, 5D, 10D, 20D')

# '''
# import pandas as pd
# import numpy as np
# close_df = pd.read_pickle("I:/SYB_PHM/Proj_1/logs/global_close_dict_multithread.pkl")
# global_volume_df = pd.read_pickle("I:/SYB_PHM/Proj_1/logs/global_volume_dict_multithread.pkl")

# def volume_shocker_analysis():
#     for k in [k for k in global_volume_df if len(global_volume_df[k])==0]:
#         del global_volume_df[k]

#     strategies_list_vol = list( set(k[0] for k in global_volume_df) )
#     time_in_mins_list = [60, 120, 240,1440] 
#     rows = []

#     for strategy in strategies_list_vol:
#         for time_in_mins in time_in_mins_list:
#             key = (strategy, str(time_in_mins)+'min')
#             df = global_volume_df.get(key)

#             if df is None or df.empty:
#                 # global_volume_df[key] = pd.DataFrame()
#                 continue
#             else:
#                 for col in df.columns:
#                     series = df[col]
#                     idx = 0 if series.index[0] > series.index[-1] else -1
#                     latest_time = series.index[idx].strftime('%H:%M:%S')
                    
#                     df1 = series[ series.index.strftime('%H:%M:%S') == latest_time ]
#                     idx = 0 if df1.index[0] > df1.index[-1] else -1
#                     if df1.empty or df1.isna().any():
#                         continue
#                     mul=1
#                     if idx == 0:
#                         mul=-1
#                     else:
#                         avg_5period_volume = df1.iloc[idx- mul*6 : idx].mean()
#                         avg_10period_volume = df1.iloc[idx- mul*11 : idx].mean()
#                         avg_20period_volume = df1.iloc[idx- mul*21 : idx].mean()
#                         rows.append(
#                             [
#                                 strategy,
#                                 time_in_mins,
#                                 key,
#                                 col,
#                                 df1.iloc[idx],
#                                 idx,
#                                 df1.index[idx] ,
#                                 avg_5period_volume,
#                                 avg_10period_volume,
#                                 avg_20period_volume,
#                                 df1.iloc[idx] / avg_5period_volume if avg_5period_volume != 0 else np.nan,
#                                 df1.iloc[idx] / avg_10period_volume if avg_10period_volume != 0 else np.nan,
#                                 df1.iloc[idx] / avg_20period_volume if avg_20period_volume != 0 else np.nan,
#                             ])
                            
#     df = pd.DataFrame(rows, columns=['strategy', 'time_in_mins', 'key','product', 'volume', 'idx', 'series_idx',
#                                                     'avg_5period_volume', 'avg_10period_volume', 'avg_20period_volume','5pd_multiple', '10pd_multiple',  '20pd_multiple'])
#     return df
# df = volume_shocker_analysis()
# df = df.loc[ (df["5pd_multiple"]>1) & (df["volume"]>999) ,["strategy" ,"time_in_mins" ,"product" ,"volume", 
#                                                            "5pd_multiple","10pd_multiple","20pd_multiple",] ]
# ## MACD + RSI SIGNAL
# resampled_df = pd.read_pickle("I:/SYB_PHM/Proj_1/logs/resampled_data_dict_multithread.pkl")
# df = resampled_df['SRA_3MS', 'SRAZ25-H26', '60min'].copy()
# import ta
# df['rsi_14'] = ta.momentum.rsi(df['close'], window=14, fillna=True)
# macd_oject = ta.trend.MACD(df['close'], window_slow=26, window_fast=12, window_sign=9, fillna=True)
# df['macd_diff'] = macd_oject.macd_diff()
# df['macd'] = macd_oject.macd()
# df['macd_signal'] = macd_oject.macd_signal()
# df['rsi_percentiles'] = df['rsi_14'].rank(pct=True) * 100
# df['macd_diff_percentiles'] = df['macd_diff'].rank(pct=True) * 100
# '''