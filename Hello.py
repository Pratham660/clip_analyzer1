import streamlit as st
# from auth import login
# login()


st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.write("# Welcome to Streamlit! 👋")

st.sidebar.success("Select an app.")

st.markdown(
    """
Select App from sidebar
"""
)