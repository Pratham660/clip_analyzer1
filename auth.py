import streamlit as st
import json
def load_credentials():
    with open("clip_analyzer/credentials.json", "r") as f:
        return json.load(f)

USER_CREDENTIALS = load_credentials()
# print(USER_CREDENTIALS)

def login():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        # st.title("Login Required")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_button = st.button("Login")

        if login_button:
            if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
                st.session_state.authenticated = True
                st.success("Login successful")
                st.rerun()
            else:
                st.error("Invalid credentials")
        st.stop()  # Prevent app code from running until logged in
