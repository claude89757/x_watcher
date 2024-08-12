#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 23:08
@Author  : claude
@File    : 3️⃣️_AI_Analyze_Data.py
@Software: PyCharm
"""
import logging

import streamlit as st
from config import CONFIG


# Configure logger
logging.basicConfig(format="\n%(asctime)s\n%(message)s", level=logging.INFO, force=True)


if st.session_state.get('access_code') and st.session_state.get('access_code') in CONFIG['access_code_list']:
    # session中有缓存
    st.query_params.access_code = st.session_state.access_code
elif st.query_params.get('access_code') and st.query_params.get('access_code') in CONFIG['access_code_list']:
    # URL中有缓存
    st.session_state.access_code = st.query_params.access_code
else:
    st.warning("Access not Granted!")
    st.switch_page("Home.py", )

# Initialize session state
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "raw_data_filename" not in st.session_state:
    st.session_state.raw_data_filename = ""

# Force responsive layout for columns also on mobile
st.write(
    """<style>
    [data-testid="column"] {
        width: calc(50% - 1rem);
        flex: 1 1 calc(50% - 1rem);
        min-width: calc(50% - 1rem);
    }
    </style>""",
    unsafe_allow_html=True,
)

# Hide Streamlit elements
hide_streamlit_style = """
            <style>
            .stDeployButton {visibility: hidden;}
            [data-testid="stToolbar"] {visibility: hidden !important;}
            footer {visibility: hidden !important;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

st.title("Step 4: Send Promotional Msg")
st.markdown("Automate the sending of personalized promotional messages based on AI analysis results")

st.error("Coming soon...")



