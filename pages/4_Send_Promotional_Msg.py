#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 23:08
@Author  : claude
@File    : 3_AI_Analyze_Data.py
@Software: PyCharm
"""

import os
import logging
import shutil
import openai  # Assuming you will use OpenAI's GPT model via an API call

import pandas as pd
import streamlit as st
from utils import load_comments_from_csv
from config import ACCESS_CODE_LIST

# Configure logger
logging.basicConfig(format="\n%(asctime)s\n%(message)s", level=logging.INFO, force=True)

if st.session_state.get('access_code') and st.session_state.get('access_code') in ACCESS_CODE_LIST:
    # session中有缓存
    st.query_params.access_code = st.session_state.access_code
elif st.query_params.get('access_code') and st.query_params.get('access_code') in ACCESS_CODE_LIST:
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

st.title("Step 3: Send Promotional Msg")
st.markdown("Automate the sending of personalized promotional messages based on AI analysis results")

st.error("Coming soon...")



