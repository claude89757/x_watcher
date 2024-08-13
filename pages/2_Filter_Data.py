#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 18:52
@Author  : claude
@File    : 1_Collect_Data.py
@Software: PyCharm
"""
import os
import logging
import time
import shutil
import pandas as pd
import streamlit as st
from config import CONFIG

from common.cos import list_latest_files
from common.cos import download_file

# Configure logger
logging.basicConfig(format="\n%(asctime)s\n%(message)s", level=logging.INFO, force=True)


st.set_page_config(page_title="Filter Data", page_icon="🤖", layout="wide")

# Initialize session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "selected_file" not in st.session_state:
    st.session_state.selected_file = st.query_params.get("selected_file")

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
else:
    st.warning("Access not Granted!")
    time.sleep(3)
    st.switch_page("Home.py", )

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

st.title("Step 2: Preprocessing and Filter Data")
st.markdown("Preprocessing and filtering data, including selecting fields, choosing files,"
            " and applying necessary preprocessing steps.")

src_dir = f"./data/{st.session_state.access_code}/raw/"
dst_dir = f"./data/{st.session_state.access_code}/processed/"

files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
if files:
    st.session_state.selected_file = st.selectbox("Select a file to analyze:", files)
    selected_file_path = os.path.join(src_dir, st.session_state.selected_file)
    st.subheader(f"File Data Preview: {st.session_state.selected_file}")
    # 选择确定处理的文件
    if st.session_state.selected_file:
        st.query_params.selected_file = st.session_state.selected_file
        local_file_path = os.path.join(src_dir, st.session_state.selected_file)
        # 检查本地是否已有文件
        try:
            data = pd.read_csv(local_file_path)
            # 展示数据
            if data is not None:
                st.dataframe(data)
            else:
                st.write("No data to display.")
        except Exception as e:
            st.error(f"Error loading data from local file: {e}")
    else:
        st.error("No selected file.")
else:
    st.warning("No processed data, return to filter data...")
    time.sleep(3)
    st.switch_page("pages/2_Filter_Data.py")


col1, col2 = st.columns(2)

with col1:
    # Button to confirm the file
    if st.button("Confirm File ", type="primary"):
        src_file_path = os.path.join(src_dir, st.session_state.selected_file)
        dst_file_path = os.path.join(dst_dir, st.session_state.selected_file)
        shutil.move(src_file_path, dst_file_path)
        st.success(f"Confirmed date successfully, entering step...")
        time.sleep(3)
        st.switch_page("pages/3_AI_Analyze_Data.py")

with col2:
    # Button to process Dat
    if st.button("Process Dat "):
        st.warning("Coming soon...")
