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
import shutil
import time

import pandas as pd
import streamlit as st
from config import CONFIG

from common.cos import list_latest_files
from common.cos import download_file

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
if "matching_files" not in st.session_state:
    st.session_state.matching_files = []

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

# 刷新展示文件列表按钮
if st.button(label="Show Collected Data"):
    try:
        # 从 COS 中获取文件列表
        all_files = list_latest_files(prefix=f"{st.session_state.access_code}/")
        st.session_state.matching_files = [
            str(file_key).split('/')[-1] for file_key in all_files if st.session_state.search_keyword in file_key
        ]
    except Exception as e:
        st.error(f"Error retrieving files from COS: {e}")


# 检查目录是否存在文件
if not st.session_state.matching_files:
    st.warning("Not Collected Data, return to collect data...")
    time.sleep(3)
    st.switch_page("pages/1_Collect_Data.py")  # 切换到收集数据页面
else:
    # Filter and display files based on the selected keyword
    pass


# 选择确定处理的文件
selected_file = st.selectbox("Select file to process", st.session_state.matching_files)
if selected_file:
    st.session_state.selected_file = selected_file
    st.query_params.selected_file = selected_file
    local_file_path = os.path.join(f"./data/{st.session_state.access_code}/", selected_file)
    # 检查本地是否已有文件
    if not os.path.exists(local_file_path):
        try:
            download_file(object_key=f"{st.session_state.access_code}/{selected_file}",
                          local_file_path=local_file_path)
            st.success("File downloaded from COS.")
        except Exception as e:
            st.error(f"Error loading file from COS: {e}")

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


col1, col2 = st.columns(2)

with col1:
    # Button to confirm the file
    if st.button("Confirm File ", type="primary"):
        st.success(f"Confirmed date successfully, entering step...")
        time.sleep(3)
        st.switch_page("pages/3_AI_Analyze_Data.py")

with col2:
    # Button to process Dat
    if st.button("Process Dat "):
        st.warning("Coming soon...")


