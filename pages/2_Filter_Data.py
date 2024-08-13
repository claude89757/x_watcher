#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 18:52
@Author  : claude
@File    : 1_Collect_Data.py
@Software: PyCharm
"""
import os
import time
import datetime
import shutil
import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger

# Configure logger
logger = setup_logger(__name__)

# set page config
st.set_page_config(page_title="Filter Data", page_icon="🤖", layout="wide")

# Initialize session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
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
# 从最新到最旧排序
files.sort(key=lambda f: os.path.getmtime(os.path.join(src_dir, f)), reverse=True)
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
            # 获取文件信息
            data = pd.read_csv(local_file_path)
            file_size = os.path.getsize(local_file_path)  # 文件大小（字节）
            file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(local_file_path))  # 文件修改时间

            # 显示文件信息
            st.write(f"File Size: {file_size / 1024:.2f} KB")  # 转换为 KB
            st.write(f"Number of Rows: {data.shape[0]}")
            st.write(f"Last Modified Time: {file_mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

            if data is not None:
                st.dataframe(data.head(500))
            else:
                st.write("No data to display.")
        except Exception as e:
            st.error(f"Error loading data from local file: {e}")
    else:
        st.error("No selected file.")
else:
    st.warning("No processed data, return to filter data...")
    time.sleep(1)
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
