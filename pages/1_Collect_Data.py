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
from datetime import datetime

import pandas as pd
import streamlit as st
from x_watcher import collect_data_from_x
from x_watcher import check_service_status
from utils import load_comments_from_csv
from config import ACCESS_CODE_LIST


if st.session_state.get('access_code') and st.session_state.get('access_code') in ACCESS_CODE_LIST:
    # session中有缓存
    st.query_params.access_code = st.session_state.access_code
elif st.query_params.get('access_code') and st.query_params.get('access_code') in ACCESS_CODE_LIST:
    # URL中有缓存
    st.session_state.access_code = st.query_params.access_code
else:
    st.warning("Access not Granted!")
    st.switch_page("Home.py", )

# Configure logger
logging.basicConfig(format="\n%(asctime)s\n%(message)s", level=logging.INFO, force=True)

# Configure Streamlit pages and state
st.set_page_config(page_title="(Demo)X_AI_Marketing", page_icon="🤖")

# 从URL读取缓存数据
if "max_post_num" not in st.session_state:
    st.session_state.max_post_num = int(st.query_params.get("max_post_num", 3))
if "service_status" not in st.session_state:
    st.session_state.service_status = st.query_params.get("service_status", "Unchecked")
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword", "")

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

# health checker
status_indicator = st.empty()
if st.session_state.get("service_status") != "Available":
    with st.spinner("Checking service status..."):
        st.session_state.service_status = check_service_status()
status_indicator = st.empty()
if st.session_state.service_status == "Available":
    status_indicator.success("Service Status: Available")
elif st.session_state.service_status == "Unavailable":
    status_indicator.error("Service Status: Unavailable")
st.query_params.service_status = st.session_state.service_status

st.title("Step 1: Collect Data")
st.markdown("""Collecting data from X, which may take some time to complete.""")

# st.session_state.username = st.text_input(label="Username", value=st.session_state.username)
# st.session_state.email = st.text_input(label="Email", value=st.session_state.email)
# st.session_state.password = st.text_input(label="Password", value=st.session_state.password, type="password")
st.session_state.search_keyword = st.text_input(label="Search Keyword", value=st.session_state.search_keyword)
st.session_state.max_post_num = st.selectbox(
    label="Max Post Number",
    options=[1, 3, 5, 10, 20, 50],
    index=[1, 3, 5, 10, 20, 50].index(st.session_state.max_post_num)
)

# 将用户输入的数据保存到 URL 参数
st.query_params.search_keyword = st.session_state.search_keyword
st.query_params.max_post_num = st.session_state.max_post_num

if st.button(label="Start Collecting Data"):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    cache_filename = f"./data/{st.session_state.access_code}/raw_{st.session_state.search_keyword}_{timestamp}.csv"

    # (todo: claude)Initialize progress elements
    # progress_bar = st.progress(0)
    status_text = st.empty()
    try:
        # 使用 st.spinner 显示加载中的图标
        with st.spinner("Collecting data..."):
            # Start collecting data with progress
            st.session_state.raw_data_filename = cache_filename
            st.session_state.data = collect_data_from_x(
                st.session_state.search_keyword,
                st.session_state.max_post_num,
                st.session_state.raw_data_filename
            )

        # 数据采集完成后更新状态
        status_text.text(f"Collected data for keyword: {st.session_state.search_keyword}")
        st.success("Data collection complete!")
    except Exception as e:
        # Log the error
        logging.error(f"Error occurred during data collection: {e}")
        st.error(f"An error occurred: {e}")

    finally:
        # Always set the filename in session state, even if an error occurs
        st.session_state.raw_data_filename = cache_filename

# 展示已收集的数据
selected_file_name = None
data_dir = f"./data/{st.session_state.access_code}/"
if st.query_params.get("search_keyword"):
    # 列出 /data 目录下所有以 raw_ 开头且包含 search_keyword 的文件
    matching_files = []
    for file_name in os.listdir(data_dir):
        if file_name.startswith("raw_") and str(st.session_state.search_keyword) in file_name:
            file_path = os.path.join(data_dir, file_name)
            file_size = os.path.getsize(file_path)
            # 加载数据并计算数据行数
            data = pd.read_csv(file_path)
            num_rows = len(data)
            matching_files.append((file_name, file_size, num_rows))

    # 如果匹配的文件存在，显示文件名称、大小和数据行数，并允许用户选择
    if matching_files:
        file_options = [
            f"{name} ({size/1024:.2f} KB, {num_rows} rows)"
            for name, size, num_rows in matching_files
        ]
        selected_file = st.selectbox("Select a file to load", file_options)

        # 获取用户选择的文件名并复制给 raw_data_filename
        selected_file_name = matching_files[file_options.index(selected_file)][0]
        st.session_state.raw_data_filename = os.path.join(data_dir, selected_file_name)
    else:
        st.write("No matching files found.")

# Display collected data
if selected_file_name:
    st.subheader(f"Current Data: {selected_file_name}")
    # Load data from the CSV file
    selected_file_path = os.path.join(data_dir, selected_file_name)
    data = load_comments_from_csv(selected_file_path)
    if data is not None:
        # Display data in a table format
        st.dataframe(data)
    else:
        st.write("No data to display.")



