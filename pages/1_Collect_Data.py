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
import requests
import datetime

import pandas as pd
import streamlit as st

from config import CONFIG
from common.cos import list_latest_files
from common.cos import download_file


def call_collect_data_from_x(username, search_key_word, max_post_num, access_code):
    """
    调用 /collect_data_from_x API 接口

    :param username: 用户名
    :param search_key_word: 搜索关键字
    :param max_post_num: 最大帖子数
    :param access_code: 访问代码
    :return: 返回 API 响应的状态和内容
    """
    collector_url = CONFIG['collector_urls'][0]
    api_endpoint = f'http://{collector_url}/collect_data_from_x'  # 在这里定义 API 端点 URL
    headers = {'Content-Type': 'application/json'}
    data = {
        'username': username,
        'search_key_word': search_key_word,
        'max_post_num': max_post_num,
        'access_code': access_code
    }
    try:
        logging.info(f"sending request...")
        response = requests.post(api_endpoint, json=data, headers=headers)
        response.raise_for_status()  # 抛出 HTTPError 异常（如果发生）
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        logging.error(f'Error calling API: {e}')
        return None, str(e)


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


# 从URL读取缓存数据
if "max_post_num" not in st.session_state:
    st.session_state.max_post_num = int(st.query_params.get("max_post_num", 3))
if "service_status" not in st.session_state:
    st.session_state.service_status = st.query_params.get("service_status", "Unchecked")
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword", "")
if "selected_file" not in st.session_state:
    st.session_state.selected_file = st.query_params.get("selected_file", "")
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

# health checker
# TODO(claude): 从redis中读取服务的状态

st.title("Step 1: Collect Data")
st.markdown("""Collecting data from X, which may take some time to complete.""")


st.session_state.search_keyword = st.text_input(label="Search Keyword", value=st.session_state.search_keyword)
st.session_state.max_post_num = st.selectbox(
    label="Max Post Number",
    options=[1, 3, 5, 10, 20, 50],
    index=[1, 3, 5, 10, 20, 50].index(st.session_state.max_post_num)
)

# 将用户输入的数据保存到 URL 参数
st.query_params.search_keyword = st.session_state.search_keyword
st.query_params.max_post_num = st.session_state.max_post_num


# 创建两个并排的列
col1, col2 = st.columns(2)

with col1:
    if st.button(label="Start Collecting Data", type="primary"):
        # (todo: claude)Initialize progress elements
        # progress_bar = st.progress(0)
        # status_text = st.empty()
        try:
            # 使用 st.spinner 显示加载中的图标
            task_num = 0
            with st.spinner("Triggering collectors..."):
                for alive_username in ['Zacks89757']:
                    call_collect_data_from_x(
                        alive_username,
                        st.session_state.search_keyword,
                        st.session_state.max_post_num,
                        st.session_state.access_code,
                    )
                    task_num += 1
                    time.sleep(10)
            # status_text.text(f"Triggered {task_num} tasks for keyword: {st.session_state.search_keyword}")

            # (todo(claudexie): 查询进度)等待数据收集完成，异步等待
            st.success("Data collection complete!")
        except Exception as e:
            # Log the error
            logging.error(f"Error occurred during data collection: {e}")
            st.error(f"An error occurred: {e}")

with col2:
    download_button = st.button(label="Download File")

# 如果有匹配的文件，显示文件名称并允许用户选择
selected_file = st.selectbox("Select a file to display", st.session_state.matching_files)
if st.button(label="Refresh File List"):
    with st.spinner("Retrieving files from COS..."):
        try:
            # 从 COS 中获取文件列表
            all_files = list_latest_files(prefix=f"{st.session_state.access_code}/")
            st.session_state.matching_files = [
                str(file_key).split('/')[-1] for file_key in all_files if st.session_state.search_keyword in file_key
            ]
        except Exception as e:
            st.error(f"Error retrieving files from COS: {e}")

if selected_file and download_button:
    st.session_state.selected_file = selected_file
    st.query_params.selected_file = selected_file
    local_file_path = os.path.join(f"./data/{st.session_state.access_code}/raw/", selected_file)
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
    pass


# 展示已下载文件列表的逻辑
st.header("Downloaded Files")

# 获取已下载文件的列表
downloaded_files_dir = f"./data/{st.session_state.access_code}/raw/"
if os.path.exists(downloaded_files_dir):
    downloaded_files = os.listdir(downloaded_files_dir)
    if downloaded_files:
        file_info_list = []
        for file in downloaded_files:
            file_path = os.path.join(downloaded_files_dir, file)
            file_size = os.path.getsize(file_path)
            file_mtime = os.path.getmtime(file_path)
            formatted_mtime = datetime.datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S')
            file_info_list.append({
                "File Name": file,
                "Size (bytes)": file_size,
                "Last Modified": formatted_mtime
            })

        # 创建 DataFrame 并展示
        file_info_df = pd.DataFrame(file_info_list)
        st.dataframe(file_info_df)

        # Next
        if st.button(label="Next: Filter Data", type="primary"):
            st.success("Ready to filter data...")
            time.sleep(3)
            st.switch_page("pages/2_Filter_Data.py")
        else:
            pass
    else:
        st.write("No files downloaded yet.")
else:
    st.write("No files downloaded yet.")


