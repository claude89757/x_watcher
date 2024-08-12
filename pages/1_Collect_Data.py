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

# Configure Streamlit pages and state
st.set_page_config(page_title="(Demo)X_AI_Marketing", page_icon="🤖")

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
col1, col2, col3 = st.columns(3)

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
    # 刷新展示文件列表按钮
    if st.button(label="Show Files"):
        try:
            # 从 COS 中获取文件列表
            all_files = list_latest_files(prefix=f"{st.session_state.access_code}/")
            st.session_state.matching_files = [
                str(file_key).split('/')[-1] for file_key in all_files if st.session_state.search_keyword in file_key
            ]
        except Exception as e:
            st.error(f"Error retrieving files from COS: {e}")

# 如果有匹配的文件，显示文件名称并允许用户选择
selected_file = st.selectbox("Select a file to display", st.session_state.matching_files)
st.subheader(f"Current Data: {selected_file}")

with col3:
    if st.button(label="Show File Details"):
        # Display collected data
        if selected_file:
            st.session_state.selected_file = selected_file
            st.query_params.selected_file = selected_file
            local_file_path = os.path.join("./data/", selected_file)
            try:
                download_file(object_key=f"{st.session_state.access_code}/{selected_file}",
                              local_file_path=local_file_path)
                data = pd.read_csv(local_file_path)
                # 展示数据
                if data is not None:
                    st.dataframe(data)
                else:
                    st.write("No data to display.")
            except Exception as e:
                st.write(f"Error loading data from COS: {e}")
        else:
            st.warning("No selected file.")
