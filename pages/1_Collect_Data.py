#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 18:52
@Author  : claude
@File    : 1_Collect_Data.py
@Software: PyCharm
"""
import os
import re
import time
import datetime

import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.cos import list_latest_files
from common.cos import download_file
from common.log_config import setup_logger
from common.collector_sdk import call_collect_data_from_x
from common.collector_sdk import query_status
from sidebar import sidebar
from sidebar import cache_file_counts

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="Collect Data", page_icon="🤖", layout="wide")

# 从URL读取缓存数据
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "max_post_num" not in st.session_state:
    st.session_state.max_post_num = int(st.query_params.get("max_post_num", 3))
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword", "")

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
    sidebar()
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

# health checker
# TODO(claude): 从redis中读取服务的状态

st.title("Step 1: Collect Data")
st.markdown("Collecting comment data from popular posts found through keyword searches on X, "
            "which may take some time to complete.")

st.session_state.search_keyword = st.text_input(label="Search Keyword", value=st.session_state.search_keyword)
st.session_state.max_post_num = st.selectbox(
    label="Max Post Number",
    options=[1, 3, 5, 10, 20, 50],
    index=[1, 3, 5, 10, 20, 50].index(st.session_state.max_post_num)
)

# 检查当前用户是否有任务在运行中，如果有任务运行中，不运行触发
# 显示转圈圈图标表示检查任务状态
with st.spinner(f'Checking {st.session_state.access_code} tasks...'):
    code, tasks = query_status(st.session_state.access_code)
    if code != 200:
        st.error(tasks)

running_task = ""
if tasks:
    st.write('History Collect Tasks:')
    for task_name, status in tasks.items():
        if 'RUNNING' in status:
            st.write(f'任务: {task_name}, 状态: {status}  - 🔄')  # 运行中任务显示转圈圈图标
            running_task = f"{task_name} {status}"
        elif 'SUCCESS' in status:
            st.write(f'任务: {task_name}, 状态: {status}  - ✅')  # 成功任务显示对勾图标
        elif 'FAILED' in status:
            st.write(f'任务: {task_name}, 状态: {status}  - ❌')  # 失败任务显示叉图标
        else:
            st.write(f'任务: {task_name}, 状态: {status}')
else:
    pass

if not running_task:
    if st.button(label="Collect Data"):
        # (todo: claude)Initialize progress elements
        try:
            task_num = 0
            with st.spinner("Collecting..."):
                # todo: 这里要增加并发任务的逻辑
                for alive_username in ['Zacks89757']:
                    call_collect_data_from_x(
                        alive_username,
                        st.session_state.search_keyword,
                        st.session_state.max_post_num,
                        st.session_state.access_code,
                    )
                    task_num += 1
                # status_text.text(f"Triggered {task_num} tasks for keyword: {st.session_state.search_keyword}")
                # (todo(claudexie): 查询进度)等待数据收集完成，异步等待
                st.success("Data collection complete!")
        except Exception as e:
            # Log the error
            st.error(f"An error occurred: {e}")
else:
    with st.spinner(running_task):
        while True:
            try:
                _, tasks = query_status(st.session_state.access_code)
            except Exception as error:
                st.error(f"query_status: {error}")
                break
            running_task_list = []
            if tasks:
                for task_name, status in tasks.items():
                    if 'RUNNING' in status:
                        running_task_list.append(task_name)
            else:
                pass
            if not running_task_list:
                break
            else:
                # 这里一直等待任务结束
                time.sleep(5)
                continue

if st.session_state.search_keyword:
    try:
        # 从 COS 中获取文件列表
        modified_keyword = re.sub(r'\s+', '_', st.session_state.search_keyword)
        all_files = list_latest_files(prefix=f"{st.session_state.access_code}/")
        matching_files = [
            str(file_key).split('/')[-1] for file_key in all_files if modified_keyword in file_key
        ]
    except Exception as e:
        raise Exception(f"Error retrieving files from COS: {e}")
    if matching_files:
        selected_file = st.selectbox("Select a file to load", matching_files)
        # 选择加载到本地的文件
        if st.button("Load file"):
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
                st.success(f"{selected_file} is loaded")
            except Exception as e:
                st.error(f"Error loading data from local file: {e}")
    else:
        pass
else:
    pass

# 获取已下载文件的列表
local_files_dir = f"./data/{st.session_state.access_code}/raw/"
if st.session_state.raw_data_file_count:
    pass
    downloaded_files = []
else:
    downloaded_files = os.listdir(local_files_dir)
if downloaded_files or st.session_state.raw_data_file_count:
    if not downloaded_files:
        downloaded_files = os.listdir(local_files_dir)
    else:
        pass
    st.header("Loaded collected files")
    file_info_list = []
    for file in downloaded_files:
        file_path = os.path.join(local_files_dir, file)
        file_size = int(os.path.getsize(file_path) / 1024)  # 转换为KB
        file_mtime = os.path.getmtime(file_path)
        formatted_mtime = datetime.datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S')
        # 计算文件行数
        with open(file_path, 'r') as f:
            file_lines = sum(1 for line in f)

        file_info_list.append({
            "File Name": file,
            "Line Count": file_lines,
            "Size (KB)": file_size,
            "Last Modified": formatted_mtime,
        })

    # 创建 DataFrame
    file_info_df = pd.DataFrame(file_info_list)

    # 将 "Last Modified" 列转换为 datetime 类型
    file_info_df['Last Modified'] = pd.to_datetime(file_info_df['Last Modified'])

    # 按 "Last Modified" 列进行排序
    file_info_df = file_info_df.sort_values(by='Last Modified', ascending=False)

    # 重置索引
    file_info_df = file_info_df.reset_index(drop=True)

    # 展示 DataFrame
    st.dataframe(file_info_df)

    file_loaded = True

    # 更新文件计数
    cache_file_counts()
else:
    pass

# 将用户输入的数据保存到 URL 参数
st.query_params.search_keyword = st.session_state.search_keyword
st.query_params.max_post_num = st.session_state.max_post_num

# Next
if st.session_state.raw_data_file_count:
    if st.button(label="Next: Filter Data", type='primary'):
        st.success("Ready to filter data...")
        st.balloons()
        time.sleep(3)
        st.switch_page("pages/2_Preprocess_Data.py")
    else:
        pass
