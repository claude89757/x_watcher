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
import urllib.parse
import random

import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.cos import list_latest_files
from common.cos import download_file
from common.log_config import setup_logger
from common.collector_sdk import call_collect_data_from_x
from sidebar import sidebar
from sidebar import cache_file_counts
from common.redis_client import RedisClient


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


# 在侧边栏添加语言选择
language = st.sidebar.radio("选择语言 / Choose Language", ("中文", "English"), index=0 if st.query_params.get('language') == '中文' else 1)

# 将语言选择存储到 session_state 和 URL 参数
st.session_state.language = language
st.query_params.language = language

# 根据选择的语言设置文本
if language == "中文":
    page_title = "步骤 1: 收集数据"
    page_description = "从X中通过关键词搜索找到的热门帖子中收集评论数据，可能需要一些时间来完成。"
    search_keyword_label = "搜索关键词"
    max_post_num_label = "最大帖子数量"
    collect_data_button_label = "收集数据"
    data_collection_complete_message = "数据收集完成！"
    access_not_granted_message = "访问未授权！"
    log_out_button_label = "登出"
else:
    page_title = "Step 1: Collect Data"
    page_description = "Collecting comment data from popular posts found through keyword searches on X, which may take some time to complete."
    search_keyword_label = "Search Keyword"
    max_post_num_label = "Max Post Number"
    collect_data_button_label = "Collect Data"
    data_collection_complete_message = "Data collection complete!"
    access_not_granted_message = "Access not Granted!"
    log_out_button_label = "Log out"

# 使用动态文本
st.title(page_title)
st.markdown(page_description)

st.session_state.search_keyword = st.text_input(label=search_keyword_label, value=st.session_state.search_keyword)
st.session_state.max_post_num = st.selectbox(
    label=max_post_num_label,
    options=[1, 3, 5, 10, 20, 50],
    index=[1, 3, 5, 10, 20, 50].index(st.session_state.max_post_num)
)

def query_status(access_code): 
    """
    从 Redis 中查询任务状态
    :param access_code: 访问码
    :return: 返回任务状态
    """
    redis_client = RedisClient(db=0)
    task_keys = redis_client.redis_conn.keys(f"{access_code}_*_task")
    tasks = {}
    for task_key in task_keys:
        task_info = redis_client.get_json_data(task_key)
        if task_info:
            tasks[task_key] = task_info.get('status', 'Unknown')
    return tasks


# 检查当前用户是否有任务在运行中，如果有任务运行中，不运行触发
# 显示转圈圈图标表示检查任务状态
with st.spinner(f'Checking {st.session_state.access_code} tasks...'):
    tasks = query_status(st.session_state.access_code)

running_task = ""
if tasks:
    # 创建三列布局
    col1, col2, col3 = st.columns(3)
    for i, (task_name, status) in enumerate(tasks.items()):
        # 根据索引选择列
        if i % 3 == 0:
            col = col1
        elif i % 3 == 1:
            col = col2
        else:
            col = col3

        # 在选定的列中显示任务状态
        if 'RUNNING' in status:
            col.markdown(f'> {task_name} 🔄')  # 运行中任务显示转圈圈图标
            running_task = f"{task_name} {status}"
        elif 'SUCCESS' in status:
            col.markdown(f'> {task_name} ✅')  # 成功任务显示对勾图标
        elif 'FAILED' in status:
            col.markdown(f'> {task_name} ❌')  # 失败任务显示叉图标
        else:
            col.markdown(f'> {task_name} {status}')
else:
    pass

if not running_task:
    if st.button(label=collect_data_button_label):
        # (todo: claude)Initialize progress elements
        try:
            task_num = 0
            with st.spinner("Collecting..."):
                # todo: 这里要增加并发任务的逻辑
                alive_username = random.choice(['Zacks89757'])
                call_collect_data_from_x(
                    alive_username,
                    st.session_state.search_keyword,
                    st.session_state.max_post_num,
                    st.session_state.access_code,
                )
                task_num += 1
                # status_text.text(f"Triggered {task_num} tasks for keyword: {st.session_state.search_keyword}")
                # (todo(claudexie): 查询进度)等待数据收集完成，异步等待
                st.success(data_collection_complete_message)
                time.sleep(3)
                st.rerun()
        except Exception as e:
            # Log the error
            st.error(f"An error occurred: {e}")
else:
    with st.spinner(running_task):
        while True:
            try:
                tasks = query_status(st.session_state.access_code)
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
        all_files = list_latest_files(prefix=f"{st.session_state.access_code}/")

        matching_files = []
        for raw_file_name in all_files:
            file_name = str(urllib.parse.unquote(raw_file_name)).split('/')[-1]
            if st.session_state.search_keyword in file_name:
                matching_files.append(file_name)
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
        st.error("no matching_files")
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

# log out
if st.sidebar.button(label=log_out_button_label, type="primary"):
    st.query_params.clear()
    st.session_state.clear()
    st.rerun()