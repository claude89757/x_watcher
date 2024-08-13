#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 23:08
@Author  : claude
@File    : 3_AI_Analyze_Data.py
@Software: PyCharm
"""
import os
import time

import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger

# Configure logger
logger = setup_logger(__name__)

st.set_page_config(page_title="Promotional Msg", page_icon="🤖", layout="wide")


# init session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "matching_files" not in st.session_state:
    st.session_state.matching_files = ""
if "analysis_run" not in st.session_state:
    st.session_state.analysis_run = False

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

st.title("Step 4: Send Promotional Msg")
st.markdown("Automate the sending of personalized promotional messages based on AI analysis results")

st.error("Coming soon...")


def list_files(directory):
    """返回目录中的所有文件列表"""
    try:
        files = os.listdir(directory)
        files = [f for f in files if os.path.isfile(os.path.join(directory, f))]
        return files
    except FileNotFoundError:
        return []

def count_files(directory):
    """返回目录中的文件数量"""
    return len(list_files(directory))

def display_files(directory):
    """显示目录中的文件列表"""
    files = list_files(directory)
    if files:
        selected_file = st.selectbox(f"选择一个文件 (目录: {directory})", files)
        if selected_file:
            st.write(f"选择的文件: {selected_file}")
    else:
        st.write("目录为空或不存在")

# 在侧边栏中创建显示文件数量的组件
st.sidebar.header("文件统计")

folders = {
    "原始数据": f"./data/{st.session_state.access_code}/raw/",
    "处理后数据": f"./data/{st.session_state.access_code}/processed/",
    "分析后数据": f"./data/{st.session_state.access_code}/analyzed/"
}

for folder_name, folder_path in folders.items():
    count = count_files(folder_path)
    st.sidebar.write(f"{folder_name} 文件数量: {count}")

# 在侧边栏中创建一个展开器来展示文件列表
selected_folder = st.sidebar.selectbox("选择一个文件夹", list(folders.keys()))
selected_folder_path = folders[selected_folder]

with st.sidebar.expander(f"查看 {selected_folder} 文件列表", expanded=True):
    files = list_files(selected_folder_path)
    if files:
        st.sidebar.write("文件列表:")
        for file in files:
            st.sidebar.write(file)
    else:
        st.sidebar.write("目录为空或不存在")

