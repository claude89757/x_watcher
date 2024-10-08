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
import re
import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger
from sidebar import sidebar
from sidebar import cache_file_counts


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
    page_title = "步骤 2: 预处理和过滤数据"
    page_description = "预处理和过滤数据，包括选择字段、选择文件和应用必要的预处理步骤。"
    select_file_label = "选择要分析的文件:"
    no_data_warning = "没有原始数据，返回收集数据..."
    preprocess_button_label = "预处理数据"
    initial_data_count_label = "初始数据量"
    final_data_count_label = "最终数据量"
    preprocess_success_message = "数据预处理成功。"
    next_button_label = "下一步: AI 分析数据"
    log_out_button_label = "登出"
else:
    page_title = "Step 2: Preprocessing and Filter Data"
    page_description = "Preprocessing and filtering data, including selecting fields, choosing files, and applying necessary preprocessing steps."
    select_file_label = "Select a file to analyze:"
    no_data_warning = "No raw data, return to collect data..."
    preprocess_button_label = "Preprocess Data"
    initial_data_count_label = "Initial data count"
    final_data_count_label = "Final data count"
    preprocess_success_message = "Preprocess Data successfully."
    next_button_label = "Next: AI Analyze Data"
    log_out_button_label = "Log out"

# 使用动态文本
st.title(page_title)
st.markdown(page_description)

src_dir = f"./data/{st.session_state.access_code}/raw/"
dst_dir = f"./data/{st.session_state.access_code}/processed/"

files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
files.sort(key=lambda f: os.path.getmtime(os.path.join(src_dir, f)), reverse=True)
if files:
    st.session_state.selected_file = st.selectbox(select_file_label, files)
    selected_file_path = os.path.join(src_dir, st.session_state.selected_file)
    st.subheader(f"File Data Preview: {st.session_state.selected_file}")
    if st.session_state.selected_file:
        st.query_params.selected_file = st.session_state.selected_file
        local_file_path = os.path.join(src_dir, st.session_state.selected_file)
        try:
            data = pd.read_csv(local_file_path)
            if data is not None:
                st.dataframe(data.head(500), use_container_width=True, height=400)
            else:
                st.write("No data to display.")
        except Exception as e:
            st.error(f"Error loading data from local file: {e}")
    else:
        st.error("No selected file.")
else:
    st.warning(no_data_warning)
    time.sleep(1)
    st.switch_page("pages/1_Collect_Data.py")

# Button to confirm the file
if st.button(preprocess_button_label):
    with st.spinner('Preprocessing...'):
        src_file_path = os.path.join(src_dir, st.session_state.selected_file)
        dst_file_path = os.path.join(dst_dir, st.session_state.selected_file)

        df = pd.read_csv(src_file_path)
        initial_count = len(df)

        def extract_user_id(link):
            match = re.search(r"https://x\.com/([^/]+)/status/", link)
            if match:
                return match.group(1)
            return None

        df['reply_user_id'] = df['reply_user_link'].apply(extract_user_id)
        df = df[df['reply_content'].apply(lambda x: isinstance(x, str))]
        df['reply_content'] = df['reply_content'].str.replace(r'\s+', ' ', regex=True).str.strip()
        df['reply_content'] = df['reply_content'].apply(lambda x: re.sub(r'[^\w\s.,!?]', '', x, flags=re.UNICODE))
        df['reply_content'] = df['reply_content'].str.lower()
        df = df.loc[df.groupby('reply_user_id')['reply_content'].apply(lambda x: x.str.len().idxmax())]

        current_time = datetime.datetime.utcnow()
        thirty_days_ago = current_time - datetime.timedelta(days=30)
        df['post_time'] = pd.to_datetime(df['post_time'], format='%Y-%m-%dT%H:%M:%S.%fZ')
        df = df[df['post_time'] >= thirty_days_ago]
        df = df[df['reply_content'].apply(lambda x: len(x) >= 10)]
        df = df[['reply_user_id', 'reply_content']]

        df.to_csv(dst_file_path, index=False)
        final_count = len(df)

        st.write(f"{initial_data_count_label}: {initial_count}")
        st.write(f"{final_data_count_label}: {final_count}")
        st.success(preprocess_success_message)

        cache_file_counts()

# Next
if st.session_state.processed_data_file_count:
    if st.button(label=next_button_label, type='primary'):
        st.success("Process data successfully, entering next step...")
        st.balloons()
        time.sleep(3)
        st.switch_page("pages/3_AI_Analyze_Data.py")

# log out
if st.sidebar.button(label=log_out_button_label, type="primary"):
    st.query_params.clear()
    st.session_state.clear()
    st.rerun()