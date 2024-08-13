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

            if data is not None:
                st.dataframe(data.head(500), use_container_width=True, height=400)
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
        # 获取源文件路径
        src_file_path = os.path.join(src_dir, st.session_state.selected_file)
        dst_file_path = os.path.join(dst_dir, st.session_state.selected_file)

        # 读取CSV文件
        df = pd.read_csv(src_file_path)

        # 定义正则表达式来提取用户名
        def extract_user_id(link):
            match = re.search(r"https://x\.com/([^/]+)/status/", link)
            if match:
                return match.group(1)
            return None

        # 添加新列 'reply_user_id'
        df['reply_user_id'] = df['reply_user_link'].apply(extract_user_id)

        # 过滤掉'reply_content'列中非字符串类型的数据
        df = df[df['reply_content'].apply(lambda x: isinstance(x, str))]

        # 去重逻辑：根据'reply_user_id'去重，保留'reply_content'最长的记录
        df = df.loc[df.groupby('reply_user_id')['reply_content'].apply(lambda x: x.str.len().idxmax())]

        # 只保留'reply_user_id'和'reply_content'字段
        df = df[['reply_user_id', 'reply_content']]

        # 将处理后的数据保存到目标文件夹中
        df.to_csv(dst_file_path, index=False)

        # 提示成功信息并跳转到下一页面
        st.success(f"Confirmed data successfully, entering next step...")
        st.balloons()
        time.sleep(3)
        st.switch_page("pages/3_AI_Analyze_Data.py")

with col2:
    # Button to process Dat
    if st.button("Process Dat "):
        st.warning("Coming soon...")
