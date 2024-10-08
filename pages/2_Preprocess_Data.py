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
if 'language' not in st.session_state:
    st.session_state.language = st.query_params.get('language')
if "selected_file" not in st.session_state:
    st.session_state.selected_file = st.query_params.get("selected_file")

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
    st.query_params.language = st.session_state.language
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

# 根据选择的语言设置文本
if st.session_state.language == "CN":
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

st.title(page_title)
st.markdown(page_description)

src_dir = f"./data/{st.session_state.access_code}/raw/"
dst_dir = f"./data/{st.session_state.access_code}/processed/"

files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
# 从最新到最旧排序
files.sort(key=lambda f: os.path.getmtime(os.path.join(src_dir, f)), reverse=True)
if files:
    st.session_state.selected_file = st.selectbox(select_file_label, files)
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
    st.warning(no_data_warning)
    time.sleep(1)
    st.switch_page("pages/1_Collect_Data.py")


# Button to confirm the file
if st.button(preprocess_button_label):
    with st.spinner('Preprocessing...'):
        # 获取源文件路径
        src_file_path = os.path.join(src_dir, st.session_state.selected_file)
        dst_file_path = os.path.join(dst_dir, st.session_state.selected_file)

        # 读取CSV文件
        df = pd.read_csv(src_file_path)

        # 记录处理前的数据量
        initial_count = len(df)

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

        # 删除连续空格和首尾空格
        df['reply_content'] = df['reply_content'].str.replace(r'\s+', ' ', regex=True).str.strip()

        # 过滤掉 'reply_content' 列中非字符串类型的数据
        df = df[df['reply_content'].apply(lambda x: isinstance(x, str))]

        # 去除特殊字符，保留字母数字和基本标点符号
        df['reply_content'] = df['reply_content'].apply(lambda x: re.sub(r'[^\w\s.,!?]', '', x, flags=re.UNICODE))

        # 统一大小写
        df['reply_content'] = df['reply_content'].str.lower()

        # 去重逻辑：根据'reply_user_id'去重，保留'reply_content'最长的记录
        df = df.loc[df.groupby('reply_user_id')['reply_content'].apply(lambda x: x.str.len().idxmax())]

        # 过滤掉超过30天的评论
        current_time = datetime.datetime.utcnow()
        thirty_days_ago = current_time - datetime.timedelta(days=30)
        df['post_time'] = pd.to_datetime(df['post_time'], format='%Y-%m-%dT%H:%M:%S.%fZ')
        # 过滤掉超过30天的评论
        df = df[df['post_time'] >= thirty_days_ago]

        # 过滤掉长度小于10的评论
        df = df[df['reply_content'].apply(lambda x: len(x) >= 10)]

        # 只保留'reply_user_id'和'reply_content'字段
        df = df[['reply_user_id', 'reply_content']]

        # 将处理后的数据保存到目标文件夹中
        df.to_csv(dst_file_path, index=False)

        # 记录处理后的数据量
        final_count = len(df)

        # 展示处理前后的数据量
        st.write(f"{initial_data_count_label}: {initial_count}")
        st.write(f"{final_data_count_label}: {final_count}")
        st.success(preprocess_success_message)

        # 更新文件计数
        cache_file_counts()


# Next
if st.session_state.processed_data_file_count:
    if st.button(label=next_button_label, type='primary'):
        st.success(f"Process data successfully, entering next step...")
        st.balloons()
        time.sleep(3)
        st.switch_page("pages/3_AI_Analyze_Data.py")
    else:
        pass