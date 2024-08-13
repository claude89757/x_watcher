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
import datetime

import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger
from common.azure_openai import generate_promotional_sms
from sidebar import sidebar

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

st.title("Step 4: Send Promotional Msg")
st.markdown("Automate the sending of personalized promotional messages based on AI analysis results")

cur_dir = f"./data/{st.session_state.access_code}/analyzed/"
files = [f for f in os.listdir(cur_dir) if os.path.isfile(os.path.join(cur_dir, f))]
files.sort(key=lambda f: os.path.getmtime(os.path.join(cur_dir, f)), reverse=True)
st.session_state.selected_file = st.selectbox("Select a file:", files)
selected_file_path = None
if st.session_state.selected_file:
    selected_file_path = os.path.join(cur_dir, st.session_state.selected_file)
    st.subheader(f"File Data Preview: {st.session_state.selected_file}")
    # 检查本地是否已有文件
    try:
        # 获取文件信息
        data = pd.read_csv(selected_file_path)
        file_size = os.path.getsize(selected_file_path)  # 文件大小（字节）
        file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(selected_file_path))  # 文件修改时间

        if data is not None:
            st.dataframe(data.head(100), use_container_width=True, height=400)
        else:
            st.write("No data to display.")
    except Exception as e:
        st.error(f"Error loading data from local file: {e}")
else:
    st.warning("No processed data, return to filter data...")
    time.sleep(3)
    st.switch_page("pages/2_Filter_Data.py")


# 选择过滤字段
filter_column = st.selectbox("Select a column to filter by:", data.columns)

# 获取唯一值并选择过滤条件
unique_values = data[filter_column].unique()
selected_value = st.selectbox(f"Select a value from {filter_column} to filter:", unique_values)

# 过滤数据
filtered_data = data[data[filter_column] == selected_value]

# 显示过滤后的数据
st.subheader("Filtered Data")
st.dataframe(filtered_data)


if not filtered_data.empty:

    # 选择模型
    model = st.selectbox("Select a model:", ["gpt-4o-mini", "gpt-4o"])

    # 初始化 session state 中的 generated_data 变量
    if "generated_data" not in st.session_state:
        st.session_state.generated_data = False

    # 生成推广短信按钮
    generate_button_text = "Generate Promotional SMS" if not st.session_state.generated_data else "Regenerate Promotional SMS"
    if st.button(generate_button_text):
        system_prompt = "You are a marketing assistant. Your task is to generate personalized promotional SMS messages."
        result_df = generate_promotional_sms(model, system_prompt, filtered_data, batch_size=1)
        st.session_state.generated_data = True
        st.session_state.result_df = result_df

    # 预览推广短信
    if st.session_state.generated_data:
        st.subheader("Generated Promotional SMS")
        st.dataframe(st.session_state.result_df)

        # 输入推特账号、邮箱、密码
        st.subheader("Twitter Account Login")
        twitter_username = st.text_input("Twitter Username")
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")

        if st.button("Login"):
            # 模拟登录验证
            if twitter_username and email and password:
                st.success("Login successful!")
                if st.button("Send Promotional Messages"):
                    # 模拟发送推广私信
                    st.success("Promotional messages sent successfully!")
            else:
                st.error("Please enter all login details.")
