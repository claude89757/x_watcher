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

import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger
from common.azure_openai import generate_promotional_sms
from common.collector_sdk import collect_user_link_details
from sidebar import sidebar
from sidebar import cache_file_counts


# Configure logger
logger = setup_logger(__name__)

st.set_page_config(page_title="Generate Msg", page_icon="🤖", layout="wide")


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

st.title("Step 4: AI Generate Msg")
st.markdown("A personalized promotional message generated for specific customers based on AI classification results, "
            "aimed at enhancing marketing effectiveness and user engagement.")

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
    except Exception as e:
        st.error(f"Error loading data from local file: {e}")
else:
    st.warning("No processed data, return to filter data...")
    time.sleep(3)
    st.switch_page("pages/2_Preprocess_Data.py")

# 选择要过滤的列
filter_columns = st.multiselect("Select columns to filter by:", data.columns)

# 初始化过滤器
filters = {}
for column in filter_columns:
    # 显示过滤器
    st.subheader(f"Filter by {column}")
    unique_values = data[column].unique()
    selected_values = st.multiselect(f"Select values from {column} to filter:", unique_values)
    if selected_values:
        filters[column] = selected_values

# 过滤数据
filtered_data = data.copy()
for column, selected_values in filters.items():
    filtered_data = filtered_data[filtered_data[column].isin(selected_values)]

# 显示过滤后的数据
st.subheader("Filtered Data")
st.dataframe(filtered_data)


# 获取更多的用户信息
if st.button("Collect More User Details"):
    with st.spinner("Collecting More User Details..."):
        user_ids = data.iloc[:, 0].tolist()  # 假设第一列是 user_id
        total_users = len(user_ids)
        user_details = []

        # 分批查询用户信息
        # 创建进度条
        progress_bar = st.progress(0)
        batch_size = 5  # 每批查询的用户数量
        for i in range(0, total_users, batch_size):
            batch_user_ids = user_ids[i:i + batch_size]
            # 调用 collect_user_link_details 函数
            status_code, details = collect_user_link_details("Zacks89757", batch_user_ids)
            if status_code == 200:
                user_details.extend(details)
            else:
                pass

            # 更新进度条
            progress_bar.progress((i + batch_size) / total_users if (i + batch_size) < total_users else 1.0)
        st.success("User details collected successfully!")
        st.write(user_details)
        # 将 details 补充到读取的本地文件中
        details_df = pd.DataFrame(user_details)
        merged_data = pd.merge(data, details_df, on="reply_user_id", how="left")

        # 保存合并后的数据到原文件
        merged_data.to_csv(selected_file_path, index=False)
        st.success(f"Merged data saved to {selected_file_path}")
        st.balloons()
        time.sleep(3)
        st.rerun()

if not filtered_data.empty:
    # 输入示例的提示词
    system_prompt = st.text_input("Enter the prompt for generating promotional SMS:",
                                  "You are a marketing assistant. Your task is to generate personalized "
                                  "promotional SMS messages for promoting product 【XYZ】.")

    # 选择模型
    model = st.selectbox("Select a model:", ["gpt-4o-mini", "gpt-4o"])

    batch_size = st.selectbox("Select batch size", [10, 20, 30, 40, 50])

    # 生成推广短信按钮
    if st.button("Generate Promotional Msg"):
        with st.spinner('Generating Msg...'):
            result_df = generate_promotional_sms(model, system_prompt, filtered_data.iloc[:, :3], batch_size=batch_size)
            st.query_params.analysis_run = True
            if not result_df.empty:
                dst_dir = f"./data/{st.session_state.access_code}/msg/"
                output_file = os.path.join(dst_dir, f"{st.session_state.selected_file}")
                # 保存分析结果
                # logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                # logger.info(result_df.head(10))
                # logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                result_df.to_csv(output_file, index=False)

                # 显示结果output_file
                st.success(f"Analysis complete! Results saved to {output_file}.")
                st.dataframe(result_df.head(500), use_container_width=True, height=400)

                cache_file_counts()
            else:
                st.error("Failed to generate analysis results. Please check your prompt or API settings.")

# Next
if st.session_state.msg_data_file_count:
    if st.button(label="Next: Send Promotional Msg", type='primary'):
        st.success("Ready to Send Promotional Msg...")
        st.balloons()
        time.sleep(3)
        st.switch_page("pages/5_Send_Promotional_Msg.py")
