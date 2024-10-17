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
import random

import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger
from common.openai import generate_promotional_sms
from common.collector_sdk import collect_user_link_details
from sidebar import cache_file_counts


# Configure logger
logger = setup_logger(__name__)


def generate_msg():
    """
    私信生成tab
    """

    # init session state
    if 'access_code' not in st.session_state:
        st.session_state.access_code = st.query_params.get('access_code')
    if 'language' not in st.session_state:  
        st.session_state.language = st.query_params.get('language')
    if "search_keyword" not in st.session_state:
        st.session_state.search_keyword = st.query_params.get("search_keyword")
    if "matching_files" not in st.session_state:
        st.session_state.matching_files = ""
    if "analysis_run" not in st.session_state:
        st.session_state.analysis_run = False


    # 根据选择的语言设置文本
    if st.session_state.language == "CN":
        page_title = "步骤 4: AI 生成消息"
        page_description = "为特定客户生成个性化的推广消息，旨在提高营销效果和用户参与度。"
        filter_columns_label = "选择要过滤的列:"
        collect_user_details_button_label = "收集更多用户详情"
        generate_msg_button_label = "生成推广消息"
        log_out_button_label = "登出"
    else:
        page_title = "Step 4: AI Generate Msg"
        page_description = "A personalized promotional message generated for specific customers based on AI classification results, aimed at enhancing marketing effectiveness and user engagement."
        filter_columns_label = "Select columns to filter by:"
        collect_user_details_button_label = "Collect More User Details"
        generate_msg_button_label = "Generate Promotional Msg"
        log_out_button_label = "Log out"

    st.info(page_description)

    cur_dir = f"./data/{st.session_state.access_code}/analyzed/"
    files = [f for f in os.listdir(cur_dir) if os.path.isfile(os.path.join(cur_dir, f))]
    files.sort(key=lambda f: os.path.getmtime(os.path.join(cur_dir, f)), reverse=True)
    st.session_state.selected_file = st.selectbox("Select a file:", files)
    selected_file_path = None
    if st.session_state.selected_file:
        selected_file_path = os.path.join(cur_dir, st.session_state.selected_file)
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

    default_columns = []
    if 'Classification Tag' in data.columns:
        default_columns.append('Classification Tag')
    if 'enable_dm' in data.columns:
        default_columns.append('enable_dm')

    # 选择要过滤的列
    filter_columns = st.multiselect(filter_columns_label, data.columns, default=default_columns)

    # 初始化过滤器
    filters = {}
    for column in filter_columns:
        # 显示过滤器
        unique_values = data[column].unique()
        selected_values = st.multiselect(f"Select values from {column} to filter:", unique_values)
        if selected_values:
            filters[column] = selected_values

    # 过滤数据
    filtered_data = data.copy()
    for column, selected_values in filters.items():
        filtered_data = filtered_data[filtered_data[column].isin(selected_values)]

    # 显示过滤后的数据（仅在有选择值时显示）
    if filters:
        st.subheader("Filtered Data")
        st.dataframe(filtered_data)

        # 获取更多的用户信息
        if st.button(collect_user_details_button_label):
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
                    alive_username = random.choice(['Zacks89757'])
                    status_code, details = collect_user_link_details(alive_username, batch_user_ids)
                    if status_code == 200:
                        user_details.extend(details)
                    else:
                        st.error(f"Failed to collect user details: {status_code}, {details}")

                    # 更新进度条
                    progress_bar.progress((i + batch_size) / total_users if (i + batch_size) < total_users else 1.0)

                if user_details:
                    st.success("User details collected successfully!")
                    st.write(user_details)
                else:
                    st.error("Failed to collect user details. Please check your API settings.")

                # 将 details 补充到读取的本地文件中
                details_df = pd.DataFrame(user_details)

                # 确保索引重置
                data.reset_index(drop=True, inplace=True)
                details_df.reset_index(drop=True, inplace=True)

                # 合并数据
                merged_data = pd.merge(data, details_df, on="reply_user_id", how="left")

                # 保存合并后的数据到原文件
                merged_data.to_csv(selected_file_path, index=False)
                st.success(f"Merged data saved to {selected_file_path}")
                st.balloons()
                time.sleep(3)
                st.rerun()

        # 仅在采集数据后才显示
        if not filtered_data.empty and 'enable_dm' in data.columns:
            # 输入示例的提示词
            system_prompt = st.text_input("Enter the prompt for generating promotional SMS:",
                                        "You are a marketing assistant. Your task is to generate personalized "
                                        "promotional SMS messages for promoting product 【XYZ】.")

            # 选择模型
            model = st.selectbox("Select a model:", ["gpt-4o-mini", "gpt-4o"])

            batch_size = st.selectbox("Select batch size", [10, 20, 30, 40, 50])

            # 生成推广短信按钮
            if st.button(generate_msg_button_label):
                with st.spinner('Generating Msg...'):
                    result_df = generate_promotional_sms(model, system_prompt, filtered_data.iloc[:, :3],
                                                        batch_size=batch_size)
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
    else:
        st.warning("No filters applied. Please select values to filter the data.")
