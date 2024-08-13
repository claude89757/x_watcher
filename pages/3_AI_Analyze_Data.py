#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 23:08
@Author  : claude
@File    : 3_AI_Analyze_Data.py
@Software: PyCharm
"""

import os
import logging
import time

import pandas as pd
import requests
import streamlit as st
from io import StringIO

from config import CONFIG


def send_text_to_gpt(system_prompt: str, data: pd.DataFrame, batch_size: int = 1000) -> pd.DataFrame:
    """
    发送数据到GPT模型，获取分析结果。
    :param system_prompt: 系统级指令，用于提供分析上下文。
    :param data: 包含需要分析的数据的DataFrame。
    :param batch_size: 每次发送的数据行数，默认1000行。
    :return: 包含分析结果的DataFrame。
    """
    model = ""
    results = []
    max_tokens = 2000  # 假设每行输入和输出都很长

    # 初始化 Streamlit 进度条和文本显示
    progress_bar = st.progress(0)
    status_text = st.empty()

    # 计算总批次数量
    total_batches = len(data) // batch_size + (1 if len(data) % batch_size != 0 else 0)

    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size]
        batch_csv = batch.to_csv(index=False)

        batch_prompt = f"{system_prompt}\n\n" \
                       f"Analyze the following data and provide the output in CSV format " \
                       f"with the following structure:" \
                       f"\n1. Original data with all columns intact." \
                       f"\n2. A new column named 'Analysis Explanation' " \
                       f"with insights or explanations for each row based on the data." \
                       f"\n3. A new column named 'Classification Tag' with a category or tag indicating " \
                       f"the potential interest level of each row in product XYZ." \
                       f"\n\nData:\n{batch_csv}"

        payload = {
            "messages": [
                {"role": "system", "content": batch_prompt}
            ],
            "temperature": 0.7,
            "top_p": 0.95,
            "max_tokens": max_tokens
        }

        url = f"https://chatgpt3.openai.azure.com/openai/deployments/{model}/chat/completions?" \
              f"api-version=2024-02-15-preview"
        response = requests.post(url,
                                 headers={
                                     "Content-Type": "application/json",
                                     "api-key": CONFIG.get("azure_open_api_key"),
                                 },
                                 json=payload
        )
        response.raise_for_status()

        response_content = response.json()['choices'][0]['message']['content']

        if "```csv" in response_content:
            csv_content = response_content.split("```csv")[1].split("```")[0].strip()
        else:
            csv_content = response_content

        # Debugging: Print the CSV content to verify format
        print("CSV Content:", csv_content)

        # Handle potential parsing errors
        try:
            csv_rows = csv_content.splitlines()
            reader = pd.read_csv(StringIO("\n".join(csv_rows)), sep=",", iterator=True, chunksize=batch_size)

            for chunk in reader:
                results.append(chunk)

        except pd.errors.ParserError as e:
            st.error(f"Error parsing CSV data: {e}")
            continue

        # 每次处理完一批后更新进度条和状态信息
        progress_percentage = (i + batch_size) / len(data)
        progress_bar.progress(min(progress_percentage, 1.0))
        status_text.text(
            f"Batch {i // batch_size + 1}/{total_batches}: Sent {len(batch)} rows, received {len(csv_rows) - 1} rows.")

    result_df = pd.concat(results, ignore_index=True)

    # 处理完成后，清除状态信息
    progress_bar.empty()
    status_text.text("Processing completed!")

    return result_df

# 配置日志记录器
logging.basicConfig(format="\n%(asctime)s\n%(message)s", level=logging.INFO, force=True)

st.set_page_config(page_title="Analyze Data", page_icon="🤖", layout="wide")

# if st.session_state.get('access_code') and st.session_state.get('access_code') in CONFIG['access_code_list']:
#     st.query_params.access_code = st.session_state.access_code
# elif st.query_params.get('access_code') and st.query_params.get('access_code') in CONFIG['access_code_list']:
#     st.session_state.access_code = st.query_params.access_code
# else:
#     st.warning("Access not Granted!")
#     st.switch_page("Home.py", )

# 初始化session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "matching_files" not in st.session_state:
    st.session_state.matching_files = ""
if "analysis_run" not in st.session_state:
    st.session_state.analysis_run = False

if st.session_state.access_code:
    st.query_params.access_code = st.session_state.access_code
else:
    st.warning("Access not Granted!")
    time.sleep(3)
    st.switch_page("Home.py", )

# 强制响应式布局
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

# 隐藏Streamlit元素
hide_streamlit_style = """
            <style>
            .stDeployButton {visibility: hidden;}
            [data-testid="stToolbar"] {visibility: hidden !important;}
            footer {visibility: hidden !important;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

st.title("Step 3: AI Analyze Data")
st.markdown("Sending data to a large model for analysis, where the model will process "
            "and generate insights based on the provided data.")

src_dir = f"./data/{st.session_state.access_code}/processed/"
dst_dir = f"./data/{st.session_state.access_code}/analyzed/"
files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
selected_file = st.selectbox("Select a file to analyze:", files)
selected_file_path = None
if selected_file:
    selected_file_path = os.path.join(src_dir, selected_file)
    st.subheader(f"File Data Preview: {selected_file}")
    data = pd.read_csv(selected_file_path)
    st.write(data)
else:
    st.warning("No processed data, return to filter data...")
    time.sleep(3)
    st.switch_page("pages/2_Filter_Data.py")

prompt = st.text_area("Enter your prompt for analysis:",
                      value="Analyze the data and identify potential customers interested in purchasing product XYZ")

# 根据是否已经运行分析来确定按钮标签
analyze_button = st.button("Start Analysis" if not st.session_state.analysis_run else "Reanalyze", type="primary")

# 在分析之后
if analyze_button:
    with st.spinner('Analyzing data...'):
        data = pd.read_csv(selected_file_path)
        st.write("Data loaded successfully. Starting analysis...")

        if prompt:
            # 调用AI模型进行分析
            result_df = send_text_to_gpt(prompt, data)

            if not result_df.empty:
                # 定义新的输出目录
                output_file = os.path.join(dst_dir, f"res_{selected_file}")
                # 保存分析结果
                result_df.to_csv(output_file, index=False)

                # 显示结果
                st.success(f"Analysis complete! Results saved to {output_file}.")
                st.write("## Analysis Results")
                st.write(result_df)
            else:
                st.error("Failed to generate analysis results. Please check your prompt or API settings.")
        else:
            st.warning("Prompt cannot be empty. Please provide a valid prompt.")
