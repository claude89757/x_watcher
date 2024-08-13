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
from common.azure_openai import send_text_to_gpt
from sidebar import sidebar

# Configure logger
logger = setup_logger(__name__)


# set page config
st.set_page_config(page_title="Analyze Data", page_icon="🤖", layout="wide")

# init session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "selected_file" not in st.session_state:
    st.session_state.selected_file = ""
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
st.markdown("Sending data to a LLM model for analysis, where the model will process "
            "and generate insights based on the provided data.")

src_dir = f"./data/{st.session_state.access_code}/processed/"
dst_dir = f"./data/{st.session_state.access_code}/analyzed/"
files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
files.sort(key=lambda f: os.path.getmtime(os.path.join(src_dir, f)), reverse=True)
st.session_state.selected_file = st.selectbox("Select a file to analyze:", files)
selected_file_path = None
if st.session_state.selected_file:
    selected_file_path = os.path.join(src_dir, st.session_state.selected_file)
    st.subheader(f"File Data Preview: {st.session_state.selected_file}")
    # 检查本地是否已有文件
    try:
        # 获取文件信息
        data = pd.read_csv(selected_file_path)
        file_size = os.path.getsize(selected_file_path)  # 文件大小（字节）
        file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(selected_file_path))  # 文件修改时间

        if data is not None:
            st.dataframe(data.head(500), use_container_width=True, height=400)
        else:
            st.write("No data to display.")
    except Exception as e:
        st.error(f"Error loading data from local file: {e}")
else:
    st.warning("No processed data, return to filter data...")
    time.sleep(3)
    st.switch_page("pages/2_Preprocess_Data.py")

prompt = st.text_area("Enter your prompt for analysis:",
                      value="Analyze the data and identify potential customers interested in purchasing product XYZ")


selected_model = st.selectbox("Current Model:", ["gpt-4o-mini", "gpt-4o"])

batch_size = st.selectbox("Select batch size", [10, 20, 30, 40, 50])

analyze_button = st.button("Analysis Data" if not st.session_state.get('analysis_run', False)
                           else "Reanalyze")

# 在分析之后
if analyze_button:
    st.session_state.analysis_run = True
    with st.spinner('Analyzing...'):
        data = pd.read_csv(selected_file_path)
        st.write("Data loaded successfully. Starting analysis...")

        if prompt:
            result_df = send_text_to_gpt(selected_model, prompt, data, batch_size=batch_size)

            if not result_df.empty:
                # 定义新的输出目录
                output_file = os.path.join(dst_dir, f"{st.session_state.selected_file}")
                # 保存分析结果
                # logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                # logger.info(result_df.head(10))
                # logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                result_df.to_csv(output_file, index=False)

                # 显示结果output_file
                st.success(f"Analysis complete! Results saved to {output_file}.")
                st.dataframe(result_df.head(500), use_container_width=True, height=400)

            else:
                st.error("Failed to generate analysis results. Please check your prompt or API settings.")
        else:
            st.warning("Prompt cannot be empty. Please provide a valid prompt.")

# Next
if st.session_state.analyzed_data_file_count:
    if st.button(label="Next: AI Generate Msg", type='primary'):
        st.success("Ready to AI Generate Msg...")
        st.balloons()
        time.sleep(3)
        st.switch_page("pages/4_AI_Generate_Msg.py")
    else:
        pass
