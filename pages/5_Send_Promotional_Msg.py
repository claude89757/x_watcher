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
from common.azure_openai import generate_promotional_sms
from common.collector_sdk import check_x_login_status
from common.collector_sdk import send_promotional_msg
from sidebar import sidebar

# Configure logger
logger = setup_logger(__name__)

st.set_page_config(page_title="Send Msg", page_icon="🤖", layout="wide")


# init session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "matching_files" not in st.session_state:
    st.session_state.matching_files = ""
if "login_status" not in st.session_state:
    st.session_state.login_status = st.query_params.get("login_status", "")
if "username" not in st.session_state:
    st.session_state.username = st.query_params.get("username", "")
if "email" not in st.session_state:
    st.session_state.email = st.query_params.get("email", "")
if "password" not in st.session_state:
    st.session_state.password = ""


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

st.title("Step 5: Send Promotional Msg")
st.markdown("Automate the sending of personalized promotional messages based on AI analysis results")

cur_dir = f"./data/{st.session_state.access_code}/msg/"
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
        # Display the first and last column
        data_df = data.iloc[:, [0, -1]]
        st.dataframe(data_df)

        # Add selection box for the number of private messages
        send_msg_num = st.number_input("Select the number of messages to send", min_value=1, max_value=len(data_df),
                                       value=min(5, len(data_df)))

    except Exception as e:
        st.error(f"Error loading data from local file: {e}")
else:
    st.warning("No processed data, return to AI Generate Msg...")
    time.sleep(3)
    st.switch_page("pages/4_AI_Generate_Msg.py")

st.markdown("------")
st.subheader("X Account Verify")
username = st.text_input("Twitter Username", value=st.session_state.username)
email = st.text_input("Email", value=st.session_state.username)
password = st.text_input("Password", type="password", value=st.session_state.username)

if st.button("Verify Login Status"):
    with st.spinner('Analyzing data...'):
        return_code, msg = check_x_login_status(username, email, password)
    # 登录验证
    if return_code == 200:
        st.success("Verify Login Status successful!")
        st.session_state.login_status = "online"
        st.query_params.login_status = "online"
    else:
        st.warning("Verify Login Status failed.")
    # 缓存
    st.session_state.username = username
    st.session_state.email = email
    st.session_state.password = password
    st.query_params.username = username
    st.query_params.email = email

if st.session_state.login_status == "online":
    if st.button("Send Promotional Messages"):
        # 初始化进度条
        progress_bar = st.progress(0)
        results = []
        # 发送推广私信
        for index, row in data_df.iterrows():
            user_id = row[0]
            message = row[-1]
            user_link = f"https://x.com/{user_id}"
            code, text = send_promotional_msg(username, email, password, user_link, message)
            results.append({
                'User ID': user_id,
                'Message': message,
                'Status': 'Success' if code == 200 else 'Failure',
                'Details': text
            })
            # 更新进度条
            progress_bar.progress((index + 1) / len(data_df))
            time.sleep(random.uniform(1, 10))

            if len(results) >= send_msg_num:
                # 达到上限
                break

        # 转换结果为 DataFrame
        results_df = pd.DataFrame(results)

        # 显示结果
        st.success("All promotional messages processed!")
        st.subheader("Results")
        st.table(results_df)
