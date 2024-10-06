#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 14:18
@Author  : claude
@File    : Home.py
@Software: PyCharm
"""
import time
import os
import datetime

import streamlit as st
from common.config import CONFIG
from common.log_config import setup_logger
from sidebar import sidebar
from common.redis_client import RedisClient
from common.collector_sdk import check_x_login_status

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="(Demo)X_AI_Marketing", page_icon="🤖", layout="wide")

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

# Render Streamlit pages
st.title("Demo: X AI Marketing")

access_granted = False
if st.session_state.get('access_code') and st.session_state.get('access_code') in CONFIG['access_code_list']:
    # session中有缓存
    st.query_params.access_code = st.session_state.access_code
    access_granted = True
elif st.query_params.get('access_code') and st.query_params.get('access_code') in CONFIG['access_code_list']:
    # URL中有缓存
    st.session_state.access_code = st.query_params.access_code
    access_granted = True
else:
    st.title("Please Enter the Access Code")
    code = st.text_input("Access Code", type="password")
    if st.button("Submit", type="primary"):
        if code in CONFIG['access_code_list']:
            access_granted = True
            st.query_params.access_code = code
            st.session_state.access_code = code
            st.success("Access Granted!")
            logger.info(f"{code} login successfully.")
            st.balloons()
            time.sleep(3)
            st.switch_page("pages/1_Collect_Data.py", )
        else:
            st.error("Incorrect Code. Please try again.")
            logger.warning(f"{code} login failed.")

if access_granted:
    sidebar()
    st.success("Access Granted!")
    st.markdown("-----")
    st.page_link("pages/1_Collect_Data.py", label="Collect Data", icon="1️⃣", use_container_width=True)
    st.page_link("pages/2_Preprocess_Data.py", label="Preprocess Data", icon="2️⃣", use_container_width=True)
    st.page_link("pages/3_AI_Analyze_Data.py", label="AI Analyze Data", icon="3️⃣️", use_container_width=True)
    st.page_link("pages/4_AI_Generate_Msg.py", label="AI Generate Msg", icon="4️⃣", use_container_width=True)
    st.page_link("pages/5_Send_Promotional_Msg.py", label="Send Promotional MSG", icon="5️⃣", use_container_width=True)
    st.markdown("-----")

    # 初始化 Redis 客户端
    redis_client = RedisClient(db=0)

    # 新增推特账号管理功能
    def manage_twitter_accounts():
        st.subheader("X Crawler Account Management")

        # 从 Redis 中加载现有账号
        accounts = redis_client.get_json_data('twitter_accounts') or {}

        # 显示现有账号
        if accounts:
            st.write("Existing Accounts:")
            for username, details in accounts.items():
                # 使用 emoji 显示登录状态
                status_emoji = "✅" if details.get('status') == 'Success' else "❌"
                last_checked = details.get('last_checked', 'Never')
                with st.expander(f"{status_emoji} {username}  Last Checked: {last_checked}"):
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    with col1:
                        st.write(f"Email: {details['email']}")
                    with col2:
                        # 从 Redis 中读取状态
                        st.write(f"Status: {details.get('status', 'Unknown')}")
                    with col3:
                        last_checked = details.get('last_checked', 'Never')
                        st.write(f"Last Checked: {last_checked}")
                    with col4:
                        if st.button(f"Delete {username}", key=f"delete_{username}"):
                            del accounts[username]
                            redis_client.set_json_data('twitter_accounts', accounts)
                            st.success(f"Deleted account: {username}")
                            logger.info(f"Deleted account: {username}")

        # 显示“新增账号”按钮
        if st.button("Add New Account"):
            st.session_state.show_add_account_form = True

        # 仅在点击“新增账号”按钮后显示输入表单
        if st.session_state.get('show_add_account_form', False):
            new_username = st.text_input("Username", key="new_username")
            new_email = st.text_input("Email", key="new_email")
            new_password = st.text_input("Password", type="password", key="new_password")

            if st.button("Submit New Account"):
                if new_username and new_email and new_password:
                    accounts[new_username] = {
                        'email': new_email,
                        'password': new_password,
                        'status': 'Unknown',
                        'last_checked': 'Never'
                    }
                    redis_client.set_json_data('twitter_accounts', accounts)
                    st.success(f"Added account: {new_username}")
                    logger.info(f"Added account: {new_username}")
                    st.session_state.show_add_account_form = False  # 隐藏表单
                else:
                    st.error("Please fill in all fields to add a new account.")

        # 刷新账号状态
        if st.button("Refresh Account Status"):
            for username, details in accounts.items():
                email = details['email']
                password = details['password']
                status_code, response_text = check_x_login_status(username, email, password)
                if status_code == 200:
                    accounts[username]['status'] = 'Success'
                else:
                    accounts[username]['status'] = 'Failed'
                accounts[username]['last_checked'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                redis_client.set_json_data('twitter_accounts', accounts)  # 更新 Redis 中的状态
                st.write(f"Account {username} login status: {accounts[username]['status']}")
                st.write(f"Last Checked: {accounts[username]['last_checked']}")
                logger.info(f"Account {username} login status: {accounts[username]['status']}")

            # 筛选页面
            st.rerun()

    # 调用推特账号管理功能
    manage_twitter_accounts()

    # 创建文件夹
    base_path = os.path.join("./data", st.session_state.access_code)
    folders = ['raw', 'processed', 'analyzed', 'msg', 'records']
    if not os.path.exists(base_path):
        os.makedirs(base_path)
        st.write(f"Created directory: {base_path}")
        logger.info(f"Created directory: {base_path}")
    else:
        # st.write(f"Directory already exists: {path}")
        pass
    for folder in folders:
        path = os.path.join(base_path, folder)
        if not os.path.exists(path):
            os.makedirs(path)
            st.write(f"Created directory: {path}")
            logger.info(f"Created directory: {path}")
        else:
            # st.write(f"Directory already exists: {path}")
            pass

    # todo: 显示当前用户的状态和数据信息

    if st.sidebar.button(label="Log out", type="primary"):
        st.query_params.clear()
        st.session_state.clear()
        st.rerun()
else:
    pass