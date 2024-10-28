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
from sidebar import sidebar_home
from common.redis_client import RedisClient
from common.collector_sdk import check_x_login_status

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="AI_Marketing", page_icon="🤖", layout="wide")

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

# 检查 session_state 中是否已经有语言设置
if 'language' not in st.session_state:
    # 如果没有，使用 URL 参数或默认值初始化
    st.session_state.language = st.query_params.get('language', 'CN')

# 在侧边栏添加语言选择
language = st.sidebar.radio(
    "选择语言 / Choose Language", 
    ("CN", "EN"), 
    index=0 if st.session_state.language == 'CN' else 1  # 修正 index 参数
)

# 将语言选择存储到 session_state 和 URL 参数
st.session_state.language = language
st.query_params.language = language

# 根据选择的语言设置文本
if st.session_state.language == "CN":
    page_title = "AI 智能助手"
    access_granted_message = "访问已授权！"
    access_denied_message = "访问未授权！"
    enter_access_code_message = "请输入访问码"
    submit_button_label = "提交"
    incorrect_code_message = "访问码错误，请重试。"
    log_out_button_label = "登出"
    account_management_label = "X 爬虫账号管理"
    existing_accounts_label = "现有账号:"
    add_new_account_label = "新增账号"
    username_label = "用户名"
    email_label = "电子邮件"
    password_label = "密码"
    submit_new_account_label = "提交新账号"
    refresh_account_status_label = "刷新账号状态"
    delete_account_label = "删除"
else:
    page_title = "AI Marketing"
    access_granted_message = "Access Granted!"
    access_denied_message = "Access not Granted!"
    enter_access_code_message = "Please Enter the Access Code"
    submit_button_label = "Submit"
    incorrect_code_message = "Incorrect Code. Please try again."
    log_out_button_label = "Log out"
    account_management_label = "X Crawler Account Management"
    existing_accounts_label = "Existing Accounts:"
    add_new_account_label = "Add New Account"
    username_label = "Username"
    email_label = "Email"
    password_label = "Password"
    submit_new_account_label = "Submit New Account"
    refresh_account_status_label = "Refresh Account Status"
    delete_account_label = "Delete"

# Render Streamlit pages
st.title(page_title)

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
    st.title(enter_access_code_message)
    code = st.text_input("Access Code", type="password")
    if st.button(submit_button_label, type="primary"):
        if code in CONFIG['access_code_list']:
            access_granted = True
            st.query_params.access_code = code
            st.session_state.access_code = code
            st.success(access_granted_message)
            logger.info(f"{code} login successfully.")
            st.balloons()
            # 刷新页面
            st.rerun()
        else:
            st.error(incorrect_code_message)
            logger.warning(f"{code} login failed.")

if access_granted:
    sidebar_home()
    st.success(access_granted_message)
    st.markdown("------")

    st.page_link("pages/0_智能助手_Tiktok.py", label="TikTok智能助手", icon="1️⃣", use_container_width=True)
    st.page_link("pages/1_智能助手_X.py", label="X智能助手", icon="2️⃣", use_container_width=True)
    st.markdown("-----")

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

    if st.sidebar.button(label=log_out_button_label, type="primary"):
        st.query_params.clear()
        st.session_state.clear()
        st.rerun()
else:
    pass