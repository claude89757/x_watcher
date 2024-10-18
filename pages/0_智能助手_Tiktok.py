#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 18:52
@Author  : claude
@File    : 0_Tiktok智能获客.py
@Software: PyCharm
"""
import os
import re
import time
import datetime
import urllib.parse
import random
import json
from datetime import timedelta

import pandas as pd
import streamlit as st
import requests

from common.config import CONFIG
from common.cos import list_latest_files
from common.cos import download_file
from common.log_config import setup_logger
from common.collector_sdk import call_collect_data_from_x
from sidebar import sidebar_for_tiktok
from sidebar import cache_file_counts
from common.redis_client import RedisClient
from collectors.common.mysql import MySQLDatabase
from pages.tiktok_tab.data_collect import data_collect
from pages.tiktok_tab.worker_vnc import worker_vnc
from pages.tiktok_tab.account import account_management
from pages.tiktok_tab.data_filter import data_filter
from pages.tiktok_tab.data_analyze import data_analyze
from pages.tiktok_tab.send_msg import send_msg

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="Tiktok智能助手", page_icon="🤖", layout="wide")

# 从URL读取缓存数据
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if 'language' not in st.session_state:
    st.session_state.language = st.query_params.get('language')

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
    st.query_params.language = st.session_state.language
    sidebar_for_tiktok()
else:
    st.warning("Access not Granted!")
    time.sleep(3)
    st.switch_page("主页.py", )

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

# 添加大标题
st.title("Tiktok智能助手 🤖")

# 使用 st.session_state 来存储数据库连接
if 'db' not in st.session_state:
    st.session_state.db = MySQLDatabase()
    st.session_state.db.connect()

# 使用数据库连接
db = st.session_state.db

# 定义缓存文件路径
KEYWORD_CACHE_FILE = 'tiktok_keyword_cache.json'

def load_keyword_from_cache():
    """从缓存文件加载关键字"""
    if os.path.exists(KEYWORD_CACHE_FILE):
        with open(KEYWORD_CACHE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('keyword', '')
    return ''

# 在主函数的开始处添加以下代码
if 'cached_keyword' not in st.session_state:
    st.session_state.cached_keyword = load_keyword_from_cache()

try:
    # 获取全局统计数据
    global_stats = db.get_global_stats()

    # 在侧边栏显示全局统计数据
    st.sidebar.header("全局数据统计")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.metric("已收集关键字", global_stats['keyword_count'])
        st.metric("潜在客户", global_stats['potential_customer_count'])
    with col2:
        st.metric("评论总数", global_stats['comment_count'])
        st.metric("高意向客户", global_stats['high_intent_customer_count'])

    # 创建标签页
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["评论收集", "评论过滤", "评论分析_AI", "批量私信_AI", "后台监控", "账号管理"])

    # 在创建标签页之前，确保cached_keyword已经被加载到session_state中
    if 'cached_keyword' not in st.session_state:
        st.session_state.cached_keyword = load_keyword_from_cache()

    with tab1:
        data_collect(db)

    with tab2:
        data_filter(db)

    with tab3:
        data_analyze(db)

    with tab4:
        send_msg(db)

    with tab5:
        worker_vnc(db)

    with tab6:
        account_management(db)

finally:
    # 脚本结束时关闭数据库连接
    if 'db' in st.session_state:
        st.session_state.db.disconnect()
        del st.session_state.db
