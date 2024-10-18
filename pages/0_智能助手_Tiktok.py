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
import extra_streamlit_components as stx

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
from pages.tiktok_tab.generate_msg import generate_msg
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

# 在 hide_streamlit_style 之后添加以下 CSS
tab_style = """
<style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #F0F2F6;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FFFFFF;
    }
    .stTabs [data-baseweb="tab-panel"] {
        background-color: #FFFFFF;
        border-radius: 0px 0px 4px 4px;
        border: 1px solid #D3D3D3;
        padding: 15px;
    }
</style>
"""
st.markdown(tab_style, unsafe_allow_html=True)

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
if 'current_tab' not in st.session_state:
    st.session_state.current_tab = "评论收集"
if 'tab_changed' not in st.session_state:
    st.session_state.tab_changed = False

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

    # 使用 stx.tab_bar 创建标签页
    chosen_id = stx.tab_bar(data=[
        stx.TabBarItemData(id="tab1", title="评论收集"),
        stx.TabBarItemData(id="tab2", title="评论过滤"),
        stx.TabBarItemData(id="tab3", title="评论分析_AI"),
        stx.TabBarItemData(id="tab4", title="生成文案_AI"),
        stx.TabBarItemData(id="tab5", title="触达客户"),
        stx.TabBarItemData(id="tab6", title="后台监控"),
        stx.TabBarItemData(id="tab7", title="账号管理")
    ])

    # 创建一个占位符来显示内容
    content_placeholder = st.container()

    # 根据选择的标签页加载内容
    if chosen_id == "tab1":
        with content_placeholder:
            logger.info(f"评论收集================================================")
            data_collect(db)
    elif chosen_id == "tab2":
        with content_placeholder:
            logger.info(f"评论过滤================================================")
            data_filter(db)
    elif chosen_id == "tab3":
        with content_placeholder:
            logger.info(f"评论分析_AI================================================")
            data_analyze(db)
    elif chosen_id == "tab4":
        with content_placeholder:
            logger.info(f"生成文案_AI================================================")
            generate_msg(db)
    elif chosen_id == "tab5":
        with content_placeholder:
            logger.info(f"触达客户================================================")
            send_msg(db)
    elif chosen_id == "tab6":
        with content_placeholder:
            logger.info(f"后台监控================================================")
            worker_vnc(db)
    elif chosen_id == "tab7":
        with content_placeholder:
            logger.info(f"账号管理================================================")
            account_management(db)
    else:
        content_placeholder.warning("请选择一个功能标签页")

finally:
    # 脚本结束时关闭数据库连接
    if 'db' in st.session_state:
        st.session_state.db.disconnect()
        del st.session_state.db
