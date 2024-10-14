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
from sidebar import sidebar
from sidebar import cache_file_counts
from common.redis_client import RedisClient
from collectors.common.mysql import MySQLDatabase
from pages.tiktok_tab.data_collect import data_collect

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="Tiktok智能获客", page_icon="🤖", layout="wide")

# 从URL读取缓存数据
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if 'language' not in st.session_state:
    st.session_state.language = st.query_params.get('language')

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
    st.query_params.language = st.session_state.language
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

# 创建标签页
tab1, tab2, tab3 = st.tabs(["评论收集", "评论过滤", "评论分析_AI"])

# 在文件的开头附近添加 VNC 密码
VNC_PASSWORD = os.environ.get('VNC_PASSWORD', 'default_password')  # 从环境变量获取密码，如果没有设置则使用默认密码

with tab1:
    st.header("评论收集")
    data_collect()
    
    # 添加 VNC 窗口
    st.subheader("Worker 实时画面")
    
    # 初始化数据库连接
    db = MySQLDatabase()
    db.connect()
    
    try:
        # 获取所有活跃的 workers
        active_workers = db.get_worker_list()
        
        if active_workers:
            # 创建多列布局
            cols = st.columns(2)  # 可以根据需要调整列数
            
            for index, worker in enumerate(active_workers):
                worker_ip = worker['worker_ip']
                worker_name = worker['worker_name']
                novnc_password = worker['novnc_password'] or 'default_password'
                # 构造带有密码的 VNC URL
                vnc_url = f"http://{worker_ip}:6080/vnc.html?password={urllib.parse.quote(novnc_password)}&autoconnect=true"
                
                with cols[index % 2]:  # 在两列之间交替放置 VNC 窗口
                    st.subheader(f"Worker: {worker_name} ({worker_ip})")
                    st.components.v1.iframe(vnc_url, width=400, height=300)  # 调整宽度和高度以适应布局
                    
                    # 显示 worker 状态和当前任务
                    st.write(f"状态: {worker['status']}")
                    if worker['current_task_ids']:
                        st.write(f"当前任务: {worker['current_task_ids']}")
                    else:
                        st.write("当前无任务")
        else:
            st.info("当前没有活跃的 workers")

    finally:
        # 确保在函数结束时关闭数据库连接
        db.disconnect()

with tab2:
    st.header("评论过滤")

with tab3:
    st.header("评论分析_AI")
