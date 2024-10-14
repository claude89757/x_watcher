import os
import sys
import time
import importlib
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

# 添加当前目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 导入其他py文件
x_tab_1 = importlib.import_module("x_tab.1_评论收集")
x_tab_2 = importlib.import_module("x_tab.2_评论过滤")
x_tab_3 = importlib.import_module("x_tab.3_评论分析_AI")
x_tab_4 = importlib.import_module("x_tab.4_私信生成_AI")
x_tab_5 = importlib.import_module("x_tab.5_私信发送")

# 设置页面配置
st.set_page_config(page_title="X智能获客", page_icon="🤖", layout="wide")

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
tab1, tab2, tab3, tab4, tab5 = st.tabs(["评论收集", "评论过滤", "评论分析_AI", "私信生成_AI", "私信发送"])

# 在每个标签页中运行相应的py文件内容
with tab1:
    x_tab_1.main()

with tab2:
    x_tab_2.main()

with tab3:
    x_tab_3.main()

with tab4:
    x_tab_4.main()

with tab5:
    x_tab_5.main()

