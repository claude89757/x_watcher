#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024年10月14日
@Author  : Your Name
@File    : 1_X智能获客.py
@Software: cursor
"""

import time
import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger
from sidebar import sidebar_for_x

# 导入各个标签页的函数
from x_tab.data_collect import data_collect
from x_tab.data_filter import comment_filter
from x_tab.data_analyze import comment_analyze
from x_tab.generate_msg import generate_msg
from x_tab.send_msg import send_msg

# 设置页面配置
st.set_page_config(page_title="X智能获客", page_icon="🤖", layout="wide")

# Configure logger
logger = setup_logger(__name__)

# 从URL读取缓存数据
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if 'language' not in st.session_state:
    st.session_state.language = st.query_params.get('language')
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = 0

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
    st.query_params.language = st.session_state.language
    sidebar_for_x()
else:
    st.warning("Access not Granted!")
    time.sleep(3)
    st.switch_page("Home.py")

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
tabs = ["评论收集", "评论过滤", "评论分析_AI", "私信生成_AI", "私信发送"]
tab1, tab2, tab3, tab4, tab5 = st.tabs(tabs)

# 定义导航函数
def navigate(direction):
    st.session_state.active_tab = (st.session_state.active_tab + direction) % len(tabs)
    st.experimental_rerun()

# 在每个标签页中运行相应的py文件内容
with tab1:
    if st.session_state.active_tab == 0:
        data_collect()
        col1, col2 = st.columns(2)
        with col2:
            if st.button("下一步 →", key="next_1"):
                navigate(1)

with tab2:
    if st.session_state.active_tab == 1:
        comment_filter()
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← 上一步", key="prev_2"):
                navigate(-1)
        with col2:
            if st.button("下一步 →", key="next_2"):
                navigate(1)

with tab3:
    if st.session_state.active_tab == 2:
        comment_analyze()
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← 上一步", key="prev_3"):
                navigate(-1)
        with col2:
            if st.button("下一步 →", key="next_3"):
                navigate(1)

with tab4:
    if st.session_state.active_tab == 3:
        generate_msg()
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← 上一步", key="prev_4"):
                navigate(-1)
        with col2:
            if st.button("下一步 →", key="next_4"):
                navigate(1)

with tab5:
    if st.session_state.active_tab == 4:
        send_msg()
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← 上一步", key="prev_5"):
                navigate(-1)
