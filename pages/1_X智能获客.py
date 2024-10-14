#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024年10月14日
@Author  : Your Name
@File    : 1_X智能获客.py
@Software: cursor
"""

import time
from datetime import timedelta

import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger
from sidebar import sidebar



# 设置页面配置
st.set_page_config(page_title="X智能获客", page_icon="🤖", layout="wide")

# Configure logger
logger = setup_logger(__name__)

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
    st.write("评论收集功能正在开发中")

with tab2:
    st.write("评论收集功能正在开发中")

with tab3:
    st.write("评论收集功能正在开发中")

with tab4:
    st.write("评论收集功能正在开发中")

with tab5:
    st.write("评论收集功能正在开发中")

