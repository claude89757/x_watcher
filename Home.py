#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 14:18
@Author  : claude
@File    : Home.py
@Software: PyCharm
"""

import logging
import time
import os

import streamlit as st
from config import CONFIG

# Configure logger
logging.basicConfig(format="\n%(asctime)s\n%(message)s", level=logging.INFO, force=True)

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
            time.sleep(1)
            st.switch_page("pages/1_Collect_Data.py", )
        else:
            st.error("Incorrect Code. Please try again.")

if access_granted:
    st.success("Access Granted!")
    st.markdown("-----")
    st.page_link("pages/1_Collect_Data.py", label="Collect Data", icon="1️⃣", use_container_width=True)
    st.page_link("pages/2_Filter_Data.py", label="Preprocess & Filter Data", icon="2️⃣", use_container_width=True)
    st.page_link("pages/3_AI_Analyze_Data.py", label="AI Analyze Data", icon="3️⃣️", use_container_width=True)
    st.page_link("pages/4_Send_Promotional_Msg.py", label="Send Promotional MSG", icon="4️⃣", use_container_width=True)

    # 创建文件夹
    base_path = os.path.join("./data", st.session_state.access_code)
    folders = ['raw', 'processed', 'analyzed']
    for folder in folders:
        path = os.path.join(base_path, folder)
        if not os.path.exists(path):
            os.makedirs(path)
            st.write(f"Created directory: {path}")
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
