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
import traceback
from datetime import timedelta

import pandas as pd
import streamlit as st
import requests
import websocket
from PIL import Image
import io

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

def create_vnc_websocket(worker_ip, password):
    ws_url = f"ws://{worker_ip}:6080/websockify"
    try:
        ws = websocket.create_connection(ws_url, timeout=10)
        auth_message = json.dumps({"type": "auth", "password": password})
        ws.send(auth_message)
        response = ws.recv()
        try:
            json_response = json.loads(response)
            if json_response.get("type") != "auth_success":
                raise Exception(f"VNC authentication failed: {json_response}")
        except json.JSONDecodeError:
            # 如果响应不是JSON格式，打印原始响应以进行调试
            raise Exception(f"Unexpected response format. Raw response: {response}")
        return ws
    except Exception as e:
        raise Exception(f"Failed to create WebSocket connection: {str(e)}")

def get_vnc_screen(ws):
    try:
        request = json.dumps({"type": "request_update"})
        ws.send(request)
        response = ws.recv()
        if isinstance(response, bytes):
            # 假设返回的是 PNG 格式的图像数据
            image = Image.open(io.BytesIO(response))
            return image
        else:
            # 如果响应不是字节格式，可能是错误消息
            raise Exception(f"Unexpected response format. Raw response: {response}")
    except Exception as e:
        raise Exception(f"Failed to get VNC screen: {str(e)}")

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
            # 创建选择框让用户选择要查看的 worker
            worker_options = [f"{w['worker_name']} ({w['worker_ip']})" for w in active_workers]
            selected_worker = st.selectbox("选择要查看的 Worker", options=worker_options)
            
            # 获取选中的 worker 信息
            selected_worker_info = next(w for w in active_workers if f"{w['worker_name']} ({w['worker_ip']})" == selected_worker)
            
            # 显示选中 worker 的信息
            st.write(f"状态: {selected_worker_info['status']}")
            if selected_worker_info['current_task_ids']:
                st.write(f"当前任务: {selected_worker_info['current_task_ids']}")
            else:
                st.write("当前无任务")
            
            # 创建一个占位符来显示 VNC 画面
            vnc_placeholder = st.empty()
            
            # 创建连接状态指示器
            connection_status = st.empty()
            
            # 添加手动刷新按钮
            if st.button("刷新 VNC 画面"):
                connection_status.info("正在连接 VNC...")
                try:
                    ws = create_vnc_websocket(selected_worker_info['worker_ip'], selected_worker_info['novnc_password'])
                    connection_status.success("VNC 连接成功")
                    
                    screen_data = get_vnc_screen(ws)
                    vnc_placeholder.image(screen_data, caption="VNC 画面", use_column_width=True)
                    
                    ws.close()
                except Exception as e:
                    connection_status.error(f"VNC 连接失败: {str(e)}")
                    st.error("请检查 worker 状态或稍后重试")
                    st.error(f"详细错误信息: {traceback.format_exc()}")
            
            # 自动刷新功能
            auto_refresh = st.checkbox("自动刷新 (每10秒)")
            if auto_refresh:
                try:
                    while True:
                        connection_status.info("正在连接 VNC...")
                        try:
                            ws = create_vnc_websocket(selected_worker_info['worker_ip'], selected_worker_info['novnc_password'])
                            connection_status.success("VNC 连接成功")
                            
                            screen_data = get_vnc_screen(ws)
                            vnc_placeholder.image(screen_data, caption="VNC 画面", use_column_width=True)
                            
                            ws.close()
                        except Exception as e:
                            connection_status.error(f"VNC 连接失败: {str(e)}")
                            st.error(f"详细错误信息: {traceback.format_exc()}")
                        
                        time.sleep(10)
                except st.ScriptRunnerError:
                    st.warning("自动刷新已停止")
        else:
            st.info("当前没有活跃的 workers")

    finally:
        # 确保在函数结束时关闭数据库连接
        db.disconnect()

with tab2:
    st.header("评论过滤")

with tab3:
    st.header("评论分析_AI")
