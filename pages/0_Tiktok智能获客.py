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
import base64
from Crypto.Cipher import DES

import pandas as pd
import streamlit as st
import requests
import websocket
import socket
import ssl
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
        websocket.enableTrace(True)
        ws = websocket.create_connection(
            ws_url,
            timeout=30,
            sslopt={"cert_reqs": ssl.CERT_NONE},
            header=["Pragma: no-cache", "Cache-Control: no-cache"]
        )
        
        logger.info(f"WebSocket连接已建立: {ws_url}")
        
        rfb_version = ws.recv()
        logger.info(f"Received RFB version: {rfb_version}")
        
        ws.send(b"RFB 003.008\n", opcode=websocket.ABNF.OPCODE_BINARY)
        logger.info("Sent client version")
        
        security_types = ws.recv()
        logger.info(f"Received security types: {security_types}")
        
        if security_types.startswith(b'\x00\x00\x00\x00'):
            error_msg = security_types[4:].decode('utf-8')
            raise Exception(f"VNC authentication error: {error_msg}")
        
        if b'\x02' not in security_types:
            raise Exception("VNC authentication not supported")
        
        ws.send(b"\x02", opcode=websocket.ABNF.OPCODE_BINARY)
        logger.info("Sent authentication choice")
        
        challenge = ws.recv()
        if len(challenge) != 16:
            raise Exception(f"Invalid challenge length: {len(challenge)}")
        logger.info(f"Received challenge: {challenge.hex()}")
        
        response = encrypt_password(password, challenge)
        ws.send(response, opcode=websocket.ABNF.OPCODE_BINARY)
        logger.info("Sent encrypted challenge response")
        
        auth_result = ws.recv()
        if auth_result != b"\x00\x00\x00\x00":
            raise Exception("VNC authentication failed")
        
        logger.info("VNC authentication successful")
        return ws
    except Exception as e:
        logger.error(f"Failed to create WebSocket connection: {str(e)}")
        raise

def get_vnc_screen(ws):
    try:
        # 发送 FramebufferUpdateRequest（使用二进制帧）
        ws.send(b"\x03\x00\x00\x00\x00\x00\x00\x00\x00", opcode=websocket.ABNF.OPCODE_BINARY)
        
        # 读取响应
        frame_type = ws.recv()
        if frame_type[0] == 0:  # FramebufferUpdate
            num_rects = int.from_bytes(frame_type[2:4], byteorder='big')
            
            # 假设只有一个矩形，包含整个屏幕
            rect_data = ws.recv()
            x = int.from_bytes(rect_data[0:2], byteorder='big')
            y = int.from_bytes(rect_data[2:4], byteorder='big')
            width = int.from_bytes(rect_data[4:6], byteorder='big')
            height = int.from_bytes(rect_data[6:8], byteorder='big')
            encoding_type = int.from_bytes(rect_data[8:12], byteorder='big')
            
            if encoding_type == 0:  # Raw encoding
                raw_data = ws.recv()
                image = Image.frombytes('RGBA', (width, height), raw_data)
                return image
            else:
                raise Exception(f"Unsupported encoding type: {encoding_type}")
        else:
            raise Exception(f"Unexpected frame type: {frame_type[0]}")
    except Exception as e:
        raise Exception(f"Failed to get VNC screen: {str(e)}")

def encrypt_password(password, challenge):
    def fixkey(key):
        newkey = []
        for ki in range(len(key)):
            bsrc = ord(key[ki])
            btgt = 0
            for i in range(8):
                if bsrc & (1 << i):
                    btgt = btgt | (1 << (7 - i))
            newkey.append(btgt)
        return bytes(newkey)

    key = fixkey(password.ljust(8, '\x00')[:8])
    des = DES.new(key, DES.MODE_ECB)
    return des.encrypt(challenge)

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
