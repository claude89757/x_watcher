# 标准库导入
import os
import time
import json
from datetime import timedelta
import urllib

# 第三方库导入
import streamlit as st

# 本地模块导入
from common.config import CONFIG
from common.log_config import setup_logger
from sidebar import sidebar_for_tiktok
from collectors.common.mysql import MySQLDatabase

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="后台监控", page_icon="🤖", layout="wide")

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
st.title("后台监控")
st.info("本页面用于查看和连接到活跃的 worker 的 VNC 画面。")

# 创建数据库连接
db = MySQLDatabase()
db.connect()

try:
    # 获取所有活跃的 workers
    active_workers = db.get_worker_list()

    if active_workers:
        # 创建两列布局
        col1, col2 = st.columns(2)
        
        with col1:
            # 创建选择框让用户选择要查看的 worker
            worker_options = [f"{w['worker_name']} ({w['worker_ip']})" for w in active_workers]
            selected_worker = st.selectbox("选择要查看的 Worker", options=worker_options)
            
            # 获取选中的 worker 信息
            selected_worker_info = next(w for w in active_workers if f"{w['worker_name']} ({w['worker_ip']})" == selected_worker)
            
            # 显示选中 worker 的信息
            st.write(f"状态: {selected_worker_info['status']}")
            
            # 构造 VNC URL，包含密码参数
            worker_ip = selected_worker_info['worker_ip']
            novnc_password = selected_worker_info['novnc_password']
            encoded_password = urllib.parse.quote(novnc_password)
            vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
            
            # 添加加载单个 VNC 画面的按钮
            if st.button("加载选中 Worker 的 VNC 画面"):
                # 显示 VNC 窗口
                st.components.v1.iframe(vnc_url, width=800, height=600)
        
        with col2:
            # 添加加载所有 VNC 画面的按钮
            if st.button("加载所有 Worker 的 VNC 画面"):
                # 为每个 worker 创建 VNC 窗口
                for worker in active_workers:
                    worker_ip = worker['worker_ip']
                    novnc_password = worker['novnc_password']
                    encoded_password = urllib.parse.quote(novnc_password)
                    vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
                    
                    # 显示 worker 名称和状态
                    st.write(f"Worker: {worker['worker_name']} ({worker_ip})")
                    st.write(f"状态: {worker['status']}")
                    
                    # 显示 VNC 窗口
                    st.components.v1.iframe(vnc_url, width=800, height=600)
                    
                    # 添加分隔线
                    st.markdown("---")
    
    else:
        st.info("当前没有活跃的 workers")
except Exception as e:
    st.error(f"发生错误: {str(e)}")
finally:
    # 脚本结束时关闭数据库连接
    db.disconnect()
