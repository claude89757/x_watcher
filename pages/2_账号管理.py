# 标准库导入
import os
import time
import json
from datetime import timedelta
import urllib

# 第三方库导入
import streamlit as st
import requests

# 本地模块导入
from common.config import CONFIG
from common.log_config import setup_logger
from sidebar import sidebar_for_tiktok
from collectors.common.mysql import MySQLDatabase

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="账号管理", page_icon="🤖", layout="wide")

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


def get_status_emoji(status):
    if status == 'active':
        return "✅"
    elif status == 'inactive':
        return "❌"
    else:
        return "❓"

# 创建数据库连接
db = MySQLDatabase()
db.connect()

# 添加大标题
st.title("账号管理")

# 在st.title("账号管理")之后添加以下代码
tab1, tab2 = st.tabs(["TikTok账号管理", "X账号管理"])

try:
    with tab1:
        st.info("本页面用于管理TikTok账号。")
        # 获取可用的worker IP列表
        available_worker_ips = db.get_available_worker_ips()

        # 使用会话状态来跟踪是否显示添加账号表单
        if 'show_add_account_form' not in st.session_state:
            st.session_state.show_add_account_form = False

        # 添加账号按钮
        if st.button("添加账号"):
            st.session_state.show_add_account_form = not st.session_state.show_add_account_form

        # 添加新账号
        if st.session_state.show_add_account_form:
            with st.expander("添加新账号", expanded=True):
                with st.form("add_account"):
                    username = st.text_input("用户名")
                    password = st.text_input("密码", type="password")
                    email = st.text_input("邮箱")
                    login_ips = st.multiselect("登录主机IP", options=available_worker_ips)
                    submit = st.form_submit_button("提交")

                    if submit:
                        if db.add_tiktok_account(username, password, email, login_ips):
                            st.success("账号添加成功")
                            st.session_state.show_add_account_form = False  # 添加成功后关闭表单
                        else:
                            st.error("账号添加失败")

        # 显示现有账号
        st.subheader("爬虫账号列表")
        accounts = db.get_tiktok_accounts()
        if accounts:
            for account in accounts:
                status_emoji = get_status_emoji(account['status'])
                with st.expander(f"{status_emoji} 账号: {account['username']} (ID: {account['id']}) - 状态: {account['status']}"):
                    st.write(f"邮箱: {account['email']}")
                    st.write(f"当前登录主机IP: {account['login_ips']}")

                    if st.button("删除账号", key=f"delete_{account['id']}", type="primary"):
                        if db.delete_tiktok_account(account['id']):
                            st.success("账号删除成功")
                        else:
                            st.error("账号删除失败")
                    
                    if st.button("刷新状态", key=f"refresh_{account['id']}"):
                        login_ips = account['login_ips'].split(',') if account['login_ips'] else []
                        if not login_ips:
                            st.error("该账号没有设置登录主机IP")
                        else:
                            triggered_workers = []
                            for ip in login_ips:
                                try:
                                    response = requests.post(
                                        f"http://{ip}:5000/check_tiktok_account",
                                        json={"account_id": account['id']},
                                        timeout=5  # 设置5秒超时
                                    )
                                    if response.status_code == 200:
                                        triggered_workers.append(ip)
                                    else:
                                        st.warning(f"Worker {ip} 响应状态码 {response.status_code}")
                                except requests.RequestException as e:
                                    st.error(f"触发 worker {ip} 失败: {str(e)}")
                            
                            if triggered_workers:
                                st.success(f"账号状态刷新任务已触发。已触发的workers: {', '.join(triggered_workers)}")
                                st.warning("请【人工操作】VNC窗口，通过邮箱登录（通过忘记密码的方式登录），以验证账号状态！！！")
                                
                                # 显示VNC窗口
                                for worker_ip in triggered_workers:
                                    worker_info = db.get_worker_by_ip(worker_ip)
                                    if worker_info:
                                        st.subheader(f"Worker {worker_ip} VNC窗口")
                                        novnc_password = worker_info['novnc_password']
                                        encoded_password = urllib.parse.quote(novnc_password)
                                        vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
                                        
                                        # 使用st.components.v1.iframe显示VNC窗口，增大尺寸
                                        st.components.v1.iframe(vnc_url, width=1000, height=800)
                                        
                                        # 添加一个按钮来手动关闭VNC窗口
                                        if st.button(f"关闭 Worker {worker_ip} 的VNC窗口", key=f"close_vnc_{worker_ip}"):
                                            st.info(f"Worker {worker_ip} 的VNC窗口已关闭。")
                                            st.rerun()
                                        
                                        # 添加一个提示，告诉用户如何关闭VNC窗口
                                        st.info("VNC窗口将保持打开状态。如果您想关闭它，请点击上方的'关闭VNC窗口'按钮。")
                            else:
                                st.error("未能触发任何worker")
                    

        else:
            st.write("暂无账号")


    with tab2:
        st.info("本页面用于管理X平台账号。")
            
        # 获取可用的worker IP列表
        available_worker_ips = db.get_available_worker_ips()

        # 使用会话状态来跟踪是否显示添加账号表单
        if 'show_add_x_account_form' not in st.session_state:
            st.session_state.show_add_x_account_form = False

        # 添加账号按钮
        if st.button("添加X平台账号"):
            st.session_state.show_add_x_account_form = not st.session_state.show_add_x_account_form

        # 添加新X平台账号
        if st.session_state.show_add_x_account_form:
            with st.expander("添加新X平台账号", expanded=True):
                with st.form("add_x_account"):
                    username = st.text_input("用户名")
                    password = st.text_input("密码", type="password")
                    email = st.text_input("邮箱")
                    login_ips = st.multiselect("登录主机IP", options=available_worker_ips)
                    submit = st.form_submit_button("提交")

                    if submit:
                        if db.add_x_account(username, password, email, login_ips):
                            st.success("X平台账号添加成功")
                            st.session_state.show_add_x_account_form = False  # 添加成功后关闭表单
                        else:
                            st.error("X平台账号添加失败")

        # 显示现有X平台账号
        st.subheader("X平台账号列表")
        x_accounts = db.get_x_accounts()
        if x_accounts:
            for account in x_accounts:
                status_emoji = get_status_emoji(account['status'])
                with st.expander(f"{status_emoji} 账号: {account['username']} (ID: {account['id']}) - 状态: {account['status']}"):
                    st.write(f"邮箱: {account['email']}")
                    st.write(f"当前登录主机IP: {account['login_ips']}")

                    if st.button("删除账号", key=f"delete_x_{account['id']}", type="primary"):
                        if db.delete_x_account(account['id']):
                            st.success("X平台账号删除成功")
                        else:
                            st.error("X平台账号删除失败")
                    
                    if st.button("刷新状态", key=f"refresh_x_{account['id']}"):
                        login_ips = account['login_ips'].split(',') if account['login_ips'] else []
                        if not login_ips:
                            st.error("该X平台账号没有设置登录主机IP")
                        else:
                            triggered_workers = []
                            for ip in login_ips:
                                try:
                                    response = requests.post(
                                        f"http://{ip}:5000/check_x_account",
                                        json={"account_id": account['id']},
                                        timeout=5  # 设置5秒超时
                                    )
                                    if response.status_code == 200:
                                        triggered_workers.append(ip)
                                    else:
                                        st.warning(f"Worker {ip} 响应状态码 {response.status_code}")
                                except requests.RequestException as e:
                                    st.error(f"触发 worker {ip} 失败: {str(e)}")
                            
                            if triggered_workers:
                                st.success(f"X平台账号状态刷新任务已触发。已触发的workers: {', '.join(triggered_workers)}")
                                st.warning("请【人工操作】VNC窗口，通过邮箱登录（通过忘记密码的方式登录），以验证账号状态！！！")
                                
                                # 显示VNC窗口
                                for worker_ip in triggered_workers:
                                    worker_info = db.get_worker_by_ip(worker_ip)
                                    if worker_info:
                                        st.subheader(f"Worker {worker_ip} VNC窗口")
                                        novnc_password = worker_info['novnc_password']
                                        encoded_password = urllib.parse.quote(novnc_password)
                                        vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
                                        
                                        # 使用st.components.v1.iframe显示VNC窗口，增大尺寸
                                        st.components.v1.iframe(vnc_url, width=1000, height=800)
                                        
                                        # 添加一个按钮来手动关闭VNC窗口
                                        if st.button(f"关闭 Worker {worker_ip} 的VNC窗口", key=f"close_vnc_x_{worker_ip}"):
                                            st.info(f"Worker {worker_ip} 的VNC窗口已关闭。")
                                            st.rerun()
                                        
                                        # 添加一个提示，告诉用户如何关闭VNC窗口
                                        st.info("VNC窗口将保持打开状态。如果您想关闭它，请点击上方的'关闭VNC窗口'按钮。")
                            else:
                                st.error("未能触发任何worker")
        else:
            st.write("暂无X平台账号")
except Exception as e:
    st.error(f"发生错误: {e}")
finally:
    db.disconnect()
