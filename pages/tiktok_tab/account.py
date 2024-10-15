import streamlit as st
import requests
import urllib
from collectors.common.mysql import MySQLDatabase


def account_management(db):
    st.header("账号管理")

    # 获取可用的worker IP列表
    available_workers = db.get_available_workers()

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
                login_ips = st.multiselect("登录主机IP", options=available_workers)
                submit = st.form_submit_button("提交")

                if submit:
                    if db.add_tiktok_account(username, password, email, login_ips):
                        st.success("账号添加成功")
                        st.session_state.show_add_account_form = False  # 添加成功后关闭表单
                    else:
                        st.error("账号添加失败")

    # 显示现有账号
    st.subheader("现有账号")
    accounts = db.get_tiktok_accounts()
    if accounts:
        for account in accounts:
            with st.expander(f"账号: {account['username']} (ID: {account['id']})"):
                col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
                with col1:
                    st.write(f"邮箱: {account['email']}")
                    st.write(f"当前登录主机IP: {account['login_ips']}")
                with col2:
                    st.write(f"状态: {account['status']}")
                with col3:
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
                                
                                # 显示VNC窗口
                                for worker_ip in triggered_workers:
                                    worker_info = db.get_worker_by_ip(worker_ip)
                                    if worker_info:
                                        st.subheader(f"Worker {worker_ip} VNC窗口")
                                        novnc_password = worker_info['novnc_password']
                                        encoded_password = urllib.parse.quote(novnc_password)
                                        vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
                                        st.components.v1.iframe(vnc_url, width=800, height=600)
                            else:
                                st.error("未能触发任何worker")
                with col4:
                    if st.button("删除", key=f"delete_{account['id']}"):
                        if db.delete_tiktok_account(account['id']):
                            st.success("账号删除成功")
                        else:
                            st.error("账号删除失败")
                
                # 修改登录主机IP
                st.write("修改登录主机IP:")
                new_login_ips = st.multiselect("新的登录主机IP", options=available_workers, default=account['login_ips'].split(',') if account['login_ips'] else [], key=f"new_ips_{account['id']}")
                if st.button("更新登录主机IP", key=f"update_ips_{account['id']}"):
                    if db.update_tiktok_account_login_ips(account['id'], new_login_ips):
                        st.success("登录主机IP更新成功")
                    else:
                        st.error("登录主机IP更新失败")
    else:
        st.write("暂无账号")
