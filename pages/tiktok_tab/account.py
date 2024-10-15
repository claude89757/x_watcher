import streamlit as st
from collectors.common.mysql import MySQLDatabase

def account_management():
    st.header("账号管理")
    
    db = MySQLDatabase()
    db.connect()

    try:
        # 获取可用的worker IP列表
        available_workers = db.get_available_workers()

        # 添加新账号
        with st.form("add_account"):
            st.subheader("添加新账号")
            username = st.text_input("用户名")
            password = st.text_input("密码", type="password")
            email = st.text_input("邮箱")
            status = st.selectbox("状态", ["active", "inactive"])
            login_ips = st.multiselect("登录主机IP", options=available_workers)
            submit = st.form_submit_button("添加账号")

            if submit:
                if db.add_tiktok_account(username, password, email, status, login_ips):
                    st.success("账号添加成功")
                else:
                    st.error("账号添加失败")

        # 显示现有账号
        st.subheader("现有账号")
        accounts = db.get_tiktok_accounts()
        if accounts:
            for account in accounts:
                with st.expander(f"账号: {account['username']} (ID: {account['id']})"):
                    col1, col2, col3 = st.columns([2, 2, 1])
                    with col1:
                        st.write(f"邮箱: {account['email']}")
                        st.write(f"当前登录主机IP: {account['login_ips']}")
                    with col2:
                        new_status = st.selectbox("状态", ["active", "inactive"], index=0 if account['status'] == 'active' else 1, key=f"status_{account['id']}")
                        if st.button("更新状态", key=f"update_status_{account['id']}"):
                            if db.update_tiktok_account_status(account['id'], new_status):
                                st.success("状态更新成功")
                            else:
                                st.error("状态更新失败")
                    with col3:
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

    finally:
        db.disconnect()
