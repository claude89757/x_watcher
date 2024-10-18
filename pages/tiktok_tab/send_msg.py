import streamlit as st
from common.log_config import setup_logger
import requests
import time
import concurrent.futures
import urllib.parse

# 配置日志
logger = setup_logger(__name__)

def send_single_message(user_id, message, account_id):
    try:
        response = requests.post(
            "http://localhost:5000/send_promotion_message",
            json={"user_id": user_id, "message": message, "account_id": account_id}
        )
        return response.json()
    except Exception as e:
        logger.error(f"发送消息给 {user_id} 时发生错误: {str(e)}")
        return {"error": str(e)}

def send_msg(db):
    st.info("自动批量关注、留言、发送推广信息给高意向客户")

    # 获取所有TikTok账号
    accounts = db.get_tiktok_accounts()
    account_options = [f"{account['username']} (ID: {account['id']})" for account in accounts]
    selected_account = st.selectbox("选择发送账号", account_options)
    account_id = int(selected_account.split("ID: ")[1][:-1])

    # 获取高意向客户
    keywords = db.get_all_tiktok_keywords()
    selected_keyword = st.selectbox("选择关键词", keywords)
    high_intent_customers = db.get_potential_customers(selected_keyword)

    # 设置并发数和间隔时间
    concurrency = st.slider("并发数量", min_value=1, max_value=10, value=3)
    interval = st.slider("发送间隔(秒)", min_value=1, max_value=60, value=5)

    # 输入推广消息
    promotion_message = st.text_area("输入推广消息", "")

    # 选择是否显示VNC画面
    show_vnc = st.checkbox("显示实时画面")

    if st.button("开始发送"):
        progress_bar = st.progress(0)
        status_text = st.empty()

        # 如果选择显示VNC画面，创建一个占位符
        vnc_placeholder = st.empty()

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for i, customer in enumerate(high_intent_customers):
                user_id = customer['user_id']
                futures.append(executor.submit(send_single_message, user_id, promotion_message, account_id))
                
                if (i + 1) % concurrency == 0 or i == len(high_intent_customers) - 1:
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        if "error" in result:
                            st.error(f"发送失败: {result['error']}")
                        else:
                            st.success(f"发送成功: {result['message']}")
                    
                    futures = []
                    time.sleep(interval)
                
                progress = (i + 1) / len(high_intent_customers)
                progress_bar.progress(progress)
                status_text.text(f"进度: {i+1}/{len(high_intent_customers)}")

                # 如果选择显示VNC画面，更新VNC画面
                if show_vnc:
                    account = db.get_tiktok_account_by_id(account_id)
                    if account and account['login_ips']:
                        worker_ip = account['login_ips'].split(',')[0]  # 使用第一个登录IP
                        worker_info = db.get_worker_by_ip(worker_ip)
                        if worker_info:
                            novnc_password = worker_info['novnc_password']
                            encoded_password = urllib.parse.quote(novnc_password)
                            vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
                            vnc_placeholder.components.v1.iframe(vnc_url, width=800, height=600)

        st.success("所有消息发送完成！")

        # 如果显示了VNC画面，添加一个关闭按钮
        if show_vnc:
            if st.button("关闭VNC窗口"):
                vnc_placeholder.empty()
