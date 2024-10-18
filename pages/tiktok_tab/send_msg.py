import streamlit as st
from common.log_config import setup_logger
import requests
import pandas as pd
import math
import time

# 配置日志
logger = setup_logger(__name__)

def send_msg(db):
    st.info("自动批量关注、留言、发送推广信息给高意向客户")

    # 获取所有TikTok账号
    accounts = db.get_tiktok_accounts()
    account_options = [f"{account['username']} (ID: {account['id']}, IP: {account['login_ips']})" for account in accounts]
    selected_accounts = st.multiselect("选择发送账号", account_options)
    account_ids = [int(account.split("ID: ")[1].split(",")[0]) for account in selected_accounts]

    # 获取关键词列表
    keywords = db.get_all_tiktok_keywords()
    selected_keyword = st.selectbox("选择关键词", keywords)

    # 从数据库获取推广消息
    messages = db.get_tiktok_messages(selected_keyword, status='pending')
    if not messages:
        st.warning("没有找到待发送的推广消息")
        return

    # 使用DataFrame展示推广消息
    df = pd.DataFrame(messages)
    st.dataframe(df[['id', 'user_id', 'message']])

    # 使用列布局来将设置放在一行
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_messages = st.selectbox("发送消息数量", options=[50, 100, 200, 500, 1000], index=1)
    
    with col2:
        batch_size = st.selectbox("每批发送数量", options=[1, 5, 10, 20], index=1)
    
    with col3:
        wait_time = st.selectbox("每批等待时间（秒）", options=[30, 60, 120, 300], index=1)

    if st.button("开始发送"):
        # 限制发送消息数量
        user_messages = df[['user_id', 'message']].to_dict('records')[:total_messages]
        
        if len(account_ids) == 1:
            # 单个账号发送所有消息
            account_id = account_ids[0]
            account = next(acc for acc in accounts if acc['id'] == account_id)
            worker_ip = account['login_ips'].split(',')[0]  # 使用第一个登录IP
            
            response = requests.post(
                f"http://{worker_ip}:5000/send_promotion_messages",
                json={
                    "user_messages": user_messages,
                    "account_id": account_id,
                    "batch_size": batch_size,
                    "wait_time": wait_time
                }
            )
            
            if response.status_code == 200:
                st.info(f"账号 {account['username']} 的消息发送任务已触发")
            else:
                st.error(f"账号 {account['username']} 触发发送失败: {response.json().get('error', '未知错误')}")
        else:
            # 多个账号平均发送消息
            messages_per_account = math.ceil(len(user_messages) / len(account_ids))
            for i, account_id in enumerate(account_ids):
                account = next(acc for acc in accounts if acc['id'] == account_id)
                worker_ip = account['login_ips'].split(',')[0]  # 使用第一个登录IP
                start_index = i * messages_per_account
                end_index = min((i + 1) * messages_per_account, len(user_messages))
                account_user_messages = user_messages[start_index:end_index]
                
                response = requests.post(
                    f"http://{worker_ip}:5000/send_promotion_messages",
                    json={
                        "user_messages": account_user_messages,
                        "account_id": account_id,
                        "batch_size": batch_size,
                        "wait_time": wait_time
                    }
                )
                
                if response.status_code == 200:
                    st.info(f"账号 {account['username']} 的消息发送任务已触发")
                else:
                    st.error(f"账号 {account['username']} 触发发送失败: {response.json().get('error', '未知错误')}")
        
        # 等待一段时间后检查发送状态
        st.info("等待消息发送完成...")
        time.sleep(wait_time * (len(user_messages) // batch_size + 1))  # 估算发送完成时间
        
        # 从数据库中读取发送状态
        sent_messages = db.get_tiktok_messages(selected_keyword, status='sent')
        failed_messages = db.get_tiktok_messages(selected_keyword, status='failed')
        
        success_count = len(sent_messages)
        fail_count = len(failed_messages)
        
        st.success(f"发送完成！成功发送 {success_count} 条消息，失败 {fail_count} 条。")
        
        # 显示详细结果
        if sent_messages:
            st.success("成功发送的消息:")
            for msg in sent_messages:
                st.success(f"用户ID: {msg['user_id']}")
        
        if failed_messages:
            st.error("发送失败的消息:")
            for msg in failed_messages:
                st.error(f"用户ID: {msg['user_id']}")
