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

    # 获取高意向客户
    keywords = db.get_all_tiktok_keywords()
    selected_keyword = st.selectbox("选择关键词", keywords)
    high_intent_customers = db.get_potential_customers(selected_keyword)

    # 从数据库获取推广消息
    messages = db.get_tiktok_messages(selected_keyword, status='pending')
    if not messages:
        st.warning("没有找到待发送的推广消息")
        return

    # 使用DataFrame展示推广消息
    df = pd.DataFrame(messages)
    st.dataframe(df[['id', 'message']])

    # 选择要发送的消息
    selected_message_id = st.selectbox("选择要发送的消息", options=df['id'].tolist())
    selected_message = df[df['id'] == selected_message_id]['message'].values[0]

    # 添加批量大小和等待时间的设置
    batch_size = st.slider("每批发送数量", min_value=1, max_value=20, value=5)
    wait_time = st.slider("每批等待时间（秒）", min_value=30, max_value=300, value=60)

    if st.button("开始发送"):
        user_ids = [customer['user_id'] for customer in high_intent_customers]
        
        try:
            results = []
            if len(account_ids) == 1:
                # 单个账号发送所有消息
                account_id = account_ids[0]
                account = next(acc for acc in accounts if acc['id'] == account_id)
                worker_ip = account['login_ips'].split(',')[0]  # 使用第一个登录IP
                
                response = requests.post(
                    f"http://{worker_ip}:5000/send_promotion_messages",
                    json={
                        "user_ids": user_ids,
                        "message": selected_message,
                        "account_id": account_id,
                        "keyword": selected_keyword,
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
                users_per_account = math.ceil(len(user_ids) / len(account_ids))
                for i, account_id in enumerate(account_ids):
                    account = next(acc for acc in accounts if acc['id'] == account_id)
                    worker_ip = account['login_ips'].split(',')[0]  # 使用第一个登录IP
                    start_index = i * users_per_account
                    end_index = min((i + 1) * users_per_account, len(user_ids))
                    account_user_ids = user_ids[start_index:end_index]
                    
                    response = requests.post(
                        f"http://{worker_ip}:5000/send_promotion_messages",
                        json={
                            "user_ids": account_user_ids,
                            "message": selected_message,
                            "account_id": account_id,
                            "keyword": selected_keyword,
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
            time.sleep(wait_time * (len(user_ids) // batch_size + 1))  # 估算发送完成时间
            
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
        
        except Exception as e:
            st.error(f"发送过程中发生错误: {str(e)}")
