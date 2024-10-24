import streamlit as st
from common.log_config import setup_logger
import requests
import pandas as pd
import math
import time
import urllib.parse
from collectors.common.mysql import MySQLDatabase

# 配置日志
logger = setup_logger(__name__)

def send_msg(db: MySQLDatabase):
    st.info("自动批量关注、留言、发送推广信息给高意向客户")

    # 获取关键词列表
    keywords = db.get_all_tiktok_message_keywords()
    if not keywords:
        st.warning("关键词未保存推广信息")
        return

    selected_keyword = st.selectbox("选择关键词", keywords)

    # 从数据库获取所有推广消息（包括已发送和未发送的）
    all_messages = db.get_tiktok_messages(selected_keyword)
    if not all_messages:
        st.warning("没有找到推广消息")
        return

    # 使用DataFrame展示所有推广消息
    df = pd.DataFrame(all_messages)
    st.subheader("所有推广消息")
    st.dataframe(df[['id', 'user_id', 'message', 'status', 'delivery_method', 'created_at', 'updated_at']])

    # 获取未成功发送的消息
    pending_messages = [msg for msg in all_messages if msg['status'] in ['pending', 'failed']]
    processing_messages = [msg for msg in all_messages if msg['status'] == 'processing']

    if processing_messages:
        st.warning("有正在处理中的消息，请等待处理完成后再开始新的发送任务。")
        
        # 获取正在处理消息的worker IP
        worker_ips = set(db.get_worker_ip_for_processing_messages(selected_keyword))
        
        if not worker_ips:
            st.info("当前没有worker正在处理消息，可能是处理已经完成或尚未开始。")
        else:
            # 添加一个按钮来显示VNC画面
            if st.button("显示正在处理的Worker VNC画面"):
                st.subheader("正在处理消息的Worker VNC画面")
                
                for worker_ip in worker_ips:
                    worker_info = db.get_worker_by_ip(worker_ip)
                    if worker_info:
                        novnc_password = worker_info['novnc_password']
                        encoded_password = urllib.parse.quote(novnc_password)
                        vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
                        
                        st.write(f"Worker IP: {worker_ip}")
                        st.components.v1.iframe(vnc_url, width=1000, height=750)
        
        # 显示处理进度
        st.subheader("处理进度")
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_completed = False
        start_time = time.time()
        while not all_completed and time.time() - start_time < 600:  # 最多等待10分钟
            time.sleep(20)  # 每20秒检查一次
            
            current_status = db.get_tiktok_messages_status([msg['user_id'] for msg in processing_messages])
            completed_count = sum(1 for status in current_status if status in ['sent', 'failed'])
            progress = completed_count / len(processing_messages)
            
            progress_bar.progress(progress)
            status_text.text(f"已完成: {completed_count}/{len(processing_messages)}")
            
            if completed_count == len(processing_messages):
                all_completed = True
        
        if all_completed:
            st.success("所有消息已处理完成！")
        else:
            st.warning("部分消息可能仍在处理中。")
        
        time.sleep(20)
        st.rerun()
        
        return

    if not pending_messages:
        st.warning("没有待发送的推广消息")
        return

    st.subheader("待发送的推广消息")
    pending_df = pd.DataFrame(pending_messages)
    st.dataframe(pending_df[['id', 'user_id', 'message']])

    # 使用列布局来将设置放在一行
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_messages = st.selectbox("发送消息数量", options=[50, 100, 200, 500, 1000], index=1)
    
    with col2:
        batch_size = st.selectbox("每批发送数量", options=[1, 5, 10, 20], index=1)
    
    with col3:
        wait_time = st.selectbox("每批等待时间（秒）", options=[30, 60, 120, 300], index=1)

    # 获取所有TikTok账号
    accounts = db.get_tiktok_accounts()
    account_options = [f"{account['username']} (ID: {account['id']}, IP: {account['login_ips']})" for account in accounts]
    selected_accounts = st.multiselect("选择发送账号", account_options)
    account_ids = [int(account.split("ID: ")[1].split(",")[0]) for account in selected_accounts]

    # 只有当选择了账号且按钮未被点击过时才显示
    if account_ids:
        if st.button("开始发送", key="send_msg_button", type="primary"):
            
            # 限制发送消息数量，只选择未成功发送的消息
            user_messages = pending_df[['user_id', 'message']].to_dict('records')[:total_messages]
            
            active_workers = []
            
            with st.spinner("正在启动消息发送任务..."):
                if len(account_ids) == 1:
                    # 单个账号发送所有消息
                    account_id = account_ids[0]
                    account = next(acc for acc in accounts if acc['id'] == account_id)
                    worker_ip = account['login_ips'].split(',')[0]  # 使用第一个登录IP
                    
                    response = requests.post(
                        f"http://{worker_ip}:5000/send_promotion_messages",
                        json={
                            "keyword": selected_keyword,
                            "user_messages": user_messages,
                            "account_id": account_id,
                            "batch_size": batch_size,
                            "wait_time": wait_time
                        }
                    )
                    
                    if response.status_code == 200:
                        response_data = response.json()
                        st.info(f"账号 {account['username']} 的消息发送任务已启动，执行worker IP: {response_data['worker_ip']}")
                        active_workers.append(worker_ip)
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
                            response_data = response.json()
                            st.info(f"账号 {account['username']} 的消息发送任务已启动，执行worker IP: {response_data['worker_ip']}")
                            active_workers.append(worker_ip)
                        else:
                            st.error(f"账号 {account['username']} 触发发送失败: {response.json().get('error', '未知错误')}")
            
            st.success("所有消息发送任务已启动，请稍等查看worker的VNC画面。")
            time.sleep(20)
            st.rerun()  # 重新运行页面以刷新数据
    else:
        st.warning("请选择至少一个发送账号")
