import streamlit as st
from common.log_config import setup_logger

# 配置日志
logger = setup_logger(__name__)

def send_msg(db):
    st.header("触达客户")
    
    # 从数据库获取高意向客户列表
    high_intent_customers = db.get_high_intent_customers()
    
    # 显示客户列表
    st.subheader("高意向客户列表")
    for customer in high_intent_customers:
        st.write(f"客户ID: {customer['id']}, 用户名: {customer['username']}")
    
    # 选择客户
    selected_customer = st.selectbox("选择要联系的客户", high_intent_customers, format_func=lambda x: x['username'])
    
    # 输入消息
    message = st.text_area("输入要发送的消息")
    
    # 发送按钮
    if st.button("发送消息"):
        if selected_customer and message:
            # 这里添加发送消息的逻辑
            st.success(f"消息已发送给 {selected_customer['username']}")
            logger.info(f"消息已发送给客户 {selected_customer['id']}")
        else:
            st.warning("请选择客户并输入消息")

    # 添加其他功能，如消息模板、发送历史等