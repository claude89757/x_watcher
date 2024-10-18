import streamlit as st
from common.log_config import setup_logger

# 配置日志
logger = setup_logger(__name__)

def send_msg(db):
    st.header("触达客户")

    # 添加其他功能，如消息模板、发送历史等