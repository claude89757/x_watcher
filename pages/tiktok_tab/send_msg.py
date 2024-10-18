import streamlit as st
from common.log_config import setup_logger

# 配置日志
logger = setup_logger(__name__)

def send_msg(db):
    st.info("自动批量关注、留言、发送推广信息给高意向客户")
