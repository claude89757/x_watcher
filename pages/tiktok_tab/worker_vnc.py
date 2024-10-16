import urllib
import streamlit as st
from collectors.common.mysql import MySQLDatabase


def worker_vnc(db: MySQLDatabase):    
    """
    本页面用于查看和连接到活跃的 worker 的 VNC 画面。
    """
    # 全局面板 
    st.info("本页面用于查看和连接到活跃的 worker 的 VNC 画面。")

    # 获取所有活跃的 workers
    active_workers = db.get_worker_list()
    
    if active_workers:
        # 创建选择框让用户选择要查看的 worker
        worker_options = [f"{w['worker_name']} ({w['worker_ip']})" for w in active_workers]
        selected_worker = st.selectbox("选择要查看的 Worker", options=worker_options)
        
        # 获取选中的 worker 信息
        selected_worker_info = next(w for w in active_workers if f"{w['worker_name']} ({w['worker_ip']})" == selected_worker)
        
        # 显示选中 worker 的信息
        st.write(f"状态: {selected_worker_info['status']}")
        
        # 构造 VNC URL，包含密码参数
        worker_ip = selected_worker_info['worker_ip']
        novnc_password = selected_worker_info['novnc_password']
        encoded_password = urllib.parse.quote(novnc_password)
        vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
        
        # 添加加载 VNC 画面的按钮
        if st.button("加载 VNC 画面"):
            # 显示 VNC 窗口
            st.components.v1.iframe(vnc_url, width=1000, height=800)
        
    else:
        st.info("当前没有活跃的 workers")
