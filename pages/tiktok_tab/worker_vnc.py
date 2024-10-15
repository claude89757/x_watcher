import urllib
import streamlit as st
from collectors.common.mysql import MySQLDatabase


def worker_vnc():    
    # 初始化数据库连接
    db = MySQLDatabase()
    db.connect()
    
    try:
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
            if selected_worker_info['current_task_ids']:
                st.write(f"当前任务: {selected_worker_info['current_task_ids']}")
            else:
                st.write("当前无任务")
            
            # 构造 VNC URL，包含密码参数
            st.write(selected_worker_info)
            worker_ip = selected_worker_info['worker_ip']
            novnc_password = selected_worker_info['novnc_password']
            encoded_password = urllib.parse.quote(novnc_password)
            vnc_url = f"http://{worker_ip}:6080/vnc.html?password={encoded_password}&autoconnect=true&reconnect=true"
            
            # 显示 VNC 窗口
            st.components.v1.iframe(vnc_url, width=800, height=600)
            
            # 添加刷新按钮
            if st.button("刷新 VNC 画面"):
                st.rerun()
        else:
            st.info("当前没有活跃的 workers")

    finally:
        # 确保在函数结束时关闭数据库连接
        db.disconnect()
