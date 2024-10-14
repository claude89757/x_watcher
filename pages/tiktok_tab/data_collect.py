
import os
import json
import requests
import pandas as pd
import streamlit as st
from collectors.common.mysql import MySQLDatabase

def data_collect():
     # 全局面板
    st.subheader("收集统计")
    col1, col2, col3 = st.columns(3)
    
    # 从数据库获取统计信息
    db = MySQLDatabase()
    db.connect()
    stats = db.get_tiktok_collection_stats()
    db.disconnect()

    with col1:
        st.metric("已收集关键字", stats['keyword_count'])
    with col2:
        st.metric("已收集评论数", stats['comment_count'])
 
    # 从环境变量获取API地址
    TIKTOK_WORKER_001_API_URL = os.environ.get('TIKTOK_WORKER_001_API_URL', 'http://localhost:5000')

    # 定义缓存文件路径
    KEYWORD_CACHE_FILE = 'tiktok_keyword_cache.json'

    def save_keyword_to_cache(keyword):
        """保存关键字到缓存文件"""
        with open(KEYWORD_CACHE_FILE, 'w') as f:
            json.dump({'keyword': keyword}, f)

    def load_keyword_from_cache():
        """从缓存文件加载关键字"""
        if os.path.exists(KEYWORD_CACHE_FILE):
            with open(KEYWORD_CACHE_FILE, 'r') as f:
                data = json.load(f)
                return data.get('keyword', '')
        return ''

    # 从缓存加载默认关键字
    default_keyword = load_keyword_from_cache()

    # 创建任务表单
    with st.form("create_tiktok_task"):
        search_keyword = st.text_input("搜索关键词", value=default_keyword)
        submit_task = st.form_submit_button("🚀 创建任务")

    if submit_task:
        try:
            response = requests.post(
                f"{TIKTOK_WORKER_001_API_URL}/create_tiktok_task",
                json={"keyword": search_keyword},
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()  # 如果请求失败,会抛出异常
            result = response.json()
            task_id = result.get("task_id")
            if task_id:
                st.success(f"✅ 成功创建任务,ID: {task_id}")
                # 保存关键字到缓存
                save_keyword_to_cache(search_keyword)
            else:
                st.error("❌ 创建任务失败: 未返回任务ID")
        except requests.RequestException as e:
            st.error(f"❌ 创建任务失败: {str(e)}")

    # 任务管理
    st.subheader("任务管理")
    db = MySQLDatabase()
    db.connect()
    tasks = db.get_all_tiktok_tasks()
    db.disconnect()

    if tasks:
        for task in tasks:
            status_emoji = {
                'pending': '⏳',
                'running': '▶️',
                'paused': '⏸️',
                'completed': '✅',
                'failed': '❌'
            }.get(task['status'], '❓')
            
            with st.expander(f"{status_emoji} 任务ID: {task['id']} | 关键词: {task['keyword']} | 状态: {task['status']} | 触发时间: {task['created_at'].strftime('%Y-%m-%d %H:%M:%S')}"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    if task['status'] == 'pending':
                        if st.button('▶️ 开始', key=f'start_{task["id"]}'):
                            db.connect()
                            db.update_tiktok_task_status(task['id'], 'running')
                            db.disconnect()
                            st.rerun()
                    elif task['status'] == 'running':
                        if st.button('⏸️ 暂停', key=f'pause_{task["id"]}'):
                            db.connect()
                            db.update_tiktok_task_status(task['id'], 'paused')
                            db.disconnect()
                            st.rerun()
                    elif task['status'] == 'paused':
                        if st.button('▶️ 继续', key=f'resume_{task["id"]}'):
                            try:
                                response = requests.post(
                                    f"{TIKTOK_WORKER_001_API_URL}/resume_tiktok_task",
                                    json={"task_id": task['id']},
                                    headers={"Content-Type": "application/json"}
                                )
                                response.raise_for_status()
                                st.success(f"成功恢复任务 ID: {task['id']}")
                                st.rerun()
                            except requests.RequestException as e:
                                st.error(f"恢复任务失败: {str(e)}")
                with col2:
                    if st.button('🗑️ 删除', key=f'delete_{task["id"]}'):
                        db.connect()
                        db.delete_tiktok_task(task['id'])
                        db.disconnect()
                        st.rerun()
                with col3:
                    st.write(f"🕒 更新时间: {task['updated_at'].strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.write("📭 暂无任务")

    if search_keyword:
        db = MySQLDatabase()
        db.connect()

        # 获取任务总视频数
        total_videos = db.get_total_videos_for_keyword(search_keyword)

        # 添加进度条
        processed_videos = db.get_processed_videos_for_keyword(search_keyword)
        progress = processed_videos / total_videos if total_videos > 0 else 0
        st.progress(progress)
        
        # 动态展示评论数据
        st.subheader("评论数据")
        comments = db.get_tiktok_comments_by_keyword(search_keyword)
        if comments:
            comment_df = pd.DataFrame(comments)
            comment_df['collected_at'] = pd.to_datetime(comment_df['collected_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            comment_df = comment_df[['user_id', 'reply_content', 'reply_time', 'video_url', 'collected_at', 'collected_by']]
            st.dataframe(comment_df, use_container_width=True)
        else:
            st.write("暂无相关评论")

        # 任务日志
        st.subheader("任务日志")
        logs = db.get_tiktok_task_logs_by_keyword(search_keyword)
        if logs:
            log_df = pd.DataFrame(logs)
            log_df['created_at'] = pd.to_datetime(log_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            log_df = log_df[['created_at', 'log_type', 'message']]
            st.dataframe(log_df, use_container_width=True)
        else:
            st.write("暂无相关日志")

        db.disconnect()

    # 添加刷新按钮
    if st.button("刷新数据"):
        st.rerun()
