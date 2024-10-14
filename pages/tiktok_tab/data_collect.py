import os
import json
import requests
import pandas as pd
import streamlit as st
from collectors.common.mysql import MySQLDatabase

def data_collect():
    # 初始化数据库连接
    db = MySQLDatabase()
    db.connect()

    try:
        # 全局面板
        st.subheader("收集统计")
        col1, col2, col3 = st.columns(3)
        
        # 从数据库获取统计信息
        stats = db.get_tiktok_collection_stats()

        with col1:
            st.metric("已收集关键字", stats['keyword_count'])
        with col2:
            st.metric("已收集评论数", stats['comment_count'])


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
                # 检查是否已存在相同关键字的运行中任务
                running_tasks = db.get_running_tiktok_task_by_keyword(search_keyword)
                if running_tasks:
                    st.warning(f"⚠️ 已存在关键词为 '{search_keyword}' 的运行中任务。任务ID: {running_tasks[0]['id']}")
                else:
                    # 在MySQL中创建新任务
                    task_id = db.create_tiktok_task(search_keyword)
                    if task_id:
                        st.success(f"✅ 成功在数据库中创建任务。ID: {task_id}")
                        
                        # 获取所有可用的worker
                        available_workers = db.get_available_workers()
                        successful_triggers = 0
                        
                        for worker in available_workers:
                            try:
                                worker_ip = worker['worker_ip']
                                response = requests.post(
                                    f"http://{worker_ip}:5000/trigger_tiktok_task",
                                    json={"task_id": task_id},
                                    headers={"Content-Type": "application/json"}
                                )
                                response.raise_for_status()
                                successful_triggers += 1
                            except requests.RequestException as e:
                                st.error(f"❌ 触发worker {worker_ip} 失败: {str(e)}")
                        
                        if successful_triggers > 0:
                            st.success(f"✅ 成功触发 {successful_triggers} 个worker")
                            # 保存关键字到缓存
                            save_keyword_to_cache(search_keyword)
                        else:
                            st.error("❌ 未能触发任何worker")
                    else:
                        st.error("❌ 在数据库中创建任务失败")
            except Exception as e:
                st.error(f"❌ 创建任务时出错: {str(e)}")

        # 任务管理
        st.subheader("任务管理")
        tasks = db.get_all_tiktok_tasks()

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
                                db.update_tiktok_task_status(task['id'], 'running')
                                st.rerun()
                        elif task['status'] == 'running':
                            if st.button('⏸️ 暂停', key=f'pause_{task["id"]}'):
                                db.update_tiktok_task_status(task['id'], 'paused')
                                st.rerun()
                        elif task['status'] == 'paused':
                            if st.button('▶️ 继续', key=f'resume_{task["id"]}'):
                                try:
                                    # 获取所有可用的worker
                                    available_workers = db.get_available_workers()
                                    successful_resumes = 0
                                    
                                    for worker in available_workers:
                                        try:
                                            worker_ip = worker['worker_ip']
                                            response = requests.post(
                                                f"http://{worker_ip}:5000/resume_tiktok_task",
                                                json={"task_id": task['id']},
                                                headers={"Content-Type": "application/json"}
                                            )
                                            response.raise_for_status()
                                            successful_resumes += 1
                                        except requests.RequestException as e:
                                            st.error(f"❌ 在worker {worker_ip} 上恢复任务失败: {str(e)}")
                                    
                                    if successful_resumes > 0:
                                        st.success(f"✅ 成功在 {successful_resumes} 个worker上恢复任务 ID: {task['id']}")
                                        db.update_tiktok_task_status(task['id'], 'running')
                                        st.rerun()
                                    else:
                                        st.error("❌ 未能在任何worker上恢复任务")
                                except Exception as e:
                                    st.error(f"恢复任务失败: {str(e)}")
                    with col2:
                        if st.button('🗑️ 删除', key=f'delete_{task["id"]}'):
                            # 获取所有worker
                            all_workers = db.get_worker_list()
                            
                            for worker in all_workers:
                                try:
                                    worker_ip = worker['worker_ip']
                                    response = requests.post(
                                        f"http://{worker_ip}:5000/delete_tiktok_task",
                                        json={"task_id": task['id']},
                                        headers={"Content-Type": "application/json"}
                                    )
                                    response.raise_for_status()
                                except requests.RequestException as e:
                                    st.error(f"❌ 在worker {worker_ip} 上删除任务失败: {str(e)}")
                            
                            # 在数据库中删除任务
                            if db.delete_tiktok_task(task['id']):
                                st.success(f"✅ 成功删除任务 ID: {task['id']}")
                            else:
                                st.error(f"❌ 在数据库中删除任务 ID: {task['id']} 失败")
                            st.rerun()
                    with col3:
                        st.write(f"🕒 更新时间: {task['updated_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            st.write("📭 暂无任务")

        if search_keyword:
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

        # 添加刷新按钮
        if st.button("刷新数据"):
            st.rerun()

    finally:
        # 确保在函数结束时关闭数据库连接
        db.disconnect()
