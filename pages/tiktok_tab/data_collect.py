import os
import json
import requests
import pandas as pd
import streamlit as st
from collectors.common.mysql import MySQLDatabase
from typing import List, Dict
import time
from datetime import datetime

# 定义全局变量：同时运行的最大任务数
MAX_RUNNING_TASKS = 1


def data_collect(db: MySQLDatabase):
    """
    本页面用于从TikTok收集数据并创建数据采集任务， 。
    """
    # 全局面板
    st.info(f"本页面用于从TikTok收集数据并创建数据采集任务，最多同时运行 {MAX_RUNNING_TASKS} 个任务")

    # 定义缓存文件路径
    KEYWORD_CACHE_FILE = 'tiktok_keyword_cache.json'

    def save_keyword_to_cache(keyword):
        """保存关键字到缓存文件"""
        with open(KEYWORD_CACHE_FILE, 'w') as f:
            json.dump({'keyword': keyword}, f)

    # 创建任务表单
    with st.form("create_tiktok_task"):
        if 'cached_keyword' not in st.session_state:
            default_search_keyword = ""
        else:
            default_search_keyword = st.session_state.cached_keyword
        search_keyword = st.text_input("关键词", value=default_search_keyword, key="data_collect_keyword_input")
        submit_task = st.form_submit_button("🚀 创建任务")

    if submit_task and search_keyword:
        # 保存关键字到缓存
        save_keyword_to_cache(search_keyword)
        # 检查是否已存在相同关键字的运行中任务
        running_tasks = db.get_running_tiktok_task_by_keyword(search_keyword)
        task_id = None
        
        if running_tasks:
            st.warning(f"⚠️ 已存在关键词为 '{search_keyword}' 的运行中任务。任务ID: {running_tasks[0]['id']}")
            task_id = running_tasks[0]['id']
        else:
            # 检查当前运行中的任务数量
            all_tasks = db.get_all_tiktok_tasks()
            running_tasks = get_running_tasks(all_tasks)
            if len(running_tasks) >= MAX_RUNNING_TASKS:
                st.error(f"❌ 当前已有 {MAX_RUNNING_TASKS} 个任务在运行，请等待其他任务完成后再创建新任务。")
            else:
                # 在MySQL中创建新任务
                task_id = db.create_tiktok_task(search_keyword)
                if task_id:
                    st.success(f"✅ 成功在数据库中创建任务。ID: {task_id}")
                else:
                    st.error("❌ 在数据库中创建任务失败")
                    return  # 如果创建任务失败，直接返回

        # 无论是新任务还是已存在的任务，都触发worker执行
        if task_id and len(running_tasks) < MAX_RUNNING_TASKS:
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
            else:
                st.error("❌ 未能触发任何worker")
        elif len(running_tasks) >= MAX_RUNNING_TASKS:
            st.warning(f"⚠️ 当前已有 {MAX_RUNNING_TASKS} 个任务在运行，无法触发新任务。")
        else:
            st.error("❌ 无法获取有效的任务ID")


    # 定义一个更新函数来刷新任务列表
    def update_task_list():
        tasks = db.get_all_tiktok_tasks()
        if tasks:
            task_data = []
            for task in tasks:
                status_emoji = {
                    'pending': '⏳',
                    'running': '▶️',
                    'paused': '⏸️',
                    'completed': '✅',
                    'failed': '❌'
                }.get(task['status'], '❓')
                
                total_videos = db.get_total_videos_for_keyword(task['keyword'])
                processed_videos = db.get_processed_videos_for_keyword(task['keyword'])
                pending_videos = total_videos - processed_videos
                comments_count = len(db.get_tiktok_comments_by_keyword(task['keyword']))
                
                # 计算运行时间
                if task['status'] in ['running', 'completed', 'failed', 'paused']:
                    duration = task['updated_at'] - task['created_at']
                    hours, remainder = divmod(duration.total_seconds(), 3600)
                    minutes, seconds = divmod(remainder, 60)
                    run_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
                else:
                    run_time = "00:00:00"
                
                task_data.append({
                    "ID": task['id'],
                    "关键词": task['keyword'],
                    "状态": f"{status_emoji} {task['status']}",
                    "总视频数": total_videos,
                    "待检查视频": pending_videos,
                    "已检查视频": processed_videos,
                    "已收集评论数": comments_count,
                    "已运行时间": run_time,
                    "触发时间": task['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                    "更新时间": task['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
                })
            
            df = pd.DataFrame(task_data)
            return df
        else:
            return None

    # 任务列表
    st.subheader("任务列表")
    
    # 创建一个动态更新的容器
    task_list_container = st.empty()

    # 初次显示任务列表
    df = update_task_list()
    if df is not None:
        task_list_container.dataframe(df, use_container_width=True, hide_index=True)
    else:
        task_list_container.write("📭 暂无任务")

    # 添加刷新任务状态按钮
    if st.button("刷新任务状态"):
        df = update_task_list()
        if df is not None:
            task_list_container.dataframe(df, use_container_width=True, hide_index=True)
        else:
            task_list_container.write("📭 暂无任务")

    # 任务操作（默认折叠）
    with st.expander("任务操作", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            # 获取所有任务并格式化选项
            all_tasks = db.get_all_tiktok_tasks()
            task_options = []
            for task in all_tasks:
                status_emoji = {
                    'pending': '⏳',
                    'running': '▶️',
                    'paused': '⏸️',
                    'completed': '✅',
                    'failed': '❌'
                }.get(task['status'], '❓')
                # 格式化选项文本：ID - 关键词 (状态)
                option_text = f"{task['id']} - {task['keyword']} ({status_emoji} {task['status']})"
                task_options.append((task['id'], option_text))
            
            selected_task_id = st.selectbox(
                "选择任务",
                options=[t[0] for t in task_options],
                format_func=lambda x: next(t[1] for t in task_options if t[0] == x)
            )
        with col2:
            selected_task = next((task for task in all_tasks if task['id'] == selected_task_id), None)
            if selected_task:
                st.write(f"当前状态: {selected_task['status']}")

        if selected_task:
            col1, col2, col3 = st.columns(3)
            with col1:
                if selected_task['status'] == 'pending':
                    all_tasks = db.get_all_tiktok_tasks()
                    running_tasks = get_running_tasks(all_tasks)
                    if len(running_tasks) < MAX_RUNNING_TASKS:
                        if st.button('▶️ 开始'):
                            db.update_tiktok_task_status(selected_task_id, 'running')
                            st.success(f"任务 {selected_task_id} 已开始")
                            st.rerun()
                    else:
                        st.warning(f"⚠️ 当前已有 {MAX_RUNNING_TASKS} 个任务在运行，无法开始新任务。")
                elif selected_task['status'] == 'running':
                    if st.button('⏸️ 暂停'):
                        db.update_tiktok_task_status(selected_task_id, 'paused')
                        st.success(f"任务 {selected_task_id} 已暂停")
                        st.rerun()
                elif selected_task['status'] == 'paused':
                    if st.button('▶️ 继续'):
                        try:
                            available_workers = db.get_available_workers()
                            successful_resumes = 0
                            
                            for worker in available_workers:
                                try:
                                    worker_ip = worker['worker_ip']
                                    response = requests.post(
                                        f"http://{worker_ip}:5000/resume_tiktok_task",
                                        json={"task_id": selected_task_id},
                                        headers={"Content-Type": "application/json"}
                                    )
                                    response.raise_for_status()
                                    successful_resumes += 1
                                except requests.RequestException as e:
                                    st.error(f"❌ 在worker {worker_ip} 上恢复任务失败: {str(e)}")
                            
                            if successful_resumes > 0:
                                st.success(f"✅ 成功在 {successful_resumes} 个worker上恢复任务 ID: {selected_task_id}")
                                db.update_tiktok_task_status(selected_task_id, 'running')
                                st.rerun()
                            else:
                                st.error("❌ 未能在任何worker上恢复任务")
                        except Exception as e:
                            st.error(f"恢复任务失败: {str(e)}")
            with col2:
                if st.button('🗑️ 删除', type="primary"): 
                    if db.delete_tiktok_task(selected_task_id):
                        st.success(f"✅ 成功删除任务 ID: {selected_task_id}")
                        st.rerun()
                    else:
                        st.error(f"❌ 删除任务 ID: {selected_task_id} 失败。请检查数据库日志以获取更多信息。")
            
            # 添加视频列表展示和操作
            if selected_task:
                st.markdown("---")
                st.subheader("视频列表")
                
                # 获取任务相关的视频
                videos = db.get_tiktok_task_videos(selected_task_id)
                if videos:
                    # 转换为DataFrame以便展示
                    video_data = []
                    for video in videos:
                        video_data.append({
                            "ID": video['id'],
                            "视频链接": video['video_url'],
                            "状态": video['status'],
                            "处理服务器": video['processing_server_ip'] or "未分配",
                            "作者": video['author'] or "未知",
                            "视频描述": video['description'] or "无描述",  # 增加视频描述字段
                            "点赞数": video['likes_count'] or 0,
                            "评论数": video['comments_count'] or 0,
                            "采集时间": video['collected_at'].strftime('%Y-%m-%d %H:%M:%S') if video['collected_at'] else "未知"
                        })
                    
                    video_df = pd.DataFrame(video_data)
                    
                    # 添加批量操作功能
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        status_options = {
                            "pending": "待处理",
                            "processing": "处理中",
                            "completed": "已完成",
                            "failed": "失败",
                            "skipped": "跳过"
                        }
                        selected_status = st.selectbox(
                            "选择要更新的状态",
                            options=list(status_options.keys()),
                            format_func=lambda x: f"{x} - {status_options[x]}",
                            key="video_status_select"
                        )
                    with col2:
                        if st.button("批量更新所选视频"):
                            selected_videos = st.session_state.get('selected_videos', [])
                            if selected_videos:
                                success_count = 0
                                for video_id in selected_videos:
                                    if db.update_tiktok_video_status(video_id, selected_status):
                                        success_count += 1
                                st.success(f"成功更新 {success_count} 个视频的状态")
                                st.rerun()
                            else:
                                st.warning("请先选择要更新的视频")
                    
                    # 使用 data_editor 来支持选择功能
                    edited_df = st.data_editor(
                        video_df,
                        hide_index=True,
                        use_container_width=True,
                        num_rows="dynamic",
                        key="video_editor",
                        column_config={
                            "视频链接": st.column_config.LinkColumn("视频链接"),
                            "状态": st.column_config.SelectboxColumn(
                                "状态",
                                options=["pending", "processing", "completed", "failed", "skipped"],
                                help="pending-待处理, processing-处理中, completed-已完成, failed-失败, skipped-已跳过"
                            )
                        }
                    )
                    
                    # 保存选中的视频ID到session_state
                    if 'selected_videos' not in st.session_state:
                        st.session_state.selected_videos = []
                    st.session_state.selected_videos = edited_df.index[edited_df.index.isin(edited_df.index)].tolist()
                    
                    # 显示统计信息
                    st.markdown("---")
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("总视频数", len(video_df))
                    with col2:
                        st.metric("待处理", len(video_df[video_df['状态'] == 'pending']))
                    with col3:
                        st.metric("处理中", len(video_df[video_df['状态'] == 'processing']))
                    with col4:
                        st.metric("已完成", len(video_df[video_df['状态'] == 'completed']))
                    with col5:
                        st.metric("跳过", len(video_df[video_df['状态'] == 'skipped']))
                else:
                    st.info("该任务暂无相关视频")

    if search_keyword:
        # 动态展示评论数据
        st.subheader("评论数据")
        comments = db.get_tiktok_comments_by_keyword(search_keyword)
        st.info(f"当前关键字 '{search_keyword}' 的评论数量：{len(comments)}")
        if comments:
            # 显示当前关键字的评论数量
            comment_df = pd.DataFrame(comments)
            comment_df['collected_at'] = pd.to_datetime(comment_df['collected_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # 重新排序列，将 'keyword' 放在第一列
            comment_df = comment_df[['keyword', 'user_id', 'reply_content', 'reply_time', 'video_url', 'collected_at', 'collected_by']]
            
            # 展示评论数据，包括关键字列
            st.dataframe(comment_df, use_container_width=True)
        else:
            st.write("暂无相关评论")

def get_running_tasks(tasks: List[Dict]) -> List[Dict]:
    """获取所有正在运行的任务"""
    return [task for task in tasks if task['status'] == 'running']
