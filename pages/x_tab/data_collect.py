#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 18:52
@Author  : claude
@File    : 1_Collect_Data.py
@Software: PyCharm
"""

import json
from datetime import datetime

import pandas as pd
import streamlit as st
import requests

from common.config import CONFIG
from common.log_config import setup_logger
from collectors.common.mysql import MySQLDatabase
from typing import List, Dict

# Configure logger
logger = setup_logger(__name__)

# 定义全局变量：同时运行的最大任务数
MAX_RUNNING_TASKS = 2

def data_collect(db: MySQLDatabase):
    """
    本页面用于从X收集数据并创建数据采集任务。
    """
    # 全局面板
    st.info(f"本页面用于从X收集数据并创建数据采集任务，最多同时运行 {MAX_RUNNING_TASKS} 个任务")

    # 定义缓存文件路径
    KEYWORD_CACHE_FILE = 'x_keyword_cache.json'

    def save_keyword_to_cache(keyword):
        """保存关键字到缓存文件"""
        with open(KEYWORD_CACHE_FILE, 'w') as f:
            json.dump({'keyword': keyword}, f)

    # 创建任务表单
    with st.form("create_x_task"):
        if 'cached_keyword' not in st.session_state:
            default_search_keyword = ""
        else:
            default_search_keyword = st.session_state.cached_keyword
        search_keyword = st.text_input("搜索关键词", value=default_search_keyword, key="data_collect_keyword_input")
        submit_task = st.form_submit_button("🚀 创建任务")

    if submit_task and search_keyword:
        # 保存关键字到缓存
        save_keyword_to_cache(search_keyword)
        # 检查是否已存在相同关键字的运行中任务
        running_tasks = db.get_running_x_task_by_keyword(search_keyword)
        task_id = None
        
        if running_tasks:
            st.warning(f"⚠️ 已存在关键词为 '{search_keyword}' 的运行中任务。任务ID: {running_tasks[0]['id']}")
            task_id = running_tasks[0]['id']
        else:
            # 检查当前运行中的任务数量
            all_tasks = db.get_all_x_tasks()
            running_tasks = [task for task in all_tasks if task['status'] == 'running']
            if len(running_tasks) >= MAX_RUNNING_TASKS:
                st.error(f"❌ 当前已有 {MAX_RUNNING_TASKS} 个任务在运行，请等待其他任务完成后再创建新任务。")
            else:
                # 在MySQL中创建新任务
                task_id = db.create_x_task(search_keyword)
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
                        f"http://{worker_ip}:5000/trigger_x_task",
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
        tasks = db.get_all_x_tasks()
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
                
                total_tweets = db.get_total_tweets_for_keyword(task['keyword'])
                processed_tweets = db.get_processed_tweets_for_keyword(task['keyword'])
                pending_tweets = total_tweets - processed_tweets
                comments_count = len(db.get_x_comments_by_keyword(task['keyword']))
                
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
                    "总推文数": total_tweets,
                    "待检查推文": pending_tweets,
                    "已检查推文": processed_tweets,
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
            selected_task_id = st.selectbox("选择任务ID", [task['id'] for task in db.get_all_x_tasks()])
        with col2:
            selected_task = next((task for task in db.get_all_x_tasks() if task['id'] == selected_task_id), None)
            if selected_task:
                st.write(f"当前状态: {selected_task['status']}")

        if selected_task:
            col1, col2, col3 = st.columns(3)
            with col1:
                if selected_task['status'] == 'pending':
                    all_tasks = db.get_all_x_tasks()
                    running_tasks = [task for task in all_tasks if task['status'] == 'running']
                    if len(running_tasks) < MAX_RUNNING_TASKS:
                        if st.button('▶️ 开始'):
                            db.update_x_task_status(selected_task_id, 'running')
                            st.success(f"任务 {selected_task_id} 已开始")
                            st.rerun()
                    else:
                        st.warning(f"⚠️ 当前已有 {MAX_RUNNING_TASKS} 个任务在运行，无法开始新任务。")
                elif selected_task['status'] == 'running':
                    if st.button('⏸️ 暂停'):
                        db.update_x_task_status(selected_task_id, 'paused')
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
                                        f"http://{worker_ip}:5000/resume_x_task",
                                        json={"task_id": selected_task_id},
                                        headers={"Content-Type": "application/json"}
                                    )
                                    response.raise_for_status()
                                    successful_resumes += 1
                                except requests.RequestException as e:
                                    st.error(f"❌ 在worker {worker_ip} 上恢复任务失败: {str(e)}")
                            
                            if successful_resumes > 0:
                                st.success(f"✅ 成功在 {successful_resumes} 个worker上恢复任务 ID: {selected_task_id}")
                                db.update_x_task_status(selected_task_id, 'running')
                                st.rerun()
                            else:
                                st.error("❌ 未能在任何worker上恢复任务")
                        except Exception as e:
                            st.error(f"恢复任务失败: {str(e)}")
            with col2:
                if st.button('🗑️ 删除'): 
                    if db.delete_x_task(selected_task_id):
                        st.success(f"✅ 成功删除任务 ID: {selected_task_id}")
                        st.rerun()
                    else:
                        st.error(f"❌ 删除任务 ID: {selected_task_id} 失败。请检查数据库日志以获取更多信息。")
 
    if search_keyword:
        # 动态展示评论数据
        st.subheader("评论数据")
        comments = db.get_x_comments_by_keyword(search_keyword)
        st.info(f"当前关键字 '{search_keyword}' 的评论数量：{len(comments)}")
        if comments:
            # 显示当前关键字的评论数量
            comment_df = pd.DataFrame(comments)
            comment_df['collected_at'] = pd.to_datetime(comment_df['collected_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # 重新排序列，将 'keyword' 放在第一列
            comment_df = comment_df[['keyword', 'user_id', 'reply_content', 'reply_time', 'tweet_url', 'collected_at', 'collected_by']]
            
            # 展示评论数据，包括关键字列
            st.dataframe(comment_df, use_container_width=True)
        else:
            st.write("暂无相关评论")

