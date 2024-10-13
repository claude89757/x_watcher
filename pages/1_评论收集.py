#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 18:52
@Author  : claude
@File    : 1_Collect_Data.py
@Software: PyCharm
"""
import os
import re
import time
import datetime
import urllib.parse
import random
import json
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import requests

from common.config import CONFIG
from common.cos import list_latest_files
from common.cos import download_file
from common.log_config import setup_logger
from common.collector_sdk import call_collect_data_from_x
from sidebar import sidebar
from sidebar import cache_file_counts
from common.redis_client import RedisClient
from collectors.common.mysql import MySQLDatabase

# Configure logger
logger = setup_logger(__name__)

# Configure Streamlit pages and state
st.set_page_config(page_title="评论收集", page_icon="🤖", layout="wide")

# 从URL读取缓存数据
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if 'language' not in st.session_state:
    st.session_state.language = st.query_params.get('language')
if "max_post_num" not in st.session_state:
    st.session_state.max_post_num = int(st.query_params.get("max_post_num", 3))
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword", "")

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
    st.query_params.language = st.session_state.language
    sidebar()
else:
    st.warning("Access not Granted!")
    time.sleep(3)
    st.switch_page("Home.py", )



# Force responsive layout for columns also on mobile
st.write(
    """<style>
    [data-testid="column"] {
        width: calc(50% - 1rem);
        flex: 1 1 calc(50% - 1rem);
        min-width: calc(50% - 1rem);
    }
    </style>""",
    unsafe_allow_html=True,
)

# Hide Streamlit elements
hide_streamlit_style = """
            <style>
            .stDeployButton {visibility: hidden;}
            [data-testid="stToolbar"] {visibility: hidden !important;}
            footer {visibility: hidden !important;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# 创建标签页
tab1, tab2 = st.tabs(["Twitter评论收集", "TikTok评论收集"])

with tab1:
    st.header("Twitter论集")
    # 原有的Twitter评论收集代码
    # 根据选择的语言设置文本
    if st.session_state.language == "CN":
        page_title = "步骤 1: 收集数据"
        page_description = "从X中通过关键词搜索找到的热门帖子中收集评论数据，可能需要一些时间来完成。"
        search_keyword_label = "搜索关键词"
        max_post_num_label = "最大帖子数量"
        collect_data_button_label = "🚀开始收集评论数据"
        data_collection_complete_message = "数据收集完成！"
        access_not_granted_message = "访问未授权！"
        log_out_button_label = "登出"
        no_search_keyword_message = "请输入搜索关键词。"
        select_file_label = "选择要加载的文件"
        load_file_button_label = "加载文件"
        file_downloaded_message = "文件已从COS下载。"
        file_loaded_message = "{} 已加载"
        error_loading_file_message = "从本地文件加载数据时出错：{}"
        no_matching_files_message = "没有匹配的文件"
        loaded_collected_files_header = "已加载的收集文件"
        next_button_label = "下一步: 过滤数据"
        ready_to_filter_message = "准备过滤数据..."
    else:
        page_title = "Step 1: Collect Data"
        page_description = "Collecting comment data from popular posts found through keyword searches on X, which may take some time to complete."
        search_keyword_label = "Search Keyword"
        max_post_num_label = "Max Post Number"
        collect_data_button_label = "Collect Data"
        data_collection_complete_message = "Data collection complete!"
        access_not_granted_message = "Access not Granted!"
        log_out_button_label = "Log out"
        no_search_keyword_message = "Please enter a search keyword."
        select_file_label = "Select a file to load"
        load_file_button_label = "Load file"
        file_downloaded_message = "File downloaded from COS."
        file_loaded_message = "{} is loaded"
        error_loading_file_message = "Error loading data from local file: {}"
        no_matching_files_message = "No matching files"
        loaded_collected_files_header = "Loaded collected files"
        next_button_label = "Next: Filter Data"
        ready_to_filter_message = "Ready to filter data..."

    st.title(page_title)
    st.markdown(page_description)

    st.session_state.search_keyword = st.text_input(label=search_keyword_label, value=st.session_state.search_keyword)
    st.session_state.max_post_num = st.selectbox(
        label=max_post_num_label,
        options=[1, 3, 5, 10, 20, 50],
        index=[1, 3, 5, 10, 20, 50].index(st.session_state.max_post_num)
    )

    def query_status(access_code): 
        """
        从 Redis 中查询任务状态
        :param access_code: 访问码
        :return: 返回任务状态
        """
        redis_client = RedisClient(db=0)
        task_keys = redis_client.redis_conn.keys(f"{access_code}_*_task")
        tasks = {}
        for task_key in task_keys:
            task_info = redis_client.get_json_data(task_key)
            if task_info:
                tasks[task_key] = task_info.get('status', 'Unknown')
        return tasks


    # 检查当前用户是否有任务在运行中，如果有任务运行中，不运行触发
    # 显示转圈圈图标表示检查任务状态
    with st.spinner(f'Checking {st.session_state.access_code} tasks...'):
        tasks = query_status(st.session_state.access_code)

    running_task = ""
    if tasks:
        with st.expander("查看历史任务列表"):
            # 准备任务数据
            task_data = []
            for task_name, status in tasks.items():
                if 'RUNNING' in status:
                    status_icon = '🔄'
                    running_task = f"{task_name} {status}"
                elif 'SUCCESS' in status:
                    status_icon = '✅'
                elif 'FAILED' in status:
                    status_icon = '❌'
                else:
                    status_icon = status

                task_data.append({"任务名称": task_name, "状态": f"{status_icon} {status}"})

            # 使用表格展示任务状态
            st.table(task_data)
    else:
        pass

    if not running_task:
        if st.button(label=collect_data_button_label):
            if st.session_state.search_keyword:            
                try:
                    task_num = 0
                    with st.spinner("Collecting..."):
                        # todo: 这里要增加并发任务的逻辑
                        alive_username = random.choice(['Zacks89757'])
                        call_collect_data_from_x(
                            alive_username,
                            st.session_state.search_keyword,
                            st.session_state.max_post_num,
                            st.session_state.access_code,
                        )
                        task_num += 1
                        # status_text.text(f"Triggered {task_num} tasks for keyword: {st.session_state.search_keyword}")
                        # (todo(claudexie): 查询进度)等待数据收集完成，异等待
                        st.success(data_collection_complete_message)
                        time.sleep(3)
                        st.rerun()
                except Exception as e:
                    # Log the error
                    st.error(f"An error occurred: {e}")
            else:
                st.error(no_search_keyword_message)
    else:
        with st.spinner(running_task):
            while True:
                try:
                    tasks = query_status(st.session_state.access_code)
                except Exception as error:
                    st.error(f"query_status: {error}")
                    break
                running_task_list = []
                if tasks:
                    for task_name, status in tasks.items():
                        if 'RUNNING' in status:
                            running_task_list.append(task_name)
                else:
                    pass
                if not running_task_list:
                    break
                else:
                    # 这里一直等待任务结束
                    time.sleep(5)
                    continue

    if st.session_state.search_keyword:
        try:
            # 从 COS 中获取文件列表
            all_files = list_latest_files(prefix=f"{st.session_state.access_code}/")

            matching_files = []
            for raw_file_name in all_files:
                file_name = str(urllib.parse.unquote(raw_file_name)).split('/')[-1]
                if st.session_state.search_keyword in file_name:
                    matching_files.append(file_name)
        except Exception as e:
            raise Exception(f"Error retrieving files from COS: {e}")
        if matching_files:
            selected_file = st.selectbox(select_file_label, matching_files)
            # 选择加载到本地的文件
            if st.button(load_file_button_label):
                local_file_path = os.path.join(f"./data/{st.session_state.access_code}/raw/", selected_file)
                # 检查本地是否已有文件
                if not os.path.exists(local_file_path):
                    try:
                        download_file(object_key=f"{st.session_state.access_code}/{selected_file}",
                                    local_file_path=local_file_path)
                        st.success(file_downloaded_message)
                    except Exception as e:
                        st.error(f"Error loading file from COS: {e}")
                try:
                    st.success(file_loaded_message.format(selected_file))
                except Exception as e:
                    st.error(error_loading_file_message.format(e))
        else:
            st.error(no_matching_files_message)
            pass
    else:
        pass

    # 获取已下载文件的列表
    local_files_dir = f"./data/{st.session_state.access_code}/raw/"
    if st.session_state.raw_data_file_count:
        pass
        downloaded_files = []
    else:
        downloaded_files = os.listdir(local_files_dir)
    if downloaded_files or st.session_state.raw_data_file_count:
        if not downloaded_files:
            downloaded_files = os.listdir(local_files_dir)
        else:
            pass
        st.header(loaded_collected_files_header)
        file_info_list = []
        for file in downloaded_files:
            file_path = os.path.join(local_files_dir, file)
            file_size = int(os.path.getsize(file_path) / 1024)  # 转换为KB
            file_mtime = os.path.getmtime(file_path)
            formatted_mtime = datetime.datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S')
            # 计算文件行数
            with open(file_path, 'r') as f:
                file_lines = sum(1 for line in f)

            file_info_list.append({
                "File Name": file,
                "Line Count": file_lines,
                "Size (KB)": file_size,
                "Last Modified": formatted_mtime,
            })

        # 创建 DataFrame
        file_info_df = pd.DataFrame(file_info_list)

        # 将 "Last Modified" 列转换为 datetime 类型
        file_info_df['Last Modified'] = pd.to_datetime(file_info_df['Last Modified'])

        # 按 "Last Modified" 列进行排序
        file_info_df = file_info_df.sort_values(by='Last Modified', ascending=False)

        # 重置索引
        file_info_df = file_info_df.reset_index(drop=True)

        # 展示 DataFrame
        st.dataframe(file_info_df)

        file_loaded = True

        # 更新文件计数
        cache_file_counts()
    else:
        pass

    # 将用户输入的数据保存到 URL 参数
    st.query_params.search_keyword = st.session_state.search_keyword
    st.query_params.max_post_num = st.session_state.max_post_num

    # Next
    if st.session_state.raw_data_file_count:
        if st.button(label=next_button_label, type='primary'):
            st.success(ready_to_filter_message)
            st.balloons()
            time.sleep(3)
            st.switch_page("pages/2_评论过滤.py")
        else:
            pass


with tab2:
    st.header("TikTok评论收集")
    
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
    with col3:
        if 'collection_start_time' not in st.session_state:
            st.session_state.collection_start_time = datetime.now()
        running_time = datetime.now() - st.session_state.collection_start_time
        st.metric("运行时间", str(timedelta(seconds=int(running_time.total_seconds()))))

    # 从环境变量获取API地址
    TIKTOK_API_URL = os.environ.get('TIKTOK_WORKER_001_API_URL', 'http://localhost:5000')

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
                TIKTOK_API_URL,
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
                            db.connect()
                            db.update_tiktok_task_status(task['id'], 'running')
                            db.disconnect()
                            st.rerun()
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

        # 动态展示评论数据
        st.subheader("实时评论数据")
        comments_placeholder = st.empty()
        
        # 任务日志
        st.subheader("任务日志")
        logs_placeholder = st.empty()

        # 添加一个停止按钮
        stop_button = st.button("停止刷新")

        while not stop_button:
            # 获取评论数据
            comments = db.get_tiktok_comments_by_keyword(search_keyword)
            if comments:
                comment_df = pd.DataFrame(comments)
                comment_df['collected_at'] = pd.to_datetime(comment_df['collected_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
                comment_df = comment_df[['user_id', 'reply_content', 'reply_time', 'video_url', 'collected_at', 'collected_by']]
                comments_placeholder.dataframe(comment_df, use_container_width=True)
            else:
                comments_placeholder.write("暂无相关评论")

            # 获取任务日志
            logs = db.get_tiktok_task_logs_by_keyword(search_keyword)
            if logs:
                log_df = pd.DataFrame(logs)
                log_df['created_at'] = pd.to_datetime(log_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
                log_df = log_df[['created_at', 'log_type', 'message']]
                logs_placeholder.dataframe(log_df, use_container_width=True)
            else:
                logs_placeholder.write("暂无相关日志")

            # 等待一段时间后刷新
            time.sleep(10)  # 每10秒刷新一次

        db.disconnect()

    # 动态更新运行时间
    if search_keyword:
        placeholder = st.empty()
        while True:
            with placeholder.container():
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("已收集关键字", stats['keyword_count'])
                with col2:
                    st.metric("已收集评论数", stats['comment_count'])
                with col3:
                    running_time = datetime.now() - st.session_state.collection_start_time
                    st.metric("运行时间", str(timedelta(seconds=int(running_time.total_seconds()))))
            
            time.sleep(1)  # 每秒更新一次
            
            if st.button("停止更新"):
                break