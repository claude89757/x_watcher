#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/13 22:37
@Author  : claude
@File    : sidebar.py
@Software: PyCharm
"""
import streamlit as st
import os


def cache_file_counts():
    # This function should set the file counts in st.session_state
    # For example:
    if "raw_data_file_count" not in st.session_state:
        st.session_state.raw_data_file_count = 0
    if "processed_data_file_count" not in st.session_state:
        st.session_state.processed_data_file_count = 0
    if "analyzed_data_file_count" not in st.session_state:
        st.session_state.analyzed_data_file_count = 0
    if "msg_data_file_count" not in st.session_state:
        st.session_state.msg_data_file_count = 0

    st.session_state.raw_data_file_count = count_files(f"./data/{st.session_state.access_code}/raw/")
    st.session_state.processed_data_file_count = count_files(f"./data/{st.session_state.access_code}/processed/")
    st.session_state.analyzed_data_file_count = count_files(f"./data/{st.session_state.access_code}/analyzed/")
    st.session_state.msg_data_file_count = count_files(f"./data/{st.session_state.access_code}/msg/")


def count_files(folder_path):
    # This function should return the number of files in the given folder
    return len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])


def clear_folder(folder_path):
    # This function should clear all files in the given folder
    import os
    import shutil
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


def sidebar():
    try:
        # Cache file counts
        cache_file_counts()

        # 在侧边栏添加语言选择
        language = st.sidebar.radio("选择语言 / Choose Language", ("CN", "EN"), index=0 if st.query_params.get('language') == 'CN' else 1)

        # 将语言选择存储到 session_state 和 URL 参数
        st.session_state.language = language
        st.query_params.language = language

        # 根据选择的语言设置文本
        if language == "CN":
            file_statistics_label = "文件统计"
            clear_button_label = "清除"
            refresh_button_label = "刷新"
            file_labels = {
                "raw": "原始数据文件",
                "processed": "处理后数据文件",
                "analyzed": "分析后数据文件",
                "msg": "消息数据文件"
            }
        else:
            file_statistics_label = "File Statistics"
            clear_button_label = "Clear"
            refresh_button_label = "Refresh"
            file_labels = {
                "raw": "Raw Data Files",
                "processed": "Processed Data Files",
                "analyzed": "Analyzed Data Files",
                "msg": "Msg Data Files"
            }

        # Create a component in the sidebar to display file counts
        st.sidebar.subheader(file_statistics_label)

        # Display counts and add buttons in a horizontal layout
        with st.sidebar:
            for key, folder in file_labels.items():
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.caption(f"{folder}: {st.session_state[f'{key}_data_file_count']}")
                with col2:
                    if st.button(clear_button_label, key=f"clear_{key}"):
                        clear_folder(f"./data/{st.session_state.access_code}/{key}/")
                        st.session_state[f'{key}_data_file_count'] = \
                            count_files(f"./data/{st.session_state.access_code}/{key}/")

            # Add refresh button for file counts
            if st.button(refresh_button_label):
                cache_file_counts()
                st.rerun()
    except:
        st.write("...")
