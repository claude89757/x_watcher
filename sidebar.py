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
    # Cache file counts
    cache_file_counts()

    # Create a component in the sidebar to display file counts
    st.sidebar.subheader("File Statistics")

    # Display counts and add buttons in a horizontal layout
    with st.sidebar:
        for label, key, folder in [
            ("Raw Data Files", "raw", "raw"),
            ("Processed Data Files", "processed", "processed"),
            ("Analyzed Data Files", "analyzed", "analyzed"),
            ("Msg Data Files", "msg", "msg")
        ]:
            col1, col2 = st.columns([1, 2])
            with col1:
                st.caption(f"{label}: {st.session_state[f'{key}_data_file_count']}")
            with col2:
                if st.button("Clear", key=f"clear_{key}"):
                    clear_folder(f"./data/{st.session_state.access_code}/{folder}/")
                    st.session_state[f'{key}_data_file_count'] = \
                        count_files(f"./data/{st.session_state.access_code}/{folder}/")

        # Add refresh button for file counts
        if st.button("Refresh"):
            cache_file_counts()
            st.rerun()
