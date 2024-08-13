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


def count_files(folder_path):
    try:
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        return len(files)
    except FileNotFoundError:
        return 0


def cache_file_counts():
    # Ensure that we only calculate the counts once per session
    if 'raw_data_file_count' not in st.session_state:
        st.session_state.raw_data_file_count = count_files(f"./data/{st.session_state.access_code}/raw/")
        st.session_state.processed_data_file_count = count_files(f"./data/{st.session_state.access_code}/processed/")
        st.session_state.analyzed_data_file_count = count_files(f"./data/{st.session_state.access_code}/analyzed/")
        st.session_state.msg_data_file_count = count_files(f"./data/{st.session_state.access_code}/msg/")


def clear_folder(folder_path):
    try:
        for f in os.listdir(folder_path):
            file_path = os.path.join(folder_path, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
    except FileNotFoundError:
        pass


def sidebar():
    # Cache file counts
    cache_file_counts()

    # Create a component in the sidebar to display file counts
    st.sidebar.subheader("File Statistics")

    # Display counts and add buttons in a horizontal layout
    with st.sidebar:
        col1, col2 = st.columns(2)
        with col1:
            st.caption(f"Raw Data Files: {st.session_state.raw_data_file_count}")
        with col2:
            if st.button("Clear", key="clear_raw"):
                clear_folder(f"./data/{st.session_state.access_code}/raw/")
                st.session_state.raw_data_file_count = count_files(f"./data/{st.session_state.access_code}/raw/")

        col1, col2 = st.columns(2)
        with col1:
            st.caption(f"Processed Data Files: {st.session_state.processed_data_file_count}")
        with col2:
            if st.button("Clear", key="clear_processed"):
                clear_folder(f"./data/{st.session_state.access_code}/processed/")
                st.session_state.processed_data_file_count = count_files(f"./data/{st.session_state.access_code}/processed/")

        col1, col2 = st.columns(2)
        with col1:
            st.caption(f"Analyzed Data Files: {st.session_state.analyzed_data_file_count}")
        with col2:
            if st.button("Clear", key="clear_analyzed"):
                clear_folder(f"./data/{st.session_state.access_code}/analyzed/")
                st.session_state.analyzed_data_file_count = count_files(f"./data/{st.session_state.access_code}/analyzed/")

        col1, col2 = st.columns(2)
        with col1:
            st.caption(f"Msg Data Files: {st.session_state.msg_data_file_count}")
        with col2:
            if st.button("Clear", key="clear_msg"):
                clear_folder(f"./data/{st.session_state.access_code}/msg/")
                st.session_state.msg_data_file_count = count_files(f"./data/{st.session_state.access_code}/msg/")