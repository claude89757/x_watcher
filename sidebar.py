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

    # Display counts from session state
    st.sidebar.caption(f"Raw Data Files: {st.session_state.raw_data_file_count}")
    st.sidebar.caption(f"Processed Data Files: {st.session_state.processed_data_file_count}")
    st.sidebar.caption(f"Analyzed Data Files: {st.session_state.analyzed_data_file_count}")
    st.sidebar.caption(f"Msg Data Files: {st.session_state.msg_data_file_count}")

    # Add buttons to clear files in the folders
    if st.sidebar.button("Clear Raw Data Files"):
        clear_folder(f"./data/{st.session_state.access_code}/raw/")
        st.session_state.raw_data_file_count = count_files(f"./data/{st.session_state.access_code}/raw/")

    if st.sidebar.button("Clear Processed Data Files"):
        clear_folder(f"./data/{st.session_state.access_code}/processed/")
        st.session_state.processed_data_file_count = count_files(f"./data/{st.session_state.access_code}/processed/")

    if st.sidebar.button("Clear Analyzed Data Files"):
        clear_folder(f"./data/{st.session_state.access_code}/analyzed/")
        st.session_state.analyzed_data_file_count = count_files(f"./data/{st.session_state.access_code}/analyzed/")

    if st.sidebar.button("Clear Msg Data Files"):
        clear_folder(f"./data/{st.session_state.access_code}/msg/")
        st.session_state.msg_data_file_count = count_files(f"./data/{st.session_state.access_code}/msg/")
