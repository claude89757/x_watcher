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

    # Define a function to create a row with file count and clear button
    def create_row(label, count, key, folder_path):
        st.sidebar.markdown(
            f"""
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span>{label}: {count}</span>
                <button onclick="window.location.href='/?clear={key}'" style="background-color: #f0f0f0; border: none; padding: 5px 10px; cursor: pointer;">Clear</button>
            </div>
            """,
            unsafe_allow_html=True
        )
        if st.experimental_get_query_params().get('clear') == [key]:
            clear_folder(folder_path)
            st.experimental_set_query_params()  # Clear the query params
            st.experimental_rerun()  # Rerun the app to update the counts

    # Create rows for each file type
    create_row("Raw Data Files", st.session_state.raw_data_file_count, "raw", f"./data/{st.session_state.access_code}/raw/")
    create_row("Processed Data Files", st.session_state.processed_data_file_count, "processed", f"./data/{st.session_state.access_code}/processed/")
    create_row("Analyzed Data Files", st.session_state.analyzed_data_file_count, "analyzed", f"./data/{st.session_state.access_code}/analyzed/")
    create_row("Msg Data Files", st.session_state.msg_data_file_count, "msg", f"./data/{st.session_state.access_code}/msg/")
