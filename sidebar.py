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
        return len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
    except FileNotFoundError:
        return 0


def cache_file_counts():
    # Ensure that we only calculate the counts once per session
    if 'file_counts' not in st.session_state:
        folders = {
            "Raw Data": f"./data/{st.session_state.access_code}/raw/",
            "Processed Data": f"./data/{st.session_state.access_code}/processed/",
            "Analyzed Data": f"./data/{st.session_state.access_code}/analyzed/"
        }
        st.session_state.file_counts = {folder_name: count_files(folder_path)
                                        for folder_name, folder_path in folders.items()}


def sidebar():
    # Cache file counts
    cache_file_counts()

    # Create a component in the sidebar to display file counts
    st.sidebar.subheader("File Statistics")

    for folder_name, count in st.session_state.file_counts.items():
        st.sidebar.caption(f"{folder_name} File: {count}")
