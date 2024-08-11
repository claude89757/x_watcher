#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 18:52
@Author  : claude
@File    : 1_Collect_Data.py
@Software: PyCharm
"""
import os
import logging
import shutil
import time

import pandas as pd
import streamlit as st
from utils import load_comments_from_csv
from config import ACCESS_CODE_LIST

# Configure logger
logging.basicConfig(format="\n%(asctime)s\n%(message)s", level=logging.INFO, force=True)

if st.session_state.get('access_code') and st.session_state.get('access_code') in ACCESS_CODE_LIST:
    # session中有缓存
    st.query_params.access_code = st.session_state.access_code
elif st.query_params.get('access_code') and st.query_params.get('access_code') in ACCESS_CODE_LIST:
    # URL中有缓存
    st.session_state.access_code = st.query_params.access_code
else:
    st.warning("Access not Granted!")
    st.switch_page("Home.py", )

# Initialize session state
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "raw_data_filename" not in st.session_state:
    st.session_state.raw_data_filename = ""

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

st.title("Step 2: Preprocessing and Filter Data")
st.markdown("Preprocessing and filtering data, including selecting fields, choosing files,"
            " and applying necessary preprocessing steps.")

# Collect filenames and extract keywords
main_data_dir = f"./data/{st.session_state.access_code}/"
search_keywords = set()
for file_name in os.listdir(main_data_dir):
    if file_name.startswith("raw_"):
        parts = file_name.split('_')
        if len(parts) > 1:
            keyword = parts[1]
            search_keywords.add(keyword)

# Convert set to sorted list
search_keywords = sorted(search_keywords)

# Allow user to select a keyword if available
if search_keywords:
    selected_keyword = st.selectbox("Select Key Word", search_keywords)
else:
    st.write("No matching files found.")
    selected_keyword = None

# Filter and display files based on the selected keyword
selected_file_name = None
destination_dir = f"./data/{st.session_state.access_code}/processed/"
if selected_keyword:
    matching_files = []
    for file_name in os.listdir(main_data_dir):
        if file_name.startswith(f"raw_{selected_keyword}"):
            file_path = os.path.join(main_data_dir, file_name)
            file_size = os.path.getsize(file_path)
            data = pd.read_csv(file_path)
            num_rows = len(data)
            matching_files.append((file_name, file_size, num_rows))

    # If matching files are found, display them in a dropdown with a button
    if matching_files:
        file_options = [
            f"{name} ({size/1024:.2f} KB, {num_rows} rows)"
            for name, size, num_rows in matching_files
        ]
        selected_file_option = st.selectbox("Select file to process", file_options)

        # Get the selected filename based on the dropdown selection
        selected_file_name = matching_files[file_options.index(selected_file_option)][0]

        # Ensure the destination directory exists
        os.makedirs(destination_dir, exist_ok=True)

        # Button to move the file
        if st.button("Confirm File ", type="primary"):
            src_file_path = os.path.join(main_data_dir, selected_file_name)
            dst_file_path = os.path.join(destination_dir, selected_file_name)
            shutil.move(src_file_path, dst_file_path)
            st.success(f"File moved to {destination_dir}, entering step...")
            time.sleep(3)
            st.switch_page("pages/3_AI_Analyze_Data.py")
    else:
        st.write("No matching files found.")
else:
    st.warning(f"Not raw data, collect raw data first, return to collect data...")
    time.sleep(3)
    st.switch_page("pages/1_Collect_Data.py")

# Display the selected data if available
if selected_file_name:
    st.subheader(f"Current Data: {selected_file_name}")
    selected_file_path = os.path.join(main_data_dir, selected_file_name)
    # Display file data
    data = pd.read_csv(selected_file_path)
    st.write(data.head())  # Show the first few rows of the data




