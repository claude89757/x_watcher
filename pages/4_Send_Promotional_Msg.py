#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 23:08
@Author  : claude
@File    : 3_AI_Analyze_Data.py
@Software: PyCharm
"""
import os
import time

import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger

# Configure logger
logger = setup_logger(__name__)

st.set_page_config(page_title="Promotional Msg", page_icon="🤖", layout="wide")


# init session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "matching_files" not in st.session_state:
    st.session_state.matching_files = ""
if "analysis_run" not in st.session_state:
    st.session_state.analysis_run = False

# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
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

st.title("Step 4: Send Promotional Msg")
st.markdown("Automate the sending of personalized promotional messages based on AI analysis results")

st.error("Coming soon...")


def get_file_list(directory):
    """Returns a list of all files in the directory"""
    try:
        files = os.listdir(directory)
        files = [f for f in files if os.path.isfile(os.path.join(directory, f))]
        return files
    except FileNotFoundError:
        return []


def count_files(directory):
    """Returns the number of files in the directory"""
    return len(get_file_list(directory))


# Create a component in the sidebar to display file counts
st.sidebar.header("File Statistics")

folders = {
    "Raw Data": f"./data/{st.session_state.access_code}/raw/",
    "Processed Data": f"./data/{st.session_state.access_code}/processed/",
    "Analyzed Data": f"./data/{st.session_state.access_code}/analyzed/"
}

for folder_name, folder_path in folders.items():
    count = count_files(folder_path)
    st.sidebar.write(f"{folder_name} File Count: {count}")

# Create an expander in the sidebar to display the file list
selected_folder = st.sidebar.selectbox("Select a Folder", list(folders.keys()))
selected_folder_path = folders[selected_folder]

with st.sidebar.expander(f"View {selected_folder} File List", expanded=True):
    file_list = get_file_list(selected_folder_path)
    if file_list:
        st.sidebar.write("File List:")
        for file in file_list:
            st.sidebar.write(file)
    else:
        st.sidebar.write("Directory is empty or does not exist")
