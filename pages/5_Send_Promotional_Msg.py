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
import random

import pandas as pd
import streamlit as st

from common.config import CONFIG
from common.log_config import setup_logger
from common.azure_openai import generate_promotional_sms
from common.collector_sdk import check_x_login_status
from common.collector_sdk import send_promotional_msg
from sidebar import sidebar

# Configure logger
logger = setup_logger(__name__)

st.set_page_config(page_title="Send Msg", page_icon="🤖", layout="wide")


# init session state
if 'access_code' not in st.session_state:
    st.session_state.access_code = st.query_params.get('access_code')
if "search_keyword" not in st.session_state:
    st.session_state.search_keyword = st.query_params.get("search_keyword")
if "matching_files" not in st.session_state:
    st.session_state.matching_files = ""
if "login_status" not in st.session_state:
    st.session_state.login_status = st.query_params.get("login_status", "")
if "username" not in st.session_state:
    st.session_state.username = st.query_params.get("username", "")
if "email" not in st.session_state:
    st.session_state.email = st.query_params.get("email", "")
if "password" not in st.session_state:
    st.session_state.password = ""


# check access
if st.session_state.access_code and st.session_state.access_code in CONFIG['access_code_list']:
    st.query_params.access_code = st.session_state.access_code
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

st.title("Step 5: Send Promotional Msg")
st.markdown("Automate the sending of AI-generated promotional messages.")

cur_dir = f"./data/{st.session_state.access_code}/msg/"
records_dir = f"./data/{st.session_state.access_code}/records/"
files = [f for f in os.listdir(cur_dir) if os.path.isfile(os.path.join(cur_dir, f))]
files.sort(key=lambda f: os.path.getmtime(os.path.join(cur_dir, f)), reverse=True)
st.session_state.selected_file = st.selectbox("Select a file:", files)

selected_file_path = None
if st.session_state.selected_file:
    selected_file_path = os.path.join(cur_dir, st.session_state.selected_file)
    st.subheader(f"Promotional Msg Preview: {st.session_state.selected_file}")

    # Read and display data
    try:
        data = pd.read_csv(selected_file_path)
        data_df = data.iloc[:, [0, -1]]

        # Add text area to edit the last column
        last_col_data = "\n".join(data_df.iloc[:, -1].astype(str).tolist())
        edited_last_col = st.text_area("Edit Promotional Msg:", value=last_col_data, height=300)

        # Convert edited data back to dataframe
        edited_data = edited_last_col.split('\n')
        if len(edited_data) == len(data_df):
            data_df.iloc[:, -1] = edited_data
            st.dataframe(data_df)

            # Button to save changes
            if st.button("Save Changes"):
                try:
                    data_df.to_csv(selected_file_path, index=False)
                    st.success("File saved successfully.")
                except Exception as e:
                    st.error(f"Error saving file: {e}")

        # Add selection box for the number of private messages
        send_msg_num = st.number_input("Select the number of messages to send", min_value=1, max_value=len(data_df),
                                       value=min(5, len(data_df)))

    except Exception as e:
        st.error(f"Error loading data from local file: {e}")
else:
    st.warning("No processed data, return to AI Generate Msg...")
    time.sleep(3)
    st.switch_page("pages/4_AI_Generate_Msg.py")


st.markdown("------")
st.subheader("X Account Verify")
username = st.text_input("Twitter Username", value=st.session_state.username)
email = st.text_input("Email", value=st.session_state.email)
if st.session_state.password:
    password = st.text_input("Password", type="password", value=st.session_state.password)
else:
    password = st.text_input("Password", type="password")
st.session_state.password = password


def load_records():
    if os.path.exists(records_file):
        return pd.read_csv(records_file)
    return pd.DataFrame(columns=['User ID', 'Message', 'Status'])


def append_record(record):
    df = pd.DataFrame([record])
    df.to_csv(records_file, mode='a', header=not os.path.exists(records_file), index=False)


if st.session_state.login_status == "online" and st.session_state.password:
    records_dir = f"./data/{st.session_state.access_code}/records/"
    os.makedirs(records_dir, exist_ok=True)
    records_file = os.path.join(records_dir, "send_msg_records.csv")

    st.success("🟢 **Online**")  # Emoji for online status

    if st.button("Send Promotional Messages", type='primary'):
        with st.spinner('Sending Promotional Msg...'):
            progress_bar = st.progress(0)
            results = []
            success_count = 0
            failure_count = 0

            success_placeholder = st.empty()
            failure_placeholder = st.empty()

            for index, row in data_df.iterrows():
                user_id = row[0]
                message = row[-1]
                user_link = f"https://x.com/{user_id}"
                code, text = send_promotional_msg(username, email, password, user_link, message)
                logger.warning(f"sending {user_id} {email} {len(password)} {user_id} {message}")
                result = {
                    'User ID': user_id,
                    'Message': message,
                    'Status': text
                }
                results.append(result)
                append_record(result)

                if text == "Success":
                    success_count += 1
                else:
                    failure_count += 1

                progress_bar.progress((index + 1) / send_msg_num)
                success_placeholder.markdown(f"<span style='color: green;'>Success: {success_count}</span>",
                                             unsafe_allow_html=True)
                failure_placeholder.markdown(f"<span style='color: red;'>Failure: {failure_count}</span>",
                                             unsafe_allow_html=True)

                time.sleep(random.uniform(1, 10))

                if len(results) >= send_msg_num:
                    break

            results_df = pd.DataFrame(results)
            st.success("All promotional messages processed!")
            st.subheader("Results")
            st.table(results_df)

    st.subheader("Cached Records")
    cached_records = load_records()
    st.dataframe(cached_records)
else:
    if st.button("Verify Login Status"):
        with st.spinner('Verifying Login Status...'):
            return_code, msg = check_x_login_status(username, email, password)
        if return_code == 200:
            st.success("Verify Login Status successful!")
            st.session_state.login_status = "online"
            st.query_params.login_status = "online"
        else:
            st.error("Verify Login Status failed.")
        st.session_state.username = username
        st.session_state.email = email
        st.session_state.password = password
        st.query_params.username = username
        st.query_params.email = email
        st.rerun()
