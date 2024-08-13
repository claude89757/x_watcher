#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/13 21:35
@Author  : claudexie
@File    : azure_openai.py
@Software: PyCharm
"""
import traceback
import csv

import pandas as pd
import requests
import streamlit as st
from io import StringIO

from common.config import CONFIG
from common.log_config import setup_logger

# Configure logger
logger = setup_logger(__name__)


def send_text_to_gpt(model: str, system_prompt: str, data: pd.DataFrame, batch_size: int = 100) -> pd.DataFrame:
    """
    发送数据到GPT模型，获取分析结果。
    :param system_prompt: 系统级指令，用于提供分析上下文。
    :param data: 包含需要分析的数据的DataFrame。
    :param batch_size: 每次发送的数据行数，默认1000行。
    :return: 包含分析结果的DataFrame。
    """
    results = []
    max_tokens = 2000  # 假设每行输入和输出都很长

    # 初始化 Streamlit 进度条和文本显示
    progress_bar = st.progress(0)
    status_text = st.empty()

    # 计算总批次数量
    total_batches = len(data) // batch_size + (1 if len(data) % batch_size != 0 else 0)

    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size]
        batch_csv = batch.to_csv(index=False)

        batch_prompt = f"{system_prompt}\n\n" \
                       f"Analyze the following data and provide the output in CSV format " \
                       f"with the following structure:" \
                       f"\n1. Original data with all columns intact." \
                       f"\n2. A new column named 'Analysis Explanation' " \
                       f"with insights or explanations for each row based on the data. " \
                       f"Each explanation should be enclosed in double quotes and limited to 20 characters." \
                       f"\n3. A new column named 'Classification Tag' with a category or tag indicating " \
                       f"the potential interest level of each row in product XYZ. " \
                       f"This tag should also be enclosed in double quotes." \
                       f"\n\nData:\n{batch_csv}"

        payload = {
            "messages": [
                {"role": "system", "content": batch_prompt}
            ],
            "temperature": 0.7,
            "top_p": 0.95,
            "max_tokens": max_tokens
        }

        logger.info("input==============================================")
        logger.info(batch_prompt)
        logger.info("input==============================================")

        url = f"https://chatgpt3.openai.azure.com/openai/deployments/{model}/chat/completions?" \
              f"api-version=2024-02-15-preview"
        try:
            response = requests.post(url,
                                     headers={
                                         "Content-Type": "application/json",
                                         "api-key": CONFIG.get("azure_open_api_key"),
                                     },
                                     json=payload
            )
            response.raise_for_status()
            logger.info(f"response: {response.json()}")
            response_content = response.json()['choices'][0]['message']['content']
        except Exception as error:
            # Log the full traceback and error details
            error_message = traceback.format_exc()
            st.error(f"Batch{i // batch_size + 1}/{total_batches} failed: {error_message}")
            logger.error(f"Exception details: {error_message}")
            continue

        if "```csv" in response_content:
            csv_content = response_content.split("```csv")[1].split("```")[0].strip()
        else:
            csv_content = response_content

        # Debugging: Print the CSV content to verify format
        logger.info("output==============================================")
        logger.info(csv_content)
        logger.info("output==============================================")

        # Handle potential parsing errors
        csv_rows = []
        try:
            csv_reader = csv.reader(StringIO(csv_content), delimiter=',', quotechar='"')
            for row in csv_reader:
                if len(row) == 4:
                    results.append(row)
                    csv_rows.append(row)
                else:
                    st.warning(f"Skipping row with unexpected number of fields: {row}")
        except csv.Error as e:
            st.error(f"Error parsing CSV: {e}")

        # 每次处理完一批后更新进度条和状态信息
        progress_percentage = (i + batch_size) / len(data)
        progress_bar.progress(min(progress_percentage, 1.0))
        # 构建状态信息字符串
        status_message = (
            f"Batch {i // batch_size + 1}/{total_batches}: Sent {len(batch)} rows, "
            f"received {len(csv_rows)} rows."
        )

        if csv_rows:
            # 将 CSV 行转换为 DataFrame 并选择第一列和最后一列
            df = pd.DataFrame(csv_rows[1:], columns=csv_rows[0])  # 使用第一行作为列名
            df_subset = df.iloc[:, [0, -1]]  # 选择第一列和最后一列
            markdown_table = df_subset.to_markdown(index=False)  # 转换为 Markdown 表格格式
            status_message += f"\n\nData:\n{markdown_table}"
        else:
            status_message += "\nNo data."

        # 更新 Streamlit 状态文本为 Markdown 格式
        status_text.markdown(status_message)

    # 合并所有 DataFrame
    if results:
        result_df = pd.DataFrame(results[1:], columns=results[0])
    else:
        result_df = pd.DataFrame()

    # 处理完成后，清除状态信息
    progress_bar.empty()
    status_text.text("Processing completed!")

    return result_df


def generate_promotional_sms(model: str, system_prompt: str, user_data: pd.DataFrame, batch_size: int = 100) -> pd.DataFrame:
    """
    使用大模型根据用户和评论信息生成推广短信。
    :param model: 使用的GPT模型名称。
    :param system_prompt: 系统级指令，用于提供分析上下文。
    :param user_data: 包含用户和评论信息的DataFrame。
    :param batch_size: 每次发送的数据行数，默认100行。
    :return: 包含推广短信的DataFrame。
    """
    results = []
    max_tokens = 2000  # 假设每行输入和输出都很长

    # 初始化 Streamlit 进度条和文本显示
    progress_bar = st.progress(0)
    status_text = st.empty()

    # 计算总批次数量
    total_batches = len(user_data) // batch_size + (1 if len(user_data) % batch_size != 0 else 0)

    for i in range(0, len(user_data), batch_size):
        batch = user_data.iloc[i:i + batch_size]
        batch_csv = batch.to_csv(index=False)

        batch_prompt = f"{system_prompt}\n\n" \
                       f"Generate a promotional SMS for each user based on their information and comments. " \
                       f"The output should be in CSV format with the following structure:" \
                       f"\n1. Original data with all columns intact." \
                       f"\n2. A new column named 'Promotional SMS' with a personalized promotional message " \
                       f"for each user. Each message should be enclosed in double quotes." \
                       f"\n\nData:\n{batch_csv}"

        payload = {
            "messages": [
                {"role": "system", "content": batch_prompt}
            ],
            "temperature": 0.7,
            "top_p": 0.95,
            "max_tokens": max_tokens
        }

        logger.info("input==============================================")
        logger.info(batch_prompt)
        logger.info("input==============================================")

        url = f"https://chatgpt3.openai.azure.com/openai/deployments/{model}/chat/completions?" \
              f"api-version=2024-02-15-preview"
        try:
            response = requests.post(url,
                                     headers={
                                         "Content-Type": "application/json",
                                         "api-key": CONFIG.get("azure_open_api_key"),
                                     },
                                     json=payload
            )
            response.raise_for_status()
            logger.info(f"response: {response.json()}")
            response_content = response.json()['choices'][0]['message']['content']
        except Exception as error:
            # Log the full traceback and error details
            error_message = traceback.format_exc()
            st.error(f"Batch {i // batch_size + 1}/{total_batches} failed: {error_message}")
            logger.error(f"Exception details: {error_message}")
            continue

        if "```csv" in response_content:
            csv_content = response_content.split("```csv")[1].split("```")[0].strip()
        else:
            csv_content = response_content

        # Debugging: Print the CSV content to verify format
        logger.info("output==============================================")
        logger.info(csv_content)
        logger.info("output==============================================")

        # Handle potential parsing errors
        csv_rows = []
        try:
            csv_reader = csv.reader(StringIO(csv_content), delimiter=',', quotechar='"')
            for row in csv_reader:
                if len(row) == 5:
                    results.append(row)
                    csv_rows.append(row)
                else:
                    st.warning(f"Skipping row with unexpected number of fields: {row}")
        except csv.Error as e:
            st.error(f"Error parsing CSV: {e}")

        # 每次处理完一批后更新进度条和状态信息
        progress_percentage = (i + batch_size) / len(user_data)
        progress_bar.progress(min(progress_percentage, 1.0))
        # 构建状态信息字符串
        status_message = (
            f"Batch {i // batch_size + 1}/{total_batches}: Sent {len(batch)} rows, "
            f"received {len(csv_rows)} rows."
        )

        if csv_rows:
            # 将 CSV 行转换为 DataFrame 并选择第一列和最后一列
            df = pd.DataFrame(csv_rows[1:], columns=csv_rows[0])  # 使用第一行作为列名
            df_subset = df.iloc[:, [0, -1]]  # 选择第一列和最后一列
            markdown_table = df_subset.to_markdown(index=False)  # 转换为 Markdown 表格格式
            status_message += f"\n\nData:\n{markdown_table}"
        else:
            status_message += "\nNo data."

        # 更新 Streamlit 状态文本为 Markdown 格式
        status_text.markdown(status_message)

    # 合并所有 DataFrame
    if results:
        result_df = pd.DataFrame(results[1:], columns=results[0])
    else:
        result_df = pd.DataFrame()

    # 处理完成后，清除状态信息
    progress_bar.empty()
    status_text.text("Processing completed!")

    return result_df
