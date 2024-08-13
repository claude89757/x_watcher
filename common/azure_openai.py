#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/13 21:35
@Author  : claudexie
@File    : azure_openai.py
@Software: PyCharm
"""
import traceback

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
                       f"with short and simple insights or explanations for each row based on the data." \
                       f"\n3. A new column named 'Classification Tag' with a category or tag indicating " \
                       f"the potential interest level of each row in product XYZ." \
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
        try:
            csv_rows = csv_content.splitlines()
            reader = pd.read_csv(StringIO("\n".join(csv_rows)), sep=",", iterator=True)

            for chunk in reader:
                results.append(chunk)

        except pd.errors.ParserError as e:
            st.error(f"Error parsing CSV data: {e}\n{csv_content}")

            continue

        # 每次处理完一批后更新进度条和状态信息
        progress_percentage = (i + batch_size) / len(data)
        progress_bar.progress(min(progress_percentage, 1.0))
        status_text.text(
            f"Batch {i // batch_size + 1}/{total_batches}: Sent {len(batch)} rows, received {len(csv_rows) - 1} rows.")

    result_df = pd.concat(results, ignore_index=True)

    # 处理完成后，清除状态信息
    progress_bar.empty()
    status_text.text("Processing completed!")

    return result_df