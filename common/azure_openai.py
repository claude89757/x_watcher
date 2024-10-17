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
import os
import json
import pandas as pd
import openai
from io import StringIO
import streamlit as st

from common.log_config import setup_logger

# 配置日志
logger = setup_logger(__name__)

def get_openai_api_key():
    """
    从环境变量或本地文件缓存中获取 OPENAI_API_KEY
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        return api_key
    
    # 如果环境变量中没有，尝试从本地文件读取
    cache_file = os.path.expanduser("~/.openai_api_key")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                data = json.load(f)
                return data.get("api_key")
        except Exception as e:
            logger.error(f"从本地文件读取 API 密钥失败：{e}")
    
    logger.error("未找到 OPENAI_API_KEY，请设置环境变量或在本地文件中配置")
    return None

# 获取 API 密钥
OPENAI_API_KEY = get_openai_api_key()

# 设置 OpenAI API 密钥
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
else:
    logger.error("未设置 OPENAI_API_KEY，某些功能可能无法正常工作")

def send_text_to_gpt(model: str, system_prompt: str, data: pd.DataFrame, batch_size: int = 100) -> pd.DataFrame:
    """
    发送数据到GPT模型，获取分析结果。
    :param model: 使用的GPT模型名称。
    :param system_prompt: 系统级指令，用于提供分析上下文。
    :param data: 包含需要分析的数据的DataFrame。
    :param batch_size: 每次发送的数据行数，默认100行。
    :return: 包含分析结果的DataFrame。
    """
    results = []
    max_tokens = 2000

    # 初始化Streamlit进度条和文本显示
    progress_bar = st.progress(0)
    status_text = st.empty()

    # 计算总批次数量
    total_batches = len(data) // batch_size + (1 if len(data) % batch_size != 0 else 0)

    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size]
        batch_csv = batch.to_csv(index=False)

        batch_prompt = f"{system_prompt}\n\n" \
               f"分析以下数据并以CSV格式提供输出，结构如下：" \
               f"\n1. 保持原始数据的所有列不变。" \
               f"\n2. 新增一列名为'分析说明'，" \
               f"为每行数据提供基于数据的见解或解释。" \
               f"每个解释应用双引号括起，并限制在20个字符以内。" \
               f"\n3. 新增一列名为'分类标签'，包含两个固定类别或标签之一：" \
               f'"高兴趣"或"低兴趣"，表示每行对产品XYZ的潜在兴趣水平。' \
               f"\n\n数据：\n{batch_csv}"

        try:
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": batch_prompt}
                ],
                temperature=0.7,
                top_p=0.95,
                max_tokens=max_tokens
            )
            response_content = response.choices[0].message.content
        except Exception as error:
            error_message = traceback.format_exc()
            st.error(f"批次{i // batch_size + 1}/{total_batches}失败：{error_message}")
            logger.error(f"异常详情：{error_message}")
            continue

        # 处理响应内容
        if "```csv" in response_content:
            csv_content = response_content.split("```csv")[1].split("```")[0].strip()
        else:
            csv_content = response_content

        logger.info("输出==============================================")
        logger.info(csv_content)
        logger.info("输出==============================================")

        # 处理CSV内容
        csv_rows = []
        try:
            csv_reader = csv.reader(StringIO(csv_content), delimiter=',', quotechar='"')
            for row in csv_reader:
                if len(row) == 4:
                    results.append(row)
                    csv_rows.append(row)
                else:
                    st.warning(f"跳过字段数量不符的行：{row}")
        except csv.Error as e:
            st.error(f"CSV解析错误：{e}")

        # 更新进度和状态
        progress_percentage = (i + batch_size) / len(data)
        progress_bar.progress(min(progress_percentage, 1.0))
        status_message = (
            f"> 批次 {i // batch_size + 1}/{total_batches}：已发送 {len(batch)} 行，"
            f"已接收 {len(csv_rows)} 行。"
        )
        status_text.markdown(status_message)

    # 合并结果
    if results:
        result_df = pd.DataFrame(results[1:], columns=results[0])
    else:
        result_df = pd.DataFrame()

    # 清理进度显示
    progress_bar.empty()
    status_text.text("处理完成！")

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
    max_tokens = 2000

    # 初始化 Streamlit 进度条和文本显示
    progress_bar = st.progress(0)
    status_text = st.empty()

    # 计算总批次数量
    total_batches = len(user_data) // batch_size + (1 if len(user_data) % batch_size != 0 else 0)

    for i in range(0, len(user_data), batch_size):
        batch = user_data.iloc[i:i + batch_size]
        batch_csv = batch.to_csv(index=False)

        batch_prompt = f"{system_prompt}\n\n" \
                       f"根据每个用户的信息和评论生成推广短信。" \
                       f"输出应为CSV格式，结构如下：" \
                       f"\n1. 保持原始数据的所有列不变。" \
                       f"\n2. 新增一列名为'推广短信'，包含为每个用户定制的推广信息。" \
                       f"每条信息应用双引号括起。" \
                       f"\n\n数据：\n{batch_csv}"

        try:
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": batch_prompt}
                ],
                temperature=0.7,
                top_p=0.95,
                max_tokens=max_tokens
            )
            response_content = response.choices[0].message.content
        except Exception as error:
            error_message = traceback.format_exc()
            st.error(f"批次 {i // batch_size + 1}/{total_batches} 失败：{error_message}")
            logger.error(f"异常详情：{error_message}")
            continue

        if "```csv" in response_content:
            csv_content = response_content.split("```csv")[1].split("```")[0].strip()
        else:
            csv_content = response_content

        logger.info("输出==============================================")
        logger.info(csv_content)
        logger.info("输出==============================================")

        # 处理CSV内容
        csv_rows = []
        try:
            csv_reader = csv.reader(StringIO(csv_content), delimiter=',', quotechar='"')
            for row in csv_reader:
                if len(row) == 4:
                    results.append(row)
                    csv_rows.append(row)
                else:
                    st.warning(f"跳过字段数量不符的行：{row}")
        except csv.Error as e:
            st.error(f"CSV解析错误：{e}")

        # 更新进度和状态
        progress_percentage = (i + batch_size) / len(user_data)
        progress_bar.progress(min(progress_percentage, 1.0))
        status_message = (
            f"> 批次 {i // batch_size + 1}/{total_batches}：已发送 {len(batch)} 行，"
            f"已接收 {len(csv_rows)} 行。"
        )
        status_text.markdown(status_message)

    # 合并结果
    if results:
        result_df = pd.DataFrame(results[1:], columns=results[0])
    else:
        result_df = pd.DataFrame()

    # 清理进度显示
    progress_bar.empty()
    status_text.text("处理完成！")

    return result_df

def process_with_gpt(model: str, prompt: str, max_tokens: int = 2000, temperature: float = 0.7, 
                     top_p: float = 0.95) -> str:
    """
    使用GPT模型处理单次请求数据。
    
    :param model: 使用的GPT模型名称。
    :param prompt: 完整的提示��包括系统提示、用户提示和数据。
    :param max_tokens: 模型返回的最大token数，默认2000。
    :param temperature: 控制输出随机性，默认0.7。
    :param top_p: 控制输出多样性，默认0.95。
    :return: GPT模型的响应内容。
    """
    try:
        response = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": prompt}
            ],
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens
        )
        response_content = response.choices[0].message.content
    except Exception as error:
        error_message = traceback.format_exc()
        logger.error(f"GPT处理失败：{error_message}")
        raise

    logger.info("输出==============================================")
    logger.info(response_content)
    logger.info("输出==============================================")

    return response_content
