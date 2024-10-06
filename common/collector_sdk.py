#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/14 00:23
@Author  : claudexie
@File    : collector_sdk.py
@Software: PyCharm
"""

import logging
import requests
from common.redis_client import RedisClient

from common.config import CONFIG


def query_status(access_code):
    """
    从 Redis 中查询任务状态

    :param access_code: 访问码
    :return: 返回状态码和任务信息
    """
    try:
        # 初始化 Redis 客户端
        redis_client = RedisClient(db=0)

        # 获取所有与 access_code 相关的任务键
        task_keys = redis_client.keys(f"{access_code}_*_task")

        tasks = {}
        for task_key in task_keys:
            # 从 Redis 中获取任务状态
            task_info = redis_client.get_json_data(task_key)
            if task_info:
                tasks[task_key] = task_info.get('status', 'Unknown')

        return 200, tasks
    except Exception as e:
        logging.error(f'Error querying task status: {e}')
        return 500, str(e)


def call_collect_data_from_x(username, search_key_word, max_post_num, access_code):
    """
    调用 /collect_data_from_x API 接口

    :param username: 用户名
    :param search_key_word: 搜索关键字
    :param max_post_num: 最大帖子数
    :param access_code: 访问代码
    :return: 返回 API 响应的状态和内容
    """
    collector_url = CONFIG['collector_urls'][0]
    api_endpoint = f'http://{collector_url}/collect_data_from_x'  # 在这里定义 API 端点 URL
    headers = {'Content-Type': 'application/json'}
    data = {
        'username': username,
        'search_key_word': search_key_word,
        'max_post_num': max_post_num,
        'access_code': access_code
    }
    try:
        logging.info(f"sending request...")
        response = requests.post(api_endpoint, json=data, headers=headers)
        response.raise_for_status()  # 抛出 HTTPError 异常（如果发生）
        if response.status_code == 200:
            return response.status_code, response.text
        else:
            raise Exception(f"calling API failed: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f'Error calling API: {e}')
        return None, str(e)


def collect_user_link_details(username, user_id_list):
    """
    调用 /collect_data_from_x API 接口

    :param username: 用户名
    :param user_id_list:
    :return: 返回 API 响应的状态和内容
    """
    collector_url = CONFIG['collector_urls'][0]
    api_endpoint = f'http://{collector_url}/collect_user_link_detail'  # 在这里定义 API 端点 URL
    headers = {'Content-Type': 'application/json'}
    data = {
        'username': username,
        'user_id_list': user_id_list,
    }
    try:
        logging.info(f"sending request...")
        response = requests.post(api_endpoint, json=data, headers=headers)
        response.raise_for_status()  # 抛出 HTTPError 异常（如果发生）
        if response.status_code == 200:
            return response.status_code, response.json()['data']
        else:
            raise Exception(f"calling API failed: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f'Error calling API: {e}')
        return None, str(e)


def check_x_login_status(username, email, password):
    """
    调用 /check_login_status API 接口

    :param username:
    :param email:
    :param password:
    :return: 返回 API 响应的状态和内容
    """
    promoter_url = CONFIG['promoter_urls'][0]
    api_endpoint = f'http://{promoter_url}/check_login_status'  # 在这里定义 API 端点 URL
    headers = {'Content-Type': 'application/json'}
    data = {
        'username': username,
        'email': email,
        'password': password
    }
    try:
        logging.info(f"sending request...")
        response = requests.post(api_endpoint, json=data, headers=headers)
        response.raise_for_status()  # 抛出 HTTPError 异常（如果发生）
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        logging.error(f'Error calling API: {e}')
        return None, str(e)


def send_promotional_msg(username, email, password, to_user_link, msg):
    """
    调用 /check_login_status API 接口

    :param msg:
    :param to_user_link:
    :param username:
    :param email:
    :param password:
    :return: 返回 API 响应的状态和内容
    """
    promoter_url = CONFIG['promoter_urls'][0]
    api_endpoint = f'http://{promoter_url}/send_msg_to_user'  # 在这里定义 API 端点 URL
    headers = {'Content-Type': 'application/json'}
    data = {
        'username': username,
        'email': email,
        'password': password,
        'to_user_link': to_user_link,
        'msg': msg,
    }
    try:
        logging.info(f"sending request...")
        response = requests.post(api_endpoint, json=data, headers=headers)
        response.raise_for_status()  # 抛出 HTTPError 异常（如果发生）
        return response.status_code, response.text
    except requests.exceptions.RequestException as e:
        logging.error(f'Error calling API: {e}')
        return None, str(e)
