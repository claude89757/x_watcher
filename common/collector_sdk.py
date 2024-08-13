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


from common.config import CONFIG


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
    api_endpoint = f'http://{promoter_url}/check_login_status'  # 在这里定义 API 端点 URL
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
