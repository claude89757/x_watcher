#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11
@Author  : claude
@File    : refresh_account_status.py
@Software: PyCharm
"""

import datetime
import logging
from common.redis_client import RedisClient
from common.collector_sdk import check_x_login_status

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def refresh_account_status():
    # 初始化 Redis 客户端
    redis_client = RedisClient(db=0)

    # 从 Redis 中加载现有账号
    accounts = redis_client.get_json_data('twitter_accounts') or {}

    # 刷新每个账号的登录状态
    for username, details in accounts.items():
        email = details['email']
        password = details['password']
        status_code, response_text = check_x_login_status(username, email, password)
        if status_code == 200:
            accounts[username]['status'] = 'Success'
        else:
            accounts[username]['status'] = 'Failed'
        accounts[username]['last_checked'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Account {username} login status: {accounts[username]['status']}")

    # 更新 Redis 中的账号信息
    redis_client.set_json_data('twitter_accounts', accounts)

if __name__ == "__main__":
    refresh_account_status()
