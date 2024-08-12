#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/12 18:43
@Author  : claude
@File    : x_collector_api.py
@Software: PyCharm
"""

import asyncio
import logging

from quart import Quart
from quart import request

from config import CONFIG
from x_collect import TwitterWatcher

# 配置日志
logging.basicConfig(level=logging.INFO)

app = Quart(__name__)


async def async_collect_data_from_x(username, email, password, search_key_word, max_post_num, access_code):
    """
    异步收集数据
    :param username:
    :param email:
    :param password:
    :param search_key_word:
    :param max_post_num:
    :param access_code:
    :return:
    """
    logging.info("start collecting data.")
    watcher = TwitterWatcher('/usr/local/bin/chromedriver', username, email, password, search_key_word)
    watcher.run(max_post_num, access_code)
    logging.info("done collecting data.")
    return "Collected data"


@app.route('/collect_data_from_x', methods=['POST'])
async def webhook():
    if request.method == 'POST':
        app.logger.info('Received POST request on /collect_data_from_x ')
        data = await request.get_json()
        username = data.get('username')
        search_key_word = data.get('search_key_word')
        max_post_num = data.get('max_post_num')
        access_code = data.get('access_code')

        if not username:
            return 'Missing input username', 500

        try:
            collector_username_infos = CONFIG['x_collector_account_infos']
            if collector_username_infos.get(username):
                email = collector_username_infos[username]['email']
                password = collector_username_infos[username]['password']

                # 异步调用数据收集函数
                await async_collect_data_from_x(username=username, email=email, password=password,
                                                search_key_word=search_key_word, max_post_num=max_post_num,
                                                access_code=access_code)
                return 'Success', 200
            else:
                return 'Missing username\'s info', 500
        except Exception as e:
            app.logger.error(f'Internal Server Error: {e}')
            return 'Internal Server Error', 500
    else:
        app.logger.warning('Received non-POST request on /webhook')
        return 'Invalid request', 400


if __name__ == '__main__':
    app.logger.info('Starting Quart server...')
    app.run(host='0.0.0.0', port=8080, debug=True)
