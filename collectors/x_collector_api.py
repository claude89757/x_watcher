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


from pathlib import Path

# 获取当前工作目录
current_path = Path.cwd()
print("当前工作目录:", current_path)

# 获取当前脚本的路径
script_path = Path(__file__).resolve()
print("当前脚本路径:", script_path)

# 获取当前脚本所在的目录
script_dir = script_path.parent
print("当前脚本所在目录:", script_dir)

# 配置日志
logging.basicConfig(level=logging.INFO)

app = Quart(__name__)


async def collect_data_from_x(username, email, password, search_key_word, max_post_num, access_code):
    # 模拟异步数据收集
    await asyncio.sleep(1)
    # 这里写你的实际数据收集逻辑
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
                await collect_data_from_x(username=username, email=email, password=password,
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
