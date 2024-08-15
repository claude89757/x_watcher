#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/12 18:43
@Author  : claude
@File    : x_collector_api.py
@Software: PyCharm
"""
import os
import json
import traceback
import logging
import datetime
import aiofiles

from quart import Quart
from quart import request
from quart import jsonify

from common.config import CONFIG
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
    task_file_path = f"/root/{access_code}_{username}_{search_key_word}_task"

    try:
        # 创建文件并写入 "RUNNING" 和当前时间
        with open(task_file_path, 'w') as task_file:
            task_file.write(f"RUNNING at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 数据收集
        logging.info("start collecting data.")
        watcher = TwitterWatcher('/usr/local/bin/chromedriver', username, email, password, search_key_word)
        watcher.run(max_post_num, access_code)
        logging.info("done collecting data.")

        # 数据收集完成后将文件内容改为 "SUCCESS" 和当前时间
        with open(task_file_path, 'w') as task_file:
            task_file.write(f"SUCCESS at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        # 如果发生异常，将文件内容改为 "FAILED" 和当前时间
        with open(task_file_path, 'w') as task_file:
            task_file.write(f"FAILED at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {e}")


async def async_check_login_status(username, email, password):
    """
    验证登录情况
    :param username:
    :param email:
    :param password:
    :return:
    """
    logging.info("start collecting data.")
    watcher = TwitterWatcher('/usr/local/bin/chromedriver', username, email, password, "cat")
    return watcher.check_login_status()


@app.route('/query_status', methods=['GET'])
async def query_status():
    access_code = request.args.get('access_code')

    if not access_code:
        return 'Missing query parameter: access_code', 400

    task_files_dir = "/root"
    task_files = [f for f in os.listdir(task_files_dir) if f.startswith(access_code) and f.endswith('_task')]

    if not task_files:
        return 'No task files found for the given access_code', 404

    statuses = {}
    for task_file in task_files:
        task_file_path = os.path.join(task_files_dir, task_file)
        try:
            async with aiofiles.open(task_file_path, 'r') as file:
                statuses[task_file] = await file.read()
        except Exception as e:
            app.logger.error(f'Error reading task file {task_file}: {e}')
            statuses[task_file] = 'Error reading file'

    return jsonify(statuses), 200


@app.route('/collect_data_from_x', methods=['POST'])
async def collect_data_from_x():
    # 使用爬虫号
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
                app.logger.info('running...')
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


@app.route('/check_login_status', methods=['POST'])
async def check_login_status():
    if request.method == 'POST':
        app.logger.info('Received POST request on /check_login_status ')
        data = await request.get_json()
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        if not username or not email or not password:
            return 'Missing input username or email or password', 500

        try:
            watcher = TwitterWatcher('/usr/local/bin/chromedriver', username, email, password, "cat")
            if watcher.check_login_status():
                return 'Success', 200
            else:
                return 'Unauthorized', 401
        except Exception as e:
            app.logger.error(f'Internal Server Error: {e}')
            return 'Internal Server Error', 500
    else:
        app.logger.warning('Received non-POST request on /webhook')
        return 'Invalid request', 400


@app.route('/send_msg_to_user', methods=['POST'])
async def send_msg_to_user():
    # 使用非爬虫号
    if request.method == 'POST':
        app.logger.info('Received POST request on /send_msg_to_user ')
        data = await request.get_json()
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        to_user_link = data.get('to_user_link')
        msg = data.get('msg')
        if not username or not email or not password or not to_user_link or not msg:
            app.logger.error(f'Missing input username or email or password or to_user_link or msg')
            return 'Missing input username or email or password or to_user_link or msg', 499

        try:
            watcher = TwitterWatcher('/usr/local/bin/chromedriver', username, email, password)
            return_msg = watcher.send_msg_to_user(to_user_link, msg)
            return return_msg, 200
        except Exception as e:
            error_message = traceback.format_exc()
            print(error_message)
            app.logger.info(f'Internal Server Error: {e}')
            return 'Internal Server Error', 500
    else:
        print('Received non-POST request on /webhook')
        return 'Invalid request', 400


@app.route('/collect_user_link_detail', methods=['POST'])
async def collect_user_link_detail():
    # 使用爬虫号
    if request.method == 'POST':
        app.logger.info('Received POST request on /collect_user_link_detail ')
        data = await request.get_json()
        username = data.get('username')
        user_id_list = data.get('user_id_list')
        if not username or not user_id_list:
            app.logger.error(f'Missing input username or email or password or to_user_link or msg')
            return 'Missing input username or email or password or to_user_link or msg', 499
        try:
            collector_username_infos = CONFIG['x_collector_account_infos']
            if collector_username_infos.get(username):
                email = collector_username_infos[username]['email']
                password = collector_username_infos[username]['password']
                watcher = TwitterWatcher('/usr/local/bin/chromedriver', username, email, password)
                data = watcher.collect_user_link_detail(user_id_list)
                data_str = json.dumps({"data": data})
                return data_str, 200
            else:
                return 'Missing username\'s info', 401
        except Exception as e:
            error_message = traceback.format_exc()
            print(error_message)
            app.logger.info(f'Internal Server Error: {e}')
            return 'Internal Server Error', 500
    else:
        print('Received non-POST request on /webhook')
        return 'Invalid request', 400


if __name__ == '__main__':
    app.logger.info('Starting Quart server...')
    app.run(host='0.0.0.0', port=8080, debug=True)
