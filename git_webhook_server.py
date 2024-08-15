#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/12 02:28
@Author  : claudexie
@File    : git_webhook_api.py
@Software: PyCharm
"""

from flask import Flask, request
import logging
from logging.handlers import RotatingFileHandler
import subprocess

app = Flask(__name__)

# 设置日志记录
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
log_file = 'webhook_server.log'

# 创建一个旋转文件处理器，最大文件大小为10MB，最多保留5个备份
file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

# 将处理器添加到应用的日志记录器
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        app.logger.info('Received POST request on /webhook')
        try:
            # 使用 subprocess 运行 git pull 命令并捕获输出
            result = subprocess.run(
                ['git', 'pull', 'origin', 'main'],
                cwd='./',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            app.logger.info(f'git pull output: {result.stdout}')
            if result.returncode != 0:
                app.logger.error(f'git pull error: {result.stderr}')

            app.logger.info('Successfully updated repository')
            return 'Success', 200
        except Exception as e:
            app.logger.error(f'Error updating repository: {e}')
            return 'Internal Server Error', 500
    else:
        app.logger.warning('Received non-POST request on /webhook')
        return 'Invalid request', 400


if __name__ == '__main__':
    app.logger.info('Starting Flask server')
    app.run(host='0.0.0.0', port=5000)
