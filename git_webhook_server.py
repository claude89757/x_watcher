#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/12 02:28
@Author  : claudexie
@File    : git_webhook_server.py
@Software: PyCharm
"""

from flask import Flask, request
import os

app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        os.system('cd /root/x_watcher && git fetch origin main && git reset --hard origin/main')
        return 'Success', 200
    else:
        return 'Invalid request', 400


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
