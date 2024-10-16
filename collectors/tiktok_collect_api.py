from flask import Flask, request, jsonify
from common.mysql import MySQLDatabase
import threading
from tiktok_collect_by_uc import process_task, get_public_ip, check_account_status
import logging
import os
import socket
import psutil

app = Flask(__name__)

# 初始化logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建控制台处理器并设置日志级别
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 创建格式化器并将其添加到处理器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# 将处理器添加到logger
logger.addHandler(console_handler)

# 全局变量存储worker信息
worker_ip = get_public_ip()
worker_name = socket.gethostname()
novnc_password = os.environ.get('VNC_PASSWORD', 'test123')

# 设置最大并发Chrome进程数
MAX_CONCURRENT_CHROME = 2

def get_chrome_process_count():
    """获取当前运行的Chrome进程数"""
    return len([p for p in psutil.process_iter(['name']) if 'chrome' in p.info['name'].lower()])

def register_worker():
    db = MySQLDatabase()
    db.connect()
    try:
        db.add_or_update_worker(worker_ip, worker_name, status='active', novnc_password=novnc_password)
        logger.info(f"Worker registered: IP {worker_ip}, Name {worker_name}")
    finally:
        db.disconnect()

def update_worker_status(status='active'):
    db = MySQLDatabase()
    db.connect()
    try:
        db.update_worker_status(worker_ip, status)
        logger.info(f"Worker status updated: IP {worker_ip}, Status {status}")
    finally:
        db.disconnect()

@app.before_request
def before_request():
    update_worker_status('active')

@app.route('/trigger_tiktok_task', methods=['POST'])
def trigger_tiktok_task():
    task_id = request.json.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        # 检查当前运行的Chrome进程数
        chrome_count = get_chrome_process_count()
        if chrome_count >= MAX_CONCURRENT_CHROME:
            return jsonify({"error": f"Maximum number of concurrent Chrome processes ({MAX_CONCURRENT_CHROME}) reached for this host"}), 429

        # 检查任务是否存在
        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": f"Task ID {task_id} does not exist"}), 404

        # 更新任务状态为运行中并添加服务器IP
        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, worker_ip)

        # 启动新的线程来处理任务
        task_thread = threading.Thread(target=process_task, args=(task_id, task['keyword'], worker_ip))
        task_thread.start()

        return jsonify({"message": "Task triggered successfully", "task_id": task_id}), 200
    finally:
        db.disconnect()

@app.route('/resume_tiktok_task', methods=['POST'])
def resume_tiktok_task():
    task_id = request.json.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        # 检查当前运行的Chrome进程数
        chrome_count = get_chrome_process_count()
        if chrome_count >= MAX_CONCURRENT_CHROME:
            return jsonify({"error": f"Maximum number of concurrent Chrome processes ({MAX_CONCURRENT_CHROME}) reached for this host"}), 429

        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        if task['status'] != 'paused':
            return jsonify({"error": "Task is not paused"}), 400

        # 更新任务状态为running
        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, worker_ip)

        # 在新线程中处理任务
        task_thread = threading.Thread(target=process_task, args=(task_id, task['keyword'], worker_ip))
        task_thread.start()

        return jsonify({"message": "Task resumed successfully", "task_id": task_id}), 200
    finally:
        db.disconnect()

@app.route('/check_tiktok_account', methods=['POST'])
def check_tiktok_account():
    account_id = request.json.get('account_id')
    if not account_id:
        return jsonify({"error": "Missing account_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        account = db.get_tiktok_account_by_id(account_id)
        if not account:
            return jsonify({"error": "Account not found"}), 404

        # 检查当前运行的Chrome进程数
        chrome_count = get_chrome_process_count()
        if chrome_count >= MAX_CONCURRENT_CHROME:
            return jsonify({"error": f"Maximum number of concurrent Chrome processes ({MAX_CONCURRENT_CHROME}) reached for this host"}), 429

        # 启动新的线程来检查账号状态
        check_thread = threading.Thread(target=check_account_status, args=(account_id, account['username'], account['email']))
        check_thread.start()

        return jsonify({
            "message": "Account status check started",
            "account_id": account_id,
            "username": account['username'],
            "current_status": account['status']
        }), 200
    except Exception as e:
        logger.error(f"Error in check_tiktok_account: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db.disconnect()

if __name__ == '__main__':
    register_worker()
    app.run(host='0.0.0.0', port=5000)
