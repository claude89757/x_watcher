from flask import Flask, request, jsonify
from common.mysql import MySQLDatabase
import threading
import requests
from tiktok_collect_by_uc import process_task, get_public_ip
import logging
import os
import socket

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

# 全局字典来存储正在运行的任务线程
running_tasks = {}

# 全局变量存储worker信息
worker_ip = get_public_ip()
worker_name = socket.gethostname()
novnc_password = os.environ.get('VNC_PASSWORD', 'test123')

# 设置最大并发任务数
MAX_CONCURRENT_TASKS = 3

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
        # Check the number of tasks running on the current host
        if len(running_tasks) >= MAX_CONCURRENT_TASKS:
            return jsonify({"error": f"Maximum number of concurrent tasks ({MAX_CONCURRENT_TASKS}) reached for this host"}), 429

        # Check if the task exists
        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": f"Task ID {task_id} does not exist"}), 404

        # Check if the task is already running on this worker
        if task_id in running_tasks:
            return jsonify({"error": f"Task ID {task_id} is already running on this worker"}), 400

        # Update task status to running and add server IP
        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, worker_ip)

        # Update worker status and task
        current_tasks = db.get_worker_current_tasks(worker_ip)
        if task_id not in current_tasks:
            current_tasks.append(task_id)
            db.update_worker_task(worker_ip, current_tasks)

        # 启动新的线程来处理任务
        task_thread = threading.Thread(target=process_task, args=(task_id, task['keyword'], worker_ip))
        task_thread.start()
        running_tasks[task_id] = task_thread

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
        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        if task['status'] != 'paused':
            return jsonify({"error": "Task is not paused"}), 400

        # 更新任务状态为running
        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, worker_ip)

        # Update worker task list
        current_tasks = db.get_worker_current_tasks(worker_ip) or []
        if task_id not in current_tasks:
            current_tasks.append(task_id)
            db.update_worker_task(worker_ip, ','.join(map(str, current_tasks)))

        # 在新线程中处理任务
        task_thread = threading.Thread(target=process_task, args=(task_id, task['keyword'], worker_ip))
        task_thread.start()
        running_tasks[task_id] = task_thread

        return jsonify({"message": "Task resumed successfully", "task_id": task_id}), 200
    finally:
        db.disconnect()

@app.route('/delete_tiktok_task', methods=['POST'])
def delete_tiktok_task():
    task_id = request.json.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        # 检查任务是否在此worker上运行
        if task_id in running_tasks:
            # 停止正在运行的线程
            running_tasks[task_id].do_run = False
            running_tasks[task_id].join(timeout=5)  # 等待线程结束，最多等待5秒
            del running_tasks[task_id]

            # Update worker task list
            current_tasks = db.get_worker_current_tasks(worker_ip) or []
            if task_id in current_tasks:
                current_tasks.remove(task_id)
                db.update_worker_task(worker_ip, ','.join(map(str, current_tasks)))

        return jsonify({"message": "Task deleted successfully", "task_id": task_id}), 200
    finally:
        db.disconnect()

if __name__ == '__main__':
    register_worker()
    app.run(host='0.0.0.0', port=5000)
