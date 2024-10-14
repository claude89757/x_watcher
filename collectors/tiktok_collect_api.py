from flask import Flask, request, jsonify
from common.mysql import MySQLDatabase
import threading
import requests
from tiktok_collect_by_uc import process_task, get_public_ip
import logging

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

@app.route('/trigger_tiktok_task', methods=['POST'])
def trigger_tiktok_task():
    task_id = request.json.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id parameter"}), 400

    public_ip = get_public_ip()
    if not public_ip:
        return jsonify({"error": "Failed to get public IP"}), 500

    db = MySQLDatabase()
    db.connect()
    try:
        # Check the number of tasks running on the current host
        running_tasks = db.get_running_tiktok_task_by_ip(public_ip)
        if len(running_tasks) >= 1:
            return jsonify({"error": "Maximum number of concurrent tasks (1) reached for this host"}), 429

        # Check if the task exists
        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": f"Task ID {task_id} does not exist"}), 404

        # Check if the server IP is already in the task
        server_ips = task['server_ips'].split(',') if task['server_ips'] else []
        if public_ip in server_ips:
            return jsonify({"error": f"This server IP {public_ip} is already in task {task_id}"}), 400

        # Update task status to running and add server IP
        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, public_ip)

        # 启动新的线程来处理任务
        task_thread = threading.Thread(target=process_task, args=(task_id, task['keyword'], public_ip))
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

    public_ip = get_public_ip()
    if not public_ip:
        return jsonify({"error": "Failed to get public IP"}), 500

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
        db.update_tiktok_task_server_ip(task_id, public_ip)

        # 在新线程中处理任务
        threading.Thread(target=process_task, args=(task_id, task['keyword'], public_ip)).start()

        return jsonify({"message": "Task resumed successfully", "task_id": task_id}), 200
    finally:
        db.disconnect()

@app.route('/delete_tiktok_task', methods=['POST'])
def delete_tiktok_task():
    task_id = request.json.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id parameter"}), 400

    public_ip = get_public_ip()
    if not public_ip:
        return jsonify({"error": "Failed to get public IP"}), 500

    db = MySQLDatabase()
    db.connect()
    try:
        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        # 检查任务是否在此worker上运行
        if task['status'] == 'running' and public_ip in (task['server_ips'] or '').split(','):
            # 停止正在运行的线程
            if task_id in running_tasks:
                running_tasks[task_id].do_run = False
                running_tasks[task_id].join(timeout=5)  # 等待线程结束，最多等待5秒
                del running_tasks[task_id]

        return jsonify({"message": "Task deleted successfully", "task_id": task_id}), 200
    finally:
        db.disconnect()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
