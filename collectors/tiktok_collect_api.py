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

@app.route('/trigger_tiktok_task', methods=['POST'])
def trigger_tiktok_task():
    keyword = request.json.get('keyword')
    if not keyword:
        return jsonify({"error": "Missing keyword parameter"}), 400

    public_ip = get_public_ip()
    if not public_ip:
        return jsonify({"error": "Failed to get public IP"}), 500

    db = MySQLDatabase()
    db.connect()
    try:
        # 检查当前主机运行的任务数量
        running_tasks = db.get_running_tiktok_task_by_ip(public_ip)
        if len(running_tasks) >= 2:
            return jsonify({"error": "Maximum number of concurrent tasks (2) reached for this host"}), 429

        # 检查是否有正在运行的任务
        running_tasks = db.get_running_tiktok_task_by_keyword(keyword)
        if running_tasks:
            task_id = running_tasks[0]['id']
            logger.info(f"加入现有的TikTok任务: ID {task_id}, 关键词 {keyword}")
        else:
            # 如果没有运行中的任务，创建一个新任务
            task_id = db.create_tiktok_task(keyword)
            logger.info(f"创建了新的TikTok任务: ID {task_id}, 关键词 {keyword}")

        # 更新任务状态和服务器IP
        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, public_ip)

        # 在新线程中处理任务
        threading.Thread(target=process_task, args=(task_id, keyword, public_ip)).start()

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
