from flask import Flask, request, jsonify
from common.mysql import MySQLDatabase
import threading
from tiktok_collect_by_uc import process_task, get_public_ip, check_account_status, send_promotion_messages
import logging
import os
import socket
import psutil
import signal
import time
from apscheduler.schedulers.background import BackgroundScheduler
import concurrent.futures

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

# 设置最大并发Chrome进程数
MAX_CONCURRENT_CHROME = 30

def get_chrome_process_count():
    """获取当前运行的Chrome进程数"""
    return len([p for p in psutil.process_iter(['name']) if 'chrome' in p.info['name'].lower()])

def register_worker():
    db = MySQLDatabase()
    db.connect()
    try:
        db.add_or_update_worker(worker_ip, worker_name, status='active')
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

def kill_chrome_processes():
    """强制终止所有Chrome进程"""
    killed_count = 0
    for proc in psutil.process_iter(['name']):
        if 'chrome' in proc.info['name'].lower():
            try:
                proc.send_signal(signal.SIGTERM)
                killed_count += 1
            except psutil.NoSuchProcess:
                pass
    return killed_count

@app.route('/force_stop_all_tasks', methods=['POST'])
def force_stop_all_tasks():
    try:
        # 终止所有Chrome进程
        killed_count = kill_chrome_processes()
        return jsonify({
            "message": f"强制停止了 {killed_count} 个Chrome进程"
        }), 200
    except Exception as e:
        logger.error(f"强制停止任务时发生错误: {str(e)}")
        return jsonify({"error": "内部服务器错误"}), 500

def check_and_execute_tasks():
    """检查并执行待处理的任务"""
    logger.info("开始检查并执行待处理的任务...")
    if get_chrome_process_count() == 0:
        db = MySQLDatabase()
        db.connect()
        try:
            # 获取待处理的任务
            pending_tasks = db.get_pending_tiktok_tasks()
            # 获取正在运行的任务
            running_tasks = db.get_running_tiktok_tasks()
            
            # 确保 pending_tasks 和 running_tasks 都是列表
            pending_tasks = list(pending_tasks) if pending_tasks else []
            running_tasks = list(running_tasks) if running_tasks else []
            
            # 合并待处理和正在运行的任务
            tasks = pending_tasks + running_tasks
            
            for task in tasks:
                if get_chrome_process_count() >= MAX_CONCURRENT_CHROME:
                    break
                
                # 如果任务状态为待处理，则更新为运行中
                if task['status'] == 'pending':
                    db.update_tiktok_task_status(task['id'], 'running')
                    db.update_tiktok_task_server_ip(task['id'], worker_ip)
                
                task_thread = threading.Thread(target=process_task, args=(task['id'], task['keyword'], worker_ip))
                task_thread.start()
                
                logger.info(f"自动开始执行任务: {task['id']}")
                time.sleep(60)  # 等待60秒后再处理下一个任务
        except Exception as e:
            logger.error(f"检查和执行任务时发生错误: {str(e)}")
        finally:
            db.disconnect()
    else:
        logger.info("当前有Chrome进程正在运行，跳过任务检查")

@app.route('/send_promotion_messages', methods=['POST'])
def api_send_promotion_messages():
    data = request.json
    user_ids = data.get('user_ids')
    message = data.get('message')
    account_id = data.get('account_id')
    keyword = data.get('keyword')
    batch_size = data.get('batch_size', 5)
    wait_time = data.get('wait_time', 60)
    
    if not all([user_ids, message, account_id, keyword]):
        return jsonify({"error": "缺少必要参数"}), 400
    
    db = MySQLDatabase()
    db.connect()
    
    try:
        results = send_promotion_messages(user_ids, message, account_id, batch_size, wait_time)
        
        for result in results:
            if result['success']:
                db.update_tiktok_message_status(keyword, result['user_id'], 'sent')
            else:
                db.update_tiktok_message_status(keyword, result['user_id'], 'failed')
        
        return jsonify({"results": results}), 200
    except Exception as e:
        logger.error(f"批量发送推广消息时发生错误: {str(e)}")
        return jsonify({"error": "发送消息失败"}), 500
    finally:
        db.disconnect()

if __name__ == '__main__':
    register_worker()
    
    # 创建并启动定时任务
    scheduler = BackgroundScheduler()
    scheduler.add_job(check_and_execute_tasks, 'interval', minutes=1)
    scheduler.start()
    
    try:
        app.run(host='0.0.0.0', port=5000)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
