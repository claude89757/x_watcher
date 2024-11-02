import concurrent.futures
import logging
import os
import psutil
import signal
import socket
import threading
import time
import uuid
import subprocess
import sys
from pathlib import Path
from functools import wraps
from contextlib import contextmanager
import hmac
import hashlib
import json

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify

from common.mysql import MySQLDatabase
from tiktok_collect_by_uc import process_task, get_public_ip, check_account_status, send_promotion_messages
from x_collect import check_x_account_status


# Flask应用初始化
app = Flask(__name__)

# 常量定义
MAX_CONCURRENT_CHROME = 50
PROJECT_PATH = Path(__file__).parent.parent

# 日志配置
def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 文件处理器
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_dir / 'collector_api.log')
    file_handler.setLevel(logging.INFO)
    
    # 统的格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

logger = setup_logger()

# 全局变量
worker_ip = get_public_ip()
worker_name = socket.gethostname()

# 工具函数
def get_chrome_process_count():
    """获取当前运行的Chrome进程数"""
    return len([p for p in psutil.process_iter(['name']) if 'chrome' in p.info['name'].lower()])

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

def pull_and_restart():
    """拉取最新代码并重启服务"""
    try:
        os.chdir(PROJECT_PATH)
        result = subprocess.check_output(['git', 'pull'], stderr=subprocess.STDOUT)
        logger.info(f"Git pull result: {result.decode()}")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        logger.error(f"Error during pull and restart: {str(e)}")
        raise

# Worker管理函数
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

# 任务管理函数
def check_and_execute_tasks():
    """检查并执行待处理的任务"""
    current_chrome_count = get_chrome_process_count()
    if current_chrome_count > 0:
        logger.info(f"当前有 {current_chrome_count} 个Chrome进程正在运行，跳过任务检查")
        return

    db = MySQLDatabase()
    db.connect()
    try:
        pending_tasks = list(db.get_pending_tiktok_tasks() or [])
        running_tasks = list(db.get_running_tiktok_tasks() or [])
        tasks = pending_tasks + running_tasks
        
        if len(tasks) == 0:
            logger.info("没有待处理的任务")
            return
        
        for task in tasks:
            if get_chrome_process_count() >= MAX_CONCURRENT_CHROME:
                break
            
            if task['status'] == 'pending':
                db.update_tiktok_task_status(task['id'], 'running')
                db.update_tiktok_task_server_ip(task['id'], worker_ip)
            
            task_thread = threading.Thread(
                target=process_task, 
                args=(task['id'], task['keyword'], worker_ip)
            )
            task_thread.start()
            
            logger.info(f"自动开始执行任务: {task['id']}")
            time.sleep(60)
    except Exception as e:
        logger.error(f"检查和执行任务时发生错误: {str(e)}")
    finally:
        db.disconnect()

def process_messages_async(user_messages, account_id, batch_size, wait_time, keyword):
    db = MySQLDatabase()
    try:
        db.connect()
        for message in user_messages:
            db.update_tiktok_message_status_and_worker(message['user_id'], 'processing', worker_ip)
        db.disconnect()

        results = send_promotion_messages(user_messages, account_id, batch_size, wait_time, keyword)
        
        db.connect()
        for result in results:
            status = 'sent' if result['success'] else 'failed'
            delivery_method = result.get('action') if result['success'] else None
            db.update_tiktok_message_status(result['user_id'], status, delivery_method=delivery_method)
    except Exception as e:
        logger.error(f"批量发送推广消息时发生错误: {str(e)}")
        db.connect()
        for message in user_messages:
            db.update_tiktok_message_status(message['user_id'], 'failed')
    finally:
        if db.is_connected():
            db.disconnect()

# Flask中间件
@app.before_request
def before_request():
    update_worker_status('active')

# API路由
@app.route('/github-webhook', methods=['POST'])
def github_webhook():
    """GitHub webhook 回调接口"""
    try:
        # 获取原始请求数据
        raw_data = request.get_data()
        if not raw_data:
            return jsonify({'status': 'error', 'message': 'Empty request body'}), 400

        # 根据 Content-Type 处理数据
        content_type = request.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            payload = request.json
        elif 'application/x-www-form-urlencoded' in content_type:
            payload_str = request.form.get('payload', '{}')
            try:
                payload = json.loads(payload_str)
            except json.JSONDecodeError:
                logger.error("无法解析 payload JSON")
                return jsonify({'status': 'error', 'message': 'Invalid JSON payload'}), 400
        else:
            # 尝试直接解析原始数据
            try:
                payload = json.loads(raw_data)
            except json.JSONDecodeError:
                logger.error(f"不支持的 Content-Type: {content_type} 且无法解析数据")
                return jsonify({'status': 'error', 'message': 'Unable to parse request data'}), 400

        # 获取事件类型
        event = request.headers.get('X-GitHub-Event')
        if event != 'push':
            logger.info(f"忽略非push事件: {event}")
            return jsonify({'status': 'ignored', 'message': f'Event {event} is not handled'}), 200

        # 验证分支
        ref = payload.get('ref')
        if ref not in ['refs/heads/main', 'refs/heads/master']:
            logger.info(f"忽略非主分支推送: {ref}")
            return jsonify({'status': 'ignored', 'message': f'Branch {ref} is not monitored'}), 200

        # 处理提交信息
        commits = payload.get('commits', [])
        commit_messages = [commit.get('message', '') for commit in commits]
        logger.info(f"收到GitHub推送: \n分支: {ref}\n提交信息: {commit_messages}")

        # 执行更新和重启
        pull_and_restart()
        
        return jsonify({
            'status': 'success',
            'message': 'Code updated and service restarted',
            'branch': ref,
            'commits': commit_messages
        }), 200

    except Exception as e:
        error_msg = f"处理GitHub webhook时发生错误: {str(e)}"
        logger.error(error_msg, exc_info=True)  # 添加完整的错误堆栈
        return jsonify({'status': 'error', 'message': error_msg}), 500

# TikTok相关API路由
@app.route('/trigger_tiktok_task', methods=['POST'])
def trigger_tiktok_task():
    """触发TikTok采集任务"""
    task_id = request.json.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        chrome_count = get_chrome_process_count()
        if chrome_count >= MAX_CONCURRENT_CHROME:
            return jsonify({
                "error": f"Maximum number of concurrent Chrome processes ({MAX_CONCURRENT_CHROME}) reached"
            }), 429

        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": f"Task ID {task_id} does not exist"}), 404

        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, worker_ip)

        task_thread = threading.Thread(
            target=process_task,
            args=(task_id, task['keyword'], worker_ip)
        )
        task_thread.start()

        return jsonify({
            "message": "Task triggered successfully",
            "task_id": task_id
        }), 200
    finally:
        db.disconnect()

@app.route('/resume_tiktok_task', methods=['POST'])
def resume_tiktok_task():
    """恢复暂停的TikTok任务"""
    task_id = request.json.get('task_id')
    if not task_id:
        return jsonify({"error": "Missing task_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        chrome_count = get_chrome_process_count()
        if chrome_count >= MAX_CONCURRENT_CHROME:
            return jsonify({
                "error": f"Maximum number of concurrent Chrome processes ({MAX_CONCURRENT_CHROME}) reached"
            }), 429

        task = db.get_tiktok_task_by_id(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        if task['status'] != 'paused':
            return jsonify({"error": "Task is not paused"}), 400

        db.update_tiktok_task_status(task_id, 'running')
        db.update_tiktok_task_server_ip(task_id, worker_ip)

        task_thread = threading.Thread(
            target=process_task,
            args=(task_id, task['keyword'], worker_ip)
        )
        task_thread.start()

        return jsonify({
            "message": "Task resumed successfully",
            "task_id": task_id
        }), 200
    finally:
        db.disconnect()

@app.route('/check_tiktok_account', methods=['POST'])
def check_tiktok_account():
    """检查TikTok账号状态"""
    account_id = request.json.get('account_id')
    if not account_id:
        return jsonify({"error": "Missing account_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        account = db.get_tiktok_account_by_id(account_id)
        if not account:
            return jsonify({"error": "Account not found"}), 404

        chrome_count = get_chrome_process_count()
        if chrome_count >= MAX_CONCURRENT_CHROME:
            return jsonify({
                "error": f"Maximum number of concurrent Chrome processes ({MAX_CONCURRENT_CHROME}) reached"
            }), 429

        check_thread = threading.Thread(
            target=check_account_status,
            args=(account_id, account['username'], account['email'])
        )
        check_thread.start()

        return jsonify({
            "message": "Account status check started",
            "account_id": account_id,
            "username": account['username'],
            "current_status": account['status']
        }), 200
    finally:
        db.disconnect()

@app.route('/force_stop_all_tasks', methods=['POST'])
def force_stop_all_tasks():
    """强制停止所有任务"""
    try:
        killed_count = kill_chrome_processes()
        return jsonify({
            "message": f"强制停止了 {killed_count} 个Chrome进程"
        }), 200
    except Exception as e:
        logger.error(f"强制停止任务时发生错误: {str(e)}")
        return jsonify({"error": "内部服务器错误"}), 500

@app.route('/send_promotion_messages', methods=['POST'])
def api_send_promotion_messages():
    """发送推广消息"""
    data = request.json
    keyword = data.get('keyword')
    user_messages = data.get('user_messages')
    account_id = data.get('account_id')
    batch_size = data.get('batch_size', 5)
    wait_time = data.get('wait_time', 60)
    
    if not all([user_messages, account_id]):
        return jsonify({"error": "缺少必要参数"}), 400
    
    chrome_count = get_chrome_process_count()
    if chrome_count >= MAX_CONCURRENT_CHROME:
        return jsonify({
            "error": f"当前主机已达到最大并发Chrome进程数 ({MAX_CONCURRENT_CHROME})"
        }), 429
    
    thread = threading.Thread(
        target=process_messages_async,
        args=(user_messages, account_id, batch_size, wait_time, keyword)
    )
    thread.start()
    
    return jsonify({
        "message": "消息发送任务已启动",
        "worker_ip": worker_ip
    }), 200

# X平台相关API路由
@app.route('/check_x_account', methods=['POST'])
def check_x_account():
    """检查X平台账号状态"""
    account_id = request.json.get('account_id')
    if not account_id:
        return jsonify({"error": "Missing account_id parameter"}), 400

    db = MySQLDatabase()
    db.connect()
    try:
        account = db.get_x_account_by_id(account_id)
        if not account:
            return jsonify({"error": "Account not found"}), 404

        chrome_count = get_chrome_process_count()
        if chrome_count >= MAX_CONCURRENT_CHROME:
            return jsonify({
                "error": f"Maximum number of concurrent Chrome processes ({MAX_CONCURRENT_CHROME}) reached"
            }), 429

        check_thread = threading.Thread(
            target=check_x_account_status,
            args=(account_id, account['username'], account['email'], account['password'])
        )
        check_thread.start()

        return jsonify({
            "message": "Account status check started",
            "account_id": account_id,
            "username": account['username'],
            "current_status": account['status']
        }), 200
    except Exception as e:
        logger.error(f"Error in check_x_account: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db.disconnect()

# 在主程序入口之前添加
def graceful_shutdown(signum, frame):
    """优雅关闭处理"""
    logger.info("收关闭信号，开始清理...")
    
    # # 停止定时任务
    # scheduler.shutdown()
    
    # 结束所有Chrome进程
    kill_chrome_processes()
    
    # 更新worker状态
    update_worker_status('inactive')
    
    logger.info("清理完成，退出程序")
    sys.exit(0)

# 主程序入口
if __name__ == '__main__':
    # 注册信号处理  
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)
    
    register_worker()
    
    # scheduler = BackgroundScheduler()
    # scheduler.add_job(check_and_execute_tasks, 'interval', minutes=1)
    # scheduler.start()
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)
    except (KeyboardInterrupt, SystemExit):
        graceful_shutdown(None, None)

