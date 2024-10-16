#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024年10月13日
@Author  : claude by cursor
@File    : mysql.py
@Software: PyCharm
@Description: MySQL数据库操作的通用模块，使用pymysql库
"""

import os
import pymysql
import logging
from pymysql.converters import escape_string
import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MySQLDatabase:
    def __init__(self):
        self.host = os.environ['MYSQL_HOST']
        self.port = int(os.environ.get('MYSQL_PORT', 29838))  # 添加端口配置，默认为29838
        self.user = os.environ['MYSQL_USER']
        self.password = os.environ['MYSQL_PASSWORD']
        self.database = os.environ['MYSQL_DATABASE']
        if not all([self.host, self.user, self.password, self.database]):
            raise ValueError("缺少必要的MySQL连接环境变量配置")
        self.connection = None

    def log_sql(self, query, params=None):
        """记录 SQL 查询"""
        if isinstance(params, str):  # 处理批量插入的情况
            formatted_query = f"{query} {params}"
        elif params:
            # 使用 pymysql.converters.escape_string 来正确转义参数值
            escaped_params = tuple(escape_string(str(p)) for p in params)
            # 使用 SQL 的格式化方法来插入参数
            formatted_query = query % escaped_params
        else:
            formatted_query = query
        
        logger.info(f"执行 SQL: {formatted_query}")

    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,  # 添加端口参数
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info(f"成功连接到MySQL数据库，地址：{self.host}:{self.port}")
        except pymysql.Error as e:
            logger.error(f"连接数据库时出错: {e}")

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")

    def execute_query(self, query, params=None):
        """执行查询操作"""
        self.log_sql(query, params)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                result = cursor.fetchall()
            return result
        except pymysql.Error as e:
            logger.error(f"执行查询时出错: {e}")
            return None

    def execute_update(self, query, params=None):
        """执行更新操作（插入、更新、删除）"""
        self.log_sql(query, params)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
            self.connection.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            logger.error(f"执行更新时出错: {e}")
            self.connection.rollback()
            return -1

    def insert_many(self, query, data):
        """批量插入数据"""
        self.log_sql(query, f"(批量插入 {len(data)} 条记录)")
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, data)
            self.connection.commit()
            return cursor.rowcount
        except pymysql.Error as e:
            logger.error(f"批量插入数据时出错: {e}")
            self.connection.rollback()
            return -1

    def initialize_tables(self):
        """初始化并创建所需的表"""
        create_tables_queries = [
            """
            CREATE TABLE IF NOT EXISTS tiktok_tasks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                keyword VARCHAR(255) NOT NULL,
                status ENUM('pending', 'running', 'completed', 'failed', 'paused') DEFAULT 'pending',
                max_videos INT DEFAULT 100,
                max_comments_per_video INT DEFAULT 1000,
                start_time TIMESTAMP NULL,
                end_time TIMESTAMP NULL,
                retry_count INT DEFAULT 0,
                server_ips TEXT,
                total_videos_processed INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tiktok_videos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_id INT,
                video_url VARCHAR(255) NOT NULL,
                keyword VARCHAR(255),
                status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
                processing_server_ip VARCHAR(45),
                author VARCHAR(255),
                description TEXT,
                likes_count INT,
                comments_count INT,
                shares_count INT,
                views_count INT,
                duration FLOAT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tiktok_tasks(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tiktok_comments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_id INT,
                keyword VARCHAR(255),
                user_id VARCHAR(255),
                reply_content TEXT,
                reply_time VARCHAR(255),
                likes_count INT,
                is_pinned BOOLEAN DEFAULT FALSE,
                parent_comment_id INT NULL,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                collected_by VARCHAR(255),
                video_url VARCHAR(255),
                FOREIGN KEY (video_id) REFERENCES tiktok_videos(id),
                FOREIGN KEY (parent_comment_id) REFERENCES tiktok_comments(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tiktok_task_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_id INT,
                log_level ENUM('INFO', 'WARNING', 'ERROR', 'DEBUG') DEFAULT 'INFO',
                log_type ENUM('info', 'warning', 'error'),
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tiktok_tasks(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS worker_infos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                worker_ip VARCHAR(45) NOT NULL,
                worker_name VARCHAR(255),
                status ENUM('active', 'inactive', 'busy') DEFAULT 'inactive',
                last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                novnc_password VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_worker_ip (worker_ip)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tiktok_accounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                password VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                status ENUM('active', 'inactive') DEFAULT 'active',
                login_ips TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        ]

        for query in create_tables_queries:
            self.execute_update(query)
        
        # 添加唯一索引
        add_index_query = """
        ALTER TABLE tiktok_comments
        ADD UNIQUE INDEX idx_user_content (user_id, reply_content(255))
        """
        self.execute_update(add_index_query)
        
        logger.info("所有必要的表和索引已创建或已存在")

        create_filtered_comments_table = """
        CREATE TABLE IF NOT EXISTS tiktok_filtered_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_id INT,
            keyword VARCHAR(255),
            user_id VARCHAR(255),
            reply_content TEXT,
            reply_time VARCHAR(255),
            likes_count INT,
            is_pinned BOOLEAN DEFAULT FALSE,
            parent_comment_id INT NULL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            collected_by VARCHAR(255),
            video_url VARCHAR(255),
            FOREIGN KEY (video_id) REFERENCES tiktok_videos(id),
            FOREIGN KEY (parent_comment_id) REFERENCES tiktok_filtered_comments(id)
        )
        """
        self.execute_update(create_filtered_comments_table)

        create_analyzed_comments_table = """
        CREATE TABLE IF NOT EXISTS tiktok_analyzed_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword VARCHAR(255),
            user_id VARCHAR(255),
            reply_content TEXT,
            classification VARCHAR(50),
            analysis_reason TEXT,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_comment (keyword, user_id, reply_content(255))
        )
        """
        self.execute_update(create_analyzed_comments_table)

        create_second_round_analyzed_comments_table = """
        CREATE TABLE IF NOT EXISTS tiktok_second_round_analyzed_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword VARCHAR(255),
            user_id VARCHAR(255),
            reply_content TEXT,
            first_round_classification VARCHAR(50),
            second_round_classification VARCHAR(50),
            analysis_reason TEXT,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_comment (keyword, user_id, reply_content(255))
        )
        """
        self.execute_update(create_second_round_analyzed_comments_table)

    def create_tiktok_task(self, keyword):
        """创建TikTok任务,如果已存在相同关键字的待处理任务则返回该任务ID"""
        # 首先检查是否存在相同关键字的待处理任务
        check_query = f"""
        SELECT id FROM tiktok_tasks 
        WHERE keyword = '{keyword}' AND status = 'pending'
        LIMIT 1
        """
        existing_task = self.execute_query(check_query)
        
        if existing_task:
            existing_task_id = existing_task[0]['id']
            logger.info(f"已存在关键字为 '{keyword}' 的待处理任务,任务ID: {existing_task_id}")
            return existing_task_id
        
        # 如果不存在,则创建新任务
        insert_query = f"INSERT INTO tiktok_tasks (keyword) VALUES ('{keyword}')"
        result = self.execute_update(insert_query)
        
        if result > 0:
            new_task_id = self.execute_query("SELECT LAST_INSERT_ID() as id")[0]['id']
            logger.info(f"成功创建关键字为 '{keyword}' 的新任务,任务ID: {new_task_id}")
            return new_task_id
        else:
            logger.error(f"创建关键字为 '{keyword}' 的任务失败")
            return None

    def update_tiktok_task_status(self, task_id, status):
        """更新TikTok任务状态"""
        valid_statuses = ['pending', 'running', 'completed', 'failed', 'paused']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {', '.join(valid_statuses)}")
        
        query = """
        UPDATE tiktok_tasks 
        SET status = %s, 
            start_time = CASE WHEN %s = 'running' AND start_time IS NULL THEN NOW() ELSE start_time END
        WHERE id = %s
        """
        return self.execute_update(query, (status, status, task_id))

    def delete_tiktok_task(self, task_id):
        """删除TikTok任务及相关数据"""
        queries = [
            f"DELETE FROM tiktok_task_logs WHERE task_id = {task_id}",
            f"DELETE FROM tiktok_comments WHERE video_id IN (SELECT id FROM tiktok_videos WHERE task_id = {task_id})",
            f"DELETE FROM tiktok_videos WHERE task_id = {task_id}",
            f"DELETE FROM tiktok_tasks WHERE id = {task_id}"
        ]
        try:
            for query in queries:
                self.execute_update(query)
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"删除任务时出错: {e}")
            self.connection.rollback()
            return False

    def get_tiktok_task_status(self, task_id):
        """获取TikTok任务状态"""
        query = f"SELECT status FROM tiktok_tasks WHERE id = {task_id}"
        result = self.execute_query(query)
        return result[0]['status'] if result else None

    def add_tiktok_video(self, task_id, video_url, keyword):
        """添加TikTok视频"""
        query = f"INSERT INTO tiktok_videos (task_id, video_url, keyword) VALUES ({task_id}, '{video_url}', '{keyword}')"
        return self.execute_update(query)

    def add_tiktok_comment(self, video_id, user_id, reply_content, reply_time, keyword, collected_by, video_url):
        """添加TikTok评论，忽略重复的user_id和reply_content组合"""
        query = """
        INSERT IGNORE INTO tiktok_comments 
        (video_id, user_id, reply_content, reply_time, keyword, collected_by, video_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (video_id, user_id, reply_content, reply_time, keyword, collected_by, video_url)
        return self.execute_update(query, params)

    def add_tiktok_task_log(self, task_id, log_type, message):
        """添加TikTok任务日志"""
        query = f"INSERT INTO tiktok_task_logs (task_id, log_type, message) VALUES ({task_id}, '{log_type}', '{message}')"
        return self.execute_update(query)

    def get_pending_tiktok_tasks(self):
        """获取待处理的TikTok任务"""
        query = "SELECT * FROM tiktok_tasks WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1"
        return self.execute_query(query)

    def get_tiktok_task_videos(self, task_id):
        """获取TikTok任务相关的视频"""
        query = f"SELECT * FROM tiktok_videos WHERE task_id = {task_id}"
        return self.execute_query(query)

    def get_tiktok_video_comments(self, video_id):
        """获取TikTok视频的评论"""
        query = f"SELECT * FROM tiktok_comments WHERE video_id = {video_id}"
        return self.execute_query(query)

    def update_tiktok_task_details(self, task_id, **kwargs):
        """更新TikTok任务的详细信息"""
        allowed_fields = ['status', 'max_videos', 'max_comments_per_video', 'start_time', 'end_time', 'retry_count']
        update_fields = []

        for key, value in kwargs.items():
            if key in allowed_fields:
                if isinstance(value, (str, datetime.datetime, datetime.date)):
                    update_fields.append(f"{key} = '{value}'")
                else:
                    update_fields.append(f"{key} = {value}")

        if not update_fields:
            logger.warning("没有提供有效的更新字")
            return 0

        query = f"UPDATE tiktok_tasks SET {', '.join(update_fields)} WHERE id = {task_id}"
        return self.execute_update(query)

    def update_tiktok_task_server_ip(self, task_id, server_ip):
        """更新TikTok任务的服务器IP列表"""
        query = f"""
        UPDATE tiktok_tasks 
        SET server_ips = 
            CASE 
                WHEN server_ips IS NULL OR server_ips = '' THEN '{server_ip}'
                WHEN FIND_IN_SET('{server_ip}', server_ips) > 0 THEN server_ips
                ELSE CONCAT(server_ips, ',{server_ip}')
            END
        WHERE id = {task_id}
        """
        return self.execute_update(query)

    def get_running_tiktok_task_by_ip(self, server_ip):
        """获取指定IP上正在运行的TikTok任务"""
        query = f"""
        SELECT * FROM tiktok_tasks 
        WHERE status = 'running' AND FIND_IN_SET('{server_ip}', server_ips)
        """
        return self.execute_query(query)

    def get_pending_tiktok_task_by_keyword(self, keyword):
        """获取指定关键词的待处TikTok任务"""
        query = f"""
        SELECT * FROM tiktok_tasks 
        WHERE status = 'pending' AND keyword = '{keyword}' 
        ORDER BY created_at ASC LIMIT 1
        """
        return self.execute_query(query)

    def get_next_pending_video(self, task_id, server_ip):
        """获取下一个待处理的视频，优先返回正在处理中的本机视频"""
        try:
            with self.connection.cursor() as cursor:
                # 步骤1：查找正在处理中且由本机IP处理的视频
                processing_query = f"""
                SELECT id, video_url, 'processing' as status FROM tiktok_videos
                WHERE task_id = {task_id} AND status = 'processing' AND processing_server_ip = '{server_ip}'
                LIMIT 1
                """
                cursor.execute(processing_query)
                processing_result = cursor.fetchone()

                if processing_result:
                    logger.info(f"找到正在理中的本机视频：ID {processing_result['id']}, URL {processing_result['video_url']}")
                    return processing_result

                # 步骤2：如果没有正在处理的本机视频，则查找新的待处理视频
                select_query = f"""
                SELECT id, video_url FROM tiktok_videos
                WHERE task_id = {task_id} AND status = 'pending'
                LIMIT 1
                FOR UPDATE
                """
                cursor.execute(select_query)
                result = cursor.fetchone()

                if result:
                    video_id, video_url = result['id'], result['video_url']
                    
                    # 更新视频状态
                    update_query = f"""
                    UPDATE tiktok_videos
                    SET status = 'processing', processing_server_ip = '{server_ip}'
                    WHERE id = {video_id}
                    """
                    cursor.execute(update_query)
                    self.connection.commit()

                    logger.info(f"成功更新新的待处理视频状态：ID {video_id}, URL {video_url}")
                    return {'id': video_id, 'video_url': video_url, 'status': 'processing'}
                else:
                    logger.info(f"任务 {task_id} 没有更多待理的视频")
                    return None

        except pymysql.Error as e:
            logger.error(f"获取下一个待处理视频时出错: {e}")
            self.connection.rollback()
            return None

    def update_task_progress(self, task_id, videos_processed):
        """更新任务进度"""
        query = f"""
        UPDATE tiktok_tasks
        SET total_videos_processed = total_videos_processed + {videos_processed}
        WHERE id = {task_id}
        """
        return self.execute_update(query)

    def mark_video_completed(self, video_id):
        """标记视频为已完成"""
        query = f"UPDATE tiktok_videos SET status = 'completed' WHERE id = {video_id}"
        return self.execute_update(query)

    def get_tiktok_videos_for_task(self, task_id, limit=100):
        """获取指定任务的待处理视频列表"""
        query = f"""
        SELECT id, video_url FROM tiktok_videos
        WHERE task_id = {task_id} AND status = 'pending'
        LIMIT {limit}
        """
        return self.execute_query(query)

    def add_tiktok_videos_batch(self, task_id, video_urls, keyword):
        """批量添加TikTok视频到任务，忽略重复的视频链接"""
        query = """
        INSERT IGNORE INTO tiktok_videos (task_id, video_url, keyword) 
        VALUES (%s, %s, %s)
        """
        data = [(task_id, url, keyword) for url in video_urls]
        
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, data)
            self.connection.commit()
            affected_rows = cursor.rowcount
            logger.info(f"成功插入 {affected_rows} 条新视频记录，忽略了 {len(data) - affected_rows} 条重复记录")
            return affected_rows
        except pymysql.Error as e:
            logger.error(f"批量插入视频数据时出错: {e}")
            self.connection.rollback()
            return -1

    def get_and_lock_pending_task(self, server_ip):
        """获取并锁定一个待处理的任务"""
        query = f"""
        UPDATE tiktok_tasks
        SET status = 'running', server_ips = '{server_ip}'
        WHERE id = (
            SELECT id FROM (
                SELECT id FROM tiktok_tasks
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            ) AS subquery
        )
        """
        self.log_sql(query)
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            if cursor.rowcount > 0:
                cursor.execute("SELECT id, keyword FROM tiktok_tasks WHERE id = LAST_INSERT_ID()")
                return cursor.fetchone()
        return None

    def get_running_tiktok_task_by_keyword(self, keyword):
        """获取指定关键词的正在运行的TikTok任务"""
        query = f"""
        SELECT * FROM tiktok_tasks 
        WHERE status = 'running' AND keyword = '{keyword}'
        """
        return self.execute_query(query)

    def get_all_tiktok_tasks(self):
        """获取所有TikTok任务"""
        query = "SELECT * FROM tiktok_tasks ORDER BY created_at DESC"
        return self.execute_query(query)

    def get_tiktok_comments_by_task(self, task_id):
        """获取指定任务的TikTok评论"""
        query = f"""
        SELECT c.* FROM tiktok_comments c
        JOIN tiktok_videos v ON c.video_id = v.id
        WHERE v.task_id = {task_id}
        LIMIT 100
        """
        return self.execute_query(query)

    def get_tiktok_task_logs(self, task_id):
        """获取指定任务的日志"""
        query = f"SELECT * FROM tiktok_task_logs WHERE task_id = {task_id} ORDER BY created_at DESC"
        return self.execute_query(query)

    def get_tiktok_tasks_by_keyword(self, keyword):
        """获取指定关键词的TikTok任务"""
        query = "SELECT * FROM tiktok_tasks WHERE keyword LIKE %s ORDER BY created_at DESC"
        params = (f"%{keyword}%",)
        return self.execute_query(query, params)

    def get_tiktok_comments_by_keyword(self, keyword):
        """获取指定关键词的TikTok评论"""
        query = """
        SELECT c.* FROM tiktok_comments c
        JOIN tiktok_videos v ON c.video_id = v.id
        JOIN tiktok_tasks t ON v.task_id = t.id
        WHERE t.keyword LIKE %s
        LIMIT 99999
        """
        params = (f"%{keyword}%",)
        return self.execute_query(query, params)

    def get_tiktok_task_logs_by_keyword(self, keyword):
        """获取指定关键词的任务日志"""
        query = """
        SELECT l.* FROM tiktok_task_logs l
        JOIN tiktok_tasks t ON l.task_id = t.id
        WHERE t.keyword LIKE %s
        ORDER BY l.created_at DESC
        LIMIT 99999
        """
        params = (f"%{keyword}%",)
        return self.execute_query(query, params)

    def get_tiktok_collection_stats(self):
        """获取TikTok收集统计信息"""
        stats = {}
        
        # 获取已收集的关键字数量
        query = "SELECT COUNT(DISTINCT keyword) as keyword_count FROM tiktok_tasks"
        result = self.execute_query(query)
        stats['keyword_count'] = result[0]['keyword_count'] if result else 0
        
        # 获取已收集的评论数量
        query = "SELECT COUNT(*) as comment_count FROM tiktok_comments"
        result = self.execute_query(query)
        stats['comment_count'] = result[0]['comment_count'] if result else 0
        
        return stats

    def get_earliest_running_task_start_time(self):
        """获取最早的运行中任务的开时间"""
        query = """
        SELECT MIN(start_time) as earliest_start_time
        FROM tiktok_tasks
        WHERE status = 'running'
        """
        result = self.execute_query(query)
        if result and result[0]['earliest_start_time']:
            return result[0]['earliest_start_time']
        return None

    def get_total_videos_for_keyword(self, keyword):
        """获取指定关键词的总视频数"""
        query = """
        SELECT COUNT(*) as total_videos
        FROM tiktok_videos v
        JOIN tiktok_tasks t ON v.task_id = t.id
        WHERE t.keyword LIKE %s
        """
        params = (f"%{keyword}%",)
        result = self.execute_query(query, params)
        return result[0]['total_videos'] if result else 0

    def get_processed_videos_for_keyword(self, keyword):
        """获取指定关键词的已处理视频数"""
        query = """
        SELECT COUNT(*) as processed_videos
        FROM tiktok_videos v
        JOIN tiktok_tasks t ON v.task_id = t.id
        WHERE t.keyword LIKE %s AND v.status IN ('completed', 'failed')
        """
        params = (f"%{keyword}%",)
        result = self.execute_query(query, params)
        return result[0]['processed_videos'] if result else 0

    def get_tiktok_task_by_id(self, task_id):
        """获取指定ID的TikTok任务"""
        query = f"SELECT * FROM tiktok_tasks WHERE id = {task_id}"
        result = self.execute_query(query)
        return result[0] if result else None

    def get_existing_user_ids_for_keyword(self, keyword):
        query = """
        SELECT DISTINCT user_id
        FROM tiktok_comments
        WHERE keyword = %s
        """
        results = self.execute_query(query, (keyword,))
        return [result['user_id'] for result in results]

    def get_running_tiktok_tasks(self):
        """获取所有的运行中TikTok任务"""
        query = "SELECT * FROM tiktok_tasks WHERE status = 'running'"
        return self.execute_query(query)

    def add_or_update_worker(self, worker_ip, worker_name=None, status='inactive'):
        """添加或更新 worker 信息"""
        query = """
        INSERT INTO worker_infos (worker_ip, worker_name, status, novnc_password, last_heartbeat)
        VALUES (%s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
        worker_name = VALUES(worker_name),
        status = VALUES(status),
        last_heartbeat = NOW()
        """
        params = (worker_ip, worker_name, status)
        return self.execute_update(query, params)

    def get_worker_list(self):
        """获取所有 worker 的列表"""
        query = "SELECT * FROM worker_infos ORDER BY last_heartbeat DESC"
        return self.execute_query(query)

    def update_worker_status(self, worker_ip, status):
        """更新 worker 的状态"""
        query = "UPDATE worker_infos SET status = %s, last_heartbeat = NOW() WHERE worker_ip = %s"
        params = (status, worker_ip)
        return self.execute_update(query, params)

    def get_available_workers(self):
        """获取可用的 workers（状态为 active）"""
        query = """
        SELECT * FROM worker_infos 
        WHERE status = 'active'
        ORDER BY last_heartbeat DESC
        """
        return self.execute_query(query)

    def remove_inactive_workers(self, inactive_threshold_minutes=10):
        """移除长时间未活动的 workers"""
        query = f"""
        DELETE FROM worker_infos
        WHERE last_heartbeat < NOW() - INTERVAL {inactive_threshold_minutes} MINUTE
        """
        return self.execute_update(query)

    def update_worker_novnc_password(self, worker_ip, novnc_password):
        """更新 worker 的 noVNC 密码"""
        query = "UPDATE worker_infos SET novnc_password = %s WHERE worker_ip = %s"
        params = (novnc_password, worker_ip)
        return self.execute_update(query, params)

    def update_tiktok_video_status(self, video_id, status):
        """更新TikTok视频的状态"""
        valid_statuses = ['pending', 'processing', 'completed', 'failed']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {', '.join(valid_statuses)}")
        
        query = """
        UPDATE tiktok_videos 
        SET status = %s,
            processing_server_ip = CASE 
                WHEN %s = 'processing' THEN processing_server_ip 
                ELSE NULL 
            END
        WHERE id = %s
        """
        params = (status, status, video_id)
        return self.execute_update(query, params)

    def add_tiktok_account(self, username, password, email, login_ips):
        """添加新的TikTok账号"""
        query = """
        INSERT INTO tiktok_accounts (username, password, email, login_ips)
        VALUES (%s, %s, %s, %s)
        """
        return self.execute_update(query, (username, password, email, ','.join(login_ips)))

    def get_tiktok_accounts(self):
        """获取所有TikTok账号"""
        query = "SELECT * FROM tiktok_accounts"
        return self.execute_query(query)

    def update_tiktok_account_status(self, account_id, status):
        """更新TikTok账号状态"""
        query = "UPDATE tiktok_accounts SET status = %s WHERE id = %s"
        return self.execute_update(query, (status, account_id))

    def delete_tiktok_account(self, account_id):
        """删除TikTok账号"""
        query = "DELETE FROM tiktok_accounts WHERE id = %s"
        return self.execute_update(query, (account_id,))

    def update_tiktok_account_login_ips(self, account_id, login_ips):
        """更新TikTok账号的登录主机IP"""
        query = "UPDATE tiktok_accounts SET login_ips = %s WHERE id = %s"
        return self.execute_update(query, (','.join(login_ips), account_id))

    def get_tiktok_account_by_id(self, account_id):
        """获取指定ID的TikTok账号"""
        query = "SELECT * FROM tiktok_accounts WHERE id = %s"
        result = self.execute_query(query, (account_id,))
        return result[0] if result else None

    def update_tiktok_account_status(self, account_id, status):
        """更新TikTok账号状态"""
        query = "UPDATE tiktok_accounts SET status = %s WHERE id = %s"
        return self.execute_update(query, (status, account_id))

    def get_worker_by_ip(self, worker_ip):
        """获取指定IP的worker信息"""
        query = "SELECT * FROM worker_infos WHERE worker_ip = %s"
        result = self.execute_query(query, (worker_ip,))
        return result[0] if result else None

    def get_all_tiktok_keywords(self):
        """获取TikTok关键字"""
        query = "SELECT DISTINCT keyword FROM tiktok_tasks"
        results = self.execute_query(query)
        return [result['keyword'] for result in results]

    def save_filtered_comments(self, filtered_comments):
        """保存过滤后的评论到新表"""
        query = """
        INSERT INTO tiktok_filtered_comments 
        (video_id, keyword, user_id, reply_content, reply_time, likes_count, is_pinned, 
        parent_comment_id, collected_at, collected_by, video_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = [(
            comment.get('video_id'),
            comment['keyword'],
            comment['user_id'],
            comment['reply_content'],
            comment['reply_time'],
            comment.get('likes_count', 0),
            comment.get('is_pinned', False),
            comment.get('parent_comment_id'),
            comment['collected_at'],
            comment['collected_by'],
            comment['video_url']
        ) for comment in filtered_comments]
        
        return self.insert_many(query, values)

    def get_filtered_tiktok_comments_by_keyword(self, keyword, limit=1000):
        query = """
        SELECT * FROM tiktok_filtered_comments
        WHERE keyword = %s
        LIMIT %s
        """
        return self.execute_query(query, (keyword, limit))

    def save_analyzed_comments(self, keyword, analyzed_data):
        """保存分析后的评论数据"""
        query = """
        INSERT INTO tiktok_analyzed_comments 
        (keyword, user_id, reply_content, classification, analysis_reason)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        classification = VALUES(classification),
        analysis_reason = VALUES(analysis_reason),
        analyzed_at = CURRENT_TIMESTAMP
        """
        values = [
            (keyword, row['用户ID'], row['评论内容'], row['分类结果'], row['分析理由'])
            for _, row in analyzed_data.iterrows()
        ]
        return self.insert_many(query, values)

    def get_analyzed_comments(self, keyword, limit=1000):
        """获取指定关键词的分析后评论数据"""
        query = """
        SELECT * FROM tiktok_analyzed_comments
        WHERE keyword = %s
        ORDER BY analyzed_at DESC
        LIMIT %s
        """
        return self.execute_query(query, (keyword, limit))

    def get_filtered_tiktok_comments_count(self, keyword):
        """获取指定关键词的已过滤评论数量"""
        query = """
        SELECT COUNT(*) as count
        FROM tiktok_filtered_comments
        WHERE keyword = %s
        """
        result = self.execute_query(query, (keyword,))
        return result[0]['count'] if result else 0

    def save_second_round_analyzed_comments(self, keyword, analyzed_data):
        """保存第二轮分析后的评论数据"""
        query = """
        INSERT INTO tiktok_second_round_analyzed_comments 
        (keyword, user_id, reply_content, first_round_classification, second_round_classification, analysis_reason)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        second_round_classification = VALUES(second_round_classification),
        analysis_reason = VALUES(analysis_reason),
        analyzed_at = CURRENT_TIMESTAMP
        """
        values = [
            (keyword, row['用户ID'], row['评论内容'], row['第一轮分类结果'], row['第二轮分类结果'], row['分析理由'])
            for _, row in analyzed_data.iterrows()
        ]
        return self.insert_many(query, values)

    def get_second_round_analyzed_comments(self, keyword, limit=1000):
        """获取指定关键词的第二轮分析后评论数据"""
        query = """
        SELECT * FROM tiktok_second_round_analyzed_comments
        WHERE keyword = %s
        ORDER BY analyzed_at DESC
        LIMIT %s
        """
        return self.execute_query(query, (keyword, limit))

    def get_potential_customers_count(self, keyword):
        """获取指定关键词的潜在客户数量"""
        query = """
        SELECT COUNT(*) as count
        FROM tiktok_analyzed_comments
        WHERE keyword = %s AND classification = '潜在客户'
        """
        result = self.execute_query(query, (keyword,))
        return result[0]['count'] if result else 0

    def get_potential_customers(self, keyword, limit=1000):
        """获取指定关键词的潜在客户评论数据"""
        query = """
        SELECT * FROM tiktok_analyzed_comments
        WHERE keyword = %s AND classification = '潜在客户'
        ORDER BY analyzed_at DESC
        LIMIT %s
        """
        return self.execute_query(query, (keyword, limit))

    def get_global_stats(self):
        """获取全局统计数据"""
        stats = {}

        # 获取已收集的关键字数量
        query = "SELECT COUNT(DISTINCT keyword) as keyword_count FROM tiktok_tasks"
        result = self.execute_query(query)
        stats['keyword_count'] = result[0]['keyword_count'] if result else 0

        # 获取已收集的评论总数
        query = "SELECT COUNT(*) as comment_count FROM tiktok_comments"
        result = self.execute_query(query)
        stats['comment_count'] = result[0]['comment_count'] if result else 0

        # 获取潜在客户数量
        query = "SELECT COUNT(*) as potential_customer_count FROM tiktok_analyzed_comments WHERE classification = '潜在客户'"
        result = self.execute_query(query)
        stats['potential_customer_count'] = result[0]['potential_customer_count'] if result else 0

        # 获取高意向客户数量
        query = "SELECT COUNT(*) as high_intent_customer_count FROM tiktok_second_round_analyzed_comments WHERE second_round_classification = '高意向客户'"
        result = self.execute_query(query)
        stats['high_intent_customer_count'] = result[0]['high_intent_customer_count'] if result else 0

        return stats

    def get_available_worker_ips(self):
        """获取所有可用的 worker IP 地址"""
        query = """
        SELECT worker_ip 
        FROM worker_infos 
        WHERE status = 'active'
        ORDER BY last_heartbeat DESC
        """
        results = self.execute_query(query)
        return [result['worker_ip'] for result in results] if results else []

    def clear_first_round_analysis_by_keyword(self, keyword):
        """清空指定关键字的第一轮分析结果"""
        query = "DELETE FROM tiktok_analyzed_comments WHERE keyword = %s"
        return self.execute_update(query, (keyword,))

    def clear_second_round_analysis_by_keyword(self, keyword):
        """清空指定关键字的第二轮分析结果"""
        query = "DELETE FROM tiktok_second_round_analyzed_comments WHERE keyword = %s"
        return self.execute_update(query, (keyword,))

# 使用示例
if __name__ == "__main__":
    db = MySQLDatabase("localhost", "your_username", "your_password", "your_database")
    db.connect()
    db.initialize_tables()  # 初始化表

    # 查询示例
    results = db.execute_query("SELECT * FROM your_table WHERE condition = '%s'", ("value",))
    if results:
        for row in results:
            print(row)

    # 更新示例
    affected_rows = db.execute_update("UPDATE your_table SET column = '%s' WHERE id = %s", ("new_value", 1))
    print(f"更新影响的行数: {affected_rows}")

    # 批量插入示例
    data = [("value1", "value2"), ("value3", "value4")]
    inserted_rows = db.insert_many("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", data)
    print(f"插入的行数: {inserted_rows}")
    db.disconnect()