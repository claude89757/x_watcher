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

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MySQLDatabase:
    def __init__(self, host, user, password, database):
        self.host = os.environ.get('MYSQL_HOST', host)
        self.user = os.environ.get('MYSQL_USER', user)
        self.password = os.environ.get('MYSQL_PASSWORD', password)
        self.database = os.environ.get('MYSQL_DATABASE', database)
        self.connection = None

    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info("成功连接到MySQL数据库")
        except pymysql.Error as e:
            logger.error(f"连接数据库时出错: {e}")

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")

    def execute_query(self, query, params=None):
        """执行查询操作"""
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
                status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
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
            """
        ]

        for query in create_tables_queries:
            self.execute_update(query)
        
        logger.info("所有必要的表已创建或已存在")

    def create_tiktok_task(self, keyword):
        """创建新的TikTok任务"""
        query = "INSERT INTO tiktok_tasks (keyword) VALUES (%s)"
        return self.execute_update(query, (keyword,))

    def update_tiktok_task_status(self, task_id, status):
        """更新TikTok任务状态"""
        query = "UPDATE tiktok_tasks SET status = %s WHERE id = %s"
        return self.execute_update(query, (status, task_id))

    def get_tiktok_task_status(self, task_id):
        """获取TikTok任务状态"""
        query = "SELECT status FROM tiktok_tasks WHERE id = %s"
        result = self.execute_query(query, (task_id,))
        return result[0]['status'] if result else None

    def add_tiktok_video(self, task_id, video_url, keyword):
        """添加TikTok视频"""
        query = "INSERT INTO tiktok_videos (task_id, video_url, keyword) VALUES (%s, %s, %s)"
        return self.execute_update(query, (task_id, video_url, keyword))

    def add_tiktok_comment(self, video_id, user_id, reply_content, reply_time, keyword):
        """添加TikTok评论"""
        query = """
        INSERT INTO tiktok_comments (video_id, user_id, reply_content, reply_time, keyword)
        VALUES (%s, %s, %s, %s, %s)
        """
        return self.execute_update(query, (video_id, user_id, reply_content, reply_time, keyword))

    def add_tiktok_task_log(self, task_id, log_type, message):
        """添加TikTok任务日志"""
        query = "INSERT INTO tiktok_task_logs (task_id, log_type, message) VALUES (%s, %s, %s)"
        return self.execute_update(query, (task_id, log_type, message))

    def get_pending_tiktok_tasks(self):
        """获取待处理的TikTok任务"""
        query = "SELECT * FROM tiktok_tasks WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1"
        return self.execute_query(query)

    def get_tiktok_task_videos(self, task_id):
        """获取TikTok任务相关的视频"""
        query = "SELECT * FROM tiktok_videos WHERE task_id = %s"
        return self.execute_query(query, (task_id,))

    def get_tiktok_video_comments(self, video_id):
        """获取TikTok视频的评论"""
        query = "SELECT * FROM tiktok_comments WHERE video_id = %s"
        return self.execute_query(query, (video_id,))

    def update_tiktok_task_details(self, task_id, **kwargs):
        """更新TikTok任务的详细信息"""
        allowed_fields = ['status', 'max_videos', 'max_comments_per_video', 'start_time', 'end_time', 'retry_count']
        update_fields = []
        update_values = []

        for key, value in kwargs.items():
            if key in allowed_fields:
                update_fields.append(f"{key} = %s")
                update_values.append(value)

        if not update_fields:
            logger.warning("没有提供有效的更新字段")
            return 0

        query = f"UPDATE tiktok_tasks SET {', '.join(update_fields)} WHERE id = %s"
        update_values.append(task_id)

        return self.execute_update(query, tuple(update_values))

    def update_tiktok_task_server_ip(self, task_id, server_ip):
        """更新TikTok任务的服务器IP列表"""
        query = """
        UPDATE tiktok_tasks 
        SET server_ips = IF(server_ips IS NULL, %s, CONCAT(server_ips, ',%s'))
        WHERE id = %s
        """
        return self.execute_update(query, (server_ip, server_ip, task_id))

    def get_running_tiktok_task_by_ip(self, server_ip):
        """获取指定IP上正在运行的TikTok任务"""
        query = """
        SELECT * FROM tiktok_tasks 
        WHERE status = 'running' AND FIND_IN_SET(%s, server_ips)
        """
        return self.execute_query(query, (server_ip,))

    def get_pending_tiktok_task_by_keyword(self, keyword):
        """获取指定关键词的待处理TikTok任务"""
        query = """
        SELECT * FROM tiktok_tasks 
        WHERE status = 'pending' AND keyword = %s 
        ORDER BY created_at ASC LIMIT 1
        """
        return self.execute_query(query, (keyword,))

    def get_next_pending_video(self, task_id, server_ip):
        """获取取下一个待处理的视频"""
        query = """
        UPDATE tiktok_videos
        SET status = 'processing', processing_server_ip = %s
        WHERE id = (
            SELECT id FROM (
                SELECT id FROM tiktok_videos
                WHERE task_id = %s AND status = 'pending'
                LIMIT 1
            ) AS subquery
        )
        RETURNING id, video_url
        """
        result = self.execute_query(query, (server_ip, task_id))
        return result[0] if result else None

    def update_task_progress(self, task_id, videos_processed):
        """更新任务进度"""
        query = """
        UPDATE tiktok_tasks
        SET total_videos_processed = total_videos_processed + %s
        WHERE id = %s
        """
        return self.execute_update(query, (videos_processed, task_id))

    def mark_video_completed(self, video_id):
        """标记视频为已完成"""
        query = "UPDATE tiktok_videos SET status = 'completed' WHERE id = %s"
        return self.execute_update(query, (video_id,))

    def get_available_tiktok_account(self, ip):
        """获取可用的TikTok账号"""
        query = """
        SELECT * FROM tiktok_account_infos 
        WHERE status = 'normal' AND FIND_IN_SET(%s, login_ips)
        ORDER BY today_collect_count ASC, total_collect_count ASC
        LIMIT 1
        """
        result = self.execute_query(query, (ip,))
        return result[0] if result else None

    def update_tiktok_account_collect_count(self, username):
        """更新TikTok账号的收集次"""
        query = """
        UPDATE tiktok_account_infos
        SET today_collect_count = today_collect_count + 1,
            total_collect_count = total_collect_count + 1
        WHERE username = %s
        """
        return self.execute_update(query, (username,))

    def get_tiktok_videos_for_task(self, task_id, limit=100):
        """获取指定任务的待处理视频列表"""
        query = """
        SELECT id, video_url FROM tiktok_videos
        WHERE task_id = %s AND status = 'pending'
        LIMIT %s
        """
        return self.execute_query(query, (task_id, limit))

    def add_tiktok_videos_batch(self, task_id, video_urls, keyword):
        """批量添加TikTok视频到任务"""
        query = "INSERT INTO tiktok_videos (task_id, video_url, keyword) VALUES (%s, %s, %s)"
        data = [(task_id, url, keyword) for url in video_urls]
        return self.insert_many(query, data)

    def get_and_lock_pending_task(self, server_ip):
        """获取并锁定一个待处理的任务"""
        query = """
        UPDATE tiktok_tasks
        SET status = 'running', server_ips = %s
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
        with self.connection.cursor() as cursor:
            cursor.execute(query, (server_ip,))
            if cursor.rowcount > 0:
                cursor.execute("SELECT id, keyword FROM tiktok_tasks WHERE id = LAST_INSERT_ID()")
                return cursor.fetchone()
        return None

# 使用示例
if __name__ == "__main__":
    db = MySQLDatabase("localhost", "your_username", "your_password", "your_database")
    db.connect()
    db.initialize_tables()  # 初始化表

    # 查询示例
    results = db.execute_query("SELECT * FROM your_table WHERE condition = %s", ("value",))
    if results:
        for row in results:
            print(row)

    # 更新示例
    affected_rows = db.execute_update("UPDATE your_table SET column = %s WHERE id = %s", ("new_value", 1))
    print(f"更新影响的行数: {affected_rows}")

    # 批量插入示例
    data = [("value1", "value2"), ("value3", "value4")]
    inserted_rows = db.insert_many("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", data)
    print(f"插入的行数: {inserted_rows}")
    db.disconnect()