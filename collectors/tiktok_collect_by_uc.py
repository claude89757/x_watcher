#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/9 20:06
@Author  : claude
@File    : x_collect.py
@Software: PyCharm
"""
import time
import os
import re
import pickle
import random
import datetime
import logging
import traceback
import pyperclip
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import undetected_chromedriver as uc
from selenium.common.exceptions import TimeoutException

from common.mysql import MySQLDatabase

CHROME_DRIVER = '/usr/local/bin/chromedriver'

# 配置日志记录到文件
logging.basicConfig(
    filename='tiktok_collect.log',  # 日志文件名
    filemode='a',        # 追加模式 ('a') 或覆盖模式 ('w')
    level=logging.INFO,  # 日志级别
    format='%(asctime)s - %(levelname)s - %(message)s' # 日志格式
)
logger = logging.getLogger(__name__)

import platform
import json
from datetime import datetime, timedelta
import requests
import glob
from selenium.webdriver.common.keys import Keys
import atexit
import signal
import subprocess


# 预处理评论数据
def preprocess_comment(comment):
    """预处理评论数据"""
    # 只保留字母、数字、空格和部分标点符号
    comment = re.sub(r'[^\w\s.,!?]', '', comment)
    # 移除多余的空白字符
    comment = ' '.join(comment.split())
    # 截断过长的评论
    return comment[:500] if len(comment) > 500 else comment

# 存储所有Chrome相关进程
chrome_processes = []

def cleanup_chrome_processes():
    """
    清理所有Chrome相关进程。
    这个函数会遍历chrome_processes列表,尝试终止每个进程。
    如果进程在5秒内没有终止,则强制结束该进程。
    """
    for process in chrome_processes:
        try:
            process.terminate()
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        except Exception as e:
            logger.error(f"清理Chrome进程时发生错误: {str(e)}")

# 注册cleanup_chrome_processes函数,确保在脚本退出时被调用
atexit.register(cleanup_chrome_processes)

def cleanup_zombie_processes():
    """
    清理系统中的僵尸进程。
    这个函数会查找系统中的僵尸进程并尝试终止它们。
    僵尸进程是已经结束但仍然在进程表中的进程。
    """
    try:
        # 获取所有进程的状态信息
        zombie_processes = subprocess.check_output(["ps", "-A", "-ostat,ppid,pid,cmd"]).decode()
        # 筛选出状态为'Z'(僵尸)的进程
        zombie_lines = [line for line in zombie_processes.splitlines() if 'Z' in line]
        for line in zombie_lines:
            # 提取进程ID并尝试终止该进程
            pid = int(line.split()[2])
            os.kill(pid, signal.SIGKILL)
    except Exception as e:
        logger.error(f"清理僵尸进程时发生错误: {str(e)}")

def setup_driver():
    """
    设置并返回一个Selenium WebDriver实例。
    这个函数会根据当前操作系统设置适当的选项,
    然后创建一个Chrome WebDriver实例。
    如果创建失败,会清理所有Chrome进程并抛出异常。
    """
    options = uc.ChromeOptions()
    
    # 根据操作系统设置无头模式
    current_system = platform.system()
    if current_system == "Linux":
        if "Ubuntu" in platform.version():
            logger.info("检测到Ubuntu系统，禁用无头模式")
        else:
            options.add_argument('--headless')
            logger.info("检测到非Ubuntu的Linux系统，启用无头模式")
    elif current_system == "Darwin":
        logger.info("检测到macOS系统，禁用无头模式")
    else:
        logger.info(f"检测到其他系统: {current_system}，默认启用无头模式")
        options.add_argument('--headless')

    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-insecure-localhost')
    
    logger.info("正在设置WebDriver选项")
    
    # 使用全局定义的 CHROME_DRIVER 常量
    try:
        driver = uc.Chrome(driver_executable_path=CHROME_DRIVER, options=options)
        # 将新创建的Chrome进程添加到全局列表中
        chrome_processes.append(driver.service.process)
        logger.info(f"WebDriver已设置成功，使用驱动程序路径: {CHROME_DRIVER}")
        driver.maximize_window()
        logger.info("浏览器已设置为全屏模式")
        return driver
    except Exception as e:
        logger.error(f"设置WebDriver时发生错误: {str(e)}")
        # 如果设置失败,清理所有Chrome进程
        cleanup_chrome_processes()
        raise

def random_sleep(min_seconds=1, max_seconds=3):
    """随机等待一段时间，模拟人类行为。"""
    time_to_sleep = random.uniform(min_seconds, max_seconds)
    logger.info(f"随机等待 {time_to_sleep:.2f} 秒")
    time.sleep(time_to_sleep)

def save_cookies(driver, username):
    """保存当前会话的Cookies到JSON文件。"""
    logger.info("保存Cookies...")
    cookies = driver.get_cookies()
    
    # 移除可能导致问题的字段
    for cookie in cookies:
        cookie.pop('sameSite', None)
        cookie.pop('storeId', None)
        cookie.pop('origin', None)
    
    # 保存为JSON文件
    filename = f"{username}-cookies.json"
    logger.info(f"保存Cookies到 {filename}")
    with open(filename, "w") as file:
        json.dump(cookies, file, indent=2)
    logger.info(f"Cookies已保存到 {filename}")

def load_cookies(driver, username):
    """从文件加载Cookies到当前会话。"""
    try:
        # 首先导航到TikTok页
        driver.get("https://www.tiktok.com")
        logger.info("已导航到TikTok页")
        
        # 等待页面加载完成
        WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        
        # 尝试加载JSON格式的cookies
        filename = f"{username}-cookies.json"
        logger.info(f"尝试加载 {filename} 文件")
        with open(filename, "r") as file:
            cookies = json.load(file)
            logger.info(f"从文件中读取到 {len(cookies)} 个cookies")
            
            for cookie in cookies:
                # 移除可能导致问题的字段
                cookie.pop('sameSite', None)
                cookie.pop('storeId', None)
                cookie.pop('origin', None)
                
                try:
                    driver.add_cookie(cookie)
                    logger.info(f"成功添加cookie: {cookie['name']}")
                except Exception as e:
                    logger.warning(f"添加cookie失败: {cookie['name']}. 错误: {str(e)}")
        
        # 打印所有当前的cookies
        current_cookies = driver.get_cookies()
        logger.info(f"当前浏览器有 {len(current_cookies)} 个cookies")
        for cookie in current_cookies:
            logger.info(f"Cookie: {cookie['name']} = {cookie['value'][:10]}... (domain: {cookie['domain']})")
        
        logger.info(f"Cookies已从 {filename} 加载")
        return True
    except FileNotFoundError:
        logger.info(f"未找到 {filename} 文件")
        return False
    except json.JSONDecodeError:
        logger.error(f"{filename} 文件格式错误")
        return False

def is_captcha_present(driver):
    """检查面上是否存在验证码元素。"""
    try:
        # 这里假设验证码元素有一个特定的CSS选择器
        captcha_element = driver.find_element(By.CSS_SELECTOR, 'div.cap-flex')  # 替换为实际的验证码元素选择器
        logger.info("检测到证码")
        return True
    except Exception:
        logger.info("未检测到验证码")
        return False

def solve_captcha(driver):
    """处理验证码逻辑。"""
    # 这里可以添加处理验证码的逻辑例如手动解决或使用自动化工具
    logger.info("请手动解决验证码")
    input("解决验证码后按Enter继续...")

def check_login_status(driver):
    """检查当前的登录状态并返回用户ID"""
    try:
        driver.get("https://www.tiktok.com/foryou")
        WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        
        # 检查用户头像元素
        profile_icon = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-e2e="profile-icon"]'))
        )
        logger.info("检测到用户头像，登录状态有效")
        
        # 检查并提取用户ID
        profile_link = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-e2e="nav-profile"]'))
        )
        
        href = profile_link.get_attribute('href')
        user_id = href.split('/')[-1]  # 提取用户ID
        
        logger.info(f"成功提取用户ID: {user_id}")
        return user_id
    except Exception as e:
        logger.info(f"登录状态检查失败: {str(e)}")
        return None

def update_last_login_time():
    """更新最后登录时间"""
    with open("last_login.json", "w") as f:
        json.dump({"last_login": datetime.now().isoformat()}, f)

def get_last_login_time():
    """获取最后登录时间"""
    try:
        with open("last_login.json", "r") as f:
            data = json.load(f)
            return datetime.fromisoformat(data["last_login"])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None

def login_by_local_cookies(driver):
    """尝试使用本地cookies文件登录TikTok，成功则返回用户ID"""
    # 清理所有cookies
    driver.delete_all_cookies()
    logger.info("已清理所有cookies")

    # 遍历当前目录查找cookies文件
    cookie_files = glob.glob('*cookies.json')
    
    if not cookie_files:
        error_message = "当前目录下没有找到cookies文件"
        logger.error(error_message)
        raise Exception(error_message)

    for cookie_file in cookie_files:
        logger.info(f"尝试加载cookies文件: {cookie_file}")
        try:
            with open(cookie_file, 'r') as file:
                cookies = json.load(file)
            
            # 导航到TikTok主页
            driver.get("https://www.tiktok.com")
            logger.info("已导航到TikTok主页")
            
            # 等待页面加载完成
            WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            
            # 添加cookies
            for cookie in cookies:
                # 移除可能导致问题的字段
                cookie.pop('sameSite', None)
                cookie.pop('storeId', None)
                cookie.pop('origin', None)
                
                try:
                    driver.add_cookie(cookie)
                except Exception as e:
                    logger.warning(f"添加cookie失败: {cookie['name']}. 错误: {str(e)}")
            
            # 刷新页面以应用cookies
            driver.refresh()
            
            # 检查登录状态
            user_id = check_login_status(driver)
            if user_id:
                logger.info(f"使用 {cookie_file} 成功登录，用ID: {user_id}")
                return user_id  # 登录成功,返回用户ID
            else:
                logger.info(f"{cookie_file} 登录失败")
        
        except Exception as e:
            logger.error(f"使用 {cookie_file} 登录时发生错误: {str(e)}")

    # 如果所有cookies文件尝试失败,抛出异常
    error_message = "所有cookies文件都无法成功登录"
    logger.error(error_message)
    raise Exception(error_message)

def search_tiktok_videos(driver, keyword):
    """在TikTok上搜索关键字并返回视频链接列表。"""
    logger.info(f"开始搜索关键词: {keyword}")
    search_url = f"https://www.tiktok.com/search?q={keyword}"
    driver.get(search_url)
    logger.info(f"正在访问搜索页面: {search_url}")
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.TAG_NAME, 'video'))
    )
    logger.info("视频加载成功, 等待30秒")
    time.sleep(30)
 
    # 使用BeautifulSoup解析页面
    logger.info("开始解析页")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    video_links = []
    for link in soup.find_all('a', href=True):
        if '/video/' in link['href']:
            video_links.append(link['href'])
    
    logger.info(f"找到 {len(video_links)} 个视频接")
    index = 1
    for link in video_links:
        logger.info(f"第{index}个视频链接: {link}")
        index += 1
    return video_links

def collect_comments(driver, video_url, video_id, keyword, db, collected_by, task_id):
    """收集给定视频URL下的评论。"""
    logger.info(f"开始收集视频评论: {video_url}")
    driver.get(video_url)
    random_sleep(5, 10)
    logger.info("等待页面加载完成")

    try:
        video_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'video'))
        )
        driver.execute_script("arguments[0].pause();", video_element)
        logger.info("视频已暂停")
    except Exception as e:
        logger.warning("未能暂停视频，可能未到视频元素")

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="CommentItemWrapper"]'))
    )

    # 从数据库中获取已存储的用户ID
    logger.info(f"从数据库中获取已存储的用户ID")
    existing_user_ids = set(db.get_existing_user_ids_for_keyword(keyword))
    logger.info(f"从数据库中获取到 {len(existing_user_ids)} 个已存储的用户ID")

    comments_data = []
    comments_batch = []
    scroll_attempts = 0
    max_scroll_attempts = 10  # 最大滚动尝试次数
    consecutive_no_new_comments = 0
    max_consecutive_no_new = 5  # 连续无新评论的最大次数

    last_comments_count = 0
    seen_comments = set()

    # 动态调整滚动距离
    min_scroll_distance = 300
    max_scroll_distance = 1000
    current_scroll_distance = min_scroll_distance

    # 指数退避策略
    backoff_factor = 1.5
    max_backoff_time = 20

    while scroll_attempts < max_scroll_attempts:
        scroll_attempts += 1
        logger.info(f"滚动尝试次数: {scroll_attempts}/{max_scroll_attempts}")

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for comment_div in soup.select('div[class*="DivCommentItemWrapper"]'):
            user_link = comment_div.select_one('a[href^="/@"]')
            user_id = user_link.get('href', '').replace('/@', '') if user_link else ''
            
            # 如果用户ID已经在数据库中，跳过这条评论
            if user_id in existing_user_ids:
                continue

            reply_content_span = comment_div.select_one('span[data-e2e="comment-level-1"]')
            reply_content = reply_content_span.get_text(strip=True) if reply_content_span else ''
            
            # 预处理评论内容
            reply_content = preprocess_comment(reply_content)
            
            reply_time = ''
            time_span = comment_div.select_one('div.css-2c97me-DivCommentSubContentWrapper span')
            if time_span:
                reply_time = time_span.get_text(strip=True)
            
            comment_key = f"{user_id}:{reply_content}"
            if reply_content and comment_key not in seen_comments:
                comments_data.append({
                    'user_id': user_id,
                    'reply_content': reply_content,
                    'reply_time': reply_time,
                    'reply_video_url': video_url
                })
                seen_comments.add(comment_key)
                
                # 将新收集的评论添加到缓存批次
                comments_batch.append({
                    'video_id': video_id,
                    'user_id': user_id,
                    'reply_content': reply_content,
                    'reply_time': reply_time,
                    'keyword': keyword,
                    'collected_by': collected_by,
                    'video_url': video_url
                })
                
                # 如果缓存批次达到50条，尝试存储到数据库
                if len(comments_batch) >= 50:
                    try:
                        inserted_count = 0
                        for batch_comment in comments_batch:
                            result = db.add_tiktok_comment(
                                video_id=batch_comment['video_id'],
                                user_id=batch_comment['user_id'],
                                reply_content=batch_comment['reply_content'],
                                reply_time=batch_comment['reply_time'],
                                keyword=batch_comment['keyword'],
                                collected_by=batch_comment['collected_by'],
                                video_url=batch_comment['video_url']
                            )
                            if result > 0:
                                inserted_count += 1
                                existing_user_ids.add(batch_comment['user_id'])
                        logger.info(f"尝试储50条评论到数据库,成功插入 {inserted_count} 条新评论,忽略 {50 - inserted_count} 条重复评论")
                        comments_batch.clear()  # 清空缓存
                        
                        # 检查任务状态
                        task_status = db.get_tiktok_task_status(task_id)
                        if task_status != 'running':
                            logger.info(f"任务状态为 {task_status}，停止收集评论")
                            return comments_data
                    except Exception as e:
                        logger.error(f"存储评论到数据库时发生错误: {str(e)}")

        # 优化的滚动策略
        if consecutive_no_new_comments >= max_consecutive_no_new:
            logger.info("连续多次未加载新评论，尝试更激进的滚动策略")
            # 尝试快速滚动到底部然后回到顶部
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            random_sleep(1, 2)
            driver.execute_script("window.scrollTo(0, 0);")
            random_sleep(1, 2)
            consecutive_no_new_comments = 0
            current_scroll_distance = min_scroll_distance
        else:
            # 动态调整滚动距离
            scroll_distance = random.randint(current_scroll_distance, current_scroll_distance + 200)
            driver.execute_script(f"window.scrollBy(0, {scroll_distance});")
            current_scroll_distance = min(current_scroll_distance + 50, max_scroll_distance)
        
        logger.info(f"页面滚动完成，滚动距离: {scroll_distance} 像素")
        
        # 指数退避等待
        wait_time = min(0.5 * (backoff_factor ** consecutive_no_new_comments), max_backoff_time)
        random_sleep(wait_time, wait_time + 2)
        
        # 检测是否到达页面底部
        last_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        random_sleep(1, 2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        if new_height == last_height:
            logger.info("已到达页面底部")
            break
        
        if len(comments_data) > last_comments_count:
            new_comments = comments_data[last_comments_count:]
            logger.info(f"本轮新收集到 {len(new_comments)} 条评论，累计收集 {len(comments_data)} 条评论")
            consecutive_no_new_comments = 0
            current_scroll_distance = min_scroll_distance  # 重置滚动距离
        else:
            consecutive_no_new_comments += 1
            logger.info(f"未加载新评论，连续未加载次数: {consecutive_no_new_comments}")
        
        last_comments_count = len(comments_data)

        if is_captcha_present(driver):
            solve_captcha(driver)

        # 随机暂停
        if random.random() < 0.2:
            pause_time = random.uniform(3, 10)
            logger.info(f"随机暂停 {pause_time:.2f} 秒")
            time.sleep(pause_time)

    logger.info(f"滚动结束，原因: {'达到最大滚动次数' if scroll_attempts >= max_scroll_attempts else '到达页面底部'}")

    # 循环结束后，存储剩余的评论
    if comments_batch:
        try:
            inserted_count = 0
            for batch_comment in comments_batch:
                result = db.add_tiktok_comment(
                    video_id=batch_comment['video_id'],
                    user_id=batch_comment['user_id'],
                    reply_content=batch_comment['reply_content'],
                    reply_time=batch_comment['reply_time'],
                    keyword=batch_comment['keyword'],
                    collected_by=batch_comment['collected_by'],
                    video_url=batch_comment['video_url']
                )
                if result > 0:
                    inserted_count += 1
                    existing_user_ids.add(batch_comment['user_id'])
            logger.info(f"尝试存储剩余的 {len(comments_batch)} 条评论到数据库,成功插入 {inserted_count} 条新评论,忽略 {len(comments_batch) - inserted_count} 条重复评论")
        except Exception as e:
            logger.error(f"存储剩余评论到数据库时发生错误: {str(e)}")

    logger.info(f"评论收集完成，共收集 {len(comments_data)} 条评论")
    return comments_data

def take_screenshot(driver, prefix="screenshot"):
    """保存当前页面的截图，文件名包含时间戳。"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.png"
    driver.save_screenshot(filename)
    logger.info(f"截图已保存: {filename}")

def get_public_ip():
    try:
        response = requests.get('https://api.ipify.org')
        return response.text
    except Exception as e:
        logger.error(f"获取公网IP失败: {str(e)}")
        return None

def process_task(task_id, keyword, server_ip):
    db = MySQLDatabase()
    db.connect()
    driver = None
    try:
        cleanup_zombie_processes()  # 在开始任务前清理僵尸进程
        logger.info(f"开始处理任务 ID: {task_id}, 关键词: {keyword}, 服务器IP: {server_ip}")
        db.update_tiktok_task_details(task_id, status='running', start_time=datetime.now())
        db.add_tiktok_task_log(task_id, 'info', f"开始处理TikTok任务: {keyword}")

        driver = setup_driver()

        # 尝试使用本地cookies登录
        user_id = login_by_local_cookies(driver)
        logger.info(f"成功登录，用户ID: {user_id}")

        # 搜索视频并添加到数据库
        video_links = search_tiktok_videos(driver, keyword)
        db.add_tiktok_videos_batch(task_id, video_links, keyword)
        logger.info(f"为任务 {task_id} 添加了 {len(video_links)} 个视频")

        video_count = 0
        while True:
            # 检查任务状态
            task_status = db.get_tiktok_task_status(task_id)
           
            if task_status == 'running':
                logger.info(f"正在获取任务 {task_id} 的下一个待处理视频")
                next_video = db.get_next_pending_video(task_id, server_ip)
                if not next_video:
                    logger.info(f"任务 {task_id} 没有更多待处理的视频，退出循环")
                    break  # 没有更多待处理的视频

                video_id, video_url, status = next_video['id'], next_video['video_url'], next_video['status']
                if status == 'processing':
                    logger.info(f"继续处理之前未完成的视频：ID {video_id}, URL {video_url}")
                else:
                    logger.info(f"开始处理新的视频：ID {video_id}, URL {video_url}")
                try:
                    comments = collect_comments(driver, video_url, video_id, keyword, db, user_id, task_id)
                    logger.info(f"任务 {task_id} 收集到 {len(comments)} 条来自 {video_url} 的评论")
                    db.mark_video_completed(video_id)
                    video_count += 1
                    db.update_task_progress(task_id, 1)
                except Exception as e:
                    logger.error(f"处理视频 {video_url} 时出错: {str(e)}")
                    db.update_tiktok_video_status(video_id, 'failed')
            else:
                logger.info(f"任务 {task_id} 知状态 {task_status}, 退出浏览器...")
                break

        logger.info(f"任务 {task_id} 完成，处理了 {video_count} 个视频")
        if task_status == 'running':
            # 任务状态为running，更新为completed
            db.update_tiktok_task_details(task_id, status='completed', end_time=datetime.now())
        elif task_status == 'paused':
            # 任务状态为paused，更新为paused
            db.update_tiktok_task_details(task_id, status='paused', end_time=datetime.now())
        elif task_status == 'failed':
            # 任务状态为failed，更新为failed
            db.update_tiktok_task_details(task_id, status='failed', end_time=datetime.now())
    except Exception as e:
        logger.error(f"处理任务时发生错误: {str(e)}")
        # db.update_tiktok_task_details(task_id, status='failed', end_time=datetime.now())
        db.add_tiktok_task_log(task_id, 'error', str(e))
        if driver:
            take_screenshot(driver, f"error_task_{task_id}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"关闭WebDriver时发生错误: {str(e)}")
        db.disconnect()
        cleanup_chrome_processes()  # 确保在任务结束时清理所有Chrome进程

def main():
    username = "claudexie1"
    password = os.environ.get('TIKTOK_PASSWORD', "test")
    keyword = "chatgpt"
    driver = setup_driver()
    try:
        # 在登录之前清理所有cookies
        driver.delete_all_cookies()
        logger.info("已清理所有cookies")

        # 尝试使用本地cookies登录
        login_by_local_cookies(driver)
        
        video_links = search_tiktok_videos(driver, keyword)
        all_comments = {}
        for video_url in video_links:
            comments = collect_comments(driver, video_url)
            all_comments[video_url] = comments
            logger.info(f"收集到 {len(comments)} 条来自 {video_url} 的评论")

        # 处理并上传评论数据
        # process_and_upload_csv_to_cos(all_comments)
    except Exception as e:
        logger.error(f"发生错误: {traceback.format_exc()}")
        take_screenshot(driver, "error")
    finally:
        driver.quit()

def simulate_human_input(driver, element, text):
    """模拟人类输入文本，并验证输入是否正确"""
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.2, 0.5))  # 增加随机暂停时间

    # 验证输入是否正确
    input_value = element.get_attribute('value')
    if input_value != text:
        # 如果输入不正确，使用JavaScript直接设置值
        driver.execute_script("arguments[0].value = arguments[1];", element, text)
        # 触发input事件
        driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", element)
        # 触发change事件
        driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", element)

    # 最后再次验证
    WebDriverWait(driver, 10).until(
        EC.text_to_be_present_in_element_value((By.XPATH, f"//*[@name='{element.get_attribute('name')}']"), text)
    )

def check_account_status(account_id, username, email):
    db = MySQLDatabase()
    db.connect()
    driver = None
    try:
        driver = setup_driver()
        
        # 尝试使用本地cookies登录
        user_id = login_by_local_cookies(driver)
        
        if user_id:
            # 登录成功
            db.update_tiktok_account_status(account_id, 'active')
            logger.info(f"账号 {username} 状态更新为 active，使用本地cookies登录成功")
        else:
            # 登录失败,尝试重新登录
            logger.info(f"使用本地cookies登录失败,尝试重新登录账号 {username}")
            
            # 导航到TikTok登录页面
            driver.get("https://www.tiktok.com/login/phone-or-email/email")
            
            # 点击忘记密码按钮
            forgot_password_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Forgot password?')]"))
            )
            forgot_password_button.click()
            
            # 等待邮箱输入框出现
            email_input = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.NAME, "email"))
            )
            
            # 输入邮箱
            simulate_human_input(driver, email_input, email)
            
            # 随机暂停，模拟人类思考
            time.sleep(random.uniform(0.5, 1.5))

            logger.info(f"请在30分钟内手动完成验证码输入和密码重操作，完成后请按回车键继续...")
            
            # 等待用户按回车键
            input("验证完成后请按回车键继续...")
            
            logger.info("继续执行自动化操作...")
            
            # 检查登录状态
            user_id = check_login_status(driver)
            
            if user_id:
                # 登录成功，保存cookies
                save_cookies(driver, username)
                
                db.update_tiktok_account_status(account_id, 'active')
                logger.info(f"账号 {username} 状态更新为 active，并已保存新的cookies")
            else:
                db.update_tiktok_account_status(account_id, 'inactive')
                logger.info(f"账号 {username} 状态更新为 inactive（登录失败）")

    except Exception as e:
        logger.error(f"检查账号 {username} 状态时发生错误: {str(e)}")
        db.update_tiktok_account_status(account_id, 'inactive')
    finally:
        if driver:
            driver.quit()
        db.disconnect()

def send_promotion_messages(user_messages, account_id, batch_size=5, wait_time=60):
    db = MySQLDatabase()
    db.connect()
    driver = None
    results = []
    try:
        driver = setup_driver()
        
        # 获取账号信息
        account = db.get_tiktok_account_by_id(account_id)
        if not account:
            return [{"success": False, "message": "账号不存在", "action": "none", "user_id": user_msg['user_id']} for user_msg in user_messages]
        
        # 使用账号登录
        login_success = login_by_local_cookies(driver)
        if not login_success:
            return [{"success": False, "message": "登录失败", "action": "none", "user_id": user_msg['user_id']} for user_msg in user_messages]
        
        # 分批处理用户
        for i in range(0, len(user_messages), batch_size):
            batch_users = user_messages[i:i+batch_size]
            for user_msg in batch_users:
                result = send_single_promotion_message(driver, user_msg['user_id'], user_msg['message'])
                results.append(result)
            
            # 等待指定时间
            logger.info(f"已发送 {len(results)} 条消息，等待 {wait_time} 秒后继续...")
            time.sleep(wait_time)
        
        return results
    except Exception as e:
        logger.error(f"批量发送推广消息时发生错误: {str(e)}")
        return results + [{"success": False, "message": f"发生错误: {str(e)}", "action": "none", "user_id": user_msg['user_id']} for user_msg in user_messages[len(results):]]
    finally:
        if driver:
            driver.quit()
        db.disconnect()

def send_single_promotion_message(driver, user_id, message):
    try:
        # 访问用户主页
        user_profile_url = f"https://www.tiktok.com/@{user_id}"
        driver.get(user_profile_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        # 尝试关注用户
        try:
            follow_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'follow-button')]"))
            )
            follow_button.click()
            logger.info(f"成功关注用户 {user_id}")
            
            # 尝试在用户最新视频下留言
            try:
                latest_video = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'video-feed-item')]"))
                )
                latest_video.click()
                
                comment_input = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@class='public-DraftEditor-content']"))
                )
                simulate_human_input(driver, comment_input, message)
                
                post_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'post-comment-button')]"))
                )
                post_button.click()
                
                logger.info(f"成功在用户 {user_id} 的视频下留言")
                return {"success": True, "message": "成功关注并留言", "action": "follow_and_comment", "user_id": user_id}
            except Exception as e:
                logger.error(f"留言失败: {str(e)}")
        except Exception as e:
            logger.error(f"关注用户失败: {str(e)}")
        
        # 如果关注或留言失败，尝试发送私信
        try:
            message_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'message-button')]"))
            )
            message_button.click()
            
            message_input = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='public-DraftEditor-content']"))
            )
            simulate_human_input(driver, message_input, message)
            
            send_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'send-message-button')]"))
            )
            send_button.click()
            
            logger.info(f"成功发送私信给用户 {user_id}")
            return {"success": True, "message": "成功发送私信", "action": "direct_message", "user_id": user_id}
        except Exception as e:
            logger.error(f"发送私信失败: {str(e)}")
        
        return {"success": False, "message": "关注、留言和私信都失败", "action": "none", "user_id": user_id}
    except Exception as e:
        logger.error(f"发送推广消息给用户 {user_id} 时发生错误: {str(e)}")
        return {"success": False, "message": f"发生错误: {str(e)}", "action": "none", "user_id": user_id}

# for local test
if __name__ == '__main__':
    main()