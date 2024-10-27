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
    清理系统中僵尸进。
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

    # 添加更多反检测参数
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-insecure-localhost')
    
    # 添加随机UA
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    options.add_argument(f'user-agent={random.choice(user_agents)}')
    
    # 添加语言参数
    options.add_argument('--lang=zh-CN,zh,en-US,en')
    
    # 添加更多浏览器特征
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--start-maximized')
    
    logger.info("正在设置WebDriver选项")
    
    try:
        driver = uc.Chrome(driver_executable_path=CHROME_DRIVER, options=options)
        chrome_processes.append(driver.service.process)
        logger.info(f"WebDriver已设置成功，使用驱动程序路径: {CHROME_DRIVER}")
        
        # 初始化浏览器特征
        driver.maximize_window()
        init_browser_features(driver)
        
        return driver
    except Exception as e:
        logger.error(f"设置WebDriver时发生错误: {str(e)}")
        cleanup_chrome_processes()
        raise

def init_browser_features(driver):
    """初始化浏览器特征"""
    try:
        # 执行反检测JavaScript
        stealth_js = """
        // 覆盖webdriver属性
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });

        // 添加Chrome对象
        if (!window.chrome) {
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
        }

        // 修改plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => {
                return [
                    {
                        0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    },
                    {
                        0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                        description: "Portable Document Format",
                        filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                        length: 1,
                        name: "Chrome PDF Viewer"
                    }
                ];
            }
        });

        // 修改languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['zh-CN', 'zh', 'en-US', 'en']
        });

        // 添加Notification权限
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
            Promise.resolve({state: Notification.permission}) :
            originalQuery(parameters)
        );

        // WebGL数
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) {
                return 'Intel Inc.'
            }
            if (parameter === 37446) {
                return 'Intel(R) Iris(TM) Graphics 6100'
            }
            return getParameter.apply(this, [parameter]);
        };
        """
        
        driver.execute_script(stealth_js)
        logger.info("已初始化浏览器反检测特征")
        
        # 设置初始cookies
        driver.get("https://www.tiktok.com")
        driver.add_cookie({
            'name': 'tt_webid_v2',
            'value': str(random.randint(10**18, 10**19)),
            'domain': '.tiktok.com'
        })
        
        # 添加更多必要的cookies
        cookies = [
            {
                'name': 'ttwid',
                'value': ''.join(random.choices('0123456789abcdef', k=32)),
                'domain': '.tiktok.com'
            },
            {
                'name': 'msToken',
                'value': ''.join(random.choices('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_', k=107)),
                'domain': '.tiktok.com'
            }
        ]
        
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                logger.warning(f"添加cookie失败: {str(e)}")
        
        logger.info("已添加基础cookies")
        
        # 执行一些随机操作
        actions = ActionChains(driver)
        actions.move_by_offset(random.randint(0, 100), random.randint(0, 100))
        actions.perform()
        
        return True
    except Exception as e:
        logger.error(f"初始化浏览器特征时发生错误: {str(e)}")
        return False

def random_sleep(min_seconds=1, max_seconds=3):
    """随机等待一段时间，模拟人类行为。"""
    time_to_sleep = random.uniform(min_seconds, max_seconds)
    logger.info(f"随机等待 {time_to_sleep:.2f} 秒")
    time.sleep(time_to_sleep)

def save_cookies(driver, username):
    """保存当前会话的Cookies到JSON件。"""
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
        
        # 等待页面载完成
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
                    logger.warning(f"添加cookie失败: {cookie['name']}. 误: {str(e)}")
        
        # 打印所有当前的cookies
        current_cookies = driver.get_cookies()
        logger.info(f"当前浏览器有 {len(current_cookies)} 个cookies")
        for cookie in current_cookies:
            logger.info(f"Cookie: {cookie['name']} = {cookie['value'][:10]}... (domain: {cookie['domain']})")
        
        logger.info(f"Cookies从 {filename} 加载")
        return True
    except FileNotFoundError:
        logger.info(f"未找到 {filename} 文件")
        return False
    except json.JSONDecodeError:
        logger.error(f"{filename} 文件误")
        return False

def is_captcha_present(driver):
    """检查面上是否存在验证码元素。"""
    try:
        # 这里假设验证码元素有一CSS选择器
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
        
        # 检并提ID
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

def login_by_local_cookies(driver, username=None):
    """
    尝试使用本地cookies文件登录TikTok，成功则返回用户ID
    :param driver: WebDriver实例
    :param username: 可选，指定用户名来加载特定的cookie文件
    """
    # 清理所有cookies
    driver.delete_all_cookies()
    logger.info("已清理所cookies")

    if username:
        # 如果指定了用户名，只尝试加载该用的cookie文件
        cookie_files = [f"{username}-cookies.json"]
        logger.info(f"尝试加载用户 {username} 的cookies文件")
    else:
        # 否则遍历当前目录查找所有cookies文件
        cookie_files = glob.glob('*cookies.json')
        logger.info("尝试加载所有可用的cookies文件")
    
    if not cookie_files:
        error_message = "没有找到可用的cookies文件"
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
                # 移除可能导致问题的段
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
            logger.error(f"使用 {cookie_file} 时发生错误: {str(e)}")

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
    
    # 待第一个视频加载
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.TAG_NAME, 'video'))
    )
    logger.info("视频加载成功")
    
    video_data = []  # 修改为存储视频详细信息的列表
    scroll_attempts = 0
    max_scroll_attempts = 20
    consecutive_no_new = 0
    max_consecutive_no_new = 5
    
    # 动态调整滚动距离
    min_scroll_distance = 500
    max_scroll_distance = 1500
    current_scroll_distance = min_scroll_distance
    
    while len(video_data) < 20 and scroll_attempts < max_scroll_attempts:
        scroll_attempts += 1
        logger.info(f"滚动尝试次数: {scroll_attempts}/{max_scroll_attempts}, 当前已收集 {len(video_data)} 个视频")
        
        # 获取当前视频数量
        current_count = len(video_data)
        
        # 使用BeautifulSoup解析页面
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 查找所有视频容器
        video_containers = soup.find_all('div', {'data-e2e': 'search_top-item'})
        
        for container in video_containers:
            try:
                # 获取视频链接
                video_link = container.find('a', {'class': 'css-1g95xhm-AVideoContainer'})
                if not video_link or '/video/' not in video_link['href']:
                    continue
                
                video_url = video_link['href']
                
                # 检查是否已经收集过这个视频
                if any(v['video_url'] == video_url for v in video_data):
                    continue
                
                # 获取视频描述
                desc_element = container.find('div', {'data-e2e': 'search-card-desc'})
                description = desc_element.get_text(strip=True) if desc_element else ""
                
                # 获取作者信息
                author_element = container.find('p', {'data-e2e': 'search-card-user-unique-id'})
                author = author_element.get_text(strip=True) if author_element else ""
                
                # 获取观看次数 - 使用方案2
                views_count = "0"
                try:
                    views_element = container.select_one('div[data-e2e="search-card-like-container"] strong')
                    if views_element:
                        views_count = views_element.get_text(strip=True)
                        # 数值清理和格式化
                        views_count = views_count.replace('K', '000').replace('M', '000000').replace('B', '000000000')
                        views_count = ''.join(filter(str.isdigit, views_count)) or "0"
                except Exception as e:
                    logger.error(f"获取视频观看次数时发生错误: {str(e)}")
                
                # 收集视频数据
                video_info = {
                    'video_url': video_url,
                    'description': description,
                    'author': author,
                    'views_count': views_count
                }
                
                video_data.append(video_info)
                logger.info(f"收集到视频: {video_url}, 作者: {author}, 观看次数: {views_count}")
                
            except Exception as e:
                logger.error(f"处理视频容器时发生错误: {str(e)}")
                continue
        
        # 检查是否有新视频被添加
        if len(video_data) > current_count:
            consecutive_no_new = 0
            logger.info(f"本次滚动发现 {len(video_data) - current_count} 个新视频")
            current_scroll_distance = min_scroll_distance
        else:
            consecutive_no_new += 1
            logger.info(f"本次滚动未发现新视频，连续未发现次数: {consecutive_no_new}")
            current_scroll_distance = min(current_scroll_distance + 200, max_scroll_distance)
        
        # 如果连续多次未发现新视频，尝试更激进滚动策略
        if consecutive_no_new >= max_consecutive_no_new:
            logger.info("连续多次未发现新视频，尝试更激进的滚动策略")
            # 快速滚动到底部然后回到顶部
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            random_sleep(1, 2)
            driver.execute_script("window.scrollTo(0, 0);")
            random_sleep(1, 2)
            consecutive_no_new = 0
            continue
        
        # 正常滚动
        scroll_distance = random.randint(current_scroll_distance, current_scroll_distance + 300)
        driver.execute_script(f"window.scrollBy(0, {scroll_distance});")
        logger.info(f"向下滚动 {scroll_distance} 像素")
        
        # 随机暂停
        random_sleep(2, 4)
        
        # # 检查是否到达页面底部
        # last_height = driver.execute_script("return document.body.scrollHeight")
        # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # random_sleep(1, 2)
        # new_height = driver.execute_script("return document.body.scrollHeight")
        
        # if new_height == last_height:
        #     logger.info("已到达页面底部")
        #     break
        
        # 随机完整性检查
        if random.random() < 0.2:  # 20%的概率
            # 短暂向上滚动然后再向下，模拟人类行为
            up_scroll = random.randint(200, 400)
            driver.execute_script(f"window.scrollBy(0, -{up_scroll});")
            random_sleep(0.5, 1)
            driver.execute_script(f"window.scrollBy(0, {up_scroll});")
            logger.info(f"执行随机向上滚动 {up_scroll} 像素后返回")
        
        # 检查是否出现验证码
        if is_captcha_present(driver):
            logger.warning("检测到验证码，等待手动处理")
            solve_captcha(driver)
    
    logger.info(f"搜索完成，共收集到 {len(video_data)} 个视频")
    for i, video in enumerate(video_data, 1):
        logger.info(f"第{i}个视频: URL={video['video_url']}, 作者={video['author']}, 观看次数={video['views_count']}")
    
    return video_data[:20]  # 返回视频详细信息列表

def search_tiktok_video_links(driver, keyword):
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

def visit_video_page(driver, video_url):
    """
    访问视频页面并处理please wait
    
    Args:
        driver: WebDriver实例
        video_url: 要访问的视频URL
        
    Returns:
        bool: 是否成访问页面
    """
    max_attempts = 8
    attempt = 0
    
    # 获取登录相关的cookies
    auth_cookies = {
        cookie['name']: cookie['value'] for cookie in driver.get_cookies() 
        if cookie['name'] in [
            'sessionid',   # 会话ID
            'sid_tt',      # TikTok会话ID
            'uid_tt',      # TikTok用户ID
            'msToken',     # TikTok安令牌
            'ttwid',       # TikTok Web ID
            'tt_webid_v2'  # TikTok Web ID v2
        ]  # 保留关键登录cookie
    }
    logger.info(f"已获取 {len(auth_cookies)} 个登录相关cookies")
    
    while attempt < max_attempts:
        attempt += 1
        logger.info(f"第 {attempt} 次尝试访问视频页面")
        
        try:
            # 1. 清除非登录相关的cookies和缓存
            if attempt > 1:  # 第一次尝试保持原状
                logger.info("清除非必要cookies和缓存...")
                
                # 清除所有cookies
                driver.delete_all_cookies()
                
                # 只恢复登录相关的cookies
                for cookie_name, cookie_value in auth_cookies.items():
                    driver.add_cookie({
                        'name': cookie_name,
                        'value': cookie_value,
                        'domain': '.tiktok.com'
                    })
                
                # 添加新的随机cookies
                new_cookies = [
                    {
                        'name': 'tt_webid_v2',
                        'value': str(random.randint(10**18, 10**19)),
                        'domain': '.tiktok.com'
                    },
                    {
                        'name': 'ttwid',
                        'value': ''.join(random.choices('0123456789abcdef', k=32)),
                        'domain': '.tiktok.com'
                    },
                    {
                        'name': 'msToken',
                        'value': ''.join(random.choices('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_', k=107)),
                        'domain': '.tiktok.com'
                    }
                ]
                
                for cookie in new_cookies:
                    try:
                        driver.add_cookie(cookie)
                    except Exception as e:
                        logger.warning(f"添加cookie失败: {str(e)}")
                
                # 清除缓存和会话数据
                driver.execute_script("window.localStorage.clear();")
                driver.execute_script("window.sessionStorage.clear();")
                logger.info("已清除本地存储和会话存储")
                
                try:
                    # 清除其他客户端数据
                    driver.execute_cdp_cmd('Network.clearBrowserCache', {})
                    driver.execute_cdp_cmd('Network.clearBrowserCookies', {})
                except Exception as e:
                    logger.warning(f"清除浏览器缓存失败: {str(e)}")
            
            # 2. 先访问主页建立新会话
            logger.info("访问TikTok主页建立新会话...")
            driver.get("https://www.tiktok.com")
            random_sleep(2, 3)
            
            # 3. 使用JavaScript间接导航到目标URL
            logger.info(f"正在访问视频页面: {video_url}")
            driver.execute_script(f'window.location.href = "{video_url}";')
            random_sleep(2, 3)
            
            # 4. 检查是否出现please wait
            if "Please wait" in driver.page_source:
                logger.info("检测到Please wait页面")
                
                # 随机选择处理方式
                action = random.choice(['refresh', 'enter', 'both'])
                logger.info(f"选择处理方式: {action}")
                
                if action == 'refresh':
                    driver.refresh()
                elif action == 'enter':
                    actions = ActionChains(driver)
                    actions.send_keys(Keys.RETURN)
                    actions.perform()
                else:  # both
                    driver.refresh()
                    random_sleep(1, 2)
                    actions = ActionChains(driver)
                    actions.send_keys(Keys.RETURN)
                    actions.perform()
                
                # 等待一段时间
                wait_time = min(3 + attempt, 8)
                random_sleep(wait_time, wait_time + 2)
                
                # 检查页面状态
                if "Please wait" not in driver.page_source:
                    logger.info("成功跳过Please wait页面")
                    return True
                else:
                    logger.warning(f"第 {attempt} 次尝试未能跳过Please wait页面")
                    continue
            else:
                logger.info("页面正常加载，未检测到Please wait")
                return True
                
        except Exception as e:
            logger.error(f"处理过程中发生错误: {str(e)}")
            if attempt < max_attempts:
                continue
            else:
                raise
    
    logger.error(f"在 {max_attempts} 次尝试后仍未能跳过Please wait页面")
    return False


def collect_comments(driver, video_url, video_id, keyword, db, collected_by, task_id):
    """收集给定视频URL下的评论。"""
    logger.info(f"开始收集视频评论: {video_url}")
    
    # 访问视频页面
    if not visit_video_page(driver, video_url):
        error_msg = "无法访问视频页面"
        logger.error(error_msg)
        take_screenshot(driver, f"please_wait_error_{video_id}")
        raise Exception(error_msg)
    
    # 继续处理评论收集逻辑...
    try:
        # 等待视频元素并暂停
        video_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'video'))
        )
        driver.execute_script("arguments[0].pause();", video_element)
        logger.info("视频已暂停")
    except Exception as e:
        logger.warning(f"未能暂停视频: {str(e)}")
    
    # 等待评论元素加载
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="CommentItemWrapper"]'))
        )
        logger.info("评论元素加载完成")
    except Exception as e:
        logger.error(f"等待评论元素超时: {str(e)}")
        raise
    
    # 从数据库中获取已存储的用户ID
    logger.info(f"从数据库中获取已存储的用户ID")
    existing_user_ids = set(db.get_existing_user_ids_for_keyword(keyword))
    logger.info(f"从数据库中获取到 {len(existing_user_ids)} 个已存储的用户ID")

    comments_data = []
    comments_batch = []
    total_scroll_attempts = 0
    scroll_attempts = 0
    max_scroll_attempts = 10  # 最大滚动尝试次数
    consecutive_no_new_comments = 0
    max_consecutive_no_new = 5  # 连续无新评的最大次数

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
        total_scroll_attempts += 1
        logger.info(f"滚动尝试次数: {scroll_attempts}/{max_scroll_attempts}")

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for comment_div in soup.select('div[class*="DivCommentItemWrapper"]'):
            user_link = comment_div.select_one('a[href^="/@"]')
            user_id = user_link.get('href', '').replace('/@', '') if user_link else ''
            
            # 如果用户ID已经在数据中，跳过这条评论
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

        # 优化的滚动策
        if consecutive_no_new_comments >= max_consecutive_no_new:
            logger.info("连续多次未加载新评论，尝更激进的滚动策略")
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
            scroll_attempts = 0
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

    logger.info(f"动结束，原因: {'达到最大滚动次数' if scroll_attempts >= max_scroll_attempts else '到达页面底部'}")

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
            logger.info(f"尝试储剩余的 {len(comments_batch)} 条评论到数据库,成功插入 {inserted_count} 条新评论,忽略 {len(comments_batch) - inserted_count} 条重复评论")
        except Exception as e:
            logger.error(f"存储剩余评论到数据库时发生错误: {str(e)}")

    logger.info(f"评论收集完成，共收集 {len(comments_data)} 条评论")
    return comments_data

def take_screenshot(driver, prefix="screenshot"):
    """保存当前页面的截图，文件名包含时间戳。"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.png"
    driver.save_screenshot(filename)
    logger.info(f"截图已: {filename}")

def get_public_ip():
    try:
        response = requests.get('https://api.ipify.org')
        return response.text
    except Exception as e:
        logger.error(f"获取公网IP失败: {str(e)}")
        return None
    
def take_screenshot(driver, prefix="screenshot"):
    """保存当前页面的截图，文件名包含时间戳。"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.png"
    driver.save_screenshot(filename)
    logger.info(f"截图已: {filename}")

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
        video_links = search_tiktok_video_links(driver, keyword)
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
                    logger.info(f"继续处理之前未完成的视频ID {video_id}, URL {video_url}")
                else:
                    logger.info(f"开始处理新视频：ID {video_id}, URL {video_url}")
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
                logger.info(f"任务 {task_id} 状态为 {task_status}, 退出浏览器...")
                break

        logger.info(f"任务 {task_id} 完成，处理了 {video_count} 个视频")
        if task_status == 'running':
            # 任务状态running，更新为completed
            db.update_tiktok_task_details(task_id, status='completed', end_time=datetime.now())
        elif task_status == 'paused':
            # 任务态为paused，更新为paused
            db.update_tiktok_task_details(task_id, status='paused', end_time=datetime.now())
        elif task_status == 'failed':
            # 任状态为failed，更新为failed
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

def simulate_human_input(driver, element, text):
    """模拟人类输入文本，专门针对Ubuntu系统优化，确保触发发送按钮"""
    try:
        # 先清空内容并聚焦
        driver.execute_script("arguments[0].textContent = '';", element)
        element.click()
        
        # 使用更完整的JavaScript代码模拟输入
        js_code = """
        function simulateInput(element, text) {
            // 创建一个新的InputEvent
            const inputEvent = new InputEvent('input', {
                bubbles: true,
                cancelable: true,
                inputType: 'insertText',
                data: text,
                composed: true
            });
            
            // 创建一个新的KeyboardEvent
            const keyEvent = new KeyboardEvent('keydown', {
                bubbles: true,
                cancelable: true,
                key: text[text.length - 1],
                code: 'Key' + text[text.length - 1].toUpperCase(),
                keyCode: text.charCodeAt(text.length - 1),
                which: text.charCodeAt(text.length - 1),
                composed: true
            });
            
            // 设置元素内容
            element.textContent = text;
            
            // 触发事件序列
            element.dispatchEvent(keyEvent);
            element.dispatchEvent(inputEvent);
            
            // 触发compositionstart和compositionend事件
            element.dispatchEvent(new CompositionEvent('compositionstart'));
            element.dispatchEvent(new CompositionEvent('compositionend'));
            
            // 触发change事件
            element.dispatchEvent(new Event('change', {
                bubbles: true,
                cancelable: true
            }));
            
            // 触发blur和focus事件
            element.dispatchEvent(new FocusEvent('blur'));
            element.dispatchEvent(new FocusEvent('focus'));
        }
        simulateInput(arguments[0], arguments[1]);
        """
        
        # 逐字符输入，包括空格
        current_text = ""
        for char in text:
            current_text += char
            driver.execute_script(js_code, element, current_text)
            time.sleep(random.uniform(0.1, 0.3))
            
            # 每输入一个字符后，额外触发一次点击事件
            element.click()
        
        # 最后再触发一次完整的事件序列
        driver.execute_script(js_code, element, text)
        
        # 验证最终文本
        actual_text = element.get_attribute('textContent') or element.text
        if actual_text.strip() != text.strip():
            logger.warning(f"文本验证不匹配，尝试最后一次设置。预期: '{text}', 实际: '{actual_text}'")
            
            # 最后的补救措施：直接使用send_keys
            try:
                element.clear()
                element.click()
                for char in text:
                    element.send_keys(char)
                    time.sleep(random.uniform(0.1, 0.3))
            except Exception as e:
                logger.error(f"使用send_keys输入失败: {str(e)}")
                return False
            
            # 再次验证
            actual_text = element.get_attribute('textContent') or element.text
            if actual_text.strip() != text.strip():
                logger.error(f"最终文本验证失败。预期: '{text}', 实际: '{actual_text}'")
                return False
        
        # 最后再点击一次确保焦点
        element.click()
        element.send_keys(".")
        return True
            
    except Exception as e:
        logger.error(f"模拟人类输入时发生错误: {str(e)}")
        return False

def check_account_status(account_id, username, email):
    db = MySQLDatabase()
    db.connect()
    driver = None
    try:
        driver = setup_driver()
        
        # 尝试使用本地cookies登录，指定用户名
        try:
            user_id = login_by_local_cookies(driver, username)
            if user_id:
                # 登录成功，更新账号状态
                db.update_tiktok_account_status(account_id, 'active')
                logger.info(f"账号 {username} 使用本地cookies登录成功，状态更新为active")
                return  # 登录成功，直接返回
        except Exception as e:
            logger.error(f"使用本地cookies登录失败: {str(e)}")
            # 继续执行手动登录逻辑
        
        # 登录失或发生异常，尝试手动登录
        logger.info(f"尝试手动登录账号 {username}")
        
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

        logger.info(f"已输入邮箱 {email}，等待人工完成验证和登录...")
        
        # 开始15分钟的循环检查
        start_time = time.time()
        login_success = False
        check_count = 0
        while time.time() - start_time < 900:  # 900秒 = 15分钟
            check_count += 1
            logger.info(f"第 {check_count} 次检查登录状态 (已经过 {int(time.time() - start_time)} 秒)")
            try:
                # 检查用户头像元素
                profile_icon = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-e2e="profile-icon"]'))
                )
                logger.info("检测到用户头像，登录成功")
                login_success = True
                break
            except:
                logger.info("未检测到用户头像，继续等待...")
                # 每次检查后打印当前页面URL
                current_url = driver.current_url
                logger.info(f"当前页面URL: {current_url}")
                time.sleep(5)  # 每5秒检查一次
        
        if login_success:
            # 登录成，保存cookies
            save_cookies(driver, username)
            
            db.update_tiktok_account_status(account_id, 'active')
            logger.info(f"账号 {username} 状态更新为 activecookies")
        else:
            db.update_tiktok_account_status(account_id, 'inactive')
            logger.info(f"账号 {username} 状态更新为 inactive（15分钟内未检测到成功登录）")

    except Exception as e:
        logger.error(f"检查账号 {username} 状态时发生错误: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        db.update_tiktok_account_status(account_id, 'inactive')
    finally:
        if driver:
            driver.quit()
        db.disconnect()

def send_promotion_messages(user_messages, account_id, batch_size=5, wait_time=60, keyword=None):
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
            batch_results = []
            for user_msg in batch_users:
                result = send_single_promotion_message(driver, user_msg['user_id'], user_msg['message'], keyword, db)
                batch_results.append(result)
            
            results.extend(batch_results)
            
            # 查是否还有剩余的消息需要发送
            remaining_messages = len(user_messages) - len(results)
            if remaining_messages > 0:
                logger.info(f"已发送 {len(results)} 条消息，还剩 {remaining_messages} 条，等待 {wait_time} 秒后继续...")
                time.sleep(wait_time)
            else:
                logger.info("所有消息已发送完毕，结束处理。")
                break
        
        return results
    except Exception as e:
        logger.error(f"批量发送推广消息时发生错误: {str(e)}")
        return results + [{"success": False, "message": f"发生错误: {str(e)}", "action": "none", "user_id": user_msg['user_id']} for user_msg in user_messages[len(results):]]
    finally:
        if driver:
            driver.quit()
        db.disconnect()

def random_wait(min_time=1, max_time=5):
    """随机等待一段时间"""
    wait_time = random.uniform(min_time, max_time)
    logger.info(f"随机等待 {wait_time:.2f} 秒")
    time.sleep(wait_time)

def simulate_human_scroll(driver):
    """模拟人类滚动页面"""
    total_height = driver.execute_script("return document.body.scrollHeight")
    viewport_height = driver.execute_script("return window.innerHeight")
    
    # 向下滚动
    scroll_pause_time = random.uniform(0.5, 2)
    
    # 确保滚动范围有效
    max_scroll = max(total_height, int(viewport_height * 1.5))
    scroll_to = random.randint(viewport_height, max_scroll)
    
    driver.execute_script(f"window.scrollTo(0, {scroll_to});")
    logger.info(f"模拟人类向下滚动到 {scroll_to} 像素")
    time.sleep(scroll_pause_time)
    
    # 短暂停留
    time.sleep(random.uniform(1, 3))
    
    # 向上滚动回顶部
    current_position = driver.execute_script("return window.pageYOffset;")
    scroll_steps = min(random.randint(2, 4), max(1, current_position // (viewport_height // 2)))
    
    for _ in range(scroll_steps):
        scroll_up = min(random.randint(int(viewport_height * 0.3), int(viewport_height * 0.7)), current_position)
        current_position -= scroll_up
        driver.execute_script(f"window.scrollTo(0, {current_position});")
        logger.info(f"模拟人类上滚动到 {current_position} 像素")
        time.sleep(random.uniform(0.3, 0.8))
    
    # 最后确保回到顶部
    driver.execute_script("window.scrollTo(0, 0);")
    logger.info("模拟人类滚动回到顶部")
    time.sleep(random.uniform(0.5, 1))

# 尝试关注用户
def try_follow_user(driver, user_id):
    # 备注：好像关注不生效...似乎是IP问题
    try:
        logger.info(f"尝试关注用户 {user_id}")
        follow_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//button[@data-e2e='follow-button']"))
        )
        
        # 检查按钮文本
        button_text = follow_button.text.lower()
        if button_text in ['following', '已��注']:
            logger.info(f"用户 {user_id} 已经被关注")
            return True
        elif button_text not in ['follow', '关注']:
            logger.warning(f"无法确注按钮状态，按钮文本为: {button_text}")
            return False
        
        # 如果是"关注"状态，则点击关注
        follow_button.click()
        logger.info(f"点击关注按钮")
        
        # 等待关注状态更新
        try:
            WebDriverWait(driver, 5).until(
                EC.text_to_be_present_in_element((By.XPATH, "//button[@data-e2e='follow-button']"), "Following")
            )
            logger.info(f"成功关注用户 {user_id}")
            return True
        except TimeoutException:
            logger.warning(f"关注用户 {user_id} 后未能确认状态更新")
            return False

    except Exception as e:
        logger.error(f"关注用户 {user_id} 时发生错误: {str(e)}")
        return False


def send_single_promotion_message(driver, user_id, message, keyword, db):
    try:
        # 初始化操作结果
        follow_success = False
        comment_success = False
        dm_success = False
        at_comment_success = False

        # 访问用户主页
        user_profile_url = f"https://www.tiktok.com/@{user_id}"
        driver.get(user_profile_url)
        logger.info(f"正在访问用户 {user_id} 的主页: {user_profile_url}")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        logger.info("页面加载完成")

        random_wait(2, 5)
        simulate_human_scroll(driver)

        # 尝试关注用户
        follow_success = try_follow_user(driver, user_id)

        random_wait(1, 3)

        # 尝试在用户最新视频下留言
        try:
            logger.info("正在尝试找到最新视频")
            latest_video = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'video'))
            )
            logger.info("找到最新视频,正在点击")
            latest_video.click()

            random_wait(1, 3)

            WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')

            logger.info("正在等待评论输入框出现")
            comment_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-e2e='comment-input'] div.public-DraftEditor-content"))
            )
            logger.info("找到评论输入框,正在输入评论")

            logger.info("开始模拟人类输入评论")
            if simulate_human_input(driver, comment_input, message):
                logger.info("评论输入成功")
            else:
                raise Exception("评论输入失败")
            
            logger.info("正在尝试使用回车键发送评论")
            comment_input.send_keys(Keys.RETURN)

            random_wait(2, 4)

            logger.info(f"成功在用户 {user_id} 的视频下留言")
            comment_success = True
        except Exception as e:
            logger.error(f"留言失败: {str(e)}")

        if not comment_success:
            # 如果留言失败，则尝试发送私信
            try:
                logger.info(f"重新访问用户 {user_id} 的主页")
                driver.get(user_profile_url)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                logger.info("用户主页重新加载完成")

                random_wait(2, 4)

                logger.info("正尝试找到发送私信按钮")
                message_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@data-e2e='message-button']"))
                )
                logger.info("找到发送私信按钮,正在点击")
                message_button.click()

                random_wait(2, 4)

                logger.info("正在等待私信输入框出现")
                message_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-e2e='message-input-area'] div.public-DraftEditor-content"))
                )
                logger.info("找到私信输入框,正在输入私信")

                # 修改私信输入部分
                logger.info("开始模拟人类输入私信")
                if simulate_human_input(driver, message_input, message):
                    logger.info("私信输入成功")
                else:
                    raise Exception("私信输入失败")

                logger.info("正在尝试使用回车键发送私信")
                message_input.send_keys(Keys.RETURN)

                random_wait(2, 4)

                # 检查私信是否发送成功
                try:
                    # 获取发送前的消息数量
                    messages_before = driver.find_elements(By.CSS_SELECTOR, "div[data-e2e='chat-item']")
                    messages_count_before = len(messages_before)
                    
                    # 等待新消息出现或失败警告出现
                    WebDriverWait(driver, 10).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[data-e2e='chat-item']")) > messages_count_before or
                                d.find_elements(By.CSS_SELECTOR, "div[data-e2e='dm-warning']")
                    )
                    
                    # 再次获取消息数量
                    messages_after = driver.find_elements(By.CSS_SELECTOR, "div[data-e2e='chat-item']")
                    messages_count_after = len(messages_after)
                    
                    # 检查是否有新消息出现
                    if messages_count_after > messages_count_before:
                        logger.info(f"成功发送私信给用户 {user_id}")
                        dm_success = True
                    else:
                        # 检查是否出现失败警告
                        warning_elements = driver.find_elements(By.CSS_SELECTOR, "div[data-e2e='dm-warning']")
                        if warning_elements:
                            logger.warning("检测到私信发送失败警告")
                            dm_success = False
                        else:
                            logger.warning("无法确定私信发送状态")
                            dm_success = False
                except TimeoutException:
                    logger.warning("等待私信发送结果超时")
                    dm_success = False

            except Exception as e:
                logger.error(f"发送私信失败: {str(e)}")
                logger.error(f"发送私信失败的详细错误: {traceback.format_exc()}")
                dm_success = False

        if not dm_success and not comment_success:
            # 如果私信和留言都失败，则尝试在源视频下留言并艾特用户
            try:
                # 使用新方法获取源视频链接
                try:
                    db.connect()
                except Exception as e:
                    logger.error(f"连接数据库失败: {str(e)}")
                
                video_url = db.get_video_url_by_keyword_and_user_id(keyword, user_id)

                if video_url:
                    logger.info(f"找到用户 {user_id} 的源视频链接: {video_url}")
                else:
                    logger.warning(f"未找到用户 {user_id} 的源视频链接")
                    raise Exception("未找到用户 {user_id} 的源视频链接")

                driver.get(video_url)
                logger.info(f"正在访问源视频链接: {video_url}")
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                logger.info("源视频页面加载完成")

                random_wait(2, 5)
                simulate_human_scroll(driver)

                logger.info("正在等待源视频评论输入框出现")
                comment_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-e2e='comment-input'] div.public-DraftEditor-content"))
                )
                logger.info("找到源视频评论输入框,正在输入艾特")

                # 先输入@符号
                comment_input.send_keys("@")
                time.sleep(random.uniform(0.3, 0.5))

                # 逐字符输入用户ID
                input_text = "@"
                for char in user_id:
                    comment_input.send_keys(char)
                    time.sleep(random.uniform(0.1, 0.3))
                    input_text += char
                    
                    # 验证输入
                    actual_text = comment_input.text
                    if actual_text != input_text:
                        logger.warning(f"输入不匹配。预期: {input_text}, 实际: {actual_text}")
                        driver.execute_script("arguments[0].textContent = arguments[1];", comment_input, input_text)
                        driver.execute_script("var event = new Event('input', { bubbles: true }); arguments[0].dispatchEvent(event);", comment_input)

                logger.info(f"已输入艾特文本: @{user_id}")

                random_wait(1, 2)

                # 等待艾特建议列表出现
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-e2e='comment-at-user']"))
                )
                
                # 查找匹配的用户建议并点击
                mention_suggestions = driver.find_elements(By.CSS_SELECTOR, "div[data-e2e='comment-at-list']")
                correct_suggestion = None
                for suggestion in mention_suggestions:
                    unique_id_element = suggestion.find_element(By.CSS_SELECTOR, "span[data-e2e='comment-at-uniqueid']")
                    if unique_id_element.text.lower() == user_id.lower():
                        correct_suggestion = suggestion
                        logger.info(f"当前建议用户ID: {unique_id_element.text.lower()}, 目标用户ID: {user_id.lower()}")
                        logger.info(f"找到匹配的用户建议: {user_id}")
                        break
                    else:
                        logger.warning(f"当前建议用户ID: {unique_id_element.text.lower()}, 目标用户ID: {user_id.lower()}")
                
                if correct_suggestion:
                    correct_suggestion.click()
                    logger.info(f"成功选择正确的艾特用户 {user_id}")
                    
                    random_wait(1, 2)
                    
                    # 使用剪贴板输入评论内容
                    pyperclip.copy(message)
                    comment_input.send_keys(Keys.CONTROL + 'v' if platform.system() == 'Windows' else Keys.COMMAND + 'v')
                    logger.info("使用剪贴板输入艾特评论内容")

                else:
                    raise Exception(f"未找到匹配的用户建议: {user_id}")

                random_wait(1, 2)

                logger.info("正在尝试使用回车键发送评论")
                comment_input.send_keys(Keys.RETURN)

                random_wait(2, 4)

                # 检查评论是否成功发送
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//div[contains(@class, 'DivCommentItemContainer')]//a[contains(@href, '/@{user_id}')]"))
                    )
                    logger.info(f"成功在源视频 {video_url} 下留言并艾特用户 {user_id}")
                    at_comment_success = True
                except TimeoutException:
                    logger.warning(f"未能确认评论是否成功发送")
                    at_comment_success = False

            except Exception as e:
                logger.error(f"在源视频下留言并艾特用户失败: {str(e)}")
                at_comment_success = False

        # 根据操作结果返回相应的信息
        actions = []
        if follow_success:
            # 关注不记录
            pass
            # actions.append("关注")
        if comment_success:
            actions.append("用户视频留言")
        if dm_success:
            actions.append("直接私信")
        if at_comment_success:
            actions.append("源视频留言")
        action_str = "|".join(actions)

        if actions:
            return {"success": True, "message": f"成功{action_str}", "action": action_str, "user_id": user_id}
        else:
            logger.warning(f"对用户 {user_id} 的所有操作都失败")
            return {"success": False, "message": "全部操作都失败", "action": "none", "user_id": user_id}

    except Exception as e:
        logger.error(f"发送推广消息给用户 {user_id} 时发生错误: {str(e)}")
        logger.error(f"发送推广消息失败的详细错误: {traceback.format_exc()}")
        return {"success": False, "message": f"发生错误: {str(e)}", "action": "none", "user_id": user_id}
