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

# from common.cos import process_and_upload_csv_to_cospy

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

def setup_driver():
    """设置并返回一个Selenium WebDriver实例。"""
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
    
    driver = uc.Chrome(options=options)
    logger.info("WebDriver已设置完成")

    # 设置浏览器全屏
    driver.maximize_window()
    logger.info("浏览器已设置为全屏模式")

    return driver

def random_sleep(min_seconds=1, max_seconds=3):
    """随机等待一段时间，模拟人类行为。"""
    time_to_sleep = random.uniform(min_seconds, max_seconds)
    logger.info(f"随机等待 {time_to_sleep:.2f} 秒")
    time.sleep(time_to_sleep)

def save_cookies(driver, username):
    """保存当前会话的Cookies到JSON文件。"""
    cookies = driver.get_cookies()
    
    # 移除可能导致问题的字段
    for cookie in cookies:
        cookie.pop('sameSite', None)
        cookie.pop('storeId', None)
        cookie.pop('origin', None)
    
    # 保存为JSON文件
    filename = f"{username}-cookies.json"
    with open(filename, "w") as file:
        json.dump(cookies, file, indent=2)
    logger.info(f"Cookies已保存到 {filename}")

def load_cookies(driver, username):
    """从文件加载Cookies到当前会话。"""
    try:
        # 首先导航到TikTok主页
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
    """检查当前的登录状"""
    try:
        driver.get("https://www.tiktok.com/foryou")
        WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        profile_icon = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-e2e="profile-icon"]'))
        )
        logger.info("检测到用户头像，登录状态有效")
        return True
    except Exception as e:
        logger.info(f"登录状态检查失败: {str(e)}")
        return False

def refresh_login(driver, username, password):
    """刷新登录状态"""
    logger.info("开始刷新登录状态")
    login(driver, username, password)
    save_cookies(driver, username)
    update_last_login_time()

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

def login(driver, username, password):
    """使用给定的用户名和密码登录TikTok。"""
    # 清理所有cookies
    driver.delete_all_cookies()
    logger.info("已清理所有cookies")

    last_login = get_last_login_time()
    if last_login and datetime.now() - last_login < timedelta(days=5):
        if load_cookies(driver, username) and check_login_status(driver):
            logger.info("使用有效的Cookies成功登录")
            return
    
    logger.info("需要刷新登录状态")
    if load_cookies(driver, username):
        logger.info("使用Cookies登录中...")
        driver.get("https://www.tiktok.com/foryou")
        logger.info("使用Cookies登录")
        try:
            WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            logger.info(f"当前页面URL: {driver.current_url}")
            logger.info(f"当前页面标题: {driver.title}")
            
            # 检查是否存在登录状态的元素
            try:
                profile_icon = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-e2e="profile-icon"]'))
                )
                logger.info("检测到用户头像，登录成功")
                return
            except TimeoutException:
                logger.info("未检测到用户头像，可能登录失败")
            
            if "foryou" in driver.current_url or "For You" in driver.title:
                logger.info("使用Cookies登录成功")
                return
            else:
                logger.info("Cookies无效，页面未显示预期内容")
                logger.info(f"页面源代码: {driver.page_source[:1000]}...")  # 打印前1000个字符的页面源代码
        except Exception as e:
            logger.error(f"使用Cookies登录时发生错误: {str(e)}")
        
        logger.info("Cookies无效，继续使用用户名和密码登录")
    else:
        logger.info("未找到Cookies，继续使用用户名和密码登录")

    login_url = "https://www.tiktok.com/login/phone-or-email/email?lang=en"
    driver.get(login_url)
    logger.info("访问登录页面")

    # 如果Cookies登录失败，继续使用用户名和密码登录的原有逻辑
    try:
        # 等待用户名输入框加载
        username_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, 'username'))
        )
        password_input = driver.find_element(By.CSS_SELECTOR, 'input[type="password"]')
        login_button = driver.find_element(By.CSS_SELECTOR, 'button[data-e2e="login-button"]')

        # 模拟逐字输入用户名和密码
        for char in username:
            username_input.send_keys(char)
            random_sleep(0.1, 0.3)
        for char in password:
            password_input.send_keys(char)
            random_sleep(0.1, 0.3)
        logger.info("输入用户名和密码")

        # 点击登录按钮
        login_button.click()
        logger.info("点击登录按钮")

        # 检查是否出现验证码
        if is_captcha_present(driver):
            solve_captcha(driver)

        # 等待页面跳转并检查URL
        WebDriverWait(driver, 180).until(
            lambda d: "foryou" in d.current_url
        )
        logger.info("登录成功，已跳转到For You页面")

        # 保存Cookies
        save_cookies(driver, username)
    except Exception as e:
        logger.error(f"登录失败: {traceback.format_exc()}")
        raise

    # 登录成功后更新最后登录时间
    update_last_login_time()

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
    logger.info("开始解析页面")
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    video_links = []
    for link in soup.find_all('a', href=True):
        if '/video/' in link['href']:
            video_links.append(link['href'])
    
    logger.info(f"找到 {len(video_links)} 个视频链接")
    index = 1
    for link in video_links:
        logger.info(f"第{index}个视频链接: {link}")
        index += 1
    return video_links

def collect_comments(driver, video_url):
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
        logger.warning("未能暂停视频，可能未找到视频元素")

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="CommentItemWrapper"]'))
    )

    comments_data = []
    scroll_attempts = 0
    max_scroll_attempts = 20  # 增加最大尝试次数
    consecutive_no_new_comments = 0
    max_consecutive_no_new = 5  # 增加连续无新评论的最大次数

    last_comments_count = 0
    seen_comments = set()

    while scroll_attempts < max_scroll_attempts:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for comment_div in soup.select('div[class*="DivCommentItemWrapper"]'):
            user_link = comment_div.select_one('a[href^="/@"]')
            user_id = user_link.get('href', '').replace('/@', '') if user_link else ''
            
            reply_content_span = comment_div.select_one('span[data-e2e="comment-level-1"]')
            reply_content = reply_content_span.get_text(strip=True) if reply_content_span else ''
            
            # 更新获取评论时间的逻辑
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
        
        if consecutive_no_new_comments >= max_consecutive_no_new:
            logger.info("连续多次未加载新评论，尝试向上滚动")
            up_scroll_distance = random.randint(100, 300)
            ActionChains(driver).scroll_by_amount(0, -up_scroll_distance).perform()
            random_sleep(2, 4)
            down_scroll_distance = random.randint(up_scroll_distance - 50, up_scroll_distance + 50)
            ActionChains(driver).scroll_by_amount(0, down_scroll_distance).perform()
            consecutive_no_new_comments = 0
        else:
            scroll_distance = random.randint(300, 500)  # 减少滚动距离
            ActionChains(driver).scroll_by_amount(0, scroll_distance).perform()
        
        logger.info(f"页面滚动完成，滚动距离: {scroll_distance if 'scroll_distance' in locals() else down_scroll_distance} 像素")
        
        # 增加滚动后的等待时间，并添加随机性
        random_sleep(3, 7)
        
        # 检查新内容是否加载
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="CommentItemWrapper"]'))
        )
        
        if comments_data:
            new_comments = comments_data[last_comments_count:]
            if new_comments:
                logger.info(f"本轮新收集到 {len(new_comments)} 条评论:")
                for idx, comment in enumerate(new_comments, 1):
                    logger.info(f"{idx}. 用户: {comment['user_id']}, 内容: {comment['reply_content'][:30]}..., 时间: {comment['reply_time']}")
            else:
                pass
        else:
            logger.info("当前还未收集到评论")

        if last_comments_count == len(comments_data):
            consecutive_no_new_comments += 1
            scroll_attempts += 1
            logger.info(f"未加载新评论，连续未加载次数: {consecutive_no_new_comments}, 总滚动尝试次数: {scroll_attempts}")
        else:
            consecutive_no_new_comments = 0
            scroll_attempts = 0
            new_comments_count = len(comments_data) - last_comments_count
            logger.info(f"加载了 {new_comments_count} 条新评论，重置滚动尝试次数")
        
        last_comments_count = len(comments_data)

        if is_captcha_present(driver):
            solve_captcha(driver)

        # 随机暂停，模拟人类行为
        if random.random() < 0.2:  # 20%的概率
            pause_time = random.uniform(5, 15)
            logger.info(f"随机暂停 {pause_time:.2f} 秒")
            time.sleep(pause_time)

    logger.info(f"评论收集完成，共收集 {len(comments_data)} 条评论")
    return comments_data

def take_screenshot(driver, prefix="screenshot"):
    """保存当前页面的截图，文件名包含时间戳。"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.png"
    driver.save_screenshot(filename)
    logger.info(f"截图已保存: {filename}")

def main():
    username = "claudexie1"
    password = os.environ.get('TIKTOK_PASSWORD', "test")
    keyword = "chatgpt"
    driver = setup_driver()
    try:
        # 在登录之前清理所有cookies
        driver.delete_all_cookies()
        logger.info("已清理所有cookies")

        login(driver, username, password)
        if not check_login_status(driver):
            logger.info("登录状态无效，尝试刷新登录")
            refresh_login(driver, username, password)
        
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

if __name__ == '__main__':
    main()