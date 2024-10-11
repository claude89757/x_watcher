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

def setup_driver():
    """设置并返回一个Selenium WebDriver实例。"""
    options = Options()
    
    # 根据操作系统设置无头模式
    current_system = platform.system()
    if current_system == "Linux":
        options.add_argument('--headless')  # Linux系统（如Ubuntu）使用无头模式
        logger.info("检测到Linux系统，启用无头模式")
    elif current_system == "Darwin":
        logger.info("检测到macOS系统，禁用无头模式")
    else:
        logger.info(f"检测到其他系统: {current_system}，默认启用无头模式")
        options.add_argument('--headless')  # 默认使用无头模式

    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-insecure-localhost')
    
    # 设置用户代理
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')
    
    logger.info("正在设置WebDriver选项")
    
    driver = webdriver.Chrome(service=Service(CHROME_DRIVER), options=options)
    logger.info("WebDriver已设置完成")
    return driver

def random_sleep(min_seconds=1, max_seconds=3):
    """随机等待一段时间，模拟人类行为。"""
    time_to_sleep = random.uniform(min_seconds, max_seconds)
    logger.info(f"随机等待 {time_to_sleep:.2f} 秒")
    time.sleep(time_to_sleep)

def save_cookies(driver, username):
    """保存当前会话的Cookies到文件。"""
    cookies = driver.get_cookies()
    with open(f"{username}_cookies.pkl", "wb") as file:
        pickle.dump(cookies, file)
    logger.info(f"Cookies已保存到 {username}_cookies.pkl")

def load_cookies(driver, username):
    """从文件加载Cookies到当前会话。"""
    try:
        with open(f"{username}_cookies.pkl", "rb") as file:
            cookies = pickle.load(file)
            for cookie in cookies:
                driver.add_cookie(cookie)
        logger.info(f"Cookies已从 {username}_cookies.pkl 加载")
        return True
    except FileNotFoundError:
        logger.info(f"未找到 {username}_cookies.pkl 文件")
        return False

def login(driver, username, password):
    """使用给定的用户名和密码登录TikTok。"""
    login_url = "https://www.tiktok.com/login/phone-or-email/email?lang=en"
    driver.get(login_url)
    logger.info("访问登录页面")

    if load_cookies(driver, username):
        driver.refresh()
        logger.info("使用Cookies登录")
        if "foryou" in driver.current_url:
            logger.info("使用Cookies登录成功")
            return
        else:
            logger.info("Cookies无效，继续使用用户名和密码登录")

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

def search_tiktok_videos(driver, keyword):
    """在TikTok上搜索关键字并返回视频链接列表。"""
    logger.info(f"开始搜索关键词: {keyword}")
    search_url = f"https://www.tiktok.com/search?q={keyword}"
    driver.get(search_url)
    logger.info(f"正在访问搜索页面: {search_url}")
    time.sleep(10)  # 等待页面加载
    logger.info("等待页面加载完成")

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
    random_sleep(5, 10)  # 随机等待页面加载
    logger.info("等待页面加载完成")

    # 暂停视频播放
    try:
        video_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'video'))
        )
        driver.execute_script("arguments[0].pause();", video_element)
        logger.info("视频已暂停")
    except Exception as e:
        logger.warning("未能暂停视频，可能未找到视频元素")

    # 等待评论元素加载
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-1gstnae-DivCommentItemWrapper'))
    )

    # 滚动页面以加载更多评论
    comments = []
    scroll_attempts = 0
    max_scroll_attempts = 5
    logger.info(f"开始滚动页面，最大滚动尝试次数: {max_scroll_attempts}")

    while scroll_attempts < max_scroll_attempts:
        # 使用BeautifulSoup解析页面
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for comment_div in soup.select('div.css-1gstnae-DivCommentItemWrapper'):
            comment_text = comment_div.get_text(strip=True)
            if comment_text not in comments:
                comments.append(comment_text)
        
        logger.info(f"当前已收集 {len(comments)} 条评论")

        # 模拟鼠标滚动
        ActionChains(driver).scroll_by_amount(0, 1000).perform()
        logger.info("页面向下滚动1000像素")
        random_sleep(5, 15)  # 增加随机等待时间

        # 检查是否有新的评论加载
        new_soup = BeautifulSoup(driver.page_source, 'html.parser')
        new_comments = [div.get_text(strip=True) for div in new_soup.select('div.css-1gstnae-DivCommentItemWrapper')]
        if len(new_comments) == len(comments):
            scroll_attempts += 1
            logger.info(f"未加载新评论，滚动尝试次数: {scroll_attempts}")
        else:
            scroll_attempts = 0
            logger.info("加载了新评论，重置滚动尝试次数")

    logger.info(f"评论收集完成，共收集 {len(comments)} 条评论")
    return comments

def main():
    username = "claude89757@gmail.com"  # 替换为您的用户名
    password = "thanks0719HTT@"  # 替换为您的密码
    keyword = "cat"  # 替换为您要搜索的关键字
    driver = setup_driver()
    try:
        login(driver, username, password)
        video_links = search_tiktok_videos(driver, keyword)
        all_comments = {}
        for video_url in video_links:
            comments = collect_comments(driver, video_url)
            all_comments[video_url] = comments
            logger.info(f"收集到 {len(comments)} 条来自 {video_url} 的评论")

        # # 处理并上传评论数据
        # process_and_upload_csv_to_cos(all_comments)
    except Exception as e:
        logger.error(f"发生错误: {traceback.format_exc()}")
    finally:
        driver.quit()

if __name__ == '__main__':
    main()