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
import pickle
import random
import datetime
import logging


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

from common.cos import process_and_upload_csv_to_cos

# 配置日志记录到文件
logging.basicConfig(
    filename='x_collect.log',  # 日志文件名
    filemode='a',        # 追加模式 ('a') 或覆盖模式 ('w')
    level=logging.INFO,  # 日志级别
    format='%(asctime)s - %(levelname)s - %(message)s' # 日志格式
)
logger = logging.getLogger(__name__)


class TwitterWatcher:
    def __init__(self, driver_path, username, email, password, search_key_word,
                 timeout=10, cookies_file='cookies.pkl', headless: bool = True,
                 force_re_login: bool = False):
        self.driver_path = driver_path
        self.username = username
        self.email = email
        self.password = password
        self.search_key_word = search_key_word
        self.timeout = timeout
        self.interaction_timeout = 600
        self.cookies_file = cookies_file
        self.driver = None
        self.headless = headless
        self.force_re_login = force_re_login

    def setup_driver(self):
        service = Service(self.driver_path)
        chrome_options = Options()
        chrome_options.add_argument("--lang=en")
        if self.headless:
            chrome_options.add_argument("--headless")  # 添加无头模式
            chrome_options.add_argument("--disable-gpu")  # 如果需要，可以禁用GPU加速
            chrome_options.add_argument("--window-size=1920,1080")  # 设置窗口大小
            # 设置 User-Agent
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def teardown_driver(self):
        if self.driver:
            self.driver.quit()

    def print_page_source(self):
        if self.driver:
            # 使用BeautifulSoup格式化HTML源代码
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            formatted_html = soup.prettify()
            logging.info(formatted_html)

    def find_element(self, by, value):
        return WebDriverWait(self.driver, self.timeout).until(
            EC.presence_of_element_located((by, value))
        )

    def save_cookies(self):
        logging.info("saving cookies")
        with open(self.cookies_file, 'wb') as file:
            pickle.dump(self.driver.get_cookies(), file)

    def load_cookies(self):
        logging.info("loading cookies")
        if os.path.exists(self.cookies_file):
            with open(self.cookies_file, 'rb') as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    self.driver.add_cookie(cookie)

    def login(self):
        logging.info(f"try to login...")
        self.driver.get('https://twitter.com/login?lang=en')

        # 输入账号
        username_input = self.find_element(By.XPATH, '//input[@name="text" and @autocomplete="username"]')
        time.sleep(random.uniform(1, 3))
        username_input.send_keys(self.username)
        time.sleep(random.uniform(0, 1))

        # 点击“下一步”按钮
        next_button = self.find_element(By.XPATH, '//span[text()="Next"]/ancestor::button[@role="button"]')
        logging.info(f"click {next_button.text}")
        next_button.click()
        time.sleep(1)

        # 分类输入密码、输入邮箱或图片验证
        current_input = None
        try:
            current_input = self.find_element(By.XPATH, '//input[@name="password" and @type="password"]')
            input_type = "password"
        except TimeoutException:
            try:
                current_input = self.find_element(By.XPATH, "//span[text()='Phone or email']")
                input_type = "email"
            except TimeoutException:
                input_type = "check_people"
        logging.info(f"input_type: {input_type}")

        if input_type == "password":
            logging.info("直接账号密码登录")
            time.sleep(random.uniform(1, 3))
            current_input.send_keys(self.password)
            time.sleep(random.uniform(0, 1))
            # 按回车键
            current_input.send_keys(Keys.RETURN)

            # 等待浏览器页面变成home页面
            try:
                logging.info("Waiting for the home pages to load...")
                WebDriverWait(self.driver, self.interaction_timeout).until(
                    EC.url_to_be('https://x.com/home')
                )
            except Exception as error:
                logging.info(error)
                logging.info(f"可能账号受限，需要输入邮箱验证")
                # 定位并输入邮箱
                email_input = self.find_element(By.XPATH, '//input[@name="text" and @autocomplete="email" '
                                                          'and @type="email" and @data-testid="ocfEnterTextTextInput"]')
                time.sleep(random.uniform(1, 3))
                email_input.send_keys(self.email)
                time.sleep(random.uniform(0, 1))
                # 按回车键
                email_input.send_keys(Keys.RETURN)

                logging.info("Waiting for the home pages to load...")
                WebDriverWait(self.driver, self.interaction_timeout).until(
                    EC.url_to_be('https://x.com/home')
                )
        elif input_type == "email":
            logging.info("账号受限可能需要输入邮箱")
            # 定位并输入账号
            email_input = self.find_element(By.XPATH, '//input[@name="text" and @autocomplete="on" and '
                                                      '@type="text" and @data-testid="ocfEnterTextTextInput"]')
            time.sleep(random.uniform(1, 3))
            email_input.send_keys(self.email)
            time.sleep(random.uniform(0, 1))
            # 按回车键
            email_input.send_keys(Keys.RETURN)

            # 输入密码
            password_input = self.find_element(By.XPATH, '//input[@name="password" and @type="password"]')
            time.sleep(random.uniform(1, 3))
            password_input.send_keys(self.password)
            time.sleep(random.uniform(0, 1))
            # 按回车键
            password_input.send_keys(Keys.RETURN)

            # 等待浏览器页面变成home页面
            logging.info("Waiting for the home pages to load...")
            WebDriverWait(self.driver, self.interaction_timeout).until(
                EC.url_to_be('https://x.com/home')
            )
        else:
            logging.info("需人工介入")
            self.driver.save_screenshot('screenshot.png')
            time.sleep(99999999)

        self.save_cookies()
        logging.info(f"login successfully")

    def search(self):
        self.driver.get('https://twitter.com/home')
        search_input = self.find_element(By.XPATH, '//input[@aria-label="Search query"]')
        time.sleep(random.uniform(0, 3))
        search_input.send_keys(self.search_key_word)
        time.sleep(random.uniform(0, 3))
        search_input.send_keys(Keys.RETURN)

    def get_top_n_posts(self, n):
        tweets = []
        scroll_attempts = 0
        max_scroll_attempts = 30

        while len(tweets) < n and scroll_attempts < max_scroll_attempts:
            tweet_elements = self.driver.find_elements(By.XPATH,
                                                       '//div[@data-testid="cellInnerDiv"]//article[@role="article"]')
            logging.info(f"Scroll attempt {scroll_attempts + 1}: Found {len(tweet_elements)} tweets")

            for tweet in tweet_elements:
                if tweet not in tweets:
                    tweets.append(tweet)
                    if len(tweets) >= n:
                        break

            if len(tweets) < n:
                self.scroll_page()
                scroll_attempts += 1
                time.sleep(2)
            else:
                break
        logging.info(f"Final: Found {len(tweets)} tweets.")
        return self.filter_posts(tweets)

    def collect_comments_and_user_data(self, max_comments=50):
        comments_data = []
        comments_collected = 0
        scroll_attempts = 0
        max_scroll_attempts = 10

        while comments_collected < max_comments and scroll_attempts < max_scroll_attempts:
            comments = self.driver.find_elements(By.XPATH, '//article[@role="article" and @data-testid="tweet"]')
            for comment in comments:
                if comments_collected >= max_comments:
                    break
                try:
                    # 获取用户名称
                    user_element = comment.find_element(By.XPATH, './/div[@data-testid="User-Name"]')
                    user_name = user_element.find_element(By.XPATH, './/span').text

                    # 获取评论内容
                    content = comment.find_element(By.XPATH, './/div[@data-testid="tweetText"]').text

                    comments_data.append({
                        'user_name': user_name,
                        'content': content
                    })
                    comments_collected += 1
                except Exception as error:
                    logging.info(f"failed: {str(error).splitlines()[0]}")

            if comments_collected < max_comments:
                self.scroll_page()
                scroll_attempts += 1
            logging.info(f"comments_collected: {comments_collected}")
            for comment in comments_data:
                logging.info(comment)
        logging.info(f"Collected {comments_collected} comments.")
        return comments_data

    def scroll_page(self):
        try:
            scroll_pause_time = random.uniform(1, 3)  # 随机暂停时间
            scroll_distance = random.randint(500, 1500)  # 随机滚动距离

            self.driver.execute_script(f"window.scrollBy(0, {scroll_distance});")
            time.sleep(scroll_pause_time)
            logging.info(f"Scrolled the pages by {scroll_distance} pixels and paused for {scroll_pause_time} seconds.")
        except Exception as e:
            logging.info(f"Failed to scroll the pages: {e}")

    def scroll_to_top(self):
        try:
            scroll_pause_time = random.uniform(1, 3)  # 随机暂停时间

            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(scroll_pause_time)
            logging.info(f"Scrolled to the top of the pages and paused for {scroll_pause_time} seconds.")
        except Exception as e:
            logging.info(f"Failed to scroll to the top of the pages: {e}")

    def filter_posts(self, tweets):
        # 预留的推特过滤函数
        # 在这里添加你的推特过滤逻辑
        return tweets

    def filter_comment(self, user, content):
        # 预留的评论过滤函数
        # 在这里添加你的评论过滤逻辑
        return True

    def enter_post(self, post_element):
        """
        进入推特内容详情
        :return:
        """
        # 获取推文元素的尺寸
        width = post_element.size['width']
        height = post_element.size['height']

        # 使用 JavaScript 在推文元素的右上角触发点击事件
        self.driver.execute_script("""
                          var element = arguments[0];
                          var x = arguments[1];
                          var y = arguments[2];
                          var clickEvent = new MouseEvent('click', {
                              view: window,
                              bubbles: true,
                              cancelable: true,
                              clientX: x,
                              clientY: y
                          });
                          element.dispatchEvent(clickEvent);
                      """, post_element, width - 1, 1)  # 右上角的坐标 (width-1, 1)

    def check_login_status(self):
        """
        检查登录状态
        :return:
        """
        try:
            self.setup_driver()
            # 检查是否存在 cookies 文件
            if os.path.exists(self.cookies_file) and not self.force_re_login:
                try:
                    self.driver.get('https://twitter.com/home')
                    self.load_cookies()
                    self.driver.refresh()
                except Exception as error:
                    logging.info(error)
                    logging.info("Cookies are invalid, clearing cookies and re-login.")
                    self.driver.delete_all_cookies()
                    self.login()
            else:
                self.login()
            time.sleep(10)
            logging.info("Waiting for the home pages to load...")
            logging.info(self.driver.current_url)
            WebDriverWait(self.driver, self.interaction_timeout).until(
                EC.url_to_be('https://x.com/home')
            )
            logging.info("success")
            self.teardown_driver()
            return "Available"
        except Exception as error:
            logging.info(error)
            self.teardown_driver()
            return "Unavailable"

    def run(self, max_post_num: int, access_code: str):
        """
        运行程序
        :param max_post_num:
        :param access_code:
        :return:
        """
        try:
            self.setup_driver()
            # 检查是否存在 cookies 文件
            if os.path.exists(self.cookies_file) and not self.force_re_login:
                try:
                    self.driver.get('https://twitter.com/home')
                    self.load_cookies()
                    self.driver.refresh()
                    time.sleep(3)
                except Exception as error:
                    logging.info(error)
                    logging.info("Cookies are invalid, clearing cookies and re-login.")
                    self.driver.delete_all_cookies()
                    self.login()
            else:
                self.login()
            # 混淆: 随机等待时间
            time.sleep(random.uniform(1, 3))

            # 再次检查是否需要登录
            if "home" in self.driver.current_url:
                logging.info("success")
            else:
                raise Exception(f"login failed: {self.driver.current_url}")

            # 搜索关键字的推特
            self.search()

            # 搜索出的推特进行遍历

            for index in range(1, max_post_num+1):
                logging.info(f"checking post [{index}] >>>>>>>>>>>>>>>>>>>")
                # 获取前N个推特
                top_n_posts = self.get_top_n_posts(index)
                check_post = top_n_posts[index-1]
                try:
                    self.enter_post(check_post)
                    time.sleep(3)  # 等待页面加载
                    data = self.collect_comments_and_user_data()
                    self.driver.back()
                    time.sleep(3)  # 等待页面返回
                except Exception as e:
                    logging.info(f"Failed to process tweet: {e}")
                    data = []
                if data:
                    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')
                    filename = f"{self.search_key_word}_{current_time}_{len(data)}_{self.username}.csv"
                    process_and_upload_csv_to_cos(data, f"./{filename}", f"{access_code}/{filename}")
                else:
                    pass
                # 返回顶端
                self.scroll_to_top()
        finally:
            self.teardown_driver()


CHROME_DRIVER = './chromedriver'


def collect_data_from_x(username: str, email: str, password: str, search_key_word: str, max_post_num: int,
                        access_code: str):
    """
    从X收集数据，并缓存到一个cvs文件
    :param access_code:
    :param password:
    :param email:
    :param username:
    :param search_key_word:
    :param max_post_num:
    :return:
    """
    logging.info("start collecting data.")
    watcher = TwitterWatcher(CHROME_DRIVER, username, email, password, search_key_word)
    watcher.run(max_post_num, access_code)
    logging.info("done collecting data.")


def check_service_status(username: str, email: str, password: str):
    """
    检查当前服务的状态，账号是否能正常登录
    :return:
    """
    watcher = TwitterWatcher(CHROME_DRIVER, username, email, password, "cat")
    logging.info("health checking...")
    return watcher.check_login_status()