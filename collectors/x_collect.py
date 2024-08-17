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
    def __init__(self, driver_path, username, email, password, search_key_word='cat', timeout=10, headless: bool = True,
                 force_re_login: bool = False):
        self.driver_path = driver_path
        self.username = username
        self.email = email
        self.password = password
        self.search_key_word = search_key_word
        self.timeout = timeout
        self.interaction_timeout = 600
        self.cookies_file = f'{username}_cookies.pkl'
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
            # 添加 --no-sandbox 选项
        chrome_options.add_argument("--no-sandbox")
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
            raise Exception("auto login failed!")

        self.save_cookies()
        logging.info(f"login successfully")

    def search(self):
        self.driver.get('https://twitter.com/home')
        search_input = self.find_element(By.XPATH, '//input[@aria-label="Search query"]')
        time.sleep(random.uniform(0, 3))
        search_input.send_keys(self.search_key_word)
        time.sleep(random.uniform(0, 3))
        search_input.send_keys(Keys.RETURN)

    def old_get_top_n_posts(self, n):
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

    def get_top_n_posts(self, n):
        tweets = []
        seen_links = set()
        scroll_attempts = 0
        max_scroll_attempts = 30

        while len(tweets) < n and scroll_attempts < max_scroll_attempts:
            tweet_elements = self.driver.find_elements(By.XPATH,
                                                       '//div[@data-testid="cellInnerDiv"]//article[@role="article"]')
            logging.info(f"Scroll attempt {scroll_attempts + 1}: Found {len(tweet_elements)} tweets")

            for tweet in tweet_elements:
                if len(tweets) >= n:
                    break

                try:
                    xpath = './/a[@href and @role="link" and contains(@href, "/status/")]'
                    tweet_link = tweet.find_element(By.XPATH, xpath).get_attribute('href')
                except:
                    tweet_link = None

                if not tweet_link or tweet_link in seen_links:
                    continue

                try:
                    tweet_text = tweet.find_element(By.XPATH, './/div[@data-testid="tweetText"]').text
                except:
                    tweet_text = None

                try:
                    tweet_author = tweet.find_element(By.XPATH, './/div[@data-testid="User-Name"]//span').text
                except:
                    tweet_author = None

                try:
                    tweet_time = tweet.find_element(By.XPATH, './/time').get_attribute('datetime')
                except:
                    tweet_time = None

                try:
                    xpath = './/div[contains(@aria-label, "replies") and contains(@aria-label, "reposts")]'
                    tweet_stats = tweet.find_element(By.XPATH, xpath)

                    try:
                        replies = tweet_stats.\
                            find_element(By.XPATH, './/button[@data-testid="reply"]/div/div[2]/span/span/span').text
                    except:
                        replies = "0"

                    try:
                        reposts = tweet_stats.\
                            find_element(By.XPATH, './/button[@data-testid="retweet"]/div/div[2]/span/span/span').text
                    except:
                        reposts = "0"

                    try:
                        likes = tweet_stats.\
                            find_element(By.XPATH, './/button[@data-testid="like"]/div/div[2]/span/span/span').text
                    except:
                        likes = "0"

                    try:
                        views = tweet_stats.\
                            find_element(By.XPATH, './/a[contains(@aria-label, "views")]//span').text
                    except:
                        views = "0"

                except:
                    replies = reposts = likes = views = "0"

                tweet_data = {
                    'time': tweet_time,
                    'link': tweet_link,
                    'text': tweet_text,
                    'author': tweet_author,
                    'replies': replies,
                    'reposts': reposts,
                    'likes': likes,
                    'views': views
                }

                seen_links.add(tweet_link)
                tweets.append(tweet_data)

            if len(tweets) < n:
                self.scroll_page()
                scroll_attempts += 1
                time.sleep(random.uniform(0, 1))
            else:
                break
        
        if tweets:
            pass
        else:
            logging.error("saving screenshot....")
            self.driver.save_screenshot(f"./saved_screenshots/{self.username}_nothing_error.png")
        logging.info(f"Final: Found {len(tweets)} tweets.")
        return tweets

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
                    try:
                        # Locate the reply user link (tweet status URL)
                        xpath = './/a[@href and @role="link" and contains(@href, "/status/")]'
                        reply_user_link = comment.find_element(By.XPATH, xpath).get_attribute('href')

                        # 获取用户名称
                        user_element = comment.find_element(By.XPATH, './/div[@data-testid="User-Name"]')
                        reply_user_name = user_element.find_element(By.XPATH, './/span').text

                        # 获取评论内容
                        reply_content = comment.find_element(By.XPATH, './/div[@data-testid="tweetText"]').text

                        comments_data.append({
                            'reply_user_link': reply_user_link,
                            'reply_user_name': reply_user_name,
                            'reply_content': reply_content
                        })
                    except Exception as e:
                        logging.warning(f"An error occurred while locating the reply user link: {e}")

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

    def enter_post_url(self, post_url: str):
        """
        进入推特的链接, 等待页面完全加载完成
        :param post_url:
        :return:
        """
        self.driver.get(post_url)
        # Wait for the page to load completely
        try:
            # Wait until the tweet content is visible
            WebDriverWait(self.driver, self.timeout).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "article"))
            )
            print(f"{post_url} Page loaded successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")

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
            for index in range(3):
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
                    logging.info("first time login...")
                    self.login()
                # 混淆: 随机等待时间
                time.sleep(random.uniform(1, 3))

                # 再次检查是否需要登录
                if "home" in self.driver.current_url:
                    break
                else:
                    # 再试一次
                    logging.warning(f"{index} time login failed, try again...")

            # 检查是否登录成功
            if "home" in self.driver.current_url:
                logging.info("login successfully")
                return True
            else:
                # 再试一次
                current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')
                self.driver.save_screenshot(f"login_failed_page_{current_time}.png")
                return False
        except Exception as error:
            logger.error(f"login failed: {error}")
            current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')
            self.driver.save_screenshot(f"login_failed_page_{current_time}.png")
            return False


    def auto_login(self):
        self.setup_driver()
        for index in range(2):
            # 检查是否存在 cookies 文件
            if os.path.exists(self.cookies_file) and not self.force_re_login:
                try:
                    self.driver.get('https://twitter.com/home')
                    self.load_cookies()
                    self.driver.refresh()
                except Exception as error:
                    logger.info(error)
                    logger.info("Cookies are invalid, clearing cookies and re-login.")
                    self.driver.delete_all_cookies()
                    self.login()
            else:
                self.login()
                
                try:
                    # 使用 WebDriverWait 等待页面加载完成
                    WebDriverWait(self.driver, 10).until(
                        EC.url_contains("home")
                    )
                    logger.info("found home page")
                except Exception as error:
                    # 再试一次
                    logger.warning(error)
                    logger.warning(f"{index} time login failed, try again...")
            # 混淆: 随机等待时间
            time.sleep(random.uniform(0, 1))

        # 再检查是否登录成功
        if "home" in self.driver.current_url:
            logger.info("login successfully")
        else:
            current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')
            self.driver.save_screenshot(f"login_failed_page_{current_time}.png")
            raise Exception(f"login failed: {self.driver.current_url}")

    def run(self, max_post_num: int, access_code: str):
        """
        运行程序
        :param max_post_num:
        :param access_code:
        :return:
        """
        try:
            # 自动登录
            self.auto_login()

            # 搜索关键字的推特
            self.search()

            # 获取前N个推特
            top_n_posts = self.get_top_n_posts(max_post_num)

            # 遍历每个推特的链接
            data_list = []
            for post in top_n_posts:
                try:
                    print(f"checking {post}")
                    self.enter_post_url(post['link'])
                    time.sleep(random.uniform(0, 0.5))
                    try:
                        replies_str = str(post['replies']).strip().upper()  # 确保字符串是大写并去除空格
                        if replies_str.endswith("M"):
                            max_comments = int(replies_str.strip("M")) * 1000 * 1000
                        elif replies_str.endswith("K"):
                            max_comments = int(replies_str.strip("K")) * 1000
                        elif replies_str.endswith("B"):
                            max_comments = int(replies_str.strip("B")) * 1000 * 1000 * 1000
                        elif replies_str.endswith("T"):
                            max_comments = int(replies_str.strip("T")) * 1000 * 1000 * 1000 * 1000
                        elif replies_str.endswith("+"):
                            max_comments = int(replies_str.strip("+"))  # 处理带有加号的情况
                        elif replies_str.endswith(" "):
                            max_comments = int(replies_str.strip())  # 处理带有空格的情况
                        else:
                            max_comments = int(replies_str)
                    except:
                        max_comments = 100
                    data = self.collect_comments_and_user_data(max_comments=max_comments)
                    time.sleep(random.uniform(0, 0.5))
                except Exception as e:
                    logging.info(f"Failed to process tweet: {e}")
                    data = []

                if data:
                    for item in data:
                        item['post_time'] = post['time']
                        item['post_link'] = post['link']
                        item['post_author'] = post['author']
                        item['post_replies'] = post['replies']
                        item['post_reposts'] = post['reposts']
                        item['post_views'] = post['views']
                    data_list.extend(data)
                else:
                    pass
            if data_list:
                logging.info(f"uploading {len(data_list)} to cos")
                current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')
                filename = f"{self.search_key_word}_{current_time}_{len(data_list)}_{self.username}.csv"
                process_and_upload_csv_to_cos(data_list, f"./{filename}", f"{access_code}/{filename}")
            else:
                self.driver.save_screenshot(f"{self.username}_nothings_{current_time}.png")
                logging.error("found nothinng...")
        finally:
            self.teardown_driver()

    def collect_user_link_detail(self, user_id_list: list):
        """
        收集x用户的首页数据
        :param user_id_list:
        :return:
        """
        data = []
        try:
            # 自动登录
            self.auto_login()

            for user_id in user_id_list:
                # 进入推特用户主页
                to_user_url = f"https://x.com/{user_id}"
                try:
                    logging.info(f"loading {to_user_url}")
                    self.driver.get(to_user_url)
                    WebDriverWait(self.driver, self.timeout).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, "article"))
                    )
                    logging.info(f"{to_user_url} Page loaded successfully.")
                    page_loaded = "yes"
                except Exception as error:
                    self.driver.save_screenshot(f"./saved_screenshots/{user_id}_error.png")
                    page_loaded = "no"

                # 显式等待私信按钮出现
                try:
                    self.driver.find_element(By.XPATH, '//button[@data-testid="sendDMFromProfile"]')
                    enable_dm = "yes"
                except:
                    enable_dm = "no"

                # 获取用户加入推特的时间
                try:
                    user_join_date = self.driver.find_element(By.XPATH, './/span[@data-testid="UserJoinDate"]').text
                except:
                    user_join_date = ""

                # Location
                try:
                    user_location = self.driver.find_element(By.XPATH, './/span[@data-testid="UserLocation"]').text
                except:
                    user_location = ""

                # 获取用户推特简介
                try:
                    user_description = self.driver.find_element(By.XPATH, './/div[@data-testid="UserDescription"]').text
                except:
                    user_description = ""

                # 获取关注数量
                try:
                    following_count = self.driver.find_element(By.XPATH,
                                                               './/a[contains(@href, "/following")]//span[1]').text
                except:
                    following_count = ""

                # 获取粉丝数量
                try:
                    followers_count = self.driver\
                        .find_element(By.XPATH, './/a[contains(@href, "/verified_followers")]//span[1]').text
                except:
                    followers_count = ""

                data.append({
                    "reply_user_id": user_id,
                    "page_loaded": page_loaded,
                    "enable_dm": enable_dm,
                    "user_join_date": user_join_date,
                    "user_location": user_location,
                    "user_description": user_description,
                    "following_count": following_count,
                    "followers_count": followers_count,
                })

                # 反爬虫检测机制：随机关注用户
                try:
                    # 随机决定是否关注用户
                    if random.random() < 0.2:  # 10% 的概率
                        # 等待关注按钮可点击
                        follow_button = self.driver.find_elements(By.XPATH,
                                                                  f'//button[@aria-label="Follow @{user_id}"]')
                        # 点击关注按钮
                        follow_button.click()
                        logger.info("Follow button clicked.")
                    else:
                        logger.info("Decided not to follow the user this time.")
                except Exception as e:
                    logger.warning("Error:", e)
                time.sleep(random.uniform(0, 1))
            return data
        finally:
            self.teardown_driver()

    def send_msg_to_user(self, to_user_url: str, msg: str):
        """
        发送私信
        :param to_user_url:
        :param msg:
        :return:
        """
        try:
            # 自动登录
            self.auto_login()

            # 进入推特用户主页
            try:
                logging.info(f"loading {to_user_url}")
                self.driver.get(to_user_url)
                WebDriverWait(self.driver, self.timeout).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "article"))
                )
                logging.info(f"{to_user_url} Page loaded successfully.")
            except Exception as error:
                self.driver.save_screenshot(f"./saved_screenshots/{to_user_url.split('/')[-1]}_error.png")
                error_message = traceback.format_exc()
                logger.error(error)
                logger.error(error_message)
                return "load user link failed"

            # 显式等待私信按钮出现
            try:
                wait = WebDriverWait(self.driver, self.timeout)
                dm_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, '//button[@data-testid="sendDMFromProfile"]')))
                dm_button.click()
                time.sleep(random.uniform(0, 3))
                logging.info("send msg button loaded")
            except Exception as error:
                self.driver.save_screenshot(f"./saved_screenshots/{to_user_url.split('/')[-1]}_error.png")
                error_message = traceback.format_exc()
                logger.error(error)
                logger.error(error_message)
                return "No found DM button"

            # 点击输入框
            try:
                logging.info("sending msg...")
                dm_input = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//div[@data-testid="dmComposerTextInput"]')))
                time.sleep(random.uniform(0, 3))
                dm_input.click()
                logger.info(f"click input button.")
            except Exception as error:
                self.driver.save_screenshot(f"./saved_screenshots/{to_user_url.split('/')[-1]}_error.png")
                error_message = traceback.format_exc()
                logger.error(error)
                logger.error(error_message)
                return "No found DM input"
            
            # 输入消息并回车
            try:
                time.sleep(random.uniform(0, 3))
                def remove_non_bmp_characters(text):
                    # 过滤掉不在 BMP 范围内的字符
                    return re.sub(r'[^\u0000-\uFFFF]', '', text)
                # 过滤掉不支持的字符
                filtered_message = remove_non_bmp_characters(msg)
                dm_input.send_keys(filtered_message)
                logger.info(f"input msg: {msg}")
                time.sleep(random.uniform(0, 3))
                logger.info(f"enter to send.")
                dm_input.send_keys(Keys.RETURN)
                time.sleep(random.uniform(0, 3))
                return "Success"
            except Exception as error:
                self.driver.save_screenshot(f"./saved_screenshots/{to_user_url.split('/')[-1]}_error.png")
                error_message = traceback.format_exc()
                logger.error(error)
                logger.error(error_message)
                return "Send DM error"
        finally:
            self.teardown_driver()


CHROME_DRIVER = '/usr/local/bin/chromedriver'


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


# Test
if __name__ == '__main__':
    from common.config import CONFIG
    username = "stephen__gzhh"
    email = CONFIG['x_collector_account_infos'][username]['email']
    password = CONFIG['x_collector_account_infos'][username]['password']
    watcher = TwitterWatcher('/usr/local/bin/chromedriver', username, email, password, headless=False)
    watcher.check_login_status()
