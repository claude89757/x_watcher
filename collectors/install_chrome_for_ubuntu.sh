#!/bin/bash

echo "开始安装 Google Chrome 和 ChromeDriver"

# 更新系统包
echo "更新系统包"
sudo apt update && sudo apt upgrade -y

# 添加Google Chrome的APT仓库
echo "添加 Google Chrome 的 APT 仓库"
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
sudo sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'

# 更新包列表
sudo apt update

# 安装Google Chrome
echo "安装 Google Chrome"
sudo apt install -y google-chrome-stable

# 获取已安装的Google Chrome版本
GOOGLE_CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+\.\d+')
echo "已安装的 Google Chrome 版本: $GOOGLE_CHROME_VERSION"

# 获取主要版本号
MAJOR_VERSION=$(echo $GOOGLE_CHROME_VERSION | cut -d. -f1)
echo "Google Chrome 主要版本号: $MAJOR_VERSION"

# 获取最新的稳定版Chrome版本
LATEST_VERSION=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json \
  | grep -oP '"Stable"\s*:\s*\{[^}]*"version"\s*:\s*"\K[^"]+')
echo "最新的稳定版Chrome版本: $LATEST_VERSION"

# 下载匹配的ChromeDriver版本
echo "下载ChromeDriver版本 $LATEST_VERSION"
wget -N "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$LATEST_VERSION/linux64/chromedriver-linux64.zip" -O chromedriver_linux64.zip

# 解压ChromeDriver
echo "解压 ChromeDriver"
unzip -o chromedriver_linux64.zip -d ./

# 移动ChromeDriver到/usr/local/bin并赋予执行权限
echo "移动 ChromeDriver 到 /usr/local/bin 并赋予执行权限"
sudo mv ./chromedriver-linux64/chromedriver /usr/local/bin/
sudo chmod +x /usr/local/bin/chromedriver

# 清理临时文件
echo "清理临时文件"
rm -rf chromedriver-linux64 chromedriver_linux64.zip

# 显示安装的版本
echo "安装完成，显示版本信息"
CHROMEDRIVER_INSTALLED_VERSION=$(chromedriver --version)
CHROME_INSTALLED_VERSION=$(google-chrome --version)
echo "ChromeDriver 版本: $CHROMEDRIVER_INSTALLED_VERSION"
echo "Google Chrome 版本: $CHROME_INSTALLED_VERSION"

echo "安装过程完成"
