#!/bin/bash

# 更新系统包
sudo yum update -y

# 添加Google Chrome的YUM仓库
cat <<EOF | sudo tee /etc/yum.repos.d/google-chrome.repo
[google-chrome]
name=google-chrome
baseurl=https://dl.google.com/linux/chrome/rpm/stable/x86_64
enabled=1
gpgcheck=1
gpgkey=https://dl-ssl.google.com/linux/linux_signing_key.pub
EOF

# 安装Google Chrome
sudo yum install -y google-chrome-stable

# 获取已安装的Google Chrome版本
GOOGLE_CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+\.\d+')

# 获取主要版本号
MAJOR_VERSION=$(echo $GOOGLE_CHROME_VERSION | cut -d. -f1)

# 获取对应的ChromeDriver版本
CHROMEDRIVER_VERSION=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$MAJOR_VERSION")

# 如果无法获取特定版本，则获取最新版本
if [ -z "$CHROMEDRIVER_VERSION" ]; then
    CHROMEDRIVER_VERSION=$(curl -s https://chromedriver.storage.googleapis.com/LATEST_RELEASE)
fi

# 下载对应版本的ChromeDriver
wget -N "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip" -O chromedriver_linux64.zip

# 解压ChromeDriver
unzip -o chromedriver_linux64.zip -d chromedriver_linux64

# 移动ChromeDriver到/usr/local/bin并赋予执行权限
sudo mv chromedriver_linux64/chromedriver /usr/local/bin/
sudo chmod +x /usr/local/bin/chromedriver

# 清理临时文件
rm -rf chromedriver_linux64 chromedriver_linux64.zip

# 显示安装的版本
chromedriver --version
google-chrome --version
