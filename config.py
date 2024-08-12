import json
import os


def load_config():
    # 获取当前目录
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # 配置文件路径
    config_file_path = os.path.join(current_dir, 'config.json')

    # 读取配置文件
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)

    return config


# 加载配置
CONFIG = load_config()
