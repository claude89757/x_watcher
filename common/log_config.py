import logging
import os


def setup_logger(name=None, log_file=None, level=logging.INFO):
    """
    设置日志配置
    :param name: logger名称，如果为None，则配置根logger
    :param log_file: 日志文件路径。如果为None，则根据脚本名称自动生成
    :param level: 日志级别
    :return: 配置好的logger
    """
    # 创建logs目录
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 自动根据脚本名称生成日志文件名并放在logs目录下
    if log_file is None:
        script_name = os.path.basename(__file__).split('.')[0]  # 获取脚本名，不包括扩展名
        log_file = os.path.join(log_dir, f"{script_name}.log")

    logger = logging.getLogger(name)

    # 检查是否已经有handler，避免重复添加
    if not logger.hasHandlers():
        logger.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        )

        # 文件处理器
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
