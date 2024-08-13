#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/12 19:11
@Author  : claudexie
@File    : cos.py
@Software: PyCharm
"""
import os
import csv
import logging

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

from common.config import CONFIG


def upload_file(local_file_path, object_key):
    """
    上传本地文件到腾讯云COS。

    :param local_file_path: 本地文件路径。
    :param object_key: 存储在COS中的对象键（文件名）。
    :return: 上传响应。
    :raises Exception: 如果上传失败，将抛出异常。
    """
    region = CONFIG['cos_region']
    bucket_name = CONFIG['cos_name']
    secret_id = CONFIG['tencent_secret_id']
    secret_key = CONFIG['tencent_secret_key']

    # 配置腾讯云COS客户端
    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region)
    cos_client = CosS3Client(config)

    try:
        # 执行文件上传操作
        response = cos_client.upload_file(
            Bucket=bucket_name,
            LocalFilePath=local_file_path,
            Key=object_key
        )
        logging.info(f"Upload successful: {response}")
        return response
    except Exception as e:
        logging.error(f"Upload failed: {e}")
        raise


def download_file(object_key, local_file_path):
    """
    从腾讯云COS下载文件到本地。
    :param object_key: 存储在COS中的对象键（文件名）。
    :param local_file_path: 本地文件保存路径。
    :return: 下载响应。
    :raises Exception: 如果下载失败，将抛出异常。
    """
    region = CONFIG['cos_region']
    bucket_name = CONFIG['cos_name']
    secret_id = CONFIG['tencent_secret_id']
    secret_key = CONFIG['tencent_secret_key']

    # 配置腾讯云COS客户端
    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region)
    cos_client = CosS3Client(config)

    try:
        # 获取对象并将其保存到本地文件
        response = cos_client.get_object(
            Bucket=bucket_name,
            Key=object_key
        )
        with open(local_file_path, 'wb') as f:
            f.write(response['Body'].get_raw_stream().read())
        logging.info(f"Download successful: {response}")
        return response
    except Exception as e:
        logging.error(f"Download failed: {e}")
        raise


def process_and_upload_csv_to_cos(data, local_file_path, object_key):
    """
    将包含字典的列表数据保存为CSV文件，上传到腾讯云COS，并删除本地CSV文件。

    :param data: 包含字典的列表数据。
    :param local_file_path: 本地CSV文件路径。
    :param object_key: 存储在COS中的对象键（包括目录模拟路径）。
    :return: 上传响应。
    :raises Exception: 如果任何步骤失败，将抛出异常。
    """
    region = CONFIG['cos_region']
    bucket_name = CONFIG['cos_name']
    secret_id = CONFIG['tencent_secret_id']
    secret_key = CONFIG['tencent_secret_key']

    # 创建CSV文件
    if not data:
        raise ValueError("Data list is empty.")

    fieldnames = data[0].keys()

    try:
        with open(local_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        logging.info(f"Data successfully saved to CSV: {local_file_path}")

        # 上传到COS
        config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region)
        cos_client = CosS3Client(config)
        try:
            response = cos_client.upload_file(
                Bucket=bucket_name,
                LocalFilePath=local_file_path,
                Key=object_key
            )
            logging.info(f"Upload successful: {response}")
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            raise

        # 删除本地CSV文件
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            logging.info(f"Local file deleted: {local_file_path}")
        else:
            logging.warning(f"Local file does not exist: {local_file_path}")

        return response

    except Exception as e:
        logging.error(f"Failed to process and upload CSV: {e}")
        raise


def list_latest_files(prefix, max_keys=100, latest_count=30):
    """
    查询腾讯云COS中指定前缀下的最新文件列表，返回最新的指定数量的文件。

    :param prefix: 对象键前缀（类似于文件夹路径）。
    :param max_keys: 每次请求返回的最大对象数量。
    :param latest_count: 返回的最新文件数量。
    :return: 最新的文件列表。
    :raises Exception: 如果查询失败，将抛出异常。
    """
    region = CONFIG['cos_region']
    bucket_name = CONFIG['cos_name']
    secret_id = CONFIG['tencent_secret_id']
    secret_key = CONFIG['tencent_secret_key']

    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region)
    cos_client = CosS3Client(config)

    file_details = []
    try:
        while True:
            # 执行对象列表查询
            response = cos_client.list_objects(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys,
                EncodingType='url'
            )
            contents = response.get('Contents', [])
            file_details.extend(contents)

            # 如果存在更多文件，更新 Marker 以进行分页
            if 'NextMarker' in response:
                marker = response['NextMarker']
            else:
                break

        # 按最后修改时间排序并取最新的指定数量文件
        file_details_sorted = sorted(file_details, key=lambda x: x['LastModified'], reverse=True)
        latest_files = file_details_sorted[:latest_count]
        file_keys = [obj['Key'] for obj in latest_files]

        logging.info(f"Latest {latest_count} files listed under prefix '{prefix}': {file_keys}")
        return file_keys
    except Exception as e:
        logging.error(f"Failed to list files: {e}")
        raise


# 测试用例
if __name__ == '__main__':
    # 查询指定前缀下的最新30个文件
    try:
        print("Listing latest files in prefix...")
        latest_files = list_latest_files(prefix='zacks')
        print(f"Latest files: {latest_files}")
    except Exception as e:
        print(f"Failed to list files: {e}")
