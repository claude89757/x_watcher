#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/11 15:49
@Author  : claude
@File    : utils.py
@Software: PyCharm
"""

import csv
import os


def save_comments_to_csv(comments_data: list, file_name: str):
    """
    Save a list of comment data to a CSV file.

    Parameters:
    - comments_data (list of dict): List containing comments data where each dict has 'user_name' and 'content' keys.
    - file_name (str): The name of the CSV file to save the data to.
    """
    print(f"save {len(comments_data)} data to {file_name}")
    # Ensure the directory exists (in case file_name includes a directory path)
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    # Write data to CSV
    with open(file_name, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['user_name', 'content'])

        # Write the header if the file is new
        if file.tell() == 0:
            writer.writeheader()

        # Write the comment data 
        writer.writerows(comments_data)


def load_comments_from_csv(file_name: str):
    """
    Load comments data from a CSV file.

    Parameters:
    - file_name (str): The name of the CSV file to load the data from.

    Returns:
    - list of dict: A list containing comments data where each dict has 'user_name' and 'content' keys.
    """
    comments_data = []

    if os.path.isfile(file_name):
        with open(file_name, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                comments_data.append(row)

    return comments_data
