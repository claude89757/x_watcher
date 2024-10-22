#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/13 22:37
@Author  : claude
@File    : sidebar.py
@Software: PyCharm
"""
import streamlit as st
import os


def sidebar_home():
    try:
        st.sidebar.subheader("构建中")
    except:
        st.write("...")


def sidebar_for_x():
    pass

def sidebar_for_tiktok():
    pass
        
