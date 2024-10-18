import streamlit as st
import pandas as pd
from common.openai import process_with_gpt
from collectors.common.mysql import MySQLDatabase
import time
import json
import os
import re

# 定义缓存文件路径
DESCRIPTION_CACHE_FILE = 'tiktok_description_cache.json'
MESSAGES_CACHE_FILE = 'tiktok_messages_cache.json'

def load_descriptions_from_cache(keyword):
    """从缓存文件加载描述"""
    if os.path.exists(DESCRIPTION_CACHE_FILE):
        with open(DESCRIPTION_CACHE_FILE, 'r') as f:
            cache = json.load(f)
            return cache.get(keyword, None)
    return None

def load_messages_from_cache(keyword):
    """从缓存文件加载消息"""
    if os.path.exists(MESSAGES_CACHE_FILE):
        with open(MESSAGES_CACHE_FILE, 'r') as f:
            cache = json.load(f)
            return cache.get(keyword, {})
    return {}

def save_messages_to_cache(keyword, messages):
    """将消息保存到缓存文件"""
    cache = {}
    if os.path.exists(MESSAGES_CACHE_FILE):
        with open(MESSAGES_CACHE_FILE, 'r') as f:
            cache = json.load(f)
    cache[keyword] = messages
    with open(MESSAGES_CACHE_FILE, 'w') as f:
        json.dump(cache, f)

def generate_messages(model, prompt, product_info, user_comments_str, additional_prompt):
    """使用选定的GPT模型为多个用户生成个性化消息"""
    try:
        # 将 user_comments 作为一个整体字符串传入，而不是尝试格式化它
        formatted_prompt = prompt.replace("{user_comments}", user_comments_str)
    except Exception as e:
        st.error(f"格式化 prompt 时出错: {e}。请检查 prompt 模板中的占位符是否正确。")
        return {}
    
    response = process_with_gpt(model, formatted_prompt, max_tokens=5000)
    try:
        # 尝试直接解析 JSON
        messages = json.loads(response.strip())
        if not isinstance(messages, dict):
            raise ValueError("GPT 响应不是有效的 JSON 对象")
        return messages
    except (json.JSONDecodeError, ValueError) as e:
        st.error(f"解析 GPT 响应时出错: {e}")
        # 尝试提取 JSON 部分
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            try:
                messages = json.loads(json_match.group())
                if isinstance(messages, dict):
                    return messages
            except json.JSONDecodeError:
                pass
        st.error("无法从 GPT 响应中提取有效的 JSON。请检查 prompt 或重试。")
        return {}

def generate_msg(db: MySQLDatabase):
    """
    生成推广信息给高意向客户
    """
    st.info("AI生成推广文案, 自动批量关注、留言、发送推广信息给高意向客户")
    
    # 获取所有关键词
    keywords = db.get_all_tiktok_keywords()
    
    # 创建下拉框让用户选择关键字，使用session_state中的cached_keyword作为默认值
    selected_keyword = st.selectbox("关键词", keywords, 
                                    index=keywords.index(st.session_state.cached_keyword) if st.session_state.cached_keyword in keywords else 0)
    
    # 获取高意向客户数据
    high_intent_customers = db.get_second_round_analyzed_comments(selected_keyword)
    high_intent_df = pd.DataFrame(high_intent_customers)
    
    if high_intent_df.empty:
        st.warning(f"未找到关键词 '{selected_keyword}' 的高意向客户。请先进行评论分析或选择其他关键词。")
        return  # 提前结束函数
    
    # 根据可用的列筛选高意向客户
    if 'second_round_classification' in high_intent_df.columns:
        high_intent_df = high_intent_df[high_intent_df['second_round_classification'] == '高意向客户']
    else:
        st.warning("无法找到用于筛选高意向客户的列。显示所有客户数据。")
    
    st.success(f"{len(high_intent_df)} 个高意向客户")
    
    # 显示高意向客户数据
    display_columns = ['user_id', 'reply_content']
    if 'analysis_reason' in high_intent_df.columns:
        display_columns.append('analysis_reason')
    st.dataframe(high_intent_df[display_columns])
    
    # 选择模型
    model = st.selectbox("选择GPT模型", ["gpt-4o-mini", "gpt-4o"])
    
    # 从缓存加载产品描述
    cached_descriptions = load_descriptions_from_cache(selected_keyword)
    default_product_info = cached_descriptions['product_description'] if cached_descriptions else ""
    default_additional_prompt = cached_descriptions['customer_description'] if cached_descriptions else ""

    # 输入产品/服务信息和额外的提示信息
    col1, col2 = st.columns(2)
    with col1:
        product_info = st.text_area("输入产品/服务信息", value=default_product_info, key="product_info")
    with col2:
        additional_prompt = st.text_area("额外的提示信息（可选）", value=default_additional_prompt, key="additional_prompt")
    
    # 可编辑的prompt模板
    default_prompt = """基于以下信息为多个用户生成个性化的TikTok私信内容:

产品/服务信息: {product_info}
用户评论: {user_comments}
额外提示: {additional_prompt}

请为每个用户生成一条私信，确保每条消息:
1. 长度适中，不超过100字
2. 语气友好亲和
3. 与用户评论内容相关
4. 自然地引入产品/服务
5. 包含一个简单的号召性用语

请以JSON格式返回结果，格式如下:
{{"用户ID1": "为用户1生成的消息", "用户ID2": "为用户2生成的消息", ...}}

注意：请确保返回的是有效的JSON格式，不要添加额外的换行或缩进。
"""
    
    # 实时渲染product_info和additional_prompt到default_prompt
    rendered_prompt = default_prompt.format(
        product_info=product_info,
        user_comments="{user_comments}",  # 保留占位符
        additional_prompt=additional_prompt
    )
    
    prompt = st.text_area("编辑Prompt模板", rendered_prompt, height=300, key="prompt")
    
    # 选择每批处理的客户数量
    batch_size = st.selectbox("每批处理的客户数量", [5, 10, 20, 50], index=1)
    
    if 'generated_messages' not in st.session_state:
        st.session_state.generated_messages = load_messages_from_cache(selected_keyword)

    if st.button("生成推广信息"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_customers = len(high_intent_df)
        
        all_messages = {}
        for i in range(0, total_customers, batch_size):
            batch = high_intent_df.iloc[i:min(i+batch_size, total_customers)]
            
            user_comments = {}
            for _, row in batch.iterrows():
                user_id = row['user_id']
                reply_content = row['reply_content']
                if user_id and reply_content:
                    user_comments[user_id] = reply_content
            
            if not user_comments:
                st.warning("没有有效的用户评论数据。请检查数据源。")
                break
            
            # 生成消息
            user_comments_str = "\n".join([f"{user_id}: {comment}" for user_id, comment in user_comments.items()])
            messages = generate_messages(model, prompt, product_info, user_comments_str, additional_prompt)
            all_messages.update(messages)
            
            # 更新进度
            progress = min((i + batch_size) / total_customers, 1.0)
            progress_bar.progress(progress)
            status_text.text(f"已生成 {min(i+batch_size, total_customers)}/{total_customers} 条推广信息")
        
        st.session_state.generated_messages = all_messages
        
        if all_messages:
            st.success(f"成功生成 {len(all_messages)} 条推广信息!")
        else:
            st.error("没有生成任何推广信息。请检查输入信息和提示。")

    # 显示生成的推广信息并允许批量编辑
    if st.session_state.generated_messages:
        st.subheader("推广信息")
        
        # 将所有消息合并成一个字符串，每条消息占一行
        all_messages_text = "\n".join([f"{user_id}: {message}" for user_id, message in st.session_state.generated_messages.items()])
        
        # 创建一个大的文本框用于批量编辑
        edited_messages = st.text_area("编辑推广信息（每行一条消息，格式为 '用户ID: 消息内容'）", 
                                       all_messages_text, 
                                       height=400)
        
        if st.button("保存编辑后的推广信息"):
            # 解析编辑后的文本，更新 st.session_state.generated_messages
            updated_messages = {}
            for line in edited_messages.split("\n"):
                if ":" in line:
                    user_id, message = line.split(":", 1)
                    updated_messages[user_id.strip()] = message.strip()
            
            # 更新 session_state 和缓存
            st.session_state.generated_messages = updated_messages
            save_messages_to_cache(selected_keyword, updated_messages)
            
            # 保存到数据库
            for user_id, message in updated_messages.items():
                db.save_tiktok_message(selected_keyword, user_id, message)
            
            st.success(f"成功保存 {len(updated_messages)} 条推广信息到数据库!")

    # 添加清除缓存的按钮
    if st.button("清除所有生成的推广信息"):
        st.session_state.generated_messages = {}
        save_messages_to_cache(selected_keyword, {})
        st.success("已清除所有生成的推广信息。")
        st.rerun()
