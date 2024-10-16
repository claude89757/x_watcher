import streamlit as st
import pandas as pd
from common.azure_openai import process_with_gpt
from collectors.common.mysql import MySQLDatabase
import time
import json
import os

# 定义缓存文件路径
DESCRIPTION_CACHE_FILE = 'tiktok_description_cache.json'

def load_descriptions_from_cache(keyword):
    """从缓存文件加载描述"""
    if os.path.exists(DESCRIPTION_CACHE_FILE):
        with open(DESCRIPTION_CACHE_FILE, 'r') as f:
            cache = json.load(f)
            return cache.get(keyword, None)
    return None

def generate_messages(model, prompt, product_info, user_comments, additional_prompt):
    """使用选定的GPT模型为多个用户生成个性化消息"""
    formatted_prompt = prompt.format(
        product_info=product_info,
        user_comments=json.dumps(user_comments, ensure_ascii=False),
        additional_prompt=additional_prompt
    )
    
    response = process_with_gpt(model, formatted_prompt, max_tokens=5000)
    try:
        messages = json.loads(response.strip())
        return messages
    except json.JSONDecodeError:
        st.error("GPT 生成的响应不是有效的 JSON 格式。请重试或调整提示。")
        return {}

def send_msg(db: MySQLDatabase):
    """
    发送私信给高意向客户
    """
    st.info("AI生成私信, 自动批量发送给高意向客户")
    
    # 获取所有关键词
    keywords = db.get_all_tiktok_keywords()
    selected_keyword = st.selectbox("选择关键词", keywords)
    
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
        product_info = st.text_area("输入产品/服务信息", value=default_product_info)
    with col2:
        additional_prompt = st.text_area("额外的提示信息（可选）", value=default_additional_prompt)
    
    # 可编辑的prompt模板
    default_prompt = """基于以下信息为多个用户生成个性化的TikTok私信内容:

产品/服务信息: {product_info}
用户评论: {user_comments}
额外提示: {additional_prompt}

请为每个用户生成一条私信，确保每条消息:
1. 长度适中，不超过150字
2. 语气友好亲和
3. 与用户评论内容相关
4. 自然地引入产品/服务
5. 包含一个简单的号召性用语

请以JSON格式返回结果，格式如下:
{{
    "user_id1": "为用户1生成的消息",
    "user_id2": "为用户2生成的消息",
    ...
}}

生成的消息:"""
    prompt = st.text_area("编辑Prompt模板", default_prompt, height=300)
    
    # 选择每批处理的客户数量
    batch_size = st.selectbox("每批处理的客户数量", [5, 10, 20, 50], index=1)
    
    if 'generated_messages' not in st.session_state:
        st.session_state.generated_messages = {}

    if st.button("生成私信"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_customers = len(high_intent_df)
        
        for i in range(0, total_customers, batch_size):
            batch = high_intent_df.iloc[i:min(i+batch_size, total_customers)]
            
            user_comments = {row['user_id']: row['reply_content'] for _, row in batch.iterrows()}
            
            # 生成消息
            messages = generate_messages(model, prompt, product_info, user_comments, additional_prompt)
            st.session_state.generated_messages.update(messages)
            
            # 更新进度
            progress = min((i + batch_size) / total_customers, 1.0)
            progress_bar.progress(progress)
            status_text.text(f"已生成 {min(i+batch_size, total_customers)}/{total_customers} 条私信")
            
        st.success(f"成功生成 {total_customers} 条私信!")

    # 显示生成的私信并允许编辑和选择
    if st.session_state.generated_messages:
        st.subheader("生成的私信内容")
        edited_messages = {}
        selected_messages = {}
        for user_id, message in st.session_state.generated_messages.items():
            col1, col2 = st.columns([4, 1])
            with col1:
                edited_message = st.text_area(f"给用户 {user_id} 的私信", message, key=f"edit_{user_id}")
                edited_messages[user_id] = edited_message
            with col2:
                selected = st.checkbox("选择发送", key=f"select_{user_id}")
                if selected:
                    selected_messages[user_id] = edited_message

        if st.button("发送选中的私信"):
            progress_bar = st.progress(0)
            status_text = st.empty()

            for i, (user_id, message) in enumerate(selected_messages.items()):
                # 这里应该是实际发送消息的逻辑
                # 由于我们没有实际的发送功能，这里只是模拟发送
                st.write(f"发送给用户 {user_id} 的消息: {message}")
                
                # 更新进度
                progress = (i + 1) / len(selected_messages)
                progress_bar.progress(progress)
                status_text.text(f"已发送 {i+1}/{len(selected_messages)} 条私信")
                
                # 模拟发送延迟
                time.sleep(1)
            
            st.success(f"成功发送 {len(selected_messages)} 条私信!")
            # 清空已发送的消息
            for user_id in selected_messages.keys():
                del st.session_state.generated_messages[user_id]
