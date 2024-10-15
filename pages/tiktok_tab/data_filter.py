import os
import json
import streamlit as st
import pandas as pd
from collectors.common.mysql import MySQLDatabase


def data_filter(db):
    # 全局面板
    st.info("本页面用于过滤和处理TikTok评论数据。")

    col1, col2 = st.columns(2)
    
    # 从数据库获取统计信息
    stats = db.get_tiktok_collection_stats()

    with col1:
        st.metric("已收集关键字", stats['keyword_count'])
    with col2:
        st.metric("已收集评论数", stats['comment_count'])

    # 定义缓存文件路径
    KEYWORD_CACHE_FILE = 'tiktok_keyword_cache.json'

    def load_keyword_from_cache():
        """从缓存文件加载关键字"""
        if os.path.exists(KEYWORD_CACHE_FILE):
            with open(KEYWORD_CACHE_FILE, 'r') as f:
                data = json.load(f)
                return data.get('keyword', '')
        return ''

    # 从缓存加载默认关键字
    default_keyword = load_keyword_from_cache()

    # 获取所有关键字
    keywords = db.get_all_tiktok_keywords()

    # 创建下拉框让用户选择关键字，使用缓存的默认值
    selected_keyword = st.selectbox("选择关键字", keywords, 
                                    index=keywords.index(default_keyword) if default_keyword in keywords else 0,
                                    key="filter_keyword_select")  # 添加唯一的key


    if selected_keyword:
        # 获取选定关键字的评论数据
        comments = db.get_tiktok_comments_by_keyword(selected_keyword)

        if comments:
            st.write(f"原始评论数量: {len(comments)}")

            # 过滤和处理数据
            df = pd.DataFrame(comments)
            
            # 1. 过滤长度小于10的评论
            df = df[df['reply_content'].str.len() >= 10]

            # 2. 相同用户ID的评论合并成一条
            df = df.groupby('user_id').agg({
                'video_id': 'first',
                'keyword': 'first',
                'reply_content': lambda x: ' '.join(x),
                'reply_time': 'first',
                'likes_count': 'sum',
                'is_pinned': 'any',
                'parent_comment_id': 'first',
                'collected_at': 'first',
                'collected_by': 'first',
                'video_url': 'first'
            }).reset_index()

            # 3. 重复评论用户或者评论内容相同的，去重
            df = df.drop_duplicates(subset=['user_id', 'reply_content'])

            st.write(f"过滤后的评论数量: {len(df)}")

            # 数据过滤规则
            st.caption("数据过滤规则:")
            st.markdown("""
            - 过滤长度小于10的评论
            - 相同用户ID的评论合并成一条
            - 重复评论用户或者评论内容相同的，去重
            """)

            # 保存过滤后的数据
            if st.button("保存过滤后的数据", type="primary"):
                try:
                    saved_count = db.save_filtered_comments(df.to_dict('records'))
                    st.success(f"✅ 成功保存 {saved_count} 条过滤后的评论")
                except Exception as e:
                    st.error(f"❌ 保存过滤后的数据时发生错误: {str(e)}")

            # 显示过滤后的数据
            st.subheader("过滤后的评论数据")
            st.dataframe(df)

        else:
            st.warning("⚠️ 没有找到相关评论数据")
