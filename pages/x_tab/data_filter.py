import os
import json
import streamlit as st
import pandas as pd
from collectors.common.mysql import MySQLDatabase


def preprocess_comment(comment):
    """预处理评论内容，删除逗号、单引号和双引号"""
    return comment.replace(',', '').replace("'", "").replace('"', '')


def data_filter(db: MySQLDatabase):
    """
    本页面用于过滤和处理X评论数据。
    """
    # 全局面板
    st.info("本页面用于过滤和处理X评论数据。")

    # 获取所有关键字
    keywords = db.get_all_x_keywords()

    # 创建下拉框让用户选择关键字，使用session_state中的cached_keyword作为默认值
    selected_keyword = st.selectbox("选择关键字", keywords, 
                                    index=keywords.index(st.session_state.cached_keyword) if st.session_state.cached_keyword in keywords else 0,
                                    key="filter_keyword_select")

    if selected_keyword:
        # 获取选定关键字的评论数据
        comments = db.get_x_comments_by_keyword(selected_keyword)

        if comments:
            st.write(f"原始评论数量: {len(comments)}")

            # 过滤和处理数据
            df = pd.DataFrame(comments)
            
            # 预处理评论内容
            df['reply_content'] = df['reply_content'].apply(preprocess_comment)
            
            # 1. 过滤长度小于或等于5的评论
            df = df[df['reply_content'].str.len() > 5]

            # 2. 相同用户ID的评论合并成一条
            df = df.groupby('user_id').agg({
                'tweet_id': 'first',
                'keyword': 'first',
                'reply_content': lambda x: ' '.join(x),
                'reply_time': 'first',
                'likes_count': 'sum',
                'is_pinned': 'any',
                'parent_comment_id': 'first',
                'collected_at': 'first',
                'collected_by': 'first',
                'tweet_url': 'first'
            }).reset_index()

            # 3. 重复评论用户或者评论内容相同的，去重
            df = df.drop_duplicates(subset=['user_id', 'reply_content'])

            st.write(f"过滤后的评论数量: {len(df)}")

            # 数据过滤规则
            st.caption("数据过滤规则:")
            st.markdown("""
            - 删除评论中的逗号、单引号和双引号
            - 过滤长度小于或等于5的评论
            - 相同用户ID的评论合并成一条
            - 对重复的用户评论或相同内容的评论进行去重处理
            """)

            # 保存过滤后的数据
            if st.button("保存过滤后的数据", type="primary"):
                try:
                    saved_count = db.save_filtered_x_comments(df.to_dict('records'))
                    st.success(f"✅ 成功保存 {saved_count} 条过滤后的评论")
                except Exception as e:
                    st.error(f"❌ 保存过滤后的数据时发生错误: {str(e)}")

            # 显示过滤后的数据
            st.subheader("过滤后的评论数据")
            st.dataframe(df)

        else:
            st.warning("⚠️ 没有找到相关评论数据")
