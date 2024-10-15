import os
import json
import streamlit as st
import pandas as pd
from collectors.common.mysql import MySQLDatabase
from common.azure_openai import process_with_gpt

def data_analyze():
    # 初始化数据库连接
    db = MySQLDatabase()
    db.connect()

    try:
        # 全局面板
        st.info("本页面用于分析和分类TikTok评论数据。")

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
                                        key="analyze_keyword_select")  # 添加唯一的key

        # 选择每轮输入的数据量
        batch_size = st.selectbox("每轮输入的数据量", [10, 50, 100, 200], index=2)

        # 选择总共要分类的评论数量
        total_comments = st.slider("总共要分类的评论数量", min_value=100, max_value=10000, value=1000, step=100)

        # 计算并显示预估的问答次数
        estimated_rounds = (total_comments + batch_size - 1) // batch_size
        st.write(f"预估需要进行 {estimated_rounds} 轮问答")

        # 输入产品描述和目标客户描述
        product_description = st.text_area("产品描述", "请输入您的产品描述")
        customer_description = st.text_area("目标客户描述", "请描述您的目标客户")

        # 构建完整的prompt
        prompt_template = f"""
        产品描述：{product_description}
        目标客户：{customer_description}

        请分析以下评论数据，并将每条评论分类为"潜在客户"或"非目标客户"。
        对于每条评论，请提供以下输出：
        1. 原始评论内容
        2. 分类结果（"潜在客户"或"非目标客户"）
        3. 简短的分析理由（不超过50个字）

        评论数据：
        {{comments}}

        请以CSV格式输出结果，包含以下列：
        "评论内容", "分类结果", "分析理由"
        """

        # 显示完整的prompt示例
        st.subheader("Prompt示例")
        st.text_area("完整Prompt", prompt_template.replace("{comments}", "1. 这是一个示例评论\n2. 这是另一个示例评论"), height=300)

        if st.button("开始分析", type="primary"):
            # 获取过滤后的评论数据
            filtered_comments = db.get_filtered_tiktok_comments_by_keyword(selected_keyword, limit=total_comments)

            if filtered_comments:
                progress_bar = st.progress(0)
                status_text = st.empty()

                results = []
                for i in range(0, len(filtered_comments), batch_size):
                    batch = filtered_comments[i:i+batch_size]
                    comments_text = "\n".join([f"{j+1}. {comment['reply_content']}" for j, comment in enumerate(batch)])
                    
                    current_prompt = prompt_template.replace("{comments}", comments_text)
                    
                    try:
                        response = process_with_gpt("gpt-3.5-turbo", current_prompt)
                        csv_content = response.strip()
                        batch_results = pd.read_csv(pd.compat.StringIO(csv_content))
                        results.append(batch_results)
                    except Exception as e:
                        st.error(f"处理批次 {i//batch_size + 1} 时发生错误: {str(e)}")
                    
                    progress = (i + batch_size) / len(filtered_comments)
                    progress_bar.progress(min(progress, 1.0))
                    status_text.text(f"已处理 {min(i+batch_size, len(filtered_comments))}/{len(filtered_comments)} 条评论")

                # 合并所有结果
                final_results = pd.concat(results, ignore_index=True)

                # 显示分类结果
                st.subheader("分类结果")
                st.write(final_results)

                # 显示统计信息
                st.subheader("统计信息")
                classification_counts = final_results['分类结果'].value_counts()
                st.write(classification_counts)

                # 可以选择将结果保存到数据库或导出为CSV
                if st.button("保存分析结果"):
                    # 这里添加保存结果的逻辑
                    st.success("分析结果已保存")

            else:
                st.warning("没有找到相关的过滤后的评论数据")

    finally:
        # 确保在函数结束时关闭数据库连接
        db.disconnect()

