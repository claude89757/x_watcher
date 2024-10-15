import os
import json
import streamlit as st
import pandas as pd
from collectors.common.mysql import MySQLDatabase
from common.azure_openai import process_with_gpt
from io import StringIO

# 定义缓存文件路径
DESCRIPTION_CACHE_FILE = 'tiktok_description_cache.json'

def generate_descriptions(keyword):
    """生成产品描述和目标客户描述"""
    prompt = f"""
    基于关键词 "{keyword}" 生成以下内容：
    1. 产品描述（不超过50字）
    2. 目标客户描述（不超过50字）

    请以JSON格式输出，包含 "product_description" 和 "customer_description" 两个字段。

    示例输出格式：
    {{
      "product_description": "高科技医疗美容产品，减少细纹，紧致肌肤，提升面容。",
      "customer_description": "30岁以上注重外表的成年人，希望通过专业产品延缓衰老。"
    }}
    """
    try:
        response = process_with_gpt("gpt-4o-mini", prompt)
        
        # 去除可能存在的 ```json 标记
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:]
        if response.endswith("```"):
            response = response[:-3]
        response = response.strip()
        
        descriptions = json.loads(response)
        
        # 验证JSON结构
        if "product_description" not in descriptions or "customer_description" not in descriptions:
            raise ValueError("生成的描述缺少必要的字段")
        
        return descriptions
    except json.JSONDecodeError:
        st.error("生成的描述不是有效的JSON格式")
        return None
    except ValueError as e:
        st.error(f"生成描述时发生错误: {str(e)}")
        return None
    except Exception as e:
        st.error(f"生成描述时发生未知错误: {str(e)}")
        return None

def load_descriptions_from_cache(keyword):
    """从缓存文件加载描述"""
    if os.path.exists(DESCRIPTION_CACHE_FILE):
        with open(DESCRIPTION_CACHE_FILE, 'r') as f:
            cache = json.load(f)
            return cache.get(keyword, None)
    return None

def save_descriptions_to_cache(keyword, descriptions):
    """保存描述到缓存文件"""
    cache = {}
    if os.path.exists(DESCRIPTION_CACHE_FILE):
        with open(DESCRIPTION_CACHE_FILE, 'r') as f:
            cache = json.load(f)
    cache[keyword] = descriptions
    with open(DESCRIPTION_CACHE_FILE, 'w') as f:
        json.dump(cache, f)

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

        # 创建两列布局
        col1, col2 = st.columns(2)

        with col1:
            # 创建下拉框让用户选择关键字，使用缓存的默认值
            selected_keyword = st.selectbox("关键字", keywords, 
                                            index=keywords.index(default_keyword) if default_keyword in keywords else 0,
                                            key="analyze_keyword_select")

            # 选择每轮输入的数据量
            batch_size = st.selectbox("每轮输入的数据量", [10, 50, 100, 200], index=1)

        with col2:
            # 获取当前关键字的评论总数
            total_available_comments = db.get_filtered_tiktok_comments_count(selected_keyword)
            
            # 创建可选择的评论数量列表
            comment_count_options = [100, 500, 1000, 2000, 5000, 10000]
            comment_count_options = [opt for opt in comment_count_options if opt <= total_available_comments]
            if total_available_comments not in comment_count_options:
                comment_count_options.append(total_available_comments)
            comment_count_options.sort()

            # 选择总共要分类的评论数量
            total_comments = st.selectbox("总共要分类的评论数量", 
                                          options=comment_count_options, 
                                          index=len(comment_count_options) - 1)  # 默认选择最大值

            # 选择模型
            model = st.selectbox("选择模型", ["gpt-4o-mini", "gpt-4o"], index=0)

        # 显示可用的评论总数
        st.info(f"当前关键字 '{selected_keyword}' 共有 {total_available_comments} 条评论可供分析")

        # 计算并显示预估的问答次数
        estimated_rounds = (total_comments + batch_size - 1) // batch_size
        st.write(f"预估需要进行 {estimated_rounds} 轮问答")

        # 获取或生成描述
        descriptions = load_descriptions_from_cache(selected_keyword)
        if descriptions is None:
            with st.spinner("正在生成产品和客户描述..."):
                descriptions = generate_descriptions(selected_keyword)
                if descriptions:
                    save_descriptions_to_cache(selected_keyword, descriptions)

        # 创建两列布局用于产品描述和目标客户描述
        col1, col2 = st.columns(2)

        with col1:
            # 输入产品描述
            product_description = st.text_area("产品描述", 
                                               value=descriptions['product_description'] if descriptions else "请输入您的产品描述",
                                               height=150,
                                               key="product_description")

        with col2:
            # 输入目标客户描述
            customer_description = st.text_area("目标客户描述", 
                                                value=descriptions['customer_description'] if descriptions else "请描述您的目标客户",
                                                height=150,
                                                key="customer_description")

        # 检查用户是否修改了描述
        if (descriptions and 
            (product_description != descriptions['product_description'] or 
             customer_description != descriptions['customer_description'])):
            # 用户修改了描述，更新缓存
            new_descriptions = {
                "product_description": product_description,
                "customer_description": customer_description
            }
            save_descriptions_to_cache(selected_keyword, new_descriptions)
            st.success("已更新产品和客户描述缓存")

        # 构建完整的prompt
        prompt_template = f"""
        产品描述：{product_description}
        目标客户：{customer_description}

        请分析以下评论数据，并将每条评论分类为"潜在客户"或"非目标客户"。
        对于每条评论，请提供以下输出：
        1. 用户ID
        2. 原始评论内容
        3. 分类结果（"潜在客户"或"非目标客户"）
        4. 简短的分析理由（不超过50个字）

        评论数据：
        {{comments}}

        请以CSV格式输出结果，包含以下列：
        "用户ID", "评论内容", "分类结果", "分析理由"

        请确保输出的CSV格式正确，每个字段都用双引号包围，并用逗号分隔。
        """

        # 显示完整的prompt示例
        st.subheader("Prompt示例")
        example_comments = db.get_filtered_tiktok_comments_by_keyword(selected_keyword, limit=10)
        example_comments_text = "\n".join([f"{i+1}. 用户ID: {comment['user_id']}, 评论内容: {comment['reply_content']}" for i, comment in enumerate(example_comments)])
        st.text_area("完整Prompt", prompt_template.replace("{comments}", example_comments_text), height=300)

        if st.button("开始分析", type="primary"):
            # 获取过滤后的评论数据
            filtered_comments = db.get_filtered_tiktok_comments_by_keyword(selected_keyword, limit=total_comments)

            if filtered_comments:
                progress_bar = st.progress(0)
                status_text = st.empty()

                results = []
                for i in range(0, len(filtered_comments), batch_size):
                    batch = filtered_comments[i:i+batch_size]
                    comments_text = "\n".join([f"{j+1}. 用户ID: {comment['user_id']}, 评论内容: {comment['reply_content']}" for j, comment in enumerate(batch)])
                    
                    current_prompt = prompt_template.replace("{comments}", comments_text)
                    
                    try:
                        response = process_with_gpt(model, current_prompt)
                        
                        # 去除可能存在的 ```csv 标记
                        response = response.strip()
                        if response.startswith("```csv"):
                            response = response[7:]
                        if response.endswith("```"):
                            response = response[:-3]
                        csv_content = response.strip()
                        
                        # 使用 StringIO 来创建一个类文件对象
                        csv_file = StringIO(csv_content)
                        
                        # 使用 pandas 读取 CSV 内容
                        batch_results = pd.read_csv(csv_file)
                        results.append(batch_results)

                        # 保存批次结果到数据库
                        db.save_analyzed_comments(selected_keyword, batch_results)

                        # 动态展示当前批次的分析结果
                        st.subheader(f"批次 {i//batch_size + 1} 分析结果")
                        st.dataframe(batch_results)

                    except Exception as e:
                        st.error(f"处理批次 {i//batch_size + 1} 时发生错误: {str(e)}")
                    
                    progress = (i + batch_size) / len(filtered_comments)
                    progress_bar.progress(min(progress, 1.0))
                    status_text.text(f"已处理 {min(i+batch_size, len(filtered_comments))}/{len(filtered_comments)} 条评论")

                # 合并所有结果
                final_results = pd.concat(results, ignore_index=True)

                # 显示分类结果
                st.subheader("总体分类结果")
                st.write(final_results)

                # 显示统计信息
                st.subheader("统计信息")
                classification_counts = final_results['分类结果'].value_counts()
                st.write(classification_counts)

            else:
                st.warning("没有找到相关的过滤后的评论数据")

        # 3. 增加展示分析结果的逻辑
        st.subheader("查看已分析的评论")
        if st.button("加载分析结果"):
            analyzed_comments = db.get_analyzed_comments(selected_keyword)
            if analyzed_comments:
                df_analyzed = pd.DataFrame(analyzed_comments)
                st.dataframe(df_analyzed)
                
                # 显示统计信息
                st.subheader("已分析评论统计")
                classification_counts = df_analyzed['classification'].value_counts()
                st.write(classification_counts)
            else:
                st.info("没有找到已分析的评论数据")

    finally:
        # 确保在函数结束时关闭数据库连接
        db.disconnect()
