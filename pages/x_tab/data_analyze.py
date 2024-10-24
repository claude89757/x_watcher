import os
import json
import streamlit as st
import pandas as pd
from collectors.common.mysql import MySQLDatabase
from common.openai import process_with_gpt
from io import StringIO
import csv
import logging
import io
import time
from datetime import datetime

# 定义缓存文件路径
DESCRIPTION_CACHE_FILE = 'x_description_cache.json'

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

def data_analyze(db: MySQLDatabase):
    """
    本页面用于分析和分类X评论数据。
    """
    # 全局面板
    st.info("本页面用于分析和分类X评论数据。")

    # 获取所有关键字
    keywords = db.get_all_x_keywords()

    # 创建四列布局
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

    with col1:
        # 创建下拉框让用户选择关键字，使用缓存的默认值
        selected_keyword = st.selectbox("关键字", keywords, 
                                        index=keywords.index(st.session_state.cached_keyword) if st.session_state.cached_keyword in keywords else 0,
                                        key="analyze_keyword_select")

    # 获取当前关键字的评论总数
    total_available_comments = db.get_filtered_x_comments_count(selected_keyword)
    
    # 创建可选的评论数量列表
    comment_count_options = [100, 500, 1000, 2000, 5000, 10000, total_available_comments]
    comment_count_options = sorted(set([opt for opt in comment_count_options if opt <= total_available_comments]))

    with col2:
        # 选择总共要分类的评论数量
        total_comments = st.selectbox("评论数量", 
                                      options=comment_count_options, 
                                      index=0)  # 默选择最大值

    with col3:
        # 选择每轮入的数据量
        batch_size = st.selectbox("批次大小", [10, 50, 100, 200], index=1)

    with col4:
        # 选择模型
        model = st.selectbox("模型选择", ["gpt-4o-mini", "gpt-4o"], index=0)

    # 显示可用的评论总数和预估问答次数
    estimated_rounds = (total_comments + batch_size - 1) // batch_size
    st.info(f"关键字 '{selected_keyword}' 共 {total_comments} 条评论待分析, 预估需进行 {estimated_rounds} 轮问答")

    # 获取或生成描述
    descriptions = load_descriptions_from_cache(selected_keyword)
    if descriptions is None:
        with st.spinner("正在生成产品和客户描述..."):
            descriptions = generate_descriptions(selected_keyword)
            if descriptions:
                save_descriptions_to_cache(selected_keyword, descriptions)

    # 输入产品描述和目标客户描述
    col1, col2 = st.columns(2)
    with col1:
        product_description = st.text_area("产品描述", 
                                           value=descriptions['product_description'] if descriptions else "请输入您的产品描述",
                                           key="product_description")
    
    with col2:
        customer_description = st.text_area("目标客户描述", 
                                            value=descriptions['customer_description'] if descriptions else "请描述您的目标客户",
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
    prompt_template_first_round = f"""产品描述：{product_description}
目标客户：{customer_description}

请分析以下评论数据，并将每条评论分类为"潜在客户"或"非目标客户"。
对于每条评论，请提供以下输出：
1. 用户ID
2. 原始评论内容
3. 分类结果（"潜在客户"或"非目标客户"）
4. 简短的分析理由（不超过20个字）

评论数据：
{{comments}}

请以CSV格式输出结果，包含以下列：
"用户ID", "评论内容", "分类结果", "分析理由"

请确保输出的CSV格式正确，每个字段都用双引号包围，并用逗号分隔。"""

    prompt_template_second_round = f"""请对以下被识别为"潜在客户"的评论进行更深入的分析，将每条评论分类为"高意向客户"、"中等意向客户"或"低意向客户"。
对于每条评论，请提供以下输出：
1. 用户ID
2. 原始评论内容
3. 第一轮分类结果（"潜在客户"）
4. 第二轮分类结果（"高意向客户"、"中等意向客户"或"低意向客户"）
5. 简短的分析理由（不超过20个字）

评论数据：
{{comments}}

请以CSV格式输出结果，包含以下列：
"用户ID", "评论内容", "第一轮分类结果", "第二轮分类结果", "分析理由"

请确保输出的CSV格式正确每个字段都用双引号包围，并用逗号分隔。"""

    # 显示完整的prompt示例
    col1, col2 = st.columns(2)

    with col1:
        st.text_area("第一轮分析Prompt（筛选潜在客户）", prompt_template_first_round, height=250)
    with col2:
        st.text_area("第二轮分析Prompt（筛选高意向客户）", prompt_template_second_round, height=250)

    # 创建一列布局用于显示分析按钮和结果
    col1, _ = st.columns(2)

    with col1:
        if st.button("开始分析", type="primary"):
            # 在开始分析按钮之前添加警告提示
            st.warning("警告：分析过程中请勿刷新页面，否则可能导致分析中断。")
            analyze_comments(db, selected_keyword, model, batch_size, total_comments, prompt_template_first_round, prompt_template_second_round)
        
    # 使用expander来显示分析结果，默认折叠
    with st.expander("查看分析结果", expanded=True):
        display_analysis_results(db, selected_keyword)

    # 将清空分析结果的按钮移到这里，并合并两轮清空操作
    if st.button("清空所有分析结果", key="clear_all_analysis_button"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        for i in range(101):
            status_text.text(f"正在清空所有分析结果... {i}%")
            progress_bar.progress(i)
            time.sleep(0.01)  # 模拟操作耗时
        
        # 清空第一轮和第二轮分析结果
        del_res_first = db.clear_first_round_x_analysis_by_keyword(selected_keyword)
        del_res_second = db.clear_second_round_x_analysis_by_keyword(selected_keyword)
        
        if del_res_first and del_res_second:
            st.success(f"已清空关键字 '{selected_keyword}' 的所有分析结果")
        elif del_res_first:
            st.warning(f"已清空关键字 '{selected_keyword}' 的第一轮分析结果，但第二轮清空失败")
        elif del_res_second:
            st.warning(f"已清空关键字 '{selected_keyword}' 的第二轮分析结果，但第一轮清空失败")
        else:
            st.error("清空分析结果失败")
        
        status_text.empty()
        progress_bar.empty()

def analyze_comments(db, keyword, model, batch_size, total_comments, prompt_template_first, prompt_template_second):
    # 第一轮分析
    st.info("开始第一轮分析...")
    first_round_analyze(db, keyword, model, batch_size, total_comments, prompt_template_first)
    
    # 获取潜在客户数量
    potential_customers_count = db.get_x_potential_customers_count(keyword)
    st.success(f"第一轮分析完成，发现 {potential_customers_count} 个潜在客户")
    
    if potential_customers_count > 0:
        # 第二轮分析
        st.info("开始第二轮分析...")
        second_round_analyze(db, keyword, model, batch_size, prompt_template_second)
        
        # 获取第二轮分析结果
        second_round_results = db.get_second_round_analyzed_x_comments(keyword)
        if second_round_results:
            df_second_round = pd.DataFrame(second_round_results)
            classification_counts = df_second_round['second_round_classification'].value_counts()
            high_intent_customers_count = classification_counts.get('高意向客户', 0)
            medium_intent_customers_count = classification_counts.get('中等意向客户', 0)
            low_intent_customers_count = classification_counts.get('低意向客户', 0)
            
            st.success(f"第二轮分析完成，发现：\n"
                       f"高意向客户: {high_intent_customers_count} 个\n"
                       f"中等意向客户: {medium_intent_customers_count} 个\n"
                       f"低意向客户: {low_intent_customers_count} 个")
        else:
            st.warning("第二轮分析完成，但未找到分析结果")
    else:
        st.warning("未发现潜在客户，跳过第二轮分析")

def display_analysis_results(db, keyword):
    # 显示第一轮分析结果
    st.subheader("第一轮分析结果")
    analyzed_comments = db.get_analyzed_x_comments(keyword)
    if analyzed_comments:
        df_analyzed = pd.DataFrame(analyzed_comments)
        
        # 显示统计信息
        classification_counts = df_analyzed['classification'].value_counts()
        st.success(f"第一轮分析统计：潜在客户 {classification_counts.get('潜在客户', 0)} 个，非目标客户 {classification_counts.get('非目标客户', 0)} 个")
        
        st.dataframe(df_analyzed)
    else:
        st.info("没有找到第一轮分析的评论数据")

    # 显示第二轮分析结果
    st.subheader("第二轮分析结果")
    second_round_analyzed_comments = db.get_second_round_analyzed_x_comments(keyword)
    if second_round_analyzed_comments:
        df_second_round = pd.DataFrame(second_round_analyzed_comments)
        
        # 显示统计信息
        classification_counts = df_second_round['second_round_classification'].value_counts()
        st.success(f"第二轮分析统计：高意向客户 {classification_counts.get('高意向客户', 0)} 个，中等意向客户 {classification_counts.get('中等意向客户', 0)} 个，低意向客户 {classification_counts.get('低意向客户', 0)} 个")
        
        st.dataframe(df_second_round)
    else:
        st.info("没有找到第二轮分析的评论数据")

def remove_punctuation(text):
    """移除字符串开头和结尾的标点符号"""
    return text.strip('.,;:!?"\' ')

def first_round_analyze(db, keyword, model, batch_size, total_comments, prompt_template):
    # 获取过滤后的评论数据
    filtered_comments = db.get_filtered_x_comments_by_keyword(keyword, limit=total_comments)

    ignored_comments = []
    total_ignored = 0

    if filtered_comments:
        progress_bar = st.progress(0)
        status_text = st.empty()

        results = []
        for i in range(0, len(filtered_comments), batch_size):
            with st.spinner(f'正在处理第 {i//batch_size + 1} 批次...'):
                batch = filtered_comments[i:i+batch_size]
                comments_text = "\n".join([f"{j+1}. 用户ID: {comment['user_id']}, 评论内容: {comment['reply_content']}" for j, comment in enumerate(batch)])
                
                current_prompt = prompt_template.replace("{comments}", comments_text)
                
                try:
                    response = process_with_gpt(model, current_prompt, max_tokens=5000)
                    
                    # 去除可能存在的 ```csv 标记
                    response = response.strip()
                    if response.startswith("```csv"):
                        response = response[7:]
                    if response.endswith("```"):
                        response = response[:-3]
                    csv_content = response.strip()
                    
                    # 使用固定的列名
                    fixed_headers = ["用户ID", "评论内容", "分类结果", "分析理由"]
                    
                    # 使用 csv.reader 来解析 CSV 内容，并去除多余的引号
                    csv_reader = csv.reader(io.StringIO(csv_content))
                    next(csv_reader)  # 跳过 GPT 生成的标题行
                    
                    rows = []
                    for row in csv_reader:
                        if len(row) == len(fixed_headers):
                            cleaned_row = [remove_punctuation(cell) for cell in row]
                            row_dict = dict(zip(fixed_headers, cleaned_row))
                            
                            # 只保存分类结果为"潜在客户"或"非目标客户"的数据
                            if row_dict['分类结果'] in ["潜在客户", "非目标客户"]:
                                rows.append(row_dict)
                            else:
                                logging.warning(f"忽略分类结果不符合预期的行: {row_dict}")
                        else:
                            total_ignored += 1
                            if len(ignored_comments) < 5:  # 只保存前5个被忽略的评论作���示例
                                ignored_comments.append(row)
                            logging.warning(f"忽略字段数量不匹配的行: {row}")
                    
                    # 使用处理后的数据创建 DataFrame
                    if rows:
                        batch_results = pd.DataFrame(rows)
                        results.append(batch_results)

                        # 保存批次结果到数据库
                        db.save_analyzed_x_comments(keyword, batch_results)
                    else:
                        logging.warning("本批次没有有效的数据行")

                except Exception as e:
                    st.error(f"处理批次 {i//batch_size + 1} 时发生错误: {str(e)}")
                
                progress = (i + batch_size) / len(filtered_comments)
                progress_bar.progress(min(progress, 1.0))
                status_text.text(f"已处理 {min(i+batch_size, len(filtered_comments))}/{len(filtered_comments)} 条评论")

        # 合并所有结果
        if results:
            # 显示忽略的评论信息
            if total_ignored > 0:
                st.warning(f"共有 {total_ignored} 条评论因格式问题被忽略。")
                if ignored_comments:
                    st.warning(f"被忽略的评论示例：\n" + "\n".join([str(comment) for comment in ignored_comments]))
        else:
            st.warning("没有成功处理任何评论数据")

    else:
        st.warning("没有找到相关的过滤后的评论数据")

def second_round_analyze(db, keyword, model, batch_size, prompt_template):
    potential_customers = db.get_x_potential_customers(keyword)
    
    if not potential_customers:
        st.warning("没有找到潜在客户数据进行第二轮分析")
        return

    ignored_comments = []
    total_ignored = 0

    progress_bar = st.progress(0)
    status_text = st.empty()

    results = []
    for i in range(0, len(potential_customers), batch_size):
        with st.spinner(f'正在处理第 {i//batch_size + 1} 批次...'):
            batch = potential_customers[i:i+batch_size]
            comments_text = "\n".join([f"{j+1}. 用户ID: {comment['user_id']}, 评论内容: {comment['reply_content']}" for j, comment in enumerate(batch)])
            
            current_prompt = prompt_template.replace("{comments}", comments_text)
            
            try:
                response = process_with_gpt(model, current_prompt, max_tokens=5000)
                
                # 去除可能存在的 ```csv 标记
                response = response.strip()
                if response.startswith("```csv"):
                    response = response[7:]
                if response.endswith("```"):
                    response = response[:-3]
                csv_content = response.strip()
                
                # 使用固定的列名
                fixed_headers = ["用户ID", "评论内容", "第一轮分类结果", "第二轮分类结果", "分析理由"]
                
                # 使用 csv.reader 来解析 CSV 内容，并去除多余的引号
                csv_reader = csv.reader(io.StringIO(csv_content))
                next(csv_reader)  # 跳过 GPT 生成的标题行
                
                rows = []
                for row in csv_reader:
                    if len(row) == len(fixed_headers):
                        cleaned_row = [remove_punctuation(cell) for cell in row]
                        row_dict = dict(zip(fixed_headers, cleaned_row))
                        
                        # 只保存第二轮分类结果为"高意向客户"、"中等意向客户"或"低意向客户"的数据
                        if row_dict['第二轮分类结果'] in ["高意向客户", "中等意向客户", "低意向客户"]:
                            rows.append(row_dict)
                        else:
                            logging.warning(f"忽略第二轮分类结果不符合预期的行: {row_dict}")
                    else:
                        total_ignored += 1
                        if len(ignored_comments) < 5:  # 只保存前5个被忽略的评论作为示例
                            ignored_comments.append(row)
                        logging.warning(f"忽略字段数量不匹配的行: {row}")
                
                # 使用处理后的数据创建 DataFrame
                if rows:
                    batch_results = pd.DataFrame(rows)
                    results.append(batch_results)

                    # 保存批次结果到数据库
                    db.save_second_round_analyzed_x_comments(keyword, batch_results)
                else:
                    logging.warning("本批次没有有效的数据行")

            except Exception as e:
                st.error(f"处理第二轮分析批次 {i//batch_size + 1} 时发生错误: {str(e)}")
            
            progress = (i + batch_size) / len(potential_customers)
            progress_bar.progress(min(progress, 1.0))
            status_text.text(f"已处理 {min(i+batch_size, len(potential_customers))}/{len(potential_customers)} 条评论")

    # 合并所有结果
    if results:
        # 显示忽略的评论信息
        if total_ignored > 0:
            st.warning(f"第二轮中共有 {total_ignored} 条评论因格式问题被忽略。")
            if ignored_comments:
                st.warning(f"第二轮被忽略的评论示例：\n" + "\n".join([str(comment) for comment in ignored_comments]))
    else:
        st.warning("没有成功处理任何评论数据")
