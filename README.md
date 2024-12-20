# 智能助手项目

## 简介
本项目是一个基于 Streamlit 的智能助手应用，旨在通过数据收集、分析和生成个性化消息来帮助用户进行市场营销。该应用集成了多种功能模块，支持从 TikTok 和 X 平台收集用户评论，并利用 GPT 模型进行数据分析和消息生成。

## 功能
- **数据收集**：从 TikTok 和 X 平台自动化收集用户评论，支持多任务并行处理。
- **数据过滤**：对收集到的评论进行智能过滤，去除无关信息，保留有价值的用户反馈。
- **数据分析**：使用 GPT 模型分析评论，识别潜在客户和市场趋势。
- **消息生成**：为高意向客户生成个性化的推广信息，提升营销效果。
- **消息发送**：通过平台 API 自动发送生成的消息，支持批量操作。

## 环境设置
- **操作系统**：建议使用 Ubuntu 20.04 或更高版本。
- **Python 版本**：3.8 或更高版本。
- **节点需求**：建议至少使用 2 个节点，一个用于数据收集和处理，另一个用于运行 Streamlit 应用和数据库服务。
- **硬件要求**：
  - 每个节点至少 4 核 CPU 和 8GB 内存。
  - 20GB 可用磁盘空间。

## 安装
1. 克隆本仓库：
   ```bash
   git clone https://github.com/yourusername/yourproject.git
   ```
2. 进入项目目录：
   ```bash
   cd yourproject
   ```
3. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

## 使用
1. 启动 Streamlit 应用：
   ```bash
   streamlit run 主页.py
   ```
2. 在浏览器中访问 `http://localhost:8501`，根据界面提示进行操作。

## 配置
- 在 `config.json` 中配置数据库连接、API 密钥和其他必要的参数。
- 确保 `MySQLDatabase` 和其他数据库相关模块已正确配置。
- 配置 `openai.py` 中的 OpenAI API 密钥以启用 GPT 模型功能。

## 目录结构
- `pages/`：包含不同功能模块的实现，如数据收集、分析和消息生成。
- `collectors/`：包含数据收集相关的脚本和工具。
- `common/`：包含通用配置、日志和工具模块。
- `sidebar.py`：定义侧边栏的布局和功能。
- `config.json`：存储应用的配置参数。

## 贡献
欢迎贡献代码！请 fork 本仓库并提交 pull request。我们欢迎任何形式的贡献，包括但不限于代码、文档和测试。

## 许可证
本项目采用 MIT 许可证。详情请参阅 `LICENSE` 文件。

## 联系
如有任何问题或建议，请通过 [claude89757@gmail.com](mailto:claude89757@gmail.com) 联系我们。

## 常见问题
- **如何获取 OpenAI API 密钥？**
  请访问 OpenAI 的官方网站注册并获取 API 密钥。

- **如何配置数据库连接？**
  请在 `config.json` 中填写数据库的连接信息，包括主机、端口、用户名和密码。

- **应用支持哪些平台？**
  目前支持从 TikTok 和 X 平台收集数据，未来将支持更多平台。
