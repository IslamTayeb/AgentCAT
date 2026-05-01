本文档介绍如何运行知识图谱系统的各个模块。

---

## 环境准备

### 1. 安装依赖

```bash
conda activate chem
pip install tenacity tqdm neo4j openai python-dotenv flask langgraph langchain-openai
```

### 2. 配置环境变量

编辑 `.env` 文件，配置以下内容：

```env
# LLM 配置
EXTRACTOR_LLM_API_KEY=your_api_key
CYPHER_LLM_API_KEY=your_api_key

# Neo4j 配置
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password

# 路径配置
EXTRACTOR_INPUT_DIR=output
EXTRACTOR_OUTPUT_DIR=
EXTRACTOR_PDF_MAPPING_FILE=pdf_mapping.json
EXTRACTOR_ZEOLITE_REGISTRY_FILE=zeolites.json
```

### 3. 启动 Neo4j

确保 Neo4j 数据库已启动并可访问。

---

## 流水线运行（推荐）

使用 `pipeline.py` 统一入口运行：

```bash
cd d:\Downloads\PROJECT

# 完整流水线（提取 + 导入）
python pipeline.py --all

# 仅第一阶段（关系提取）
python pipeline.py --extract

# 仅第二阶段（Neo4j 导入）
python pipeline.py --import

# 指定输入目录
python pipeline.py --all --input output/

# 导入前清空数据库
python pipeline.py --all --clear

# 不显示进度条
python pipeline.py --all --no-progress
```

---

## 分步运行

### 第一阶段：关系提取

```bash
# 单文件提取
python -m kg_extractor.extractor "output/my/my_extract.json"

# 批量提取（推荐，带进度条）
python -m kg_extractor.extractor output/ --batch

# 异步并发提取（5线程）
python -m kg_extractor.extractor output/ --batch --async --workers 5
```

> 生成 `{folder}_extract_relations.txt` 文件

### 第二阶段：Neo4j 导入

```bash
# 单文件导入
python -m neo4j_tools.importer "output/my/my_extract_relations.txt"

# 批量导入（推荐，带进度条和重试）
python -m neo4j_tools.importer output/ --batch

# 清空数据库后导入
python -m neo4j_tools.importer output/ --batch --clear
```

---

## 启动 Web 服务

```bash
conda activate chem
python app.py
```

访问 http://localhost:5000 打开查询界面。

### 查询接口

提供两种查询模式：

#### 1. 简单模式 `/query`

单次 Cypher 生成 + 执行，适合简单查询：

```bash
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Pt@Sn-MFI 催化剂有哪些属性？"}'
```

#### 2. Agent 模式 `/query/agent`

Plan-and-Execute 多步查询，适合复杂问题：

```bash
curl -X POST http://localhost:5000/query/agent \
  -H "Content-Type: application/json" \
  -d '{"query": "从 C3H8 到 C3H6 的完整反应路径是什么？"}'
```

> Agent 模式使用 LangGraph 工作流：Planner 规划 → CypherGenerator 生成 → Executor 执行 → 迭代直到完成


---

## 目录结构

```
PROJECT/
├── .env                      # 环境变量配置
├── config.py                 # 配置加载
├── pipeline.py               # 流水线入口（新增）
├── app.py                    # Web 应用入口
├── labels.json               # 标签记录
├── pdf_mapping.json          # PDF ID 映射（新增）
├── zeolites.json             # Zeolite 注册表（新增）
├── output/                   # 输入 JSON 目录
│   └── {folder}/
│       ├── {folder}_extract.json
│       └── {folder}_extract_relations.txt
├── kg_extractor/             # 第一阶段：关系提取
│   ├── extractor.py          # 提取器（重试+进度条+异步）
│   ├── pdf_id_manager.py     # PDF ID 管理（新增）
│   ├── label_manager.py      # 标签管理
│   ├── molecular_normalizer.py
│   └── prompts.py
├── neo4j_tools/              # 第二阶段：Neo4j 工具
│   ├── importer.py           # 导入器（批量+重试+进度条）
│   ├── connection.py         # 连接管理
│   └── executor.py           # Cypher 执行
├── cypher_generator/         # 第三阶段：Cypher 生成
├── web_query/                # 第四阶段：Web 路由
└── templates/
    └── kg_query.html         # 前端页面
```

---

## 常见问题

### 1. LLM API 调用失败

- 检查 `.env` 中的 API 密钥和 BASE_URL
- 系统已自动重试 3 次（指数退避）

### 2. Neo4j 连接失败

```bash
# 验证连接
python -c "from neo4j_tools import Neo4jConnection; Neo4jConnection.verify_connection()"
```
### 3. 查看当前配置

```bash
python config.py
```

