# Paper Extract 修改指北

## 用户回复
## 运行命令

### 基础运行

```bash
# 完整流程（规划 → 演化 → 抽取 → 审核）
uv run python -m paper_extract_gem.main --config config.yaml

# 仅运行规划阶段，生成并保存框架
uv run python -m paper_extract.main --config config.yaml --stage plan

# 仅运行演化阶段，使用已保存的框架
uv run python -m paper_extract.main --config config.yaml --stage evo --framework output/framework.json

# 仅运行抽取阶段
uv run python -m paper_extract_gemini.main --config config_gemini.yaml --stage extract --framework output/framework.json --log-file run.log

# 仅运行审核阶段（使用 config.yaml 中配置的单文件）
uv run python -m paper_extract.main --config config.yaml --stage review --framework output/framework.json

# 覆盖输出目录
uv run python -m paper_extract.main --config config.yaml --output ./results
```

### 调试模式

```bash
# 启用 DEBUG 级别日志
uv run python -m paper_extract.main --config config.yaml --debug

# 输出日志到文件
uv run python -m paper_extract.main --config config.yaml --log-file run.log

# 完整调试（日志 + 文件）
uv run python -m paper_extract.main --config config.yaml --debug --log-file debug.log

# 调试单个阶段
uv run python -m paper_extract.main --config config.yaml --stage evo --framework output/framework.json --debug
```

### 编程式调用

```python
from paper_extract import PaperExtractPipeline, load_config, setup_logger
import logging

# 配置日志
setup_logger(level=logging.DEBUG, log_file="run.log")

# 加载配置
config = load_config("config.yaml")

# 运行 Pipeline
pipeline = PaperExtractPipeline(config)
results = pipeline.run()

# 或处理单个文件
result = pipeline.process_single("paper.pdf")
```

---

## 新增 LangGraph 模块

### 模块结构模板

```python
"""
新模块名称

模块功能描述。
"""

import json
import operator
from typing import Literal

from langchain.messages import AnyMessage
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict, Annotated

from .client import APIClient
from .config import YourConfig  # 需要在 config.py 中定义
from .logger import get_logger

logger = get_logger("paper_extract.your_module")


# 1. 定义 Prompt 模板
YOUR_PROMPT = """..."""


# 2. 定义状态类型
class YourState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]
    # 添加你的状态字段
    input_data: str
    output_data: str
    last_error: str


# 3. 定义节点函数
def your_node(state: YourState):
    """节点逻辑"""
    logger.debug("Processing...")
    # 处理逻辑
    return {**state, "output_data": "result"}


# 4. 构建 Graph
def build_your_graph(client: APIClient):
    builder = StateGraph(YourState)
    builder.add_node("your_node", your_node)
    builder.add_edge(START, "your_node")
    builder.add_edge("your_node", END)
    return builder.compile()


# 5. 提供运行入口
def run_your_module(client: APIClient, config: YourConfig, **kwargs):
    logger.info("Starting your module...")
    graph = build_your_graph(client)
    init_state = {...}
    out = graph.invoke(init_state, {"recursion_limit": 200})
    if out.get("last_error"):
        logger.error(f"Failed: {out['last_error']}")
        raise RuntimeError(out["last_error"])
    logger.info("Complete")
    return out["output_data"]
```

---

## 修改现有模块

### 注意事项

1. **Prompt 修改**：直接编辑模块顶部的 `PROMPT_TEMPLATE` / `CANDIDATE_PROMPT` / `RESOLVE_PROMPT`

2. **状态字段**：修改 `TypedDict` 定义时，确保：
   - 所有节点函数正确处理新字段
   - 初始状态包含新字段的默认值

3. **新增节点**：
   ```python
   # 在 build_xxx_graph 中添加
   builder.add_node("new_node", new_node_function)
   builder.add_edge("previous_node", "new_node")
   builder.add_edge("new_node", "next_node")
   ```

4. **条件边**：
   ```python
   def decide_branch(state) -> Literal["branch_a", "branch_b", "__end__"]:
       if state.get("condition"):
           return "branch_a"
       return "branch_b"
   
   builder.add_conditional_edges("node", decide_branch, ["branch_a", "branch_b", END])
   ```

---

## 整合新模块到 Pipeline

### 步骤

1. **在 `config.py` 中添加配置类**：

```python
@dataclass
class NewModuleConfig:
    enabled: bool = True
    your_param: str = "default"
    api: Optional[APIConfig] = None
```

2. **更新 `PipelineConfig`**：

```python
@dataclass
class PipelineConfig:
    # ... 现有字段
    new_module: NewModuleConfig = field(default_factory=NewModuleConfig)
    
    def get_new_module_api(self) -> APIConfig:
        return self.new_module.api or self.api
```

3. **创建模块文件** `paper_extract/new_module.py`

4. **在 `pipeline.py` 中整合**：

```python
from .new_module import run_new_module

class PaperExtractPipeline:
    # ...
    
    def process_single(self, pdf_path: str) -> dict:
        # ... 现有逻辑
        
        # 新阶段
        if self.config.new_module.enabled:
            logger.info("[New Module] Processing")
            try:
                result = run_new_module(
                    client=self.new_module_client,
                    config=self.config.new_module,
                    # 传入前序阶段的输出
                    input_data=schema,
                )
                # 保存结果
            except Exception as e:
                logger.error(f"[New Module] FAILED: {e}")
```

5. **更新 `config.yaml`**：

```yaml
new_module:
  enabled: true
  your_param: "value"
```

---

## 文件结构

```
paper_extract/
├── __init__.py      # 导出主接口
├── config.py        # 配置类定义
├── client.py        # API 客户端
├── logger.py        # 日志配置
├── utils.py         # 公共工具函数
├── schema_evo.py    # Schema 演化 Graph
├── extract.py       # 数据抽取 Graph
├── pipeline.py      # 统一 Pipeline
├── main.py          # CLI 入口
└── new_module.py    # 你的新模块
```
