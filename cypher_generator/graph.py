# cypher_generator/graph.py
"""
LangGraph 工作流定义
实现 Plan-and-Execute 模式：Planner 单步决策 + Cypher Generator 多条生成
"""

import os
import json
from typing import TypedDict, List, Optional, Literal

from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import CYPHER_LLM
from logger import get_logger
from .planner_prompts import PLANNER_PROMPT
from .generator import CypherGenerator  # Import the class instead of prompts

logger = get_logger(__name__)

# 创建专门的 Cypher 日志记录器
import logging
cypher_logger = logging.getLogger("cypher_log")
cypher_logger.setLevel(logging.INFO)
# 避免重复添加 handler
if not cypher_logger.handlers:
    cypher_handler = logging.FileHandler("cypher.log", encoding="utf-8")
    cypher_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
    cypher_logger.addHandler(cypher_handler)
    cypher_logger.propagate = False  # 不传递到父 logger


# ==================== 状态定义 ====================

class StepRecord(TypedDict):
    step_id: int
    description: str
    cyphers: List[str]
    result: Optional[dict]
    status: str  # success / failed


class AgentState(TypedDict):
    user_query: str
    available_labels: str
    plan: List[str]
    steps: List[StepRecord]
    all_nodes: List[dict]
    all_links: List[dict]
    current_step: int
    is_complete: bool
    error_message: Optional[str]
    max_steps: int
    retry_count: int
    max_retries: int
    last_cyphers: List[str]
    last_step_stats: str


# ==================== 工具函数 ====================

def get_llm():
    """获取 LLM 实例"""
    return ChatOpenAI(
        model=CYPHER_LLM.model,
        api_key=CYPHER_LLM.api_key,
        base_url=CYPHER_LLM.base_url,
        temperature=0,
    )


def get_available_labels() -> str:
    """从 labels.json 获取可用标签"""
    try:
        from kg_extractor import LabelManager
        label_manager = LabelManager()
        labels = label_manager.get_labels()
        if labels:
            return ", ".join(sorted(labels))
        # 使用 LabelManager 中定义的默认标签
        return ", ".join(sorted(LabelManager.DEFAULT_LABELS))
    except Exception:
        # 导入失败时的兜底
        return "zeolite, propertyCategory, propertyNode, activeSite, reactionNode, molecular"


def execute_cypher(cypher: str) -> dict:
    """执行 Cypher 查询"""
    from neo4j_tools import CypherExecutor
    executor = CypherExecutor()
    result = executor.execute(cypher)
    return {
        "success": result.success,
        "nodes": result.nodes,
        "links": result.links,
        "error": result.error,
    }


def parse_json_from_text(text: str) -> any:
    """从 LLM 输出中提取 JSON"""
    text = text.strip()
    
    # 移除 markdown 代码块
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    
    text = text.strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        import re
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
    return None


def format_results_for_planner(all_nodes: List[dict], all_links: List[dict]) -> str:
    """Format results for planner context."""
    if not all_nodes:
        return "none"

    by_label = {}
    for node in all_nodes:
        label = node.get("label", "unknown")
        name = node.get("name", "unknown")
        by_label.setdefault(label, []).append(name)

    lines = []
    for label, names in by_label.items():
        names_str = ", ".join(names[:8])
        if len(names) > 8:
            names_str += f" ... (total {len(names)})"
        lines.append(f"- {label}: {names_str}")

    lines.append(f"\nnode_count: {len(all_nodes)}, link_count: {len(all_links)}")
    return "\n".join(lines)

def format_plan_for_planner(plan: List[str]) -> str:
    """Format plan for planner context."""
    if not plan:
        return "none"
    lines = []
    for i, step in enumerate(plan, 1):
        step_text = str(step).strip()
        if not step_text:
            continue
        lines.append(f"{i}. {step_text}")
    return "\n".join(lines) if lines else "none"

def extract_entity_names(all_nodes: List[dict]) -> List[str]:
    """提取所有实体名"""
    return [n.get("name") for n in all_nodes if n.get("name")]

# ==================== 合并函数 ====================

def merge_graph_by_name(
    existing_nodes: List[dict],
    existing_links: List[dict],
    new_nodes: List[dict],
    new_links: List[dict],
) -> tuple:
    """
    Merge graph data using node name as the stable key.
    Rebuilds link source/target using the merged node ids.
    """
    nodes = existing_nodes.copy()
    links = existing_links.copy()

    node_map = {n.get("name"): n for n in nodes if n.get("name")}
    existing_ids = [n.get("id") for n in nodes if isinstance(n.get("id"), int)]
    next_id = (max(existing_ids) + 1) if existing_ids else 0

    local_map = {}
    for n in new_nodes or []:
        name = n.get("name")
        if not name:
            continue
        nid = n.get("id")
        local_map[nid] = name
        local_map[str(nid)] = name

        if name in node_map:
            continue

        new_node = dict(n)
        new_node["id"] = next_id
        next_id += 1
        node_map[name] = new_node
        nodes.append(new_node)

    link_set = {(l.get("source"), l.get("target"), l.get("value")) for l in links}

    def resolve_name(raw_id):
        if raw_id in local_map:
            return local_map[raw_id]
        return local_map.get(str(raw_id))

    for link in new_links or []:
        src_name = resolve_name(link.get("source"))
        tgt_name = resolve_name(link.get("target"))
        if not src_name or not tgt_name:
            continue
        src_id = node_map[src_name]["id"]
        tgt_id = node_map[tgt_name]["id"]
        key = (src_id, tgt_id, link.get("value"))
        if key in link_set:
            continue
        link_set.add(key)
        links.append({
            "source": src_id,
            "target": tgt_id,
            "value": link.get("value"),
        })

    return nodes, links




# ==================== 节点函数 ====================

def planner_node(state: AgentState) -> AgentState:
    """Planner：决定下一步查询"""
    logger.info("Planner: 决定下一步")
    
    llm = get_llm()
    
    # 格式化当前结果
    current_results = format_results_for_planner(
        state.get("all_nodes", []),
        state.get("all_links", [])
    )
    current_plan = format_plan_for_planner(state.get("plan", []))
    
    
    
    prompt = PLANNER_PROMPT.format(
        available_labels=state["available_labels"],
        user_query=state["user_query"],
        current_results=current_results,
        current_plan=current_plan,
        last_error=state.get("error_message") or "none",
        last_cyphers="\n".join(state.get("last_cyphers") or []) or "none",
        last_step_stats=state.get("last_step_stats") or "none",
    )

    response = llm.invoke(prompt)
    decision = parse_json_from_text(response.content)
    
    if not decision:
        logger.warning("Planner: 无法解析决策，默认查询用户提及的实体")
        decision = {
            "action": "query",
            "step": f"查找与 {state['user_query']} 相关的节点",
            "reason": "无法解析 LLM 输出",
            "plan": [f"查找与 {state['user_query']} 相关的节点"],
        }
    
    logger.info(f"Planner 决策: {decision.get('action')} - {decision.get('reason', '')[:50]}")

    plan_candidate = decision.get("plan")
    new_plan = state.get("plan", []).copy() if state.get("plan") else []
    if isinstance(plan_candidate, list):
        new_plan = [str(s).strip() for s in plan_candidate if str(s).strip()]
    elif isinstance(plan_candidate, str) and plan_candidate.strip():
        new_plan = [line.strip() for line in plan_candidate.splitlines() if line.strip()]

    if decision.get("action") == "complete":
        return {**state, "is_complete": True, "plan": new_plan}
    
    # 记录新步骤
    step_text = decision.get("step") or (new_plan[0] if new_plan else "")
    if not step_text:
        step_text = f"查找与 {state['user_query']} 相关的节点"
    step_id = len(state.get("steps", [])) + 1
    new_step = {
        "step_id": step_id,
        "description": step_text,
        "cyphers": [],
        "result": None,
        "status": "pending",
    }
    
    new_steps = state.get("steps", []).copy()
    new_steps.append(new_step)
    
    return {
        **state,
        "plan": new_plan,
        "steps": new_steps,
        "current_step": step_id - 1,  # 索引
        "retry_count": 0,
    }


def cypher_generator_node(state: AgentState) -> AgentState:
    """Cypher Generator：使用统一的 CypherGenerator 类生成"""
    current_idx = state["current_step"]
    current_step = state["steps"][current_idx]
    
    logger.info(f"CypherGen: 步骤 {current_step['step_id']} - {current_step['description'][:50]}")
    
    # 初始化生成器
    generator = CypherGenerator()
    
    # 准备参数
    step_description = current_step["description"]
    # 提取已知实体作为上下文补充
    entity_names = extract_entity_names(state.get("all_nodes", []))
    if entity_names:
        step_description += f"\n\n已知实体：{', '.join(entity_names[:15])}"
        
    # 调用统一生成方法
    # 关键点：传入 original_query 作为全局 Context
    cypher_text = generator.generate(
        query=step_description,
        available_labels=state["available_labels"],
        original_query=state["user_query"],
    )
    
    # 解析多条 Cypher（统一清理 + 拆分）
    cyphers = CypherGenerator.split_statements(cypher_text)
    
    # Fallback if nothing usable
    if not cyphers:
        cyphers = ["MATCH (n) RETURN n LIMIT 10"]
    
    logger.info(f"CypherGen: 生成 {len(cyphers)} 条 Cypher")
    for i, c in enumerate(cyphers):
        logger.debug(f"  Cypher {i+1}: {c[:100]}{'...' if len(c) > 100 else ''}")
    
    # 记录到 cypher.log
    cypher_logger.info(f"Query: {state['user_query']}")
    cypher_logger.info(f"Step {current_step['step_id']}: {current_step['description']}")
    for i, c in enumerate(cyphers):
        cypher_logger.info(f"  Cypher {i+1}: {c}")
    cypher_logger.info("-" * 60)
    
    # 更新步骤
    new_steps = state["steps"].copy()
    new_steps[current_idx] = {**new_steps[current_idx], "cyphers": cyphers}
    
    return {**state, "steps": new_steps}


def executor_node(state: AgentState) -> AgentState:
    """Executor: run all Cypher and merge results."""
    current_idx = state["current_step"]
    current_step = state["steps"][current_idx]
    cyphers = current_step["cyphers"]

    logger.info(f"Executor: 执行 {len(cyphers)} 条 Cypher")

    step_nodes = []
    step_links = []
    errors = []
    cypher_stats = []

    all_nodes = state.get("all_nodes", []).copy()
    all_links = state.get("all_links", []).copy()

    for i, cypher in enumerate(cyphers):
        logger.info(f"Executor: 执行 Cypher {i+1}: {cypher[:80]}{'...' if len(cypher) > 80 else ''}")
        result = execute_cypher(cypher)
        if result["success"]:
            local_nodes = result.get("nodes", [])
            local_links = result.get("links", [])
            step_nodes.extend(local_nodes)
            step_links.extend(local_links)
            all_nodes, all_links = merge_graph_by_name(
                all_nodes,
                all_links,
                local_nodes,
                local_links,
            )
            logger.debug(
                f"Executor: Cypher {i+1} 返回了 {len(result.get('nodes', []))} 个节点"
            )
            cypher_stats.append(
                f"{i+1}. success nodes={len(result.get('nodes', []))} links={len(result.get('links', []))}"
            )
        else:
            errors.append(f"Cypher {i+1}: {result.get('error', 'unknown')}")
            logger.warning(f"Executor: Cypher {i+1} 失败 - {result.get('error')}")
            cypher_stats.append(f"{i+1}. failed error={result.get('error', 'unknown')}")

    # 按名称去重节点
    seen_names = set()
    unique_nodes = []
    for node in step_nodes:
        name = node.get("name")
        if name and name not in seen_names:
            seen_names.add(name)
            unique_nodes.append(node)

    # 更新步骤状态
    new_steps = state["steps"].copy()
    status = "success" if unique_nodes or not errors else "failed"
    new_steps[current_idx] = {
        **new_steps[current_idx],
        "result": {"nodes": unique_nodes, "links": step_links, "errors": errors},
        "status": status,
    }

    logger.info(f"Executor: 本步得到 {len(unique_nodes)} 节点，累计 {len(all_nodes)} 个")

    return {
        **state,
        "steps": new_steps,
        "all_nodes": all_nodes,
        "all_links": all_links,
        "error_message": "; ".join(errors) if errors else None,
        "last_cyphers": cyphers,
        "last_step_stats": "\n".join(cypher_stats) if cypher_stats else "none",
    }


# ==================== 路由函数 ====================

def check_next_action(state: AgentState) -> Literal["planner", "end"]:
    """检查下一步动作"""
    if state.get("is_complete"):
        return "end"
    
    # 检查是否达到最大步骤数
    if len(state.get("steps", [])) >= state.get("max_steps", 5):
        logger.info("达到最大步骤数，结束")
        return "end"
    
    return "planner"


# ==================== 构建图 ====================

def build_graph():
    """构建 LangGraph 工作流"""
    workflow = StateGraph(AgentState)
    
    # 添加节点
    workflow.add_node("planner", planner_node)
    workflow.add_node("cypher_generator", cypher_generator_node)
    workflow.add_node("executor", executor_node)
    
    # 设置入口
    workflow.set_entry_point("planner")
    
    # Planner 决定是否需要查询
    def planner_router(state: AgentState) -> Literal["cypher_generator", "end"]:
        if state.get("is_complete"):
            return "end"
        return "cypher_generator"
    
    workflow.add_conditional_edges(
        "planner",
        planner_router,
        {"cypher_generator": "cypher_generator", "end": END}
    )
    
    # Cypher Generator -> Executor
    workflow.add_edge("cypher_generator", "executor")
    
    # Executor 后回到 Planner 决定下一步
    workflow.add_conditional_edges(
        "executor",
        check_next_action,
        {"planner": "planner", "end": END}
    )
    
    return workflow.compile()


# ==================== 主接口 ====================

class QueryAgent:
    """查询代理：封装 LangGraph 工作流"""
    
    def __init__(self, max_steps: int = 5, max_retries: int = 3):
        self.graph = build_graph()
        self.max_steps = max_steps
        self.max_retries = max_retries
    
    def query(self, user_query: str) -> dict:
        """
        执行查询
        
        Args:
            user_query: 用户自然语言查询
            
        Returns:
            包含 nodes, links, steps, success 的字典
        """
        initial_state: AgentState = {
            "user_query": user_query,
            "available_labels": get_available_labels(),
            "plan": [],
            "steps": [],
            "all_nodes": [],
            "all_links": [],
            "current_step": 0,
            "is_complete": False,
            "error_message": None,
            "max_steps": self.max_steps,
            "retry_count": 0,
            "max_retries": self.max_retries,
            "last_cyphers": [],
            "last_step_stats": "none",
        }
        
        logger.info(f"QueryAgent: 开始处理查询: {user_query}")
        
        try:
            final_state = self.graph.invoke(initial_state)
            
            nodes = final_state.get("all_nodes", [])
            links = final_state.get("all_links", [])
            steps = [
                {
                    "step_id": s["step_id"],
                    "description": s["description"],
                    "status": s["status"],
                    "node_count": len(s.get("result", {}).get("nodes", [])) if s.get("result") else 0,
                }
                for s in final_state.get("steps", [])
            ]
            
            success = len(nodes) > 0
            
            logger.info(f"QueryAgent: 完成，{len(steps)} 步，{len(nodes)} 节点")
            
            return {
                "success": success,
                "nodes": nodes,
                "links": links,
                "plan": final_state.get("plan", []),
                "steps": steps,
            }
                
        except Exception as e:
            logger.error(f"QueryAgent: 执行失败 - {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "nodes": [],
                "links": [],
                "error": str(e),
            }
