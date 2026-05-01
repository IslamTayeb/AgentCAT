# -*- coding: utf-8 -*-
"""
交互式规划模块

通过 LangGraph Human-in-the-loop 模式，引导用户描述领域和刻画要素，生成关键要素列表。
"""

import json
import os
from dataclasses import dataclass, field
from typing import Optional
from typing_extensions import TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt, Command

from .client import APIClient
from .config import PlanningConfig
from .utils import get_response_text, extract_json_with_retry
from .exceptions import FailedAttempt
from .logger import get_logger

logger = get_logger("paper_extract.planning")


@dataclass
class SubField:
    """子字段定义（关键要素提示）"""
    name: str
    description: str = ""
    sub_fields: list["SubField"] = field(default_factory=list)

    def to_dict(self) -> dict:
        """转换为字典（支持递归子字段）"""
        return {
            "name": self.name,
            "description": self.description,
            "sub_fields": [sf.to_dict() for sf in self.sub_fields],
        }

    @classmethod
    def from_dict(cls, data: object) -> "SubField":
        """从字典创建（支持递归子字段）"""
        if isinstance(data, str):
            return cls(name=data, description="")
        if not isinstance(data, dict):
            return cls(name=str(data), description="")
        sub_fields = [cls.from_dict(sf) for sf in data.get("sub_fields", [])]
        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            sub_fields=sub_fields,
        )


@dataclass
class KeyElement:
    """关键要素类别"""
    name: str
    description: str = ""
    sub_fields: list[SubField] = field(default_factory=list)

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "name": self.name,
            "description": self.description,
            "sub_fields": [sf.to_dict() for sf in self.sub_fields],
        }

    @classmethod
    def from_dict(cls, data: object) -> "KeyElement":
        """从字典创建"""
        if not isinstance(data, dict):
            return cls(name=str(data), description="")
        sub_fields = [SubField.from_dict(sf) for sf in data.get("sub_fields", [])]
        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            sub_fields=sub_fields,
        )


@dataclass
class DomainFramework:
    """领域要素框架"""
    domain_name: str
    role_description: str
    key_elements: list[KeyElement] = field(default_factory=list)
    raw_framework_text: str = ""  # 原始 LLM 输出，用于调试

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "domain_name": self.domain_name,
            "role_description": self.role_description,
            "key_elements": [e.to_dict() for e in self.key_elements],
            "raw_framework_text": self.raw_framework_text,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DomainFramework":
        """从字典创建"""
        if not isinstance(data, dict):
            data = {}
        key_elements = [KeyElement.from_dict(elem_data) for elem_data in data.get("key_elements", [])]
        
        # 处理 role_description 可能为 dict 的情况
        role_desc = data.get("role_description", "")
        if isinstance(role_desc, dict):
            role_desc = role_desc.get("description", str(role_desc))
            
        return cls(
            domain_name=data.get("domain_name", ""),
            role_description=role_desc,
            key_elements=key_elements,
            raw_framework_text=data.get("raw_framework_text", ""),
        )

    def format_for_prompt(self) -> str:
        """格式化为 Prompt 中使用的文本（关键要素列表）"""
        def format_sub_fields(sub_fields: list[SubField], indent: str) -> list[str]:
            lines = []
            for sf in sub_fields:
                desc = sf.description or ""
                lines.append(f"{indent}- {sf.name}: {desc}")
                if sf.sub_fields:
                    lines.extend(format_sub_fields(sf.sub_fields, indent + "  "))
            return lines

        lines = []
        for i, elem in enumerate(self.key_elements, 1):
            lines.append(f"{i}) {elem.name}")
            if elem.description:
                lines.append(f"   {elem.description}")
            lines.extend(format_sub_fields(elem.sub_fields, "   "))
            lines.append("")
        return "\n".join(lines)

    def save(self, path: str) -> None:
        """保存到文件"""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        logger.info(f"Framework saved to: {path}")

    @classmethod
    def load(cls, path: str) -> "DomainFramework":
        """从文件加载"""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Framework loaded from: {path}")
        return cls.from_dict(data)


# Prompt 模板
PLANNING_PROMPT_GENERIC = """You are an expert Scientific Knowledge Engineer and Ontology Designer.

### TASK
Design a hierarchical "Structured Key Elements Framework" for information extraction from scientific literature.
Your framework should capture the essential entities, relationships, and attributes specific to the user's domain.

### DESIGN PRINCIPLES
1. **Hierarchical Structure**: Top-level categories -> sub-fields -> nested attributes
2. **Domain Completeness**: Cover all critical aspects of the scientific domain
3. **Extraction-Oriented**: Each element should be extractable from text
4. **Relationship-Aware**: Consider how elements relate to each other

### OUTPUT FORMAT (JSON)
{{
  "domain_name": "...",
  "role_description": "An expert in [domain] who specializes in...",
  "key_elements": [
    {{
      "name": "category_name_snake_case",
      "description": "What this category captures",
      "sub_fields": [
        {{"name": "field_name", "description": "...", "sub_fields": [...]}}
      ]
    }}
  ]
}}

### DOMAIN BASELINE (OPTIONAL)
If the domain baseline below is relevant, use it as a starting scaffold and expand based on user requirements.
If it is not relevant, ignore it and design a similarly deep framework.

{domain_baseline}

### CRITICAL INSTRUCTIONS
1. **Priority**: If the user asks for specific physical parameters (e.g., "d-band center", "imaginary frequency"), you MUST inject them as explicit `sub_fields` into the relevant categories.
2. **Granularity**: If the user requests "micro-kinetic" or "mechanistic" depth, you must expand the `adsorption_sites`/`active_sites` and `elementary_steps` branches significantly.

### USER INPUT
{user_input}

Output ONLY valid JSON. No markdown, no explanations.
"""

DOMAIN_BASELINES = {
    "heterogeneous_catalysis": """
### DOMAIN BASELINE (HETEROGENEOUS CATALYSIS)
Use this structure as your starting foundation, expand based on user requirements:
1. catalyst_support_description: catalyst_name, framework_type, morphology, pore_system, heteroatoms, defect_types, acid_sites, hydroxyl_species, compositional_notes, structural_notes.
2. metal_modifications: metal_identity, loading, oxidation_state, electronic_state, particle_size, geometry, location, coordination_environment, metal_support_interaction, synthesis_history, treatment_history, catalytic_role.
3. adsorption_sites: adsorbate_identity, site_type, chemical_environment, adsorption_energy (value, unit, method), spectroscopic_evidence, microscopic_evidence.
4. intermediates: species_identity, species_type (surface/adsorbed/framework-bound), bound_site, detection_method, spectral_signatures, mechanistic_role.
5. elementary_steps: step_id, step_description, reactants, products, active_site_involve, interface, activation_energy, reaction_energy, mechanistic_notes, step_order.
6. secondary_paths: pathway_type (undesired/degradation), triggering_features, evidence, suppression_strategies.
7. reaction_system: reaction_type, phase, temperature, pressure, feed_composition, reactor_configuration, pretreatment, selectivity, stability, performance_notes.
8. diffusion_transport: diffusion_mechanism (pore-level/surface), spillover_processes, role_in_selectivity, role_in_stability.
""",
    "generic": "",
}

DOMAIN_KEYWORDS = {
    "heterogeneous_catalysis": {
        "strong": [
            "heterogeneous catalysis",
            "heterogeneous",
            "catalyst support",
            "metal support",
            "zeolite",
            "adsorption site",
            "异相催化",
            "异相",
            "载体",
            "沸石",
        ],
        "weak": [
            "catalysis",
            "catalyst",
            "catalytic",
            "adsorption",
            "active site",
            "reaction mechanism",
            "deoxygenation",
            "pyrolysis",
            "催化",
            "催化剂",
            "吸附",
            "活性位",
            "反应机理",
            "金属",
            "脱氧",
            "热解",
        ],
    },
}

DOMAIN_MIN_SCORE = 3


def _select_domain_baseline(user_input: str) -> tuple[str, str]:
    """根据用户输入自动选择领域模板（方案A）"""
    text = (user_input or "").casefold()
    best_domain = "generic"
    best_score = 0
    for domain, keyword_groups in DOMAIN_KEYWORDS.items():
        strong = keyword_groups.get("strong", [])
        weak = keyword_groups.get("weak", [])
        strong_hits = sum(1 for kw in strong if kw in text)
        weak_hits = sum(1 for kw in weak if kw in text)
        score = strong_hits * 3 + weak_hits
        if score > best_score:
            best_domain = domain
            best_score = score
    if best_score < DOMAIN_MIN_SCORE:
        best_domain = "generic"
    baseline = DOMAIN_BASELINES.get(best_domain, "")
    logger.info(f"Selected domain baseline: {best_domain} (score={best_score})")
    return best_domain, baseline

REFINEMENT_PROMPT = """You are an expert in scientific literature analysis and ontology design.

TASK
Refine the existing KEY ELEMENTS FRAMEWORK based on user feedback.

CURRENT FRAMEWORK:
{current_framework}

USER FEEDBACK:
{user_feedback}

INSTRUCTIONS:
- Apply the user's requested additions, deletions, or modifications
- Maintain the overall structure and naming conventions
- Ensure all sub_fields have name and description

OUTPUT FORMAT (JSON):
{{
  "domain_name": "...",
  "role_description": "...",
  "key_elements": [...]
}}

Output ONLY valid JSON. No explanations, no markdown.
"""


def _parse_framework_response(raw_text: str, on_retry: callable = None) -> Optional[DomainFramework]:
    """
    解析 LLM 响应为 DomainFramework。
    
    Args:
        raw_text: LLM 原始响应文本
        on_retry: JSON 解析失败时的回调函数，用于获取新文本
    """
    obj, failed_attempts = extract_json_with_retry(
        raw_text,
        max_attempts=3,
        on_retry=on_retry,
        section_name="framework",
    )
    
    if obj is None:
        logger.error(f"Failed to parse framework JSON after {len(failed_attempts)} attempts")
        for fa in failed_attempts:
            logger.debug(f"  Attempt {fa.attempt_number}: {fa.exception}")
        return None

    try:
        framework = DomainFramework.from_dict(obj)
        framework.raw_framework_text = raw_text
        return framework
    except Exception as e:
        logger.error(f"Failed to construct DomainFramework: {e}")
        return None


def _display_framework(framework: DomainFramework) -> None:
    """在命令行显示框架"""
    print("\n" + "=" * 60)
    print(f"领域名称: {framework.domain_name}")
    print(f"角色设定: {framework.role_description}")
    print("=" * 60)
    print("\n关键要素框架:")
    print(framework.format_for_prompt())
    print("=" * 60)


def _format_framework_display(framework: DomainFramework) -> str:
    """格式化框架为显示字符串"""
    lines = [
        "\n" + "=" * 60,
        f"领域名称: {framework.domain_name}",
        f"角色设定: {framework.role_description}",
        "=" * 60,
        "\n关键要素框架:",
        framework.format_for_prompt(),
        "=" * 60,
    ]
    return "\n".join(lines)


# ============================================================================
# LangGraph Human-in-the-loop 实现
# ============================================================================

class PlanningState(TypedDict):
    """规划阶段图状态"""
    user_input: str                        # 用户初始输入
    framework: Optional[dict]              # 当前框架 (dict 形式)
    user_feedback: str                     # 用户反馈
    action: str                            # 动作: "generate" | "confirm" | "restart" | "refine"
    done: bool                             # 是否完成


def _create_planning_graph(client: APIClient):
    """
    创建规划阶段的 StateGraph。
    
    图结构:
    START -> collect_input -> generate_framework -> ask_feedback -> route_action
    """

    def collect_input_node(state: PlanningState) -> dict:
        """收集用户初始输入"""
        # 显示提示
        prompt_text = """
============================================================
[规划阶段] 请描述你想抽取的文献领域和刻画要素。
你可以：
  1) 直接给出刻画要素列表
  2) 只提供领域概述，让我帮你设计要素框架
============================================================

请输入（输入完成后，另起一行输入 END 并回车）："""
        
        # 使用 interrupt 暂停等待用户输入
        response = interrupt({"query": prompt_text, "type": "initial_input"})
        user_input = response.get("data", "")
        
        logger.debug(f"User input: {user_input[:200]}...")
        return {"user_input": user_input, "action": "generate"}

    def generate_framework_node(state: PlanningState) -> dict:
        """调用 LLM 生成框架"""
        print("\n[生成中...] 正在根据你的描述生成关键要素框架...")
        
        _, domain_baseline = _select_domain_baseline(state["user_input"])
        prompt = PLANNING_PROMPT_GENERIC.format(
            user_input=state["user_input"],
            domain_baseline=domain_baseline,
        )
        
        try:
            resp = client.generate_content_text_only(
                prompt=prompt,
                response_mime_type="application/json",
            )
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            raise RuntimeError(f"规划阶段 LLM 调用失败: {e}")

        raw_text = get_response_text(resp)
        
        # 定义 JSON 解析失败时的重试回调
        def on_json_retry(failed_attempt: FailedAttempt) -> str:
            logger.info(f"Framework JSON 解析失败，尝试重新生成 (尝试: {failed_attempt.attempt_number})")
            fix_prompt = f"""{prompt}

IMPORTANT: Your previous response could not be parsed as valid JSON.
Error: {failed_attempt.exception}
Please ensure your output is ONLY valid JSON with no additional text or markdown formatting.
"""
            try:
                retry_resp = client.generate_content_text_only(
                    prompt=fix_prompt,
                    response_mime_type="application/json",
                )
                return get_response_text(retry_resp)
            except Exception as e:
                logger.warning(f"重试 API 调用失败: {e}")
                return ""
        
        framework = _parse_framework_response(raw_text, on_retry=on_json_retry)

        if framework is None:
            logger.error("Failed to generate initial framework")
            logger.debug(f"Raw LLM output: {raw_text}")
            raise RuntimeError("无法解析框架 JSON，请检查 LLM 输出")

        return {"framework": framework.to_dict()}

    def ask_feedback_node(state: PlanningState) -> dict:
        """展示框架并请求用户反馈"""
        framework = DomainFramework.from_dict(state["framework"])
        
        # 构建显示内容
        display_text = _format_framework_display(framework)
        
        prompt_text = f"""{display_text}

请确认是否同意此框架？
  输入 'y' 或 '是' 确认
  输入 'n' 或 '否' 重新开始
  或直接输入修改意见（如：添加XXX、删除XXX、修改XXX）

你的选择："""
        
        # 使用 interrupt 暂停等待用户反馈
        response = interrupt({"query": prompt_text, "type": "feedback"})
        user_feedback = response.get("data", "").strip()
        
        # 分析用户反馈
        if user_feedback.lower() in ("y", "yes", "是", "确认", "同意"):
            action = "confirm"
            logger.info("User confirmed the framework")
        elif user_feedback.lower() in ("n", "no", "否", "重新"):
            action = "restart"
            logger.info("User requested restart")
        else:
            action = "refine"
            logger.info(f"User feedback: {user_feedback[:100]}...")
        
        return {"user_feedback": user_feedback, "action": action}

    def refine_framework_node(state: PlanningState) -> dict:
        """根据用户反馈调整框架"""
        print("\n[修改中...] 正在根据你的意见调整框架...")
        
        framework = DomainFramework.from_dict(state["framework"])
        
        refine_prompt = REFINEMENT_PROMPT.format(
            current_framework=json.dumps(framework.to_dict(), ensure_ascii=False, indent=2),
            user_feedback=state["user_feedback"],
        )

        try:
            resp = client.generate_content_text_only(
                prompt=refine_prompt,
                response_mime_type="application/json",
            )
        except Exception as e:
            logger.error(f"Refinement LLM call failed: {e}")
            print(f"\n[错误] 修改失败: {e}，请重试")
            # 返回原框架，让用户重新输入
            return {}

        raw_text = get_response_text(resp)
        
        # 定义 JSON 解析失败时的重试回调
        def on_json_retry(failed_attempt: FailedAttempt) -> str:
            logger.info(f"Refinement JSON 解析失败，尝试重新生成 (尝试: {failed_attempt.attempt_number})")
            fix_prompt = f"""{refine_prompt}

IMPORTANT: Your previous response could not be parsed as valid JSON.
Error: {failed_attempt.exception}
Please ensure your output is ONLY valid JSON with no additional text or markdown formatting.
"""
            try:
                retry_resp = client.generate_content_text_only(
                    prompt=fix_prompt,
                    response_mime_type="application/json",
                )
                return get_response_text(retry_resp)
            except Exception as e:
                logger.warning(f"重试 API 调用失败: {e}")
                return ""
        
        new_framework = _parse_framework_response(raw_text, on_retry=on_json_retry)

        if new_framework is None:
            print("\n[错误] 无法解析修改后的框架，请重试")
            return {}

        return {"framework": new_framework.to_dict()}

    def route_action(state: PlanningState) -> str:
        """根据用户动作决定下一步"""
        action = state.get("action", "")
        if action == "confirm":
            return "end"
        elif action == "restart":
            return "collect_input"
        else:  # refine
            return "refine"

    # 构建图
    graph_builder = StateGraph(PlanningState)
    
    # 添加节点
    graph_builder.add_node("collect_input", collect_input_node)
    graph_builder.add_node("generate_framework", generate_framework_node)
    graph_builder.add_node("ask_feedback", ask_feedback_node)
    graph_builder.add_node("refine_framework", refine_framework_node)
    
    # 添加边
    graph_builder.add_edge(START, "collect_input")
    graph_builder.add_edge("collect_input", "generate_framework")
    graph_builder.add_edge("generate_framework", "ask_feedback")
    graph_builder.add_edge("refine_framework", "ask_feedback")
    
    # 添加条件边
    graph_builder.add_conditional_edges(
        "ask_feedback",
        route_action,
        {
            "end": END,
            "collect_input": "collect_input",
            "refine": "refine_framework",
        }
    )
    
    return graph_builder


def run_planning(
    client: APIClient,
    config: PlanningConfig,
    output_dir: str,
) -> DomainFramework:
    """
    运行交互式规划阶段。
    
    使用 LangGraph Human-in-the-loop 模式，通过 interrupt() 机制暂停图执行等待用户输入。

    Args:
        client: API 客户端
        config: 规划配置
        output_dir: 输出目录（用于保存框架）

    Returns:
        确认后的 DomainFramework
    """
    logger.info("Starting interactive planning phase with LangGraph")

    # 创建图和 checkpointer
    graph_builder = _create_planning_graph(client)
    memory = InMemorySaver()
    graph = graph_builder.compile(checkpointer=memory)
    
    # 配置（用于 checkpointer）
    thread_config = {"configurable": {"thread_id": "planning-session-1"}}
    
    # 初始状态
    initial_state: PlanningState = {
        "user_input": "",
        "framework": None,
        "user_feedback": "",
        "action": "",
        "done": False,
    }
    
    # 当前输入：初始为 initial_state (dict)，后续为 Command 对象
    current_input = initial_state
    framework = None
    
    while True:
        # 执行图直到遇到 interrupt 或结束
        try:
            graph.invoke(current_input, config=thread_config)
        except Exception as e:
            # 检查是否是 interrupt 导致的异常（某些版本可能抛出异常）
            error_str = str(e)
            if "interrupt" not in error_str.lower():
                raise
        
        # 获取图的当前状态快照
        state_snapshot = graph.get_state(thread_config)
        
        # 检查图是否已完成（没有待执行的节点）
        if not state_snapshot.next:
            # 图已完成，检查是否有有效的 framework
            if state_snapshot.values.get("framework"):
                framework = DomainFramework.from_dict(state_snapshot.values["framework"])
                break
            else:
                raise RuntimeError("规划阶段异常结束：未生成有效的框架")
        
        # 图未完成，说明遇到了 interrupt，需要收集用户输入
        # 从 tasks 中获取 interrupt 值
        interrupt_value = None
        if hasattr(state_snapshot, 'tasks') and state_snapshot.tasks:
            for task in state_snapshot.tasks:
                if hasattr(task, 'interrupts') and task.interrupts:
                    interrupt_value = task.interrupts[0].value
                    break
        
        if interrupt_value is None:
            logger.warning("Could not find interrupt value, using default prompt")
            interrupt_value = {"query": "请输入：", "type": "unknown"}
        
        # 显示提示并收集用户输入
        query = interrupt_value.get("query", "请输入：")
        print(query, end="")
        
        # 根据 interrupt 类型收集用户输入
        if interrupt_value.get("type") == "initial_input":
            # 多行输入模式（用于初始领域描述，输入 END 结束）
            lines = []
            while True:
                try:
                    line = input()
                    if line.strip() == "END":
                        break
                    lines.append(line)
                except EOFError:
                    break
            user_response = "\n".join(lines)
        else:
            # 单行输入模式（用于确认/反馈）
            try:
                user_response = input()
            except EOFError:
                user_response = "y"
        
        # 使用 Command 恢复执行，传递用户输入
        current_input = Command(resume={"data": user_response})

    # 保存框架
    if config.save_framework:
        os.makedirs(output_dir, exist_ok=True)
        framework_path = os.path.join(output_dir, config.framework_file)
        framework.save(framework_path)
        
        # 保存 key_elements_description 文本（供 schema evolution 使用）
        description_path = os.path.join(output_dir, "key_elements_description.txt")
        with open(description_path, "w", encoding="utf-8") as f:
            f.write(framework.format_for_prompt())
        logger.info(f"Key elements description saved to: {description_path}")

    logger.info("Planning phase complete")
    return framework
