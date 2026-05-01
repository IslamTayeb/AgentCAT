"""
Schema 演化 LangGraph 模块：根据 PDF 内容多轮演化 JSON Schema。
"""
import json
import operator
from pathlib import Path
from typing import Literal

from langchain.messages import AnyMessage, SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict, Annotated

from .client import APIClient
from .config import SchemaEvolutionConfig
from .planning import DomainFramework
from .utils import strip_code_fences, validate_json, get_response_text, extract_json_from_text, extract_json_with_retry
from .exceptions import FailedAttempt
from .logger import get_logger

logger = get_logger("paper_extract.schema_evo")


# Prompt 模板（动态填充）
PROMPT_TEMPLATE = """You are an expert in {role_description}.

TASK
Perform SCHEMA EVOLUTION (not data extraction).

Your goal is to minimally evolve a JSON schema so that it can serve as a
MASTER EXTRACTION SCHEMA capable of fully representing the structure,
content, and information granularity of the REFERENCE_ARTICLE.

This is schema evolution, not reinterpretation.
The evolved schema should allow the article to be mapped into structured fields
without loss of information or forced free-text aggregation.

Preserve backward compatibility at all times.

CONTEXT
round={{round_idx}} / max_rounds={{max_rounds}}

INPUTS
CURRENT_SCHEMA (authoritative; empty means first round):
{{current_schema_json}}

REFERENCE_ARTICLE
The reference article is provided as an attached PDF file (input_file).
Read and use its full content.

KEY ELEMENTS FRAMEWORK
The schema MUST be capable of expressing, in structured form, information of the
following categories if they appear in the article:

{key_elements_description}

Only evolve the schema when the article contains such information
AND it cannot be cleanly stored using existing structured fields.

STRICT RULES
1) CURRENT_SCHEMA is authoritative — NEVER delete existing fields or entities.
2) Do NOT rename fields unless backward-compatible mapping is explicitly possible.
3) Allowed schema evolution actions:
   - add new top-level entities if they represent recurring scientific concepts
   - add structured sub-fields to replace overloaded descriptive text
   - normalize repeated information patterns across entities
4) Forbidden actions:
   - figure-specific, table-specific, or experiment-instance-only fields
   - encoding values, mechanisms, or conclusions into field names
   - speculative entities not clearly supported by the article

DO NOT over-generalize beyond what is required to represent the article.
The schema should remain concrete and close to the data organization style
demonstrated by the reference JSON.

SCHEMA OUTPUT REQUIREMENTS (STRICT)
- Output ONLY the full UPDATED schema as raw JSON
- No explanations, no markdown, no code fences
- The output should have TOP-LEVEL SECTIONS directly as keys (e.g., "catalyst", "metal_modifications", "adsorption_sites")
- Each section is an object containing: type, required, properties, description, introduced_in_round
- For EVERY entity and field include:
  - type
  - required (true / false)
  - description
  - introduced_in_round (round number or "unchanged")
- Preserve existing field order wherever possible
- If no evolution is required:
  - output the schema unchanged
  - set introduced_in_round = "unchanged" for all fields

EXAMPLE OUTPUT STRUCTURE:
{{{{
  "section_name_1": {{{{
    "type": "object",
    "required": true,
    "description": "...",
    "introduced_in_round": i,
    "properties": {{{{
      "field_1": {{{{ "type": "string", "required": true, "description": "...", "introduced_in_round": i }}}},
      ...
    }}}}
  }}}},
  "section_name_2": {{{{
    ...
  }}}}
}}}}
"""


class EvoState(TypedDict):
    """Schema 演化状态"""
    messages: Annotated[list[AnyMessage], operator.add]
    pdf_paths: list[str]  # PDF 文件路径列表
    file_ids: list[str]  # 各 PDF 对应的已上传文件 ID
    round_idx: int
    max_rounds: int
    current_schema_json: str
    updated_schema_json: str
    last_error: str
    # 动态 Prompt 字段
    role_description: str
    key_elements_description: str


def build_schema_evo_graph(client: APIClient):
    """构建 Schema 演化 LangGraph"""

    def llm_call(state: EvoState):
        """LLM 调用节点"""
        num_pdfs = len(state["pdf_paths"])
        pdf_index = state["round_idx"] % num_pdfs
        pdf_path = state["pdf_paths"][pdf_index]
        file_ids = list(state.get("file_ids") or [])
        
        logger.debug(f"llm_call: round_idx={state['round_idx']}, pdf_index={pdf_index}, file_ids={file_ids}")

        # 确保 file_ids 列表足够长
        while len(file_ids) < num_pdfs:
            file_ids.append("")

        # 上传文件（如果还没有）
        if not file_ids[pdf_index]:
            logger.debug(f"llm_call: uploading PDF[{pdf_index}]")
            try:
                file_id = client.upload_pdf_and_wait(pdf_path)
                file_ids[pdf_index] = file_id
                logger.debug(f"Uploaded PDF [{pdf_index}]: {pdf_path} -> {file_id}")
            except Exception as e:
                logger.error(f"llm_call: upload failed: {e}")
                return {
                    "last_error": f"file upload failed: {type(e).__name__}: {e}",
                    "updated_schema_json": "",
                }
            logger.debug(f"llm_call: upload complete, returning file_ids={file_ids}")
            return {
                "file_ids": file_ids,
                "messages": [SystemMessage(content=f"FILE_UPLOADED:{file_id}")],
            }

        # Schema 演化
        current_file_id = file_ids[pdf_index]
        logger.info(f"Round {state['round_idx']}/{state['max_rounds']}: using PDF [{pdf_index}] {Path(pdf_path).name}")

        escaped_key_elements = state["key_elements_description"].replace("{", "{{").replace("}", "}}")
        prompt_with_framework = PROMPT_TEMPLATE.format(
            role_description=state["role_description"],
            key_elements_description=escaped_key_elements,
        )
        prompt = prompt_with_framework.format(
            round_idx=state["round_idx"],
            max_rounds=state["max_rounds"],
            current_schema_json=(state.get("current_schema_json", "") or "").strip(),
        )

        resp = client.generate_content(
            file_name=current_file_id,
            prompt=prompt,
            response_mime_type="application/json",
        )

        text = get_response_text(resp)
        return {"messages": [SystemMessage(content=text)]}

    def postprocess_node(state: EvoState):
        """后处理节点：验证 JSON"""
        # 如果已经有错误（如上传失败），直接返回保留错误
        existing_error = state.get("last_error")
        if existing_error:
            logger.debug(f"postprocess: existing error detected, preserving: {existing_error}")
            return {}
        
        last = state["messages"][-1]
        raw = getattr(last, "content", "") or ""
        raw = raw.strip()
        
        logger.debug(f"postprocess: message type = {raw[:50] if len(raw) > 50 else raw}...")

        if raw.startswith("FILE_UPLOADED:"):
            logger.debug("postprocess: FILE_UPLOADED detected, returning empty")
            return {"last_error": ""}

        # 定义 JSON 解析失败时的重试回调
        def on_json_retry(failed_attempt: FailedAttempt) -> str:
            """JSON 解析失败时，让 LLM 重新生成响应"""
            logger.info(f"Schema JSON 解析失败，尝试重新生成 (轮次: {state['round_idx']}, 尝试: {failed_attempt.attempt_number})")
            
            # 获取当前 PDF 信息
            num_pdfs = len(state["pdf_paths"])
            pdf_index = state["round_idx"] % num_pdfs
            current_file_id = state["file_ids"][pdf_index]
            
            # 构造修正提示
            escaped_key_elements = state["key_elements_description"].replace("{", "{{").replace("}", "}}")
            prompt_with_framework = PROMPT_TEMPLATE.format(
                role_description=state["role_description"],
                key_elements_description=escaped_key_elements,
            )
            base_prompt = prompt_with_framework.format(
                round_idx=state["round_idx"],
                max_rounds=state["max_rounds"],
                current_schema_json=(state.get("current_schema_json", "") or "").strip(),
            )
            
            fix_prompt = f"""{base_prompt}

IMPORTANT: Your previous response could not be parsed as valid JSON.
Error: {failed_attempt.exception}
Please ensure your output is ONLY valid JSON with no additional text or markdown formatting.
"""
            try:
                retry_resp = client.generate_content(
                    file_name=current_file_id,
                    prompt=fix_prompt,
                    response_mime_type="application/json",
                )
                return get_response_text(retry_resp)
            except Exception as e:
                logger.warning(f"重试 API 调用失败: {e}")
                return ""

        # 使用带重试的 JSON 解析
        obj, failed_attempts = extract_json_with_retry(
            raw,
            max_attempts=3,
            on_retry=on_json_retry,
            section_name="schema",
        )

        if obj is None:
            logger.error(f"postprocess: JSON parse failed after {len(failed_attempts)} attempts")
            for fa in failed_attempts:
                logger.debug(f"  Attempt {fa.attempt_number}: {fa.exception}")
            return {"updated_schema_json": "", "last_error": "schema JSON parse failed"}

        pretty = json.dumps(obj, ensure_ascii=False, indent=2)
        logger.debug(f"postprocess: schema parsed successfully, {len(obj)} top-level keys")
        return {
            "updated_schema_json": pretty,
            "current_schema_json": pretty,
            "last_error": "",
        }

    def increment_round_node(state: EvoState):
        """递增轮次（仅在 schema 演化成功时递增，文件上传不计数）"""
        has_error = state.get("last_error")
        has_schema = bool(state.get("updated_schema_json"))
        current_round = state["round_idx"]
        
        logger.debug(f"increment_round: round={current_round}, has_error={bool(has_error)}, has_schema={has_schema}")
        
        if has_error:
            logger.debug("increment_round: has error, not incrementing")
            return {}
        # 只有当本轮有新的 schema 输出时才递增（文件上传时 updated_schema_json 不会在本轮更新）
        if has_schema:
            logger.debug(f"increment_round: incrementing to {current_round + 1}")
            return {"round_idx": current_round + 1, "updated_schema_json": ""}
        logger.debug("increment_round: no schema update, not incrementing")
        return {}

    def decide_loop(state: EvoState) -> Literal["llm_call", "__end__"]:
        """决定是否继续循环"""
        if state.get("last_error"):
            return END
        if state["round_idx"] >= state["max_rounds"]:
            return END
        return "llm_call"

    # 构建图
    builder = StateGraph(EvoState)
    builder.add_node("llm_call", llm_call)
    builder.add_node("postprocess", postprocess_node)
    builder.add_node("increment_round", increment_round_node)

    builder.add_edge(START, "llm_call")
    builder.add_edge("llm_call", "postprocess")
    builder.add_edge("postprocess", "increment_round")
    builder.add_conditional_edges("increment_round", decide_loop, ["llm_call", END])

    return builder.compile()


def run_schema_evolution(
    client: APIClient,
    config: SchemaEvolutionConfig,
    pdf_paths: list[str],
    framework: DomainFramework,
    file_ids: list[str] | None = None,
) -> tuple[dict, str, list[str]]:
    """
    运行 Schema 演化。

    Args:
        client: API 客户端
        config: 演化配置
        pdf_paths: PDF 文件路径列表
        framework: 领域要素框架
        file_ids: 已上传的文件 ID 列表（可选）

    Returns:
        (schema_dict, schema_json, file_ids)

    Raises:
        ValueError: rounds 不能被 PDF 数量整除
    """
    num_pdfs = len(pdf_paths)
    if num_pdfs == 0:
        raise ValueError("必须提供至少一个 PDF 文件")

    if config.rounds % num_pdfs != 0:
        raise ValueError(
            f"rounds ({config.rounds}) 必须能被 PDF 数量 ({num_pdfs}) 整除"
        )

    logger.info(f"Starting schema evolution: rounds={config.rounds}, pdfs={num_pdfs}")
    for i, p in enumerate(pdf_paths):
        logger.debug(f"  PDF [{i}]: {p}")

    # 加载初始 schema
    initial_schema = ""
    if config.initial_schema:
        logger.debug(f"Loading initial schema: {config.initial_schema}")
        with open(config.initial_schema, "r", encoding="utf-8") as f:
            initial_schema = f.read()

    graph = build_schema_evo_graph(client)

    init_state: EvoState = {
        "messages": [HumanMessage(content="开始 Schema 演化")],
        "pdf_paths": pdf_paths,
        "file_ids": file_ids or [],
        "round_idx": 0,
        "max_rounds": config.rounds,
        "current_schema_json": initial_schema,
        "updated_schema_json": "",
        "last_error": "",
        "role_description": framework.role_description,
        "key_elements_description": framework.format_for_prompt(),
    }

    logger.debug("Invoking schema evolution graph")
    recursion_limit = max(200, config.rounds * 5)
    out = graph.invoke(init_state, {"recursion_limit": recursion_limit})

    if out.get("last_error"):
        logger.error(f"Schema evolution failed: {out['last_error']}")
        raise RuntimeError(f"Schema evolution failed: {out['last_error']}")

    schema_json = out.get("current_schema_json", "")
    schema_dict = json.loads(schema_json) if schema_json else {}
    logger.info(f"Schema evolution complete: {len(schema_dict)} top-level keys")

    return schema_dict, schema_json, out.get("file_ids", [])
