"""
数据抽取 LangGraph 模块

根据 Schema 从 PDF 抽取结构化数据。
"""

import json
import operator
import os
from typing import Literal, Optional

from langchain.messages import AnyMessage
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict, Annotated

from .client import APIClient
from .config import ExtractionConfig
from .exceptions import FailedAttempt
from .utils import (
    strip_code_fences,
    validate_json,
    get_response_text,
    strip_introduced_in_round,
    extract_json_from_text,
    extract_json_with_retry,
)
from .logger import get_logger

logger = get_logger("paper_extract.extract")


# Prompt 模板（动态填充角色设定）
CANDIDATE_PROMPT = """You are an expert in {role_description}.

TASK
For the given SCHEMA SECTION, extract relevant text segments from the PDF.
Organize candidates by SUB-FIELDS (first-level keys within this section).

TARGET_SCHEMA_SECTION: {section_name}

SCHEMA DEFINITION (the complete structure you need to populate):
{schema_section_json}

INSTRUCTIONS
- Scan the entire article
- For EACH sub-field in the schema, extract verbatim text segments that are relevant
- If no relevant text found for a sub-field, use empty array []
- Extract original text only, do NOT interpret

OUTPUT FORMAT
{{
  "sub_field_1": ["verbatim quote 1", "verbatim quote 2"],
  "sub_field_2": ["verbatim quote 3"],
  ...
}}

Output ONLY valid JSON.
"""

# 带审核反馈提示的 Prompt（重抽时使用）
CANDIDATE_PROMPT_WITH_HINTS = """You are an expert in {role_description}.

TASK
For the given SCHEMA SECTION, extract relevant text segments from the PDF.
This is a RE-EXTRACTION due to issues found in previous extraction.

TARGET_SCHEMA_SECTION: {section_name}

SCHEMA DEFINITION:
{schema_section_json}

IMPORTANT - ISSUES FROM PREVIOUS EXTRACTION:
{retry_hints}

Please pay special attention to the issues above and ensure accurate extraction.

INSTRUCTIONS
- Scan the entire article carefully
- For EACH sub-field, extract verbatim text segments that are relevant
- If no relevant text found for a sub-field, use empty array []
- Extract original text only, do NOT interpret
- Address the issues mentioned above

OUTPUT FORMAT
{{
  "sub_field_1": ["verbatim quote 1", "verbatim quote 2"],
  "sub_field_2": ["verbatim quote 3"],
  ...
}}

Output ONLY valid JSON.
"""


RESOLVE_PROMPT = """You are an expert in {role_description}.

TASK
Populate the SCHEMA SECTION below using the provided evidence candidates and the PDF.

EVIDENCE HIERARCHY
1) PRIMARY: Use the provided EVIDENCE_CANDIDATES as your main source
2) AUTHORITATIVE: The attached PDF is the ultimate source of truth
   - If candidates are incomplete or ambiguous, refer to the PDF directly
   - All extracted information MUST be verifiable in the PDF

STRICT RULES
1) Do NOT invent or infer information not present in the PDF
2) Prefer explicit, quantitative evidence from the PDF
3) If information is missing from both candidates AND PDF, use null or omit optional fields
4) Output must conform EXACTLY to the schema structure

SCHEMA_SECTION (defines the required output structure):
{schema_section_json}

EVIDENCE_CANDIDATES (organized by sub-field, extracted from PDF):
{candidates_json}

OUTPUT
Return ONLY valid JSON conforming exactly to the schema structure above.
The output should be: {{ "{section_name}": {{ ... }} }}
"""

# 带审核反馈提示的 Resolve Prompt（重抽时使用）
RESOLVE_PROMPT_WITH_HINTS = """You are an expert in {role_description}.

TASK
Populate the SCHEMA SECTION below using the provided evidence candidates and the PDF.
This is a RE-EXTRACTION due to issues found in previous extraction.

IMPORTANT - ISSUES FROM PREVIOUS EXTRACTION:
{retry_hints}

Please pay special attention to the issues above and ensure accurate population.

EVIDENCE HIERARCHY
1) PRIMARY: Use the provided EVIDENCE_CANDIDATES as your main source
2) AUTHORITATIVE: The attached PDF is the ultimate source of truth
   - If candidates are incomplete or ambiguous, refer to the PDF directly
   - All extracted information MUST be verifiable in the PDF

STRICT RULES
1) Do NOT invent or infer information not present in the PDF
2) Prefer explicit, quantitative evidence from the PDF
3) If information is missing from both candidates AND PDF, use null or omit optional fields
4) Output must conform EXACTLY to the schema structure
5) Address the issues mentioned above

SCHEMA_SECTION (defines the required output structure):
{schema_section_json}

EVIDENCE_CANDIDATES (organized by sub-field, extracted from PDF):
{candidates_json}

OUTPUT
Return ONLY valid JSON conforming exactly to the schema structure above.
The output should be: {{ "{section_name}": {{ ... }} }}
"""


class ExtractState(TypedDict):
    """抽取状态"""
    messages: Annotated[list[AnyMessage], operator.add]
    pdf_path: str
    file_id: str
    output_dir: str
    schema: dict
    schema_sections: list[str]
    section_idx: int
    candidates: dict
    extracted: dict
    keep_temp: bool
    last_error: str
    # 动态 Prompt 字段
    role_description: str
    # 重抽提示（审核反馈）
    retry_hints: dict  # {section_name: hint_text}


def build_extract_graph(client: APIClient):
    """构建数据抽取 LangGraph"""

    def upload_node(state: ExtractState):
        """上传 PDF"""
        if state.get("file_id"):
            return {}  # 无需更新
        try:
            file_id = client.upload_pdf_and_wait(state["pdf_path"])
            if not file_id:
                return {"last_error": "upload returned empty file_id"}
        except Exception as e:
            return {"last_error": f"upload error: {e}"}
        return {"file_id": file_id}

    def candidate_node(state: ExtractState):
        """候选提取节点"""
        section = state["schema_sections"][state["section_idx"]]
        section_schema = state["schema"][section]
        logger.debug(f"Extracting candidates for section: {section}")
        
        # 检查是否有重抽提示
        retry_hints = state.get("retry_hints", {})
        section_hint = retry_hints.get(section, "")
        
        if section_hint:
            # 使用带 hints 的 prompt
            logger.info(f"Re-extracting section {section} with hints")
            prompt = CANDIDATE_PROMPT_WITH_HINTS.format(
                role_description=state["role_description"],
                section_name=section,
                schema_section_json=json.dumps({section: section_schema}, ensure_ascii=False, indent=2),
                retry_hints=section_hint,
            )
        else:
            prompt = CANDIDATE_PROMPT.format(
                role_description=state["role_description"],
                section_name=section,
                schema_section_json=json.dumps({section: section_schema}, ensure_ascii=False, indent=2)
            )

        try:
            resp = client.generate_content(
                file_name=state["file_id"],
                prompt=prompt,
            )
        except Exception as e:
            return {"last_error": f"candidate error: {e}"}

        raw_text = get_response_text(resp)

        # 保存原始输出
        raw_path = os.path.join(state["output_dir"], f"candidate_{section}.raw.txt")
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw_text)

        # 定义 JSON 解析失败时的重试回调
        def on_json_retry(failed_attempt: FailedAttempt) -> str:
            """JSON 解析失败时，让 LLM 重新生成响应"""
            logger.info(f"JSON 解析失败，尝试重新生成 (section: {section}, 尝试: {failed_attempt.attempt_number})")
            
            # 构造修正提示
            fix_prompt = f"""{prompt}

IMPORTANT: Your previous response could not be parsed as valid JSON.
Error: {failed_attempt.exception}
Please ensure your output is ONLY valid JSON with no additional text or markdown formatting.
"""
            try:
                retry_resp = client.generate_content(
                    file_name=state["file_id"],
                    prompt=fix_prompt,
                )
                return get_response_text(retry_resp)
            except Exception as e:
                logger.warning(f"重试 API 调用失败: {e}")
                return ""

        # 使用带重试的 JSON 解析
        obj, failed_attempts = extract_json_with_retry(
            raw_text,
            max_attempts=3,
            on_retry=on_json_retry,
            section_name=section,
        )
        
        if obj is None:
            logger.error(f"Candidate JSON parse failed for section: {section} after {len(failed_attempts)} attempts")
            logger.debug(f"Raw LLM output saved to: {raw_path}")
            for fa in failed_attempts:
                logger.debug(f"  Attempt {fa.attempt_number}: {fa.exception}")
            return {"last_error": f"candidate JSON parse failed: {section}"}

        # 保存候选
        candidate_path = os.path.join(state["output_dir"], f"candidate_{section}.json")
        with open(candidate_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

        candidates = dict(state["candidates"])
        candidates[section] = obj
        return {"candidates": candidates}

    def resolve_node(state: ExtractState):
        """解析节点"""
        section = state["schema_sections"][state["section_idx"]]
        section_schema = state["schema"][section]
        candidates = state["candidates"].get(section, {})
        
        # 检查是否有重抽提示
        retry_hints = state.get("retry_hints", {})
        section_hint = retry_hints.get(section, "")
        
        if section_hint:
            # 使用带 hints 的 prompt
            logger.info(f"Resolving section {section} with hints")
            prompt = RESOLVE_PROMPT_WITH_HINTS.format(
                role_description=state["role_description"],
                section_name=section,
                schema_section_json=json.dumps({section: section_schema}, ensure_ascii=False, indent=2),
                candidates_json=json.dumps(candidates, ensure_ascii=False, indent=2),
                retry_hints=section_hint,
            )
        else:
            prompt = RESOLVE_PROMPT.format(
                role_description=state["role_description"],
                section_name=section,
                schema_section_json=json.dumps({section: section_schema}, ensure_ascii=False, indent=2),
                candidates_json=json.dumps(candidates, ensure_ascii=False, indent=2),
            )

        try:
            resp = client.generate_content(
                file_name=state["file_id"],
                prompt=prompt,
            )
        except Exception as e:
            return {"last_error": f"resolve error: {e}"}

        raw_text = get_response_text(resp)

        # 保存原始输出
        raw_path = os.path.join(state["output_dir"], f"resolve_{section}.raw.txt")
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw_text)

        # 定义 JSON 解析失败时的重试回调
        def on_json_retry(failed_attempt: FailedAttempt) -> str:
            """JSON 解析失败时，让 LLM 重新生成响应"""
            logger.info(f"JSON 解析失败，尝试重新生成 (section: {section}, 尝试: {failed_attempt.attempt_number})")
            
            # 构造修正提示
            fix_prompt = f"""{prompt}

IMPORTANT: Your previous response could not be parsed as valid JSON.
Error: {failed_attempt.exception}
Please ensure your output is ONLY valid JSON with no additional text or markdown formatting.
The output should be: {{ "{section}": {{ ... }} }}
"""
            try:
                retry_resp = client.generate_content(
                    file_name=state["file_id"],
                    prompt=fix_prompt,
                )
                return get_response_text(retry_resp)
            except Exception as e:
                logger.warning(f"重试 API 调用失败: {e}")
                return ""

        # 使用带重试的 JSON 解析
        obj, failed_attempts = extract_json_with_retry(
            raw_text,
            max_attempts=3,
            on_retry=on_json_retry,
            section_name=section,
        )

        if obj is None:
            logger.error(f"Resolve JSON parse failed for section: {section} after {len(failed_attempts)} attempts")
            logger.debug(f"Raw LLM output saved to: {raw_path}")
            for fa in failed_attempts:
                logger.debug(f"  Attempt {fa.attempt_number}: {fa.exception}")
            return {"last_error": f"resolve JSON parse failed: {section}"}

        extracted = dict(state["extracted"])
        extracted[section] = obj.get(section)
        return {"extracted": extracted}

    def next_section_node(state: ExtractState):
        """移动到下一个 section"""
        return {"section_idx": state["section_idx"] + 1}

    def decide_next(state: ExtractState) -> Literal["candidate", "__end__"]:
        """决定是否继续"""
        if state.get("last_error"):
            return END
        if state["section_idx"] >= len(state["schema_sections"]):
            return END
        return "candidate"

    def decide_after_upload(state: ExtractState) -> Literal["candidate", "__end__"]:
        """检查上传是否成功"""
        if state.get("last_error"):
            return END
        if not state.get("file_id"):
            return END
        return "candidate"

    # 构建图
    builder = StateGraph(ExtractState)
    builder.add_node("upload", upload_node)
    builder.add_node("candidate", candidate_node)
    builder.add_node("resolve", resolve_node)
    builder.add_node("next", next_section_node)

    builder.add_edge(START, "upload")
    builder.add_conditional_edges("upload", decide_after_upload, ["candidate", END])
    builder.add_edge("candidate", "resolve")
    builder.add_edge("resolve", "next")
    builder.add_conditional_edges("next", decide_next, ["candidate", END])

    return builder.compile()


def run_extraction(
    client: APIClient,
    config: ExtractionConfig,
    pdf_path: str,
    schema: dict,
    output_dir: str,
    role_description: str,
    file_id: str = "",
    retry_hints: dict = None,
    save_outputs: bool = True,
) -> tuple[dict, str]:
    """
    运行数据抽取。

    Args:
        client: API 客户端
        config: 抽取配置
        pdf_path: PDF 文件路径
        schema: Schema 字典
        output_dir: 输出目录
        role_description: 角色描述（用于 Prompt）
        file_id: 已上传的文件 ID（可选）
        retry_hints: 重抽提示（来自审核反馈），格式 {section_name: hint_text}
        save_outputs: whether to save schema.json and final extract.json (disable for re-extract)

    Returns:
        (extracted_dict, file_id)
    """
    logger.info(f"Starting extraction: {len(schema)} sections")
    logger.debug(f"PDF path: {pdf_path}")
    logger.debug(f"Output dir: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    # 清理 schema
    clean_schema = strip_introduced_in_round(schema)

    if not clean_schema:
        logger.warning("Empty schema; skipping extraction")
        return {}, file_id

    if save_outputs:
        # Save schema to output dir
        schema_path = os.path.join(output_dir, "schema.json")
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(clean_schema, f, ensure_ascii=False, indent=2)

    graph = build_extract_graph(client)

    init_state: ExtractState = {
        "messages": [],
        "pdf_path": pdf_path,
        "file_id": file_id,
        "output_dir": output_dir,
        "schema": clean_schema,
        "schema_sections": list(clean_schema.keys()),
        "section_idx": 0,
        "candidates": {},
        "extracted": {},
        "keep_temp": config.keep_temp,
        "last_error": "",
        "role_description": role_description,
        "retry_hints": retry_hints or {},
    }

    logger.debug("Invoking extraction graph")
    sections_count = len(init_state["schema_sections"])
    recursion_limit = max(200, sections_count * 5)
    out = graph.invoke(init_state, {"recursion_limit": recursion_limit})

    if out.get("last_error"):
        logger.error(f"Extraction failed: {out['last_error']}")
        raise RuntimeError(f"Extraction failed: {out['last_error']}")

    # 保存最终结果
    if save_outputs:
        # Save final result
        pdf_basename = os.path.splitext(os.path.basename(pdf_path))[0]
        final_path = os.path.join(output_dir, f"{pdf_basename}_extract.json")
        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(out["extracted"], f, ensure_ascii=False, indent=2)
        logger.info(f"Extraction complete: saved to {final_path}")


    return out["extracted"], out.get("file_id", "")
