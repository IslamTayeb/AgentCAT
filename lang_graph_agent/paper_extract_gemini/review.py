"""
审核模块

对 Extract 阶段产出的 JSON 进行验证，参照源 PDF 判断正确性。
小错误自动修正，大错误打回重抽。
"""

import json
import os
import shutil
from datetime import datetime
from typing import Optional, Literal
from typing_extensions import TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt, Command

from .client import APIClient
from .config import ReviewConfig, ExtractionConfig
from .extract import run_extraction
from .utils import strip_code_fences, validate_json, get_response_text, extract_json_from_text, extract_json_with_retry
from .exceptions import FailedAttempt
from .logger import get_logger

logger = get_logger("paper_extract.review")


# 审核 Prompt（传入 section_name 和 section_json）
REVIEW_PROMPT = """You are a rigorous scientific literature reviewer.

TASK
Compare the EXTRACTED JSON below against the attached PDF source document.
Verify accuracy and identify any discrepancies.

SECTION: {section_name}

EXTRACTED JSON TO REVIEW:
{section_json}

EVALUATION CRITERIA:
- PASS: Extracted content is accurate and faithful to the PDF
- MINOR_FIX: Minor issues that can be auto-corrected:
  - Formatting errors (units, capitalization)
  - Synonym substitutions that should use original PDF wording
  - Typos or case inconsistencies
  - Provide corrected JSON in "fixed_json"
- MAJOR_ERROR: Significant issues requiring re-extraction:
  - Factual errors not matching PDF content
  - Missing critical information present in PDF
  - Fabricated content not in PDF
  - Describe issues in "error_hints" for re-extraction guidance

IMPORTANT: Use the attached PDF as the AUTHORITATIVE source of truth.

OUTPUT FORMAT (JSON only):
{{
  "verdict": "PASS" | "MINOR_FIX" | "MAJOR_ERROR",
  "issues": ["issue description 1", ...],
  "fixed_json": {{ ... }},
  "error_hints": "...",
  "evaluation": "Brief assessment of sampling quality"
}}

Output ONLY valid JSON.
"""


class ReviewState(TypedDict):
    """审核状态"""
    pdf_path: str
    file_id: str
    output_dir: str
    schema: dict
    extracted: dict                    # 待审核的抽取结果
    schema_sections: list[str]
    section_idx: int
    review_results: dict               # 每 section 的审核结果
    final_extracted: dict              # 最终修正后的结果
    retry_sections: list[str]          # 需重抽的 sections
    retry_hints: dict                  # 重抽时的额外提示
    retry_count: int                   # 当前重试次数
    max_retries: int
    evaluation_log: dict               # 评价记录
    role_description: str
    last_error: str


def build_review_graph(client: APIClient, extract_client: APIClient, extraction_config: ExtractionConfig):
    """构建审核 LangGraph"""

    def review_section_node(state: ReviewState) -> dict:
        """审核单个 section"""
        section = state["schema_sections"][state["section_idx"]]
        section_data = state["extracted"].get(section, {})
        
        logger.debug(f"Reviewing section: {section}")
        
        prompt = REVIEW_PROMPT.format(
            section_name=section,
            section_json=json.dumps(section_data, ensure_ascii=False, indent=2),
        )
        
        try:
            resp = client.generate_content(
                file_name=state["file_id"],
                prompt=prompt,
            )
        except Exception as e:
            logger.error(f"Review LLM call failed: {e}")
            return {"last_error": f"review error: {e}"}
        
        raw_text = get_response_text(resp)
        
        # 保存原始输出
        raw_path = os.path.join(state["output_dir"], f"review_{section}.raw.txt")
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw_text)
        
        # 定义 JSON 解析失败时的重试回调
        def on_json_retry(failed_attempt: FailedAttempt) -> str:
            """JSON 解析失败时，让 LLM 重新生成响应"""
            logger.info(f"Review JSON 解析失败，尝试重新生成 (section: {section}, 尝试: {failed_attempt.attempt_number})")
            
            fix_prompt = f"""{prompt}

IMPORTANT: Your previous response could not be parsed as valid JSON.
Error: {failed_attempt.exception}
Please ensure your output is ONLY valid JSON with no additional text or markdown formatting.
The output should contain: verdict, issues, fixed_json (if applicable), error_hints, evaluation
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
            section_name=f"review_{section}",
        )
        
        if obj is None:
            logger.error(f"Review JSON parse failed for section: {section} after {len(failed_attempts)} attempts")
            # Treat parse failure as MAJOR_ERROR to trigger re-extraction
            obj = {
                "verdict": "MAJOR_ERROR",
                "issues": ["Review JSON parse failed"],
                "error_hints": "Review output was not valid JSON; re-extract this section.",
                "evaluation": "Review output parse failed; marked for re-extraction",
            }
        
        verdict = obj.get("verdict", "PASS").upper()
        review_results = dict(state["review_results"])
        final_extracted = dict(state["final_extracted"])
        retry_sections = list(state["retry_sections"])
        retry_hints = dict(state["retry_hints"])
        evaluation_log = dict(state["evaluation_log"])
        
        # 记录评价
        evaluation_log[section] = {
            "verdict": verdict,
            "comment": obj.get("evaluation", ""),
        }
        
        if verdict == "PASS":
            final_extracted[section] = section_data
            logger.info(f"Section {section}: PASS")
        elif verdict == "MINOR_FIX":
            fixed = obj.get("fixed_json", section_data)
            final_extracted[section] = fixed
            logger.info(f"Section {section}: MINOR_FIX - auto corrected")
        elif verdict == "MAJOR_ERROR":
            retry_sections.append(section)
            retry_hints[section] = obj.get("error_hints", "")
            logger.warning(f"Section {section}: MAJOR_ERROR - marked for re-extraction")
        else:
            # 未知 verdict，视为 PASS
            final_extracted[section] = section_data
        
        review_results[section] = obj
        
        return {
            "review_results": review_results,
            "final_extracted": final_extracted,
            "retry_sections": retry_sections,
            "retry_hints": retry_hints,
            "evaluation_log": evaluation_log,
        }

    def next_section_node(state: ReviewState) -> dict:
        """移动到下一个 section"""
        return {"section_idx": state["section_idx"] + 1}

    def re_extract_node(state: ReviewState) -> dict:
        """重新抽取失败的 sections"""
        retry_sections = state["retry_sections"]
        retry_hints = state["retry_hints"]
        logger.info(f"Re-extracting {len(retry_sections)} sections with hints: {retry_sections}")
        
        # 构建只包含需重抽 sections 的 schema
        retry_schema = {s: state["schema"][s] for s in retry_sections if s in state["schema"]}
        
        try:
            extracted, _ = run_extraction(
                client=extract_client,
                config=extraction_config,
                pdf_path=state["pdf_path"],
                schema=retry_schema,
                output_dir=state["output_dir"],
                role_description=state["role_description"],
                file_id=state["file_id"],
                retry_hints=retry_hints,  # 传入审核反馈的提示
                save_outputs=False,
            )
            
            # 合并重抽结果
            new_extracted = dict(state["extracted"])
            new_extracted.update(extracted)
            
            return {
                "extracted": new_extracted,
                "schema_sections": retry_sections,  # 只审核重抽的 sections
                "section_idx": 0,
                "retry_sections": [],
                "retry_hints": {},  # 清空已使用的 hints
                "retry_count": state["retry_count"] + 1,
            }
        except Exception as e:
            logger.error(f"Re-extraction failed: {e}")
            return {"last_error": f"re-extract error: {e}"}

    def decide_next_section(state: ReviewState) -> Literal["review", "check_retry", "__end__"]:
        """决定是继续审核还是检查重抽"""
        if state.get("last_error"):
            return END
        if state["section_idx"] < len(state["schema_sections"]):
            return "review"
        return "check_retry"

    def decide_retry(state: ReviewState) -> Literal["re_extract", "__end__"]:
        """决定是否需要重抽"""
        if state.get("last_error"):
            return END
        if state["retry_sections"] and state["retry_count"] < state["max_retries"]:
            return "re_extract"
        return END

    def decide_after_re_extract(state: ReviewState) -> Literal["review", "__end__"]:
        """重抽后检查是否成功"""
        if state.get("last_error"):
            return END
        return "review"

    # 构建图
    builder = StateGraph(ReviewState)
    
    builder.add_node("review", review_section_node)
    builder.add_node("next", next_section_node)
    builder.add_node("re_extract", re_extract_node)
    builder.add_node("check_retry", lambda state: state)
    
    builder.add_edge(START, "review")
    builder.add_edge("review", "next")
    builder.add_conditional_edges("next", decide_next_section, ["review", "check_retry", END])
    builder.add_conditional_edges("check_retry", decide_retry, ["re_extract", END])
    # re_extract 后检查是否成功，失败则直接结束
    builder.add_conditional_edges("re_extract", decide_after_re_extract, ["review", END])
    
    return builder.compile()


def run_review(
    client: APIClient,
    extract_client: APIClient,
    config: ReviewConfig,
    extraction_config: ExtractionConfig,
    pdf_path: str,
    file_id: str,
    extracted: dict,
    schema: dict,
    output_dir: str,
    role_description: str,
) -> tuple[dict, dict]:
    """
    审核抽取结果。
    
    Args:
        client: 审核 API 客户端
        extract_client: 抽取 API 客户端（重抽使用）
        config: 审核配置
        extraction_config: 抽取配置（重抽使用）
        pdf_path: PDF 路径
        file_id: 已上传的 file_id
        extracted: 待审核的抽取结果
        schema: Schema 定义
        output_dir: 输出目录
        role_description: 角色描述
        
    Returns:
        (final_extracted, evaluation_log)
    """
    logger.info(f"Starting review: {len(extracted)} sections")
    if not extracted:
        logger.warning("No sections to review")
        return extracted, {}
    
    graph = build_review_graph(client, extract_client, extraction_config)
    
    init_state: ReviewState = {
        "pdf_path": pdf_path,
        "file_id": file_id,
        "output_dir": output_dir,
        "schema": schema,
        "extracted": extracted,
        "schema_sections": list(extracted.keys()),
        "section_idx": 0,
        "review_results": {},
        "final_extracted": {},
        "retry_sections": [],
        "retry_hints": {},
        "retry_count": 0,
        "max_retries": config.max_retries,
        "evaluation_log": {},
        "role_description": role_description,
        "last_error": "",
    }
    
    sections_count = len(init_state["schema_sections"])
    recursion_limit = max(100, sections_count * 4)
    out = graph.invoke(init_state, {"recursion_limit": recursion_limit})
    
    if out.get("last_error"):
        logger.error(f"Review failed: {out['last_error']}")
        # 出错时返回原始结果
        return extracted, {}
    
    final = out["final_extracted"]
    eval_log = out["evaluation_log"]
    
    # 处理未审核的 sections（保留原值）
    for section in extracted:
        if section not in final:
            final[section] = extracted[section]
    
    logger.info(f"Review complete: {len(final)} sections")
    return final, eval_log


def run_review_standalone(
    client: APIClient,
    extract_client: APIClient,
    config: ReviewConfig,
    extraction_config: ExtractionConfig,
    pdf_path: str,
    extract_json_path: str,
    schema_path: str,
    role_description: str,
    output_dir: str,
) -> tuple[dict, dict]:
    """
    单独运行审核（从文件加载）。
    
    Args:
        pdf_path: PDF 路径（需重新上传）
        extract_json_path: 抽取结果 JSON 路径
        schema_path: Schema 文件路径
        其他参数同 run_review
        
    Returns:
        (final_extracted, evaluation_log)
    """
    logger.info(f"Standalone review: {extract_json_path}")
    
    # 加载抽取结果
    with open(extract_json_path, "r", encoding="utf-8") as f:
        extracted = json.load(f)
    
    # 加载 schema
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    
    # 上传 PDF
    file_id = client.upload_pdf_and_wait(pdf_path)
    
    # 备份原文件
    backup_path = extract_json_path + ".bak"
    shutil.copy2(extract_json_path, backup_path)
    logger.info(f"Backup created: {backup_path}")
    
    # 运行审核
    final, eval_log = run_review(
        client=client,
        extract_client=extract_client,
        config=config,
        extraction_config=extraction_config,
        pdf_path=pdf_path,
        file_id=file_id,
        extracted=extracted,
        schema=schema,
        output_dir=output_dir,
        role_description=role_description,
    )
    
    # 覆盖原文件
    with open(extract_json_path, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    logger.info(f"Updated: {extract_json_path}")
    
    return final, eval_log


def save_evaluation_log(log_path: str, pdf_name: str, model: str, eval_log: dict) -> None:
    """
    追加评价日志到文件。
    
    Args:
        log_path: 日志文件路径
        pdf_name: PDF 文件名
        model: 使用的模型名称
        eval_log: 评价记录
    """
    # 读取现有日志
    existing = {}
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            existing = {}
    
    # 添加新记录
    existing[pdf_name] = {
        "model": model,
        "timestamp": datetime.now().isoformat(),
        "sections": eval_log,
    }
    
    # 写回文件
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Evaluation log saved: {log_path}")
