"""
Paper Extract 公共工具函数

提供 JSON 处理、响应解析、PDF 收集等通用功能。
"""

import json
import re
from pathlib import Path
from typing import Optional, Callable, Any

from .exceptions import FailedAttempt, JSONParseError
from .logger import get_logger

logger = get_logger("paper_extract.utils")


def strip_code_fences(s: str) -> str:
    """清理 LLM 输出中的代码块标记"""
    s = (s or "").strip()
    s = re.sub(r"^\s*```json\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*```\s*", "", s)
    s = re.sub(r"\s*```\s*$", "", s)
    return s.strip()


def validate_json(s: str) -> Optional[dict]:
    """验证 JSON 字符串，返回解析后的对象或 None"""
    try:
        return json.loads(s)
    except Exception:
        return None


def extract_json_from_text(s: str) -> Optional[dict]:
    """
    从 LLM 输出中提取 JSON，支持：
    1. 纯 JSON
    2. Markdown 代码块包裹 (```json ... ```)
    3. 混合文本（自动寻找第一个有效的 JSON 对象/数组）
    4. 使用 json-repair 修复格式错误的 JSON
    """
    s = (s or "").strip()
    
    # 日志：显示输入内容的预览
    preview = s[:200] + "..." if len(s) > 200 else s
    logger.debug(f"[JSON Parse] Input preview ({len(s)} chars): {preview!r}")
    
    if not s:
        logger.warning("[JSON Parse] Input is empty")
        return None
    
    # 1. 尝试直接解析
    try:
        result = json.loads(s)
        logger.debug("[JSON Parse] Step 1: Direct parse SUCCESS")
        return result
    except json.JSONDecodeError as e:
        logger.debug(f"[JSON Parse] Step 1: Direct parse FAILED - {e}")
    
    # 2. 清理代码块后尝试
    cleaned = strip_code_fences(s)
    if cleaned != s:
        logger.debug(f"[JSON Parse] Step 2: Stripped code fences, new length: {len(cleaned)}")
    try:
        result = json.loads(cleaned)
        logger.debug("[JSON Parse] Step 2: Cleaned parse SUCCESS")
        return result
    except json.JSONDecodeError as e:
        logger.debug(f"[JSON Parse] Step 2: Cleaned parse FAILED - {e}")
    
    # 3. 使用 raw_decode 寻找第一个有效的 JSON 对象或数组
    decoder = json.JSONDecoder()
    
    # 寻找第一个 '{'
    start_idx = cleaned.find('{')
    if start_idx != -1:
        logger.debug(f"[JSON Parse] Step 3a: Found '{{' at index {start_idx}")
        try:
            result = decoder.raw_decode(cleaned, idx=start_idx)[0]
            logger.debug("[JSON Parse] Step 3a: raw_decode({{) SUCCESS")
            return result
        except json.JSONDecodeError as e:
            logger.debug(f"[JSON Parse] Step 3a: raw_decode({{) FAILED - {e}")
    else:
        logger.debug("[JSON Parse] Step 3a: No '{{' found in cleaned text")
            
    # 寻找第一个 '['
    start_idx = cleaned.find('[')
    if start_idx != -1:
        logger.debug(f"[JSON Parse] Step 3b: Found '[' at index {start_idx}")
        try:
            result = decoder.raw_decode(cleaned, idx=start_idx)[0]
            logger.debug("[JSON Parse] Step 3b: raw_decode([) SUCCESS")
            return result
        except json.JSONDecodeError as e:
            logger.debug(f"[JSON Parse] Step 3b: raw_decode([) FAILED - {e}")
    else:
        logger.debug("[JSON Parse] Step 3b: No '[' found in cleaned text")

    # 4. 正则提取最长的大括号内容
    match = re.search(r'\{[\s\S]*\}', cleaned)
    if match:
        regex_text = match.group()
        logger.debug(f"[JSON Parse] Step 4: Regex found {{...}} ({len(regex_text)} chars)")
        try:
            result = json.loads(regex_text)
            logger.debug("[JSON Parse] Step 4: Regex parse SUCCESS")
            return result
        except json.JSONDecodeError as e:
            logger.debug(f"[JSON Parse] Step 4: Regex parse FAILED - {e}")
    else:
        logger.debug("[JSON Parse] Step 4: No {{...}} pattern found by regex")
    
    # 5. 使用 json-repair 尝试修复
    try:
        from json_repair import repair_json
        logger.debug("[JSON Parse] Step 5: Trying json-repair...")
        repaired = repair_json(cleaned, return_objects=True)
        if isinstance(repaired, (dict, list)):
            logger.debug("[JSON Parse] Step 5: json-repair SUCCESS")
            return repaired
        else:
            logger.debug(f"[JSON Parse] Step 5: json-repair returned non-dict/list: {type(repaired)}")
    except ImportError:
        logger.debug("[JSON Parse] Step 5: json-repair not installed, skipping")
    except Exception as e:
        logger.debug(f"[JSON Parse] Step 5: json-repair FAILED - {e}")
    
    # 所有方法都失败，记录完整内容供调试
    logger.warning(f"[JSON Parse] ALL methods FAILED. Full content ({len(s)} chars):\n{s}")
            
    return None


def extract_json_with_retry(
    text: str,
    max_attempts: int = 3,
    on_retry: Optional[Callable[[FailedAttempt], str]] = None,
    section_name: Optional[str] = None,
    raise_on_failure: bool = False,
) -> tuple[Optional[dict], list[FailedAttempt]]:
    """
    带重试的 JSON 提取，支持回调获取新文本重新解析。
    
    当 JSON 解析失败时，如果提供了 on_retry 回调，会调用它获取新的文本重新尝试解析。
    这对于 LLM 场景特别有用：可以让 LLM 重新生成响应。
    
    Args:
        text: 初始 LLM 输出文本
        max_attempts: 最大尝试次数（包括首次）
        on_retry: 重试回调函数，接收 FailedAttempt，返回新的文本用于下次尝试。
                  如果返回空字符串或 None，将终止重试。
        section_name: 相关的 schema section 名称（用于错误信息）
        raise_on_failure: 如果为 True，解析失败时抛出 JSONParseError
        
    Returns:
        (解析结果, 失败尝试列表) 元组。
        如果成功，解析结果为 dict；如果失败，解析结果为 None。
        
    Raises:
        JSONParseError: 当 raise_on_failure=True 且所有尝试都失败时
    """
    failed_attempts: list[FailedAttempt] = []
    current_text = text
    
    for attempt_num in range(1, max_attempts + 1):
        logger.debug(f"[JSON Retry] 尝试 {attempt_num}/{max_attempts}" + 
                     (f" (section: {section_name})" if section_name else ""))
        
        # 尝试解析
        result = extract_json_from_text(current_text)
        
        if result is not None:
            if attempt_num > 1:
                logger.info(f"[JSON Retry] 第 {attempt_num} 次尝试成功解析")
            return result, failed_attempts
        
        # 解析失败，记录尝试
        failed_attempt = FailedAttempt(
            attempt_number=attempt_num,
            exception=json.JSONDecodeError("JSON parse failed", current_text[:100], 0),
            raw_response=current_text,
        )
        failed_attempts.append(failed_attempt)
        
        logger.warning(f"[JSON Retry] 第 {attempt_num} 次尝试解析失败")
        
        # 检查是否还有重试机会
        if attempt_num >= max_attempts:
            break
            
        # 调用回调获取新文本
        if on_retry is None:
            logger.debug("[JSON Retry] 无重试回调，终止重试")
            break
            
        try:
            new_text = on_retry(failed_attempt)
            if not new_text:
                logger.debug("[JSON Retry] 回调返回空文本，终止重试")
                break
            current_text = new_text
            logger.debug(f"[JSON Retry] 获取新文本 ({len(new_text)} chars)")
        except Exception as e:
            logger.error(f"[JSON Retry] 回调执行失败: {e}")
            break
    
    # 所有尝试都失败
    logger.error(f"[JSON Retry] 共 {len(failed_attempts)} 次尝试全部失败" +
                 (f" (section: {section_name})" if section_name else ""))
    
    if raise_on_failure:
        raise JSONParseError(
            f"JSON 解析失败，共尝试 {len(failed_attempts)} 次",
            raw_text=current_text,
            section_name=section_name,
            failed_attempts=failed_attempts,
        )
    
    return None, failed_attempts


def get_response_text(resp) -> str:
    """
    从 Gemini generate_content 响应中提取文本。
    """
    # Gemini SDK 响应对象有 text 属性
    if hasattr(resp, "text"):
        return resp.text or ""
    
    # 尝试从 candidates 中提取
    if hasattr(resp, "candidates") and resp.candidates:
        try:
            parts = resp.candidates[0].content.parts
            texts = [p.text for p in parts if hasattr(p, "text")]
            return "".join(texts)
        except (IndexError, AttributeError):
            pass
    
    return ""


def strip_introduced_in_round(obj):
    """移除 schema 中的 introduced_in_round 字段"""
    if isinstance(obj, dict):
        return {
            k: strip_introduced_in_round(v)
            for k, v in obj.items()
            if k != "introduced_in_round"
        }
    if isinstance(obj, list):
        return [strip_introduced_in_round(x) for x in obj]
    return obj


def calculate_schema_evolution_stats(schema: dict) -> dict[int, int]:
    """
    统计每一轮引入的 schema 条目数。

    Args:
        schema: 包含 introduced_in_round 字段的 schema 字典

    Returns:
        {round_num: count} 字典
    """
    stats = {}

    def _traverse(obj):
        if isinstance(obj, dict):
            # 检查当前对象是否有 introduced_in_round
            if "introduced_in_round" in obj:
                r = obj["introduced_in_round"]
                # 尝试转换为整数（处理字符串形式的数字）
                try:
                    r_int = int(r)
                    stats[r_int] = stats.get(r_int, 0) + 1
                except (ValueError, TypeError):
                    # 忽略 "unchanged" 或其他非数字值
                    pass

            # 递归遍历所有值
            for v in obj.values():
                _traverse(v)
        elif isinstance(obj, list):
            for item in obj:
                _traverse(item)

    _traverse(schema)
    return stats


def collect_pdfs(paths: list[str]) -> list[str]:
    """
    验证并返回 PDF 文件路径列表。

    Args:
        paths: PDF 文件路径列表

    Returns:
        PDF 文件路径列表（绝对路径）
    """
    result = []
    for p in paths:
        path = Path(p)
        if not path.exists():
            raise FileNotFoundError(f"PDF file not found: {p}")
        result.append(str(path.resolve()))
    return result


def collect_pdfs_from_dir(dir_path: str, pattern: str = "*.pdf") -> list[str]:
    """
    从目录收集 PDF 文件列表。

    Args:
        dir_path: 目录路径
        pattern: 文件匹配模式

    Returns:
        PDF 文件路径列表（绝对路径）
    """
    p = Path(dir_path)
    if p.is_file():
        return [str(p.resolve())]
    elif p.is_dir():
        return sorted([str(f.resolve()) for f in p.glob(pattern)])
    else:
        raise FileNotFoundError(f"Path not found: {dir_path}")


