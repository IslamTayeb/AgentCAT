"""
Paper Extract 异常模块

提供结构化的错误追踪能力，支持失败尝试记录和详细的错误信息。
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class FailedAttempt:
    """
    记录单次失败尝试的详细信息。
    
    用于追踪重试过程中每次失败的上下文，便于调试和日志分析。
    
    Attributes:
        attempt_number: 尝试次数（从 1 开始）
        exception: 导致失败的异常
        raw_response: LLM 原始输出（可选）
        context: 额外上下文信息（可选）
    """
    attempt_number: int
    exception: Exception
    raw_response: Optional[str] = None
    context: Optional[dict] = None
    
    def __str__(self) -> str:
        return f"Attempt {self.attempt_number}: {type(self.exception).__name__}: {self.exception}"


class PaperExtractError(Exception):
    """
    Paper Extract 基础异常类。
    
    所有自定义异常的基类，提供失败尝试追踪能力。
    """
    
    def __init__(
        self,
        message: str,
        failed_attempts: Optional[list[FailedAttempt]] = None,
        **kwargs: Any,
    ):
        self.failed_attempts = failed_attempts or []
        super().__init__(message)
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        if self.failed_attempts:
            attempts_str = "\n".join(f"  - {fa}" for fa in self.failed_attempts)
            return f"{base_msg}\nFailed attempts ({len(self.failed_attempts)}):\n{attempts_str}"
        return base_msg


class JSONParseError(PaperExtractError):
    """
    JSON 解析失败异常。
    
    当 LLM 输出无法解析为有效 JSON 时抛出，包含所有尝试的详细记录。
    
    Attributes:
        raw_text: 最终失败的原始文本
        section_name: 相关的 schema section 名称（可选）
    """
    
    def __init__(
        self,
        message: str,
        raw_text: str,
        section_name: Optional[str] = None,
        failed_attempts: Optional[list[FailedAttempt]] = None,
    ):
        self.raw_text = raw_text
        self.section_name = section_name
        super().__init__(message, failed_attempts=failed_attempts)
    
    def __str__(self) -> str:
        base_msg = super().__str__()
        preview = self.raw_text[:200] + "..." if len(self.raw_text) > 200 else self.raw_text
        section_info = f" (section: {self.section_name})" if self.section_name else ""
        return f"{base_msg}{section_info}\nRaw text preview: {preview!r}"


class APIError(PaperExtractError):
    """
    API 调用失败异常。
    
    当 Gemini API 调用失败且重试耗尽时抛出。
    """
    
    def __init__(
        self,
        message: str,
        status_code: Optional[str] = None,
        failed_attempts: Optional[list[FailedAttempt]] = None,
    ):
        self.status_code = status_code
        super().__init__(message, failed_attempts=failed_attempts)


class ExtractionError(PaperExtractError):
    """
    数据抽取失败异常。
    
    当抽取流程中发生不可恢复的错误时抛出。
    """
    
    def __init__(
        self,
        message: str,
        section_name: Optional[str] = None,
        failed_attempts: Optional[list[FailedAttempt]] = None,
    ):
        self.section_name = section_name
        super().__init__(message, failed_attempts=failed_attempts)
