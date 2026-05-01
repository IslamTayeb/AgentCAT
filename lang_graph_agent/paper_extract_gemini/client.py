"""
Paper Extract API 客户端模块 (Gemini 版本)

使用原生 Google GenAI SDK 与 Gemini API 交互。
"""

import time
import threading
from pathlib import Path
from typing import Optional, Callable, Any

from google import genai
from google.genai import types
from tenacity import (
    Retrying,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    retry_if_exception,
    RetryError,
)

from .config import APIConfig
from .exceptions import FailedAttempt, APIError
from .logger import get_logger

logger = get_logger("paper_extract_gemini.client")

class _RateLimiter:
    """Simple shared rate limiter (requests per minute)."""
    def __init__(self, rpm: int):
        self._lock = threading.Lock()
        self._last_time: Optional[float] = None
        self._rpm = max(0, int(rpm))

    @property
    def rpm(self) -> int:
        return self._rpm

    def update_rpm(self, rpm: int) -> None:
        self._rpm = max(0, int(rpm))

    def acquire(self) -> None:
        if self._rpm <= 0:
            return
        min_interval = 60.0 / float(self._rpm)
        with self._lock:
            now = time.monotonic()
            if self._last_time is None:
                self._last_time = now
                return
            elapsed = now - self._last_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
                now = time.monotonic()
            self._last_time = now


_RATE_LIMITERS: dict[str, _RateLimiter] = {}


class APIClient:
    """Gemini API 客户端封装"""

    def __init__(self, config: APIConfig, rate_limit_rpm: Optional[int] = None):
        self.config = config
        self._client: Optional[genai.Client] = None
        self._rate_limit_rpm = rate_limit_rpm if rate_limit_rpm is not None else 0
        self._rate_limiter = self._get_rate_limiter()

    def _get_rate_limiter(self) -> Optional[_RateLimiter]:
        if not self._rate_limit_rpm or self._rate_limit_rpm <= 0:
            return None
        key = f"{self.config.api_key_env}:{self.config.model}"
        limiter = _RATE_LIMITERS.get(key)
        if limiter is None:
            limiter = _RateLimiter(self._rate_limit_rpm)
            _RATE_LIMITERS[key] = limiter
        else:
            if self._rate_limit_rpm < limiter.rpm:
                limiter.update_rpm(self._rate_limit_rpm)
        return limiter

    def _throttle(self) -> None:
        if self._rate_limiter:
            self._rate_limiter.acquire()

    def _is_transient_error(self, exception: Exception) -> bool:
        """判断是否为可重试的瞬态错误"""
        error_str = str(exception)
        return "503" in error_str or "429" in error_str
    
    def _create_retrying(
        self,
        max_retries: int = 5,
        timeout: float = 120,
    ) -> Retrying:
        """
        创建 tenacity 重试器。
        
        Args:
            max_retries: 最大重试次数
            timeout: 全局超时秒数（所有重试总耗时）
            
        Returns:
            配置好的 Retrying 对象
        """
        return Retrying(
            stop=stop_after_attempt(max_retries) | stop_after_delay(timeout),
            wait=wait_exponential(multiplier=2, min=2, max=60),
            retry=retry_if_exception(self._is_transient_error),
            reraise=True,
        )
    
    def _retry_api_call(
        self,
        func: Callable[..., Any],
        *args,
        max_retries: int = 5,
        timeout: float = 120,
        **kwargs,
    ) -> Any:
        """
        使用 tenacity 执行 API 调用，支持智能重试。
        
        Args:
            func: 要执行的函数
            *args: 位置参数
            max_retries: 最大重试次数
            timeout: 全局超时秒数
            **kwargs: 关键字参数
            
        Returns:
            函数返回值
            
        Raises:
            APIError: 重试耗尽后抛出，包含所有失败尝试记录
        """
        retrying = self._create_retrying(max_retries=max_retries, timeout=timeout)
        failed_attempts: list[FailedAttempt] = []
        
        try:
            for attempt in retrying:
                with attempt:
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if self._is_transient_error(e):
                            # 记录失败尝试
                            failed_attempts.append(FailedAttempt(
                                attempt_number=attempt.retry_state.attempt_number,
                                exception=e,
                            ))
                            logger.warning(
                                f"API 瞬态错误: {e}. "
                                f"(尝试 {attempt.retry_state.attempt_number}/{max_retries})"
                            )
                        raise
        except RetryError as e:
            # 重试耗尽
            logger.error(f"API 请求在 {len(failed_attempts)} 次尝试后失败")
            raise APIError(
                f"API request failed after {len(failed_attempts)} retries",
                failed_attempts=failed_attempts,
            ) from e.last_attempt.exception()

    @property
    def client(self) -> genai.Client:
        """延迟初始化 Gemini 客户端"""
        if self._client is None:
            logger.debug(f"Connecting to Gemini API with model: {self.config.model}")
            self._client = genai.Client(api_key=self.config.api_key)
        return self._client

    @property
    def model(self) -> str:
        return self.config.model

    def upload_pdf_and_wait(
        self,
        pdf_path: str,
        poll_interval_sec: int = 2,
        max_wait_sec: int = 300,
    ) -> str:
        """
        上传 PDF 文件并等待处理完成。

        Args:
            pdf_path: PDF 文件路径
            poll_interval_sec: 轮询间隔秒数
            max_wait_sec: 最大等待秒数

        Returns:
            file_name: 上传后的文件名（用作 ID）
        """
        def _normalize_state(state_obj) -> str:
            if state_obj is None:
                return ""
            name = getattr(state_obj, "name", None)
            if name:
                return str(name).upper()
            return str(state_obj).upper()

        file_path = Path(pdf_path)
        logger.info(f"Uploading PDF: {file_path.name}")
        
        # 使用 io.BytesIO 包装文件内容，避免文件路径编码问题
        # 参考：https://ai.google.dev/gemini-api/docs/document-processing
        import io
        with open(file_path, "rb") as f:
            file_bytes = f.read()
        
        doc_io = io.BytesIO(file_bytes)
        
        # display_name 可能包含非 ASCII 字符，需要处理
        # 使用 ASCII 安全的文件名，或者省略 display_name
        display_name = file_path.stem[:50]  # 限制长度，使用 stem（无扩展名）
        # 将非 ASCII 字符替换为下划线
        safe_display_name = "".join(c if ord(c) < 128 else "_" for c in display_name) + ".pdf"
        
        file_obj = self.client.files.upload(
            file=doc_io,
            config={"mime_type": "application/pdf", "display_name": safe_display_name}
        )
        
        file_name = file_obj.name
        state = _normalize_state(getattr(file_obj, "state", None))
        logger.debug(f"Upload started: file_name={file_name}, state={state}")

        # 等待文件处理完成
        waited = 0
        while state == "PROCESSING":
            if waited >= max_wait_sec:
                logger.error(f"File processing timeout after {max_wait_sec}s: file_name={file_name}")
                raise RuntimeError(f"File still processing after {max_wait_sec}s. file_name={file_name}")
            time.sleep(poll_interval_sec)
            waited += poll_interval_sec
            file_obj = self.client.files.get(name=file_name)
            state = _normalize_state(getattr(file_obj, "state", None))
            logger.debug(f"Polling file state: waited={waited}s, state={state}")

        if state in ("FAILED", "ERROR"):
            logger.error(f"File processing failed: file_name={file_name}, state={state}")
            raise RuntimeError(f"File processing failed. file_name={file_name}, state={state}")

        logger.info(f"Upload complete: file_name={file_name}")
        return file_name

    def get_uploaded_file(self, file_name: str):
        """
        获取已上传的文件对象，用于传递给 generate_content。

        Args:
            file_name: 文件名（从 upload_pdf_and_wait 返回）

        Returns:
            文件对象
        """
        return self.client.files.get(name=file_name)

    def generate_content(
        self,
        file_name: str,
        prompt: str,
        response_mime_type: Optional[str] = None,
    ):
        """
        调用 Gemini 生成内容 API。

        Args:
            file_name: 已上传文件的名称
            prompt: 提示文本
            response_mime_type: 响应格式（如 'application/json'）

        Returns:
            API 响应对象
        """
        # 验证 file_name 非空
        if not file_name:
            raise ValueError("file_name is required but was empty or None")
        
        logger.debug(f"Generating content: model={self.model}")
        
        # 获取文件对象
        file_obj = self.get_uploaded_file(file_name)
        
        # 从文件对象构造 Part（需要 file_uri 和 mime_type）
        file_part = types.Part.from_uri(
            file_uri=file_obj.uri,
            mime_type=file_obj.mime_type,
        )
        
        # 构建请求参数
        kwargs = {
            "model": self.model,
            "contents": [file_part, prompt],
        }
        
        if response_mime_type:
            kwargs["config"] = {"response_mime_type": response_mime_type}

        def _call_api():
            self._throttle()
            if response_mime_type:
                # Use a copy of kwargs to avoid modifying it for retries if we were to pop config
                call_kwargs = kwargs.copy()
                return self.client.models.generate_content(**call_kwargs)
            return self.client.models.generate_content(**kwargs)

        try:
            return self._retry_api_call(_call_api)
        except Exception as e:
            if response_mime_type and "503" not in str(e) and "429" not in str(e):
                logger.warning(f"Response format failed, retrying without format: {e}")
                kwargs.pop("config", None)
                # Also retry the fallback call
                def _call_api_no_format():
                    self._throttle()
                    return self.client.models.generate_content(**kwargs)

                return self._retry_api_call(_call_api_no_format)
            raise

    def generate_content_text_only(
        self,
        prompt: str,
        response_mime_type: Optional[str] = None,
    ):
        """
        纯文本生成（不带文件）。

        Args:
            prompt: 提示文本
            response_mime_type: 响应格式

        Returns:
            API 响应对象
        """
        logger.debug(f"Generating text content: model={self.model}")
        
        kwargs = {
            "model": self.model,
            "contents": prompt,
        }
        
        if response_mime_type:
            kwargs["config"] = {"response_mime_type": response_mime_type}

        def _call_api():
            self._throttle()
            return self.client.models.generate_content(**kwargs)

        resp = self._retry_api_call(_call_api)
        logger.debug("Response received successfully")
        return resp
