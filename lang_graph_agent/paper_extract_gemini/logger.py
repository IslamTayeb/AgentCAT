"""
Paper Extract 日志模块

提供统一的日志配置，支持控制台和文件输出。
"""

import logging
import sys
from pathlib import Path
from typing import Optional


# 日志格式
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_FORMAT_DETAILED = "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 全局标记，用于跟踪是否已完成配置
_logger_configured = False


def setup_logger(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    detailed: bool = False,
) -> logging.Logger:
    """
    配置 root logger。所有子 logger 会自动继承配置。

    Args:
        level: 日志级别
        log_file: 可选的日志文件路径
        detailed: 是否使用详细格式（包含文件名和行号）

    Returns:
        配置好的 root logger
    """
    global _logger_configured
    
    # 获取 root logger
    root_logger = logging.getLogger()
    
    # 清除现有的 handlers（避免重复配置）
    root_logger.handlers.clear()
    
    root_logger.setLevel(level)
    log_format = LOG_FORMAT_DETAILED if detailed else LOG_FORMAT
    formatter = logging.Formatter(log_format, datefmt=DATE_FORMAT)

    # 控制台 handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 文件 handler（可选）
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT_DETAILED, datefmt=DATE_FORMAT))
        root_logger.addHandler(file_handler)
        root_logger.debug(f"Log file configured: {log_file}")

    _logger_configured = True
    return root_logger


def get_logger(name: str = "paper_extract") -> logging.Logger:
    """
    获取指定名称的 logger。

    如果 root logger 尚未配置，会先进行默认配置（仅控制台输出）。
    子 logger 会自动继承 root logger 的 handlers。

    Args:
        name: logger 名称（支持子 logger，如 "paper_extract.pipeline"）

    Returns:
        logger 实例
    """
    global _logger_configured
    
    # 如果尚未配置，进行默认配置
    if not _logger_configured:
        setup_logger()
    
    return logging.getLogger(name)
