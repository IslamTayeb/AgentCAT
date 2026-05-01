# logger.py - 统一日志配置
"""
知识图谱项目日志配置
支持控制台和文件双输出，可通过环境变量配置日志级别
"""

import os
import logging
from pathlib import Path

# ==================== 日志配置常量 ====================

# 日志级别映射
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# 从环境变量读取配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "logs/app.log")

# 日志格式
CONSOLE_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
FILE_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(funcName)s:%(lineno)d | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ==================== 日志初始化 ====================

_initialized = False


def setup_logging() -> None:
    """
    初始化日志配置
    
    配置根 logger，同时输出到控制台和文件
    """
    global _initialized
    if _initialized:
        return
    
    # 获取日志级别
    level = LOG_LEVELS.get(LOG_LEVEL, logging.INFO)
    
    # 配置根 logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 清除已有 handlers（避免重复添加）
    root_logger.handlers.clear()
    
    # 控制台 Handler（强制 UTF-8 编码解决 Windows 中文显示问题）
    import sys
    import io
    
    # 尝试设置控制台输出编码为 UTF-8
    if sys.platform == "win32":
        try:
            # 设置 Windows 控制台代码页为 UTF-8
            import ctypes
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        except Exception:
            pass
    
    console_handler = logging.StreamHandler(
        stream=io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    )
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(CONSOLE_FORMAT, DATE_FORMAT))
    root_logger.addHandler(console_handler)
    
    # 文件 Handler
    if LOG_FILE:
        log_path = Path(LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(FILE_FORMAT, DATE_FORMAT))
        root_logger.addHandler(file_handler)
    
    _initialized = True


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的 logger
    
    Args:
        name: logger 名称，通常使用模块名
        
    Returns:
        配置好的 Logger 实例
        
    Example:
        logger = get_logger(__name__)
        logger.info("Starting extraction...")
    """
    # 确保日志系统已初始化
    if not _initialized:
        setup_logging()
    
    return logging.getLogger(name)


# ==================== 便捷函数 ====================

def set_level(level: str) -> None:
    """
    动态设置日志级别
    
    Args:
        level: 日志级别字符串 ("DEBUG", "INFO", "WARNING", "ERROR")
    """
    log_level = LOG_LEVELS.get(level.upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)
    for handler in logging.getLogger().handlers:
        handler.setLevel(log_level)
