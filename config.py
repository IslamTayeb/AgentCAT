# config.py - 统一配置文件
"""
知识图谱项目配置文件
通过 .env 文件配置 LLM、数据库和各阶段路径
"""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# 项目根目录（必须在最前面定义）
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def _resolve_path(path: str, default: str = "") -> str:
    """
    将路径解析为绝对路径
    
    - 空字符串返回空字符串
    - 绝对路径直接返回
    - 相对路径转换为相对于 PROJECT_ROOT 的绝对路径
    """
    if not path:
        return default
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


# ==================== LLM 配置 ====================

@dataclass
class LLMConfig:
    """LLM 配置"""
    provider: str  # "deepseek" | "qwen" | "doubao" 等
    model: str
    api_key: str
    base_url: str


# 反应网络提取模块使用的 LLM
EXTRACTOR_LLM = LLMConfig(
    provider=os.getenv("EXTRACTOR_LLM_PROVIDER", "qwen"),
    model=os.getenv("EXTRACTOR_LLM_MODEL", "qwen-max"),
    api_key=os.getenv("EXTRACTOR_LLM_API_KEY", ""),
    base_url=os.getenv("EXTRACTOR_LLM_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
)

# Cypher 生成模块使用的 LLM
CYPHER_LLM = LLMConfig(
    provider=os.getenv("CYPHER_LLM_PROVIDER", "deepseek"),
    model=os.getenv("CYPHER_LLM_MODEL", "deepseek-reasoner"),
    api_key=os.getenv("CYPHER_LLM_API_KEY", ""),
    base_url=os.getenv("CYPHER_LLM_BASE_URL", "https://api.deepseek.com"),
)


# ==================== Neo4j 配置 ====================

@dataclass  
class Neo4jConfig:
    """Neo4j 数据库配置"""
    uri: str
    username: str
    password: str
    database: str
    
    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        return cls(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            username=os.getenv("NEO4J_USERNAME", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD", ""),
            database=os.getenv("NEO4J_DATABASE", "neo4j"),
        )


NEO4J_CONFIG = Neo4jConfig.from_env()


# ==================== 第一阶段：关系提取路径配置 ====================

@dataclass
class ExtractorPathConfig:
    """第一阶段：关系提取路径配置"""
    input_dir: str              # 输入 JSON 目录
    output_dir: str             # 输出关系文件目录（空=与输入同目录）
    pdf_mapping_file: str       # PDF ID 映射文件
    zeolite_registry_file: str  # Zeolite 注册表文件
    labels_file: str            # 标签管理文件
    
    @classmethod
    def from_env(cls) -> "ExtractorPathConfig":
        return cls(
            input_dir=_resolve_path(os.getenv("EXTRACTOR_INPUT_DIR", "output")),
            output_dir=_resolve_path(os.getenv("EXTRACTOR_OUTPUT_DIR", "")),
            pdf_mapping_file=_resolve_path(os.getenv("EXTRACTOR_PDF_MAPPING_FILE", "pdf_mapping.json")),
            zeolite_registry_file=_resolve_path(os.getenv("EXTRACTOR_ZEOLITE_REGISTRY_FILE", "zeolites.json")),
            labels_file=_resolve_path(os.getenv("EXTRACTOR_LABELS_FILE", "labels.json")),
        )
    
    def get_output_dir(self) -> str:
        """获取输出目录（如果未配置则返回输入目录）"""
        return self.output_dir if self.output_dir else self.input_dir


EXTRACTOR_PATHS = ExtractorPathConfig.from_env()


# ==================== 第二阶段：Neo4j 导入路径配置 ====================

@dataclass
class ImporterPathConfig:
    """第二阶段：Neo4j 导入路径配置"""
    input_dir: str  # 关系文件目录（空=与第一阶段输出相同）
    
    @classmethod
    def from_env(cls) -> "ImporterPathConfig":
        return cls(
            input_dir=_resolve_path(os.getenv("IMPORTER_INPUT_DIR", "")),
        )
    
    def get_input_dir(self) -> str:
        """获取输入目录（如果未配置则使用第一阶段输出目录）"""
        return self.input_dir if self.input_dir else EXTRACTOR_PATHS.get_output_dir()


IMPORTER_PATHS = ImporterPathConfig.from_env()


# ==================== 第四阶段：Web 服务路径配置 ====================

@dataclass
class WebPathConfig:
    """第四阶段：Web 服务路径配置"""
    templates_dir: str  # 模板目录
    
    @classmethod
    def from_env(cls) -> "WebPathConfig":
        return cls(
            templates_dir=_resolve_path(os.getenv("WEB_TEMPLATES_DIR", "templates")),
        )


WEB_PATHS = WebPathConfig.from_env()


# ==================== 兼容性：保留旧变量名 ====================

OUTPUT_DIR = EXTRACTOR_PATHS.input_dir
LABELS_FILE = EXTRACTOR_PATHS.labels_file
TEMPLATES_DIR = WEB_PATHS.templates_dir


# ==================== 调试工具 ====================

def print_config():
    """打印当前配置（用于调试）"""
    print("=" * 60)
    print("当前配置：")
    print()
    print("【LLM 配置】")
    print(f"  提取模块: {EXTRACTOR_LLM.provider} / {EXTRACTOR_LLM.model}")
    print(f"  Cypher生成: {CYPHER_LLM.provider} / {CYPHER_LLM.model}")
    print()
    print("【Neo4j 配置】")
    print(f"  URI: {NEO4J_CONFIG.uri}")
    print(f"  Database: {NEO4J_CONFIG.database}")
    print()
    print("【第一阶段路径】")
    print(f"  输入目录: {EXTRACTOR_PATHS.input_dir}")
    print(f"  输出目录: {EXTRACTOR_PATHS.get_output_dir()}")
    print(f"  PDF映射: {EXTRACTOR_PATHS.pdf_mapping_file}")
    print(f"  Zeolite注册: {EXTRACTOR_PATHS.zeolite_registry_file}")
    print(f"  标签文件: {EXTRACTOR_PATHS.labels_file}")
    print()
    print("【第二阶段路径】")
    print(f"  输入目录: {IMPORTER_PATHS.get_input_dir()}")
    print()
    print("【Web 服务路径】")
    print(f"  模板目录: {WEB_PATHS.templates_dir}")
    print("=" * 60)


if __name__ == "__main__":
    print_config()
