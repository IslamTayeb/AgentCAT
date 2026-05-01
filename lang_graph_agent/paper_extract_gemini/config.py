"""
Paper Extract 配置管理模块

支持从 YAML 文件加载配置，Schema 演化和数据抽取阶段可独立配置 API。
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class APIConfig:
    """Gemini API 连接配置"""
    model: str = "gemini-2.5-flash-preview-05-20"
    api_key_env: str = "GEMINI_API_KEY"

    @property
    def api_key(self) -> str:
        key = os.environ.get(self.api_key_env, "")
        if not key:
            raise RuntimeError(f"Missing API key: environment variable '{self.api_key_env}' not set")
        return key


@dataclass
class SchemaEvolutionConfig:
    """演化阶段配置"""
    enabled: bool = True
    rounds: int = 3
    initial_schema: Optional[str] = None
    api: Optional[APIConfig] = None
    input: Optional["SchemaEvoInputConfig"] = None


@dataclass
class ExtractionConfig:
    """抽取阶段配置"""
    enabled: bool = True
    keep_temp: bool = True
    schema_file: Optional[str] = None
    api: Optional[APIConfig] = None
    input: Optional["ExtractionInputConfig"] = None


@dataclass
class SchemaEvoInputConfig:
    """演化阶段输入配置（PDF 路径列表）"""
    path: list[str] = None

    def __post_init__(self):
        if self.path is None:
            self.path = []


@dataclass
class ExtractionInputConfig:
    """抽取阶段输入配置（目录模式）"""
    path: str = "./"  # 目录路径
    pattern: str = "*.pdf"  # 匹配模式


@dataclass
class OutputConfig:
    """输出配置"""
    dir: str = "./output"
    schema_file: str = "schema.final.json"


@dataclass
class ConcurrencyConfig:
    """并发控制配置（预留）"""
    max_workers: int = 1
    rate_limit_rpm: int = 60


@dataclass
class PlanningConfig:
    """交互式规划阶段配置"""
    enabled: bool = True
    save_framework: bool = True  # 是否保存框架到文件，便于调试
    framework_file: str = "framework.json"  # 框架保存路径（相对于 output.dir）
    api: Optional[APIConfig] = None  # 独立 API 配置，None 则使用全局


@dataclass
class ReviewInputConfig:
    """审核阶段输入配置"""
    pdf_file: Optional[str] = None           # 单文件调试：PDF 路径
    extract_json: Optional[str] = None       # 单文件调试：对应的 extract.json
    extract_result_dir: Optional[str] = None # 批量模式：抽取结果目录
    schema_file: Optional[str] = None        # schema 文件路径


@dataclass
class ReviewConfig:
    """审核阶段配置"""
    enabled: bool = True
    max_retries: int = 2                      # 最大重抽次数
    evaluation_log: str = "file_eva.log"      # 评价日志文件名
    api: Optional[APIConfig] = None           # 独立 API 配置
    input: Optional[ReviewInputConfig] = None


@dataclass
class PipelineConfig:
    """完整 Pipeline 配置"""
    api: APIConfig = field(default_factory=APIConfig)
    planning: "PlanningConfig" = field(default_factory=lambda: PlanningConfig())
    schema_evolution: SchemaEvolutionConfig = field(default_factory=SchemaEvolutionConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    review: "ReviewConfig" = field(default_factory=lambda: ReviewConfig())  # 审核阶段
    output: OutputConfig = field(default_factory=OutputConfig)
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)

    def get_planning_api(self) -> APIConfig:
        """获取规划阶段的 API 配置"""
        return self.planning.api or self.api

    def get_schema_evo_api(self) -> APIConfig:
        """获取 Schema 演化阶段的 API 配置"""
        return self.schema_evolution.api or self.api

    def get_extraction_api(self) -> APIConfig:
        """获取数据抽取阶段的 API 配置"""
        return self.extraction.api or self.api

    def get_review_api(self) -> APIConfig:
        """获取审核阶段的 API 配置"""
        return self.review.api or self.api

    def get_schema_evo_input(self) -> "SchemaEvoInputConfig":
        """获取 Schema 演化阶段的输入配置"""
        return self.schema_evolution.input or SchemaEvoInputConfig()

    def get_extraction_input(self) -> "ExtractionInputConfig":
        """获取数据抽取阶段的输入配置"""
        return self.extraction.input or ExtractionInputConfig()

    def get_review_input(self) -> "ReviewInputConfig":
        """获取审核阶段的输入配置"""
        return self.review.input or ReviewInputConfig()


def _parse_api_config(data: dict) -> APIConfig:
    """解析 API 配置"""
    return APIConfig(
        model=data.get("model", APIConfig.model),
        api_key_env=data.get("api_key_env", APIConfig.api_key_env),
    )


def _resolve_path(base_dir: Path, path_str: str | None) -> str | None:
    """
    将路径解析为相对于配置文件目录的绝对路径。

    如果 path_str 是绝对路径，直接返回。
    如果是相对路径，则相对于 base_dir 解析。
    """
    if path_str is None:
        return None
    p = Path(path_str)
    if p.is_absolute():
        return str(p)
    return str((base_dir / p).resolve())


def _resolve_paths(base_dir: Path, paths: list[str] | None) -> list[str]:
    """
    将路径列表解析为绝对路径列表。
    """
    if not paths:
        return []
    return [_resolve_path(base_dir, p) for p in paths]


def load_config(config_path: str) -> PipelineConfig:
    """从 YAML 文件加载配置，路径相对于配置文件所在目录解析"""
    path = Path(config_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config_dir = path.parent  # 配置文件所在目录

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # 解析全局 API
    global_api = _parse_api_config(data.get("api", {}))

    # 解析规划阶段配置
    plan_data = data.get("planning", {})
    plan_api = _parse_api_config(plan_data["api"]) if plan_data.get("api") else None
    planning = PlanningConfig(
        enabled=plan_data.get("enabled", True),
        save_framework=plan_data.get("save_framework", True),
        framework_file=plan_data.get("framework_file", "framework.json"),
        api=plan_api,
    )

    # 解析 Schema 演化配置
    evo_data = data.get("schema_evolution", {})
    evo_api = _parse_api_config(evo_data["api"]) if evo_data.get("api") else None
    evo_input_data = evo_data.get("input")
    evo_input = SchemaEvoInputConfig(
        path=_resolve_paths(config_dir, evo_input_data.get("path", [])),
    ) if evo_input_data else None
    schema_evo = SchemaEvolutionConfig(
        enabled=evo_data.get("enabled", True),
        rounds=evo_data.get("rounds", 3),
        initial_schema=_resolve_path(config_dir, evo_data.get("initial_schema")),
        api=evo_api,
        input=evo_input,
    )

    # 解析抽取配置
    ext_data = data.get("extraction", {})
    ext_api = _parse_api_config(ext_data["api"]) if ext_data.get("api") else None
    ext_input_data = ext_data.get("input")
    ext_input = ExtractionInputConfig(
        path=_resolve_path(config_dir, ext_input_data.get("path", "./")) or "./",
        pattern=ext_input_data.get("pattern", "*.pdf"),
    ) if ext_input_data else None
    extraction = ExtractionConfig(
        enabled=ext_data.get("enabled", True),
        keep_temp=ext_data.get("keep_temp", True),
        schema_file=_resolve_path(config_dir, ext_data.get("schema_file")),
        api=ext_api,
        input=ext_input,
    )

    output_data = data.get("output", {})
    output_cfg = OutputConfig(
        dir=_resolve_path(config_dir, output_data.get("dir", "./output")) or "./output",
        schema_file=output_data.get("schema_file", "schema.final.json"),
    )

    # 解析并发配置
    conc_data = data.get("concurrency", {})
    concurrency = ConcurrencyConfig(
        max_workers=conc_data.get("max_workers", 1),
        rate_limit_rpm=conc_data.get("rate_limit_rpm", 60),
    )

    # 解析审核配置
    review_data = data.get("review", {})
    review_api = _parse_api_config(review_data["api"]) if review_data.get("api") else None
    review_input_data = review_data.get("input")
    review_input = ReviewInputConfig(
        pdf_file=_resolve_path(config_dir, review_input_data.get("pdf_file")),
        extract_json=_resolve_path(config_dir, review_input_data.get("extract_json")),
        extract_result_dir=_resolve_path(config_dir, review_input_data.get("extract_result_dir")),
        schema_file=_resolve_path(config_dir, review_input_data.get("schema_file")),
    ) if review_input_data else None
    review = ReviewConfig(
        enabled=review_data.get("enabled", True),
        max_retries=review_data.get("max_retries", 2),
        evaluation_log=review_data.get("evaluation_log", "file_eva.log"),
        api=review_api,
        input=review_input,
    )

    return PipelineConfig(
        api=global_api,
        planning=planning,
        schema_evolution=schema_evo,
        extraction=extraction,
        review=review,
        output=output_cfg,
        concurrency=concurrency,
    )

