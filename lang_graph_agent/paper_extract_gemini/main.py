"""
Paper Extract CLI 入口模块

用于从学术论文 PDF 中抽取结构化信息。
"""

import argparse
import logging
import sys

from .config import load_config
from .pipeline import PaperExtractPipeline
from .logger import setup_logger, get_logger


def main():
    parser = argparse.ArgumentParser(
        description="Paper Extract Pipeline: Planning + Schema Evolution + Data Extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 完整流程（规划 → 演化 → 抽取）
  python -m paper_extract.main --config config.yaml

  # 仅运行规划阶段，生成并保存框架
  python -m paper_extract.main --config config.yaml --stage plan

  # 仅运行演化阶段，使用已保存的框架
  python -m paper_extract.main --config config.yaml --stage evo --framework output/framework.json

  # 仅运行抽取阶段
  python -m paper_extract.main --config config.yaml --stage extract --framework output/framework.json

  # 启用调试日志
  python -m paper_extract.main --config config.yaml --debug

  # 输出日志到文件
  python -m paper_extract.main --config config.yaml --log-file run.log
        """,
    )

    parser.add_argument(
        "--config",
        required=True,
        help="配置文件路径 (YAML)",
    )
    parser.add_argument(
        "--stage",
        choices=["plan", "evo", "extract", "review"],
        help="指定运行的阶段: plan(规划), evo(演化), extract(抽取), review(审核)。不指定则运行完整流程。",
    )
    parser.add_argument(
        "--framework",
        help="框架文件路径。跳过规划阶段时，从此文件加载框架。",
    )
    parser.add_argument(
        "--input",
        help="覆盖输入路径（单文件或目录）",
    )
    parser.add_argument(
        "--pattern",
        help="覆盖文件匹配模式（目录时使用）",
    )
    parser.add_argument(
        "--output",
        help="覆盖输出目录",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="启用调试日志级别",
    )
    parser.add_argument(
        "--log-file",
        help="日志输出文件路径",
    )
    parser.add_argument(
        "--pdf",
        help="单文件审核：PDF 路径（覆盖 config 中的 review.input.pdf_file）",
    )
    parser.add_argument(
        "--extract-json",
        help="单文件审核：抽取结果 JSON 路径（覆盖 config 中的 review.input.extract_json）",
    )

    args = parser.parse_args()

    # 配置日志
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_logger(level=log_level, log_file=args.log_file, detailed=args.debug)
    logger = get_logger("paper_extract.main")

    # 加载配置
    try:
        logger.debug(f"Loading config: {args.config}")
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}", exc_info=args.debug)
        sys.exit(1)

    # 命令行覆盖
    if args.input:
        from .config import ExtractionInputConfig, SchemaEvoInputConfig
        logger.debug(f"Overriding input path: {args.input}")
        # 覆盖抽取阶段输入
        if config.extraction.input is None:
            config.extraction.input = ExtractionInputConfig()
        config.extraction.input.path = args.input
        # 覆盖演化阶段输入（路径列表）
        if config.schema_evolution.input is None:
            config.schema_evolution.input = SchemaEvoInputConfig()
        config.schema_evolution.input.path = [args.input]
    if args.pattern:
        from .config import ExtractionInputConfig
        logger.debug(f"Overriding pattern: {args.pattern}")
        if config.extraction.input is None:
            config.extraction.input = ExtractionInputConfig()
        config.extraction.input.pattern = args.pattern
    if args.output:
        logger.debug(f"Overriding output dir: {args.output}")
        config.output.dir = args.output

    # 根据 stage 参数调整配置
    if args.stage == "plan":
        # 仅规划阶段
        config.schema_evolution.enabled = False
        config.extraction.enabled = False
        config.review.enabled = False
    elif args.stage == "evo":
        # 仅演化阶段
        config.planning.enabled = False
        config.extraction.enabled = False
        config.review.enabled = False
    elif args.stage == "extract":
        # 仅抽取阶段（含内联审核）
        config.planning.enabled = False
        config.schema_evolution.enabled = False
    elif args.stage == "review":
        # 仅审核阶段
        config.planning.enabled = False
        config.schema_evolution.enabled = False
        config.extraction.enabled = False
        # 命令行覆盖审核输入
        if args.pdf:
            if config.review.input is None:
                from .config import ReviewInputConfig
                config.review.input = ReviewInputConfig()
            config.review.input.pdf_file = args.pdf
        if args.extract_json:
            if config.review.input is None:
                from .config import ReviewInputConfig
                config.review.input = ReviewInputConfig()
            config.review.input.extract_json = args.extract_json

    # 运行 Pipeline
    pipeline = PaperExtractPipeline(config)
    results = pipeline.run(stage=args.stage, framework_path=args.framework)

    # 统计结果
    success = sum(1 for r in results if r.get("error") is None)
    failed = len(results) - success

    logger.info("=" * 60)
    if args.stage == "plan":
        logger.info("Planning Complete. Framework saved.")
    else:
        logger.info(f"Pipeline Complete. Success: {success}, Failed: {failed}")
    logger.info("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
