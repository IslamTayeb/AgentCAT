# pipeline.py
"""
知识图谱构建流水线
支持第一阶段（关系提取）和第二阶段（Neo4j导入）的合并或单独运行

使用方式：
    # 完整流水线（提取 + 导入）
    python pipeline.py --all
    
    # 仅第一阶段（提取）
    python pipeline.py --extract
    
    # 仅第二阶段（导入）
    python pipeline.py --import
    
    # 指定输入目录
    python pipeline.py --all --input output/
    
    # 导入前清空数据库
    python pipeline.py --all --clear
"""

import argparse
import sys
from pathlib import Path

from config import EXTRACTOR_PATHS, IMPORTER_PATHS
from logger import get_logger
from kg_extractor import ReactionNetworkExtractor
from neo4j_tools import RelationshipImporter, Neo4jConnection

logger = get_logger(__name__)


def run_extraction(
    input_dir: str,
    show_progress: bool = True,
) -> dict:
    """
    运行第一阶段：关系提取
    
    Args:
        input_dir: 输入 JSON 目录
        show_progress: 是否显示进度条
        
    Returns:
        提取结果统计
    """
    logger.info("=" * 50)
    logger.info("【第一阶段】关系提取")
    logger.info("=" * 50)
    
    extractor = ReactionNetworkExtractor()
    results = extractor.batch_extract_from_dir(
        base_dir=input_dir,
        show_progress=show_progress,
    )
    
    success = sum(1 for r in results.values() if r.success)
    total_relations = sum(r.relations_count for r in results.values())
    
    logger.info(f"第一阶段完成: {success}/{len(results)} 成功, 共 {total_relations} 条关系")
    
    return {
        "success": success,
        "total": len(results),
        "relations": total_relations,
        "results": results,
    }


def run_import(
    input_dir: str,
    clear_db: bool = False,
    show_progress: bool = True,
) -> dict:
    """
    运行第二阶段：Neo4j 导入
    
    Args:
        input_dir: 输入关系文件目录
        clear_db: 是否清空数据库
        show_progress: 是否显示进度条
        
    Returns:
        导入结果统计
    """
    logger.info("=" * 50)
    logger.info("【第二阶段】Neo4j 导入")
    logger.info("=" * 50)
    
    # 验证数据库连接
    if not Neo4jConnection.verify_connection():
        logger.error("Neo4j 连接失败，请检查配置")
        return {"success": 0, "error": "连接失败"}
    
    importer = RelationshipImporter()
    
    if clear_db:
        logger.warning("清空数据库...")
        importer.clear_database(confirm=True)
    
    results = importer.batch_import_from_dir(
        base_dir=input_dir,
        show_progress=show_progress,
    )
    
    success = sum(1 for s in results.values() if s.get("errors", 0) == 0)
    total_nodes = sum(s.get("nodes_created", 0) for s in results.values())
    total_rels = sum(s.get("relationships_created", 0) for s in results.values())
    
    logger.info(f"第二阶段完成: {success}/{len(results)} 成功")
    logger.info(f"创建 {total_nodes} 节点, {total_rels} 关系")
    
    return {
        "success": success,
        "total": len(results),
        "nodes_created": total_nodes,
        "relationships_created": total_rels,
        "results": results,
    }


def run_pipeline(
    input_dir: str = None,
    stages: list = None,
    clear_db: bool = False,
    show_progress: bool = True,
) -> dict:
    """
    运行完整流水线
    
    Args:
        input_dir: 输入目录（默认使用配置）
        stages: 要运行的阶段列表 ["extract", "import"]
        clear_db: 导入前是否清空数据库
        show_progress: 是否显示进度条
        
    Returns:
        各阶段结果
    """
    if stages is None:
        stages = ["extract", "import"]
    
    if input_dir is None:
        input_dir = EXTRACTOR_PATHS.input_dir
    
    results = {}
    
    logger.info("=" * 60)
    logger.info("知识图谱构建流水线")
    logger.info(f"输入目录: {input_dir}")
    logger.info(f"运行阶段: {stages}")
    logger.info("=" * 60)
    
    # 第一阶段：关系提取
    if "extract" in stages:
        results["extract"] = run_extraction(input_dir, show_progress)
    
    # 第二阶段：Neo4j 导入
    if "import" in stages:
        results["import"] = run_import(input_dir, clear_db, show_progress)
    
    logger.info("=" * 60)
    logger.info("流水线执行完成")
    logger.info("=" * 60)
    
    return results


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description="知识图谱构建流水线",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python pipeline.py --all                    # 运行完整流水线
  python pipeline.py --extract                # 仅运行第一阶段（提取）
  python pipeline.py --import                 # 仅运行第二阶段（导入）
  python pipeline.py --all --input output/    # 指定输入目录
  python pipeline.py --all --clear            # 导入前清空数据库
        """
    )
    
    # 阶段选择（互斥）
    stage_group = parser.add_mutually_exclusive_group()
    stage_group.add_argument(
        "--all", 
        action="store_true", 
        help="运行完整流水线（提取 + 导入）"
    )
    stage_group.add_argument(
        "--extract", 
        action="store_true", 
        help="仅运行第一阶段（关系提取）"
    )
    stage_group.add_argument(
        "--import", 
        dest="import_only",
        action="store_true", 
        help="仅运行第二阶段（Neo4j 导入）"
    )
    
    # 其他选项
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=None,
        help="输入目录（默认使用 .env 配置）"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="导入前清空 Neo4j 数据库"
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="不显示进度条"
    )
    
    args = parser.parse_args()
    
    # 确定运行阶段
    if args.all:
        stages = ["extract", "import"]
    elif args.extract:
        stages = ["extract"]
    elif args.import_only:
        stages = ["import"]
    else:
        # 默认运行完整流水线
        stages = ["extract", "import"]
    
    # 运行流水线
    try:
        results = run_pipeline(
            input_dir=args.input,
            stages=stages,
            clear_db=args.clear,
            show_progress=not args.no_progress,
        )
        
        # 汇总输出
        print("\n" + "=" * 40)
        print("执行结果汇总")
        print("=" * 40)
        
        if "extract" in results:
            ext = results["extract"]
            print(f"【提取】{ext['success']}/{ext['total']} 成功, {ext['relations']} 条关系")
        
        if "import" in results:
            imp = results["import"]
            print(f"【导入】{imp['success']}/{imp['total']} 成功, "
                  f"{imp['nodes_created']} 节点, {imp['relationships_created']} 关系")
        
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        sys.exit(1)
    except Exception as e:
        logger.error(f"流水线执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
