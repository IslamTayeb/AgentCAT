# neo4j_tools/importer.py
"""
关系导入器
将提取的关系导入 Neo4j 数据库

增强功能：
- 批量导入对齐第一阶段输出格式
- tqdm 进度条（仅批量导入）
- tenacity 重试机制（仅批量导入）
"""

import os
import re
from typing import List, Dict, Tuple, Optional
from pathlib import Path

from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from neo4j.exceptions import ServiceUnavailable, SessionExpired

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .connection import Neo4jConnection
from config import IMPORTER_PATHS
from logger import get_logger
from kg_extractor.pdf_id_manager import PdfIdManager

logger = get_logger(__name__)


class RelationshipImporter:
    """
    关系导入器
    导入 entity:label,entity:label 格式的关系到 Neo4j
    
    增强功能：
    - 批量导入（{folder}/{folder}_extract_relations.txt）
    - 进度条显示
    - 网络错误自动重试
    """
    
    # 默认重试配置
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_MIN_WAIT = 2  # 秒
    DEFAULT_MAX_WAIT = 30  # 秒
    
    def __init__(self, max_retries: int = DEFAULT_MAX_RETRIES, pdf_id_manager: Optional[PdfIdManager] = None):
        """
        初始化导入器
        
        Args:
            max_retries: 最大重试次数（仅批量导入使用）
            pdf_id_manager: PDF ID 管理器实例（用于反应步骤去重）
        """
        self._created_constraints = set()
        self.max_retries = max_retries
        self.pdf_id_manager = pdf_id_manager or PdfIdManager()
        # Match R1 / R2 and optional [id] suffix like R1[3]
        self._reaction_id_pattern = re.compile(r"^(R\d+)(\[\d+\])?$", re.IGNORECASE)
    
    def parse_relation(
        self,
        line: str,
        pdf_id: Optional[int] = None,
        active_site_alias_map: Optional[Dict[str, str]] = None,
    ) -> Optional[Tuple[str, str, str, str]]:
        # Parse a relation line: entity:label,entity:label
        parsed = self._split_relation_line(line)
        if not parsed:
            return None

        src_entity, src_label, tgt_entity, tgt_label = parsed

        if pdf_id:
            src_entity = self._maybe_disambiguate_reaction_node(src_entity, src_label, pdf_id)
            tgt_entity = self._maybe_disambiguate_reaction_node(tgt_entity, tgt_label, pdf_id)

        src_entity = self._maybe_disambiguate_active_site(src_entity, src_label, pdf_id, active_site_alias_map)
        tgt_entity = self._maybe_disambiguate_active_site(tgt_entity, tgt_label, pdf_id, active_site_alias_map)

        return (src_entity, src_label, tgt_entity, tgt_label)

    def _maybe_disambiguate_reaction_node(self, entity: str, label: str, pdf_id: int) -> str:
        """Append [pdf_id] for reactionNode step ids like R1/R2, if not already suffixed."""
        if label.lower() != "reactionnode":
            return entity
        m = self._reaction_id_pattern.match(entity)
        if not m:
            return entity
        if m.group(2):
            return entity
        return f"{m.group(1)}[{pdf_id}]"

    def _maybe_disambiguate_active_site(
        self,
        entity: str,
        label: str,
        pdf_id: Optional[int],
        active_site_alias_map: Optional[Dict[str, str]],
    ) -> str:
        # Disambiguate activeSite names per catalyst (if known) or per pdf_id.
        if label.lower() != "activesite":
            return entity
        if active_site_alias_map and entity in active_site_alias_map:
            return active_site_alias_map[entity]
        if self._has_disambiguation_suffix(entity):
            return entity
        if pdf_id is None:
            return entity
        return f"{entity}[{pdf_id}]"

    @staticmethod
    def _has_disambiguation_suffix(entity: str) -> bool:
        # Detect trailing [..] suffix to avoid double-disambiguation.
        return bool(re.search(r"\[[^\]]+\]$", entity))

    @staticmethod
    def _split_relation_line(line: str) -> Optional[Tuple[str, str, str, str]]:
        # Split raw relation line without any disambiguation.
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("NEW_LABEL:"):
            return None

        parts = line.split(",", 1)
        if len(parts) != 2:
            return None

        # use rsplit to avoid colons in entity names
        source_parts = parts[0].rsplit(":", 1)
        target_parts = parts[1].rsplit(":", 1)

        if len(source_parts) != 2 or len(target_parts) != 2:
            return None

        src_entity = source_parts[0].strip()
        src_label = source_parts[1].strip()
        tgt_entity = target_parts[0].strip()
        tgt_label = target_parts[1].strip()
        return (src_entity, src_label, tgt_entity, tgt_label)

    def _build_active_site_alias_map(
        self,
        relations: List[str],
        pdf_id: Optional[int],
    ) -> Dict[str, str]:
        # Build activeSite alias map using zeolite->activeSite relations.
        owner_map: Dict[str, set] = {}
        for line in relations:
            parsed = self._split_relation_line(line)
            if not parsed:
                continue
            src_entity, src_label, tgt_entity, tgt_label = parsed
            src_label_l = src_label.lower()
            tgt_label_l = tgt_label.lower()
            if src_label_l == "zeolite" and tgt_label_l == "activesite":
                owner_map.setdefault(tgt_entity, set()).add(src_entity)
            elif tgt_label_l == "zeolite" and src_label_l == "activesite":
                owner_map.setdefault(src_entity, set()).add(tgt_entity)

        alias_map: Dict[str, str] = {}
        for site_name, owners in owner_map.items():
            if self._has_disambiguation_suffix(site_name):
                alias_map[site_name] = site_name
                continue
            if len(owners) == 1:
                owner = next(iter(owners))
                if pdf_id:
                    alias_map[site_name] = f"{site_name}[@{owner}|{pdf_id}]"
                else:
                    alias_map[site_name] = f"{site_name}[@{owner}]"
            else:
                # Ambiguous: multiple catalysts share same activeSite name in one file
                if pdf_id:
                    alias_map[site_name] = f"{site_name}[{pdf_id}]"
                else:
                    alias_map[site_name] = site_name
                logger.warning(
                    "activeSite '%s' maps to multiple catalysts %s; falling back to %s",
                    site_name,
                    sorted(owners),
                    alias_map[site_name],
                )
        return alias_map

    def _infer_pdf_name(self, file_path: str) -> str:
        """Infer pdf name from file path."""
        p = Path(file_path)
        name = p.name
        for suffix in ("_extract_relations.txt", "_relations.txt"):
            if name.endswith(suffix):
                base = name[:-len(suffix)]
                if base:
                    return base
        if p.parent and p.parent.name:
            return p.parent.name
        return p.stem
    
    def import_relations(
        self,
        relations: List[str],
        batch_size: int = 100,
        pdf_id: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        导入关系列表（单次导入，无重试）
        
        Args:
            relations: 关系行列表
            batch_size: 批次大小
            
        Returns:
            统计信息字典
        """
        active_site_alias_map = self._build_active_site_alias_map(relations, pdf_id)
        parsed_relations = []
        for line in relations:
            parsed = self.parse_relation(
                line,
                pdf_id=pdf_id,
                active_site_alias_map=active_site_alias_map,
            )
            if parsed:
                parsed_relations.append(parsed)
        
        if not parsed_relations:
            return {"nodes_created": 0, "relationships_created": 0, "errors": 0}
        
        # 确保约束存在
        self._ensure_constraints(parsed_relations)
        
        # 批量导入
        stats = {"nodes_created": 0, "relationships_created": 0, "errors": 0}
        
        for i in range(0, len(parsed_relations), batch_size):
            batch = parsed_relations[i:i + batch_size]
            batch_stats = self._import_batch(batch)
            stats["nodes_created"] += batch_stats.get("nodes_created", 0)
            stats["relationships_created"] += batch_stats.get("relationships_created", 0)
            stats["errors"] += batch_stats.get("errors", 0)
        
        return stats
    
    def import_from_file(self, file_path: str, batch_size: int = 100) -> Dict[str, int]:
        """
        从文件导入关系（单文件，无重试）
        
        Args:
            file_path: 关系文件路径
            batch_size: 批次大小
            
        Returns:
            统计信息字典
        """
        pdf_name = self._infer_pdf_name(file_path)
        pdf_id = self.pdf_id_manager.get_or_create_id(pdf_name) if pdf_name else None

        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        return self.import_relations(lines, batch_size, pdf_id=pdf_id)
    
    def _import_file_with_retry(self, file_path: str, batch_size: int = 100) -> Dict[str, int]:
        """
        带重试的单文件导入
        
        Args:
            file_path: 关系文件路径
            batch_size: 批次大小
            
        Returns:
            统计信息字典
        """
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=self.DEFAULT_MIN_WAIT, max=self.DEFAULT_MAX_WAIT),
            retry=retry_if_exception_type((ServiceUnavailable, SessionExpired, ConnectionError)),
            before_sleep=before_sleep_log(logger, log_level=30),
            reraise=True,
        )
        def _do_import():
            return self.import_from_file(file_path, batch_size)
        
        try:
            return _do_import()
        except Exception as e:
            logger.error(f"导入失败（已重试 {self.max_retries} 次）: {file_path}: {e}")
            return {"nodes_created": 0, "relationships_created": 0, "errors": 1, "error": str(e)}
    
    def batch_import_from_dir(
        self,
        base_dir: Optional[str] = None,
        batch_size: int = 100,
        show_progress: bool = True,
    ) -> Dict[str, Dict[str, int]]:
        """
        批量导入目录下所有关系文件
        
        文件匹配规则：{folder}/{folder}_extract_relations.txt
        与第一阶段 kg_extractor 的输出格式对齐
        
        Args:
            base_dir: 基础目录，默认使用配置中的 IMPORTER_INPUT_DIR
            batch_size: 每批导入的关系数量
            show_progress: 是否显示进度条
            
        Returns:
            {folder_name: stats_dict} 映射
        """
        # 使用配置的默认输入目录
        if base_dir is None:
            base_dir = IMPORTER_PATHS.get_input_dir()
        
        base_dir = Path(base_dir)
        results = {}
        
        # 收集所有待导入文件
        relation_files = []
        for folder in base_dir.iterdir():
            if not folder.is_dir():
                continue
            
            folder_name = folder.name
            # 匹配第一阶段输出格式：{folder}_extract_relations.txt
            rel_file = folder / f"{folder_name}_extract_relations.txt"
            
            if rel_file.exists():
                relation_files.append((folder_name, rel_file))
        
        if not relation_files:
            logger.warning(f"在 {base_dir} 中未找到任何关系文件")
            logger.info(f"期望格式: {{folder}}/{{folder}}_extract_relations.txt")
            return results
        
        logger.info(f"找到 {len(relation_files)} 个待导入文件")
        
        # 使用 tqdm 显示进度条
        iterator = tqdm(relation_files, desc="导入进度", unit="文件") if show_progress else relation_files
        
        total_stats = {"nodes_created": 0, "relationships_created": 0, "errors": 0}
        
        for folder_name, rel_file in iterator:
            if show_progress:
                iterator.set_postfix({"当前": folder_name[:20]})
            
            # 使用带重试的导入
            stats = self._import_file_with_retry(str(rel_file), batch_size)
            results[folder_name] = stats
            
            # 累计统计
            total_stats["nodes_created"] += stats.get("nodes_created", 0)
            total_stats["relationships_created"] += stats.get("relationships_created", 0)
            total_stats["errors"] += stats.get("errors", 0)
        
        # 输出总结
        success_count = sum(1 for s in results.values() if s.get("errors", 0) == 0)
        logger.info(f"批量导入完成: {success_count}/{len(results)} 成功")
        logger.info(f"总计: 创建 {total_stats['nodes_created']} 节点, "
                   f"{total_stats['relationships_created']} 关系, "
                   f"{total_stats['errors']} 错误")
        
        return results
    
    def _ensure_constraints(self, relations: List[Tuple[str, str, str, str]]) -> None:
        """确保所有标签都有唯一约束"""
        labels = set()
        for src_entity, src_label, tgt_entity, tgt_label in relations:
            labels.add(src_label)
            labels.add(tgt_label)
        
        for label in labels:
            if label not in self._created_constraints:
                self._create_constraint(label)
                self._created_constraints.add(label)
    
    def _create_constraint(self, label: str) -> None:
        """创建唯一约束"""
        try:
            with Neo4jConnection.get_session() as session:
                # 使用 name 属性作为唯一标识
                session.run(f"""
                    CREATE CONSTRAINT IF NOT EXISTS FOR (n:`{label}`)
                    REQUIRE n.name IS UNIQUE
                """)
        except Exception as e:
            # 约束可能已存在，忽略错误
            logger.debug(f"创建约束时出现异常（可能已存在）: {label}: {e}")
    
    def _import_batch(self, batch: List[Tuple[str, str, str, str]]) -> Dict[str, int]:
        """
        导入一批关系
        
        使用 MERGE 保证幂等性：
        - 同名节点只会创建一次
        - 同一关系只会创建一次
        
        Args:
            batch: 关系元组列表
            
        Returns:
            统计信息
        """
        stats = {"nodes_created": 0, "relationships_created": 0, "errors": 0}
        
        try:
            with Neo4jConnection.get_session() as session:
                for src_entity, src_label, tgt_entity, tgt_label in batch:
                    try:
                        # 创建节点和关系（使用 MERGE 保证幂等）
                        result = session.run("""
                            MERGE (s:`{src_label}` {{name: $src_name}})
                            MERGE (t:`{tgt_label}` {{name: $tgt_name}})
                            MERGE (s)-[r:LINKS]->(t)
                            RETURN s, t, r
                        """.format(src_label=src_label, tgt_label=tgt_label),
                            src_name=src_entity,
                            tgt_name=tgt_entity,
                        )
                        
                        summary = result.consume()
                        stats["nodes_created"] += summary.counters.nodes_created
                        stats["relationships_created"] += summary.counters.relationships_created
                        
                    except Exception as e:
                        logger.error(f"导入关系失败: {src_entity}:{src_label} -> {tgt_entity}:{tgt_label}: {e}")
                        stats["errors"] += 1
                        
        except Exception as e:
            logger.error(f"批量导入失败: {e}")
            stats["errors"] += len(batch)
        
        return stats
    
    def clear_database(self, confirm: bool = False) -> bool:
        """
        清空数据库
        
        Args:
            confirm: 确认清空
            
        Returns:
            是否成功
        """
        if not confirm:
            logger.warning("clear_database 需要 confirm=True")
            return False
        
        try:
            with Neo4jConnection.get_session() as session:
                session.run("MATCH (n) DETACH DELETE n")
            logger.info("数据库已清空")
            return True
        except Exception as e:
            logger.error(f"清空数据库失败: {e}")
            return False


def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="导入关系到 Neo4j")
    parser.add_argument("input", nargs="?", help="输入关系文件或目录")
    parser.add_argument("--batch", action="store_true", help="批量导入目录下所有文件")
    parser.add_argument("--clear", action="store_true", help="导入前清空数据库")
    parser.add_argument("--batch-size", type=int, default=100, help="批次大小")
    parser.add_argument("--no-progress", action="store_true", help="不显示进度条")
    
    args = parser.parse_args()
    
    importer = RelationshipImporter()
    
    if args.clear:
        confirm = input("确认清空数据库? (y/N): ")
        if confirm.lower() == "y":
            importer.clear_database(confirm=True)
    
    if args.batch or (args.input and os.path.isdir(args.input)):
        # 批量导入
        base_dir = args.input if args.input else None
        results = importer.batch_import_from_dir(
            base_dir=base_dir,
            batch_size=args.batch_size,
            show_progress=not args.no_progress,
        )
        
        success = sum(1 for s in results.values() if s.get("errors", 0) == 0)
        logger.info(f"处理完成: {success}/{len(results)} 成功")
    elif args.input:
        # 单文件导入
        stats = importer.import_from_file(args.input, args.batch_size)
        logger.info(f"导入完成: {stats}")
    else:
        # 无参数时使用默认目录批量导入
        logger.info("未指定输入，使用默认目录批量导入")
        results = importer.batch_import_from_dir(
            batch_size=args.batch_size,
            show_progress=not args.no_progress,
        )


if __name__ == "__main__":
    main()
