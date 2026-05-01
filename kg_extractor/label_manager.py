# kg_extractor/label_manager.py
"""
Label 管理器
管理和同步 Neo4j 中的节点标签
"""

import json
import os
import re
from datetime import datetime
from typing import List, Set, Optional

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import get_logger

logger = get_logger(__name__)


class LabelManager:
    """
    Label 管理器
    - 从文件加载/保存 labels
    - 检查并控制 label 数量
    - 与 Neo4j 同步
    """
    
    # 默认标签
    DEFAULT_LABELS = {
        "zeolite",          # 分子筛催化剂
        "propertyCategory", # 属性类别
        "propertyNode",     # 具体属性值
        "activeSite",       # 活性位点
        "reactionNode",     # 基元反应步骤
        "molecular",        # 分子/反应物/生成物
    }

    # Label alias -> canonical label
    LABEL_ALIASES = {
        "activesite": "activeSite",
        "active_site": "activeSite",
        "active site": "activeSite",
        "reactionnode": "reactionNode",
        "reaction_node": "reactionNode",
        "reaction node": "reactionNode",
        "propertycategory": "propertyCategory",
        "property_category": "propertyCategory",
        "property category": "propertyCategory",
        "propertynode": "propertyNode",
        "property_node": "propertyNode",
        "property node": "propertyNode",
        "zeolites": "zeolite",
        "molecule": "molecular",
        "molecules": "molecular",
    }

    
    def __init__(self, label_file: Optional[str] = None):
        """
        初始化 Label 管理器
        
        Args:
            label_file: label 记录文件路径，默认使用项目根目录的 labels.json
        """
        if label_file is None:
            from config import EXTRACTOR_PATHS
            label_file = EXTRACTOR_PATHS.labels_file
            
        self.label_file = label_file
        self.labels: Set[str] = set()
        self._load()
    
    def _load(self) -> None:
        """从文件加载 labels"""
        if os.path.exists(self.label_file):
            try:
                with open(self.label_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.labels = set(data.get("labels", []))
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"加载 labels 文件失败 {self.label_file}: {e}")
                self.labels = self.DEFAULT_LABELS.copy()
        else:
            self.labels = self.DEFAULT_LABELS.copy()
            self.save()
    
    def save(self) -> None:
        """保存 labels 到文件"""
        data = {
            "labels": sorted(list(self.labels)),
            "last_updated": datetime.now().isoformat(),
            "description": "记录当前 Neo4j 中所有的节点标签"
        }
        
        dir_path = os.path.dirname(self.label_file)
        if dir_path:  # 只有当目录路径非空时才创建
            os.makedirs(dir_path, exist_ok=True)
        with open(self.label_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def get_labels(self) -> List[str]:
        """获取所有标签"""
        return sorted(list(self.labels))
    
    def get_labels_str(self) -> str:
        """获取标签字符串，用于 Prompt"""
        return ", ".join(self.get_labels())

    def normalize_label(self, label: str) -> Optional[str]:
        """
        ?????????? label ?????????
        """
        if not label:
            return None

        label = label.strip()
        if not label:
            return None

        # ????????
        if label in self.labels or label in self.DEFAULT_LABELS:
            return label

        lower = label.lower()
        if lower in self.LABEL_ALIASES:
            return self.LABEL_ALIASES[lower]

        compact = re.sub(r"[^a-zA-Z0-9]+", "", lower)
        if compact in self.LABEL_ALIASES:
            return self.LABEL_ALIASES[compact]

        # ???????????????
        for existing in self.labels:
            if existing.lower() == lower:
                return existing

        # ???? label??????????????
        return label
    
    def has_label(self, label: str) -> bool:
        """检查标签是否存在"""
        return label in self.labels
    
    def add_label(self, label: str, save: bool = True) -> bool:
        """
        添加新标签
        
        Args:
            label: 新标签名
            save: 是否立即保存到文件
            
        Returns:
            是否成功添加（False 表示已存在）
        """
        if label in self.labels:
            return False
        
        self.labels.add(label)
        if save:
            self.save()
        return True
    
    def check_and_add(self, label: str, max_labels: int = 20) -> bool:
        """
        检查并添加标签，控制标签数量
        
        Args:
            label: 新标签名
            max_labels: 最大标签数量
            
        Returns:
            是否成功添加
        """
        if label in self.labels:
            return True  # 已存在，视为成功
            
        if len(self.labels) >= max_labels:
            logger.warning(f"标签数量达到上限 ({max_labels})，无法添加 '{label}'")
            return False
            
        return self.add_label(label)
    
    def sync_from_neo4j(self) -> None:
        """从 Neo4j 同步现有标签"""
        try:
            from neo4j_tools.connection import Neo4jConnection
            
            driver = Neo4jConnection.get_driver()
            with driver.session() as session:
                result = session.run("CALL db.labels() YIELD label RETURN label")
                neo4j_labels = {record["label"] for record in result}
                
            # 合并标签
            self.labels = self.labels.union(neo4j_labels)
            self.save()
            logger.info(f"从 Neo4j 同步 {len(neo4j_labels)} 个标签")
            
        except Exception as e:
            logger.warning(f"从 Neo4j 同步标签失败: {e}")
    
    def parse_new_labels(self, extraction_output: str) -> List[str]:
        """
        从提取输出中解析新标签声明
        
        Args:
            extraction_output: LLM 的提取输出
            
        Returns:
            新标签列表
        """
        new_labels = []
        for line in extraction_output.strip().split("\n"):
            line = line.strip()
            if line.startswith("NEW_LABEL:"):
                parts = line.split(":", 2)
                if len(parts) >= 2:
                    label_name = parts[1].strip()
                    if label_name:
                        new_labels.append(label_name)
        return new_labels
