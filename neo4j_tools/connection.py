# neo4j_tools/connection.py
"""
Neo4j 数据库连接管理
使用官方 neo4j 驱动
"""

import os
from typing import Optional
from neo4j import GraphDatabase, Driver

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import NEO4J_CONFIG
from logger import get_logger

logger = get_logger(__name__)


class Neo4jConnection:
    """
    Neo4j 连接管理器（单例模式）
    """
    
    _driver: Optional[Driver] = None
    
    @classmethod
    def get_driver(cls) -> Driver:
        """
        获取 Neo4j 驱动实例
        
        Returns:
            Neo4j Driver 实例
        """
        if cls._driver is None:
            cls._driver = GraphDatabase.driver(
                uri=NEO4J_CONFIG.uri,
                auth=(NEO4J_CONFIG.username, NEO4J_CONFIG.password),
            )
        return cls._driver
    
    @classmethod
    def close(cls) -> None:
        """关闭数据库连接"""
        if cls._driver is not None:
            cls._driver.close()
            cls._driver = None
            logger.info("Neo4j 驱动已关闭")
    
    @classmethod
    def get_session(cls, database: Optional[str] = None):
        """
        获取数据库会话
        
        Args:
            database: 数据库名称，默认使用配置中的数据库
            
        Returns:
            Neo4j Session
        """
        driver = cls.get_driver()
        db = database or NEO4J_CONFIG.database
        return driver.session(database=db)
    
    @classmethod
    def verify_connection(cls) -> bool:
        """
        验证数据库连接
        
        Returns:
            是否连接成功
        """
        try:
            driver = cls.get_driver()
            driver.verify_connectivity()
            logger.info("Neo4j 连接验证成功")
            return True
        except Exception as e:
            logger.error(f"Neo4j 连接失败: {e}")
            return False
    
    @classmethod
    def get_schema_info(cls) -> dict:
        """
        获取图谱结构信息
        
        Returns:
            包含 labels 和 relationship_types 的字典
        """
        with cls.get_session() as session:
            # 获取所有标签
            labels_result = session.run("CALL db.labels() YIELD label RETURN label")
            labels = [record["label"] for record in labels_result]
            
            # 获取所有关系类型
            rel_result = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
            rel_types = [record["relationshipType"] for record in rel_result]
            
        return {
            "labels": labels,
            "relationship_types": rel_types,
        }
    
    @classmethod
    def get_sample_data(cls, limit: int = 10) -> list:
        """
        获取图谱示例数据
        
        Args:
            limit: 返回的最大节点数
            
        Returns:
            示例节点列表
        """
        with cls.get_session() as session:
            result = session.run(
                "MATCH (n) RETURN labels(n)[0] AS label, n.name AS name LIMIT $limit",
                limit=limit,
            )
            return [{"label": r["label"], "name": r["name"]} for r in result]
